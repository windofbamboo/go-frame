package myKafkaFrame

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	_ "github.com/smallnest/rpcx/client"
	"github.com/vmihailenco/msgpack"
	"github.com/wonderivan/logger"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type DealFunc func(in *sarama.ConsumerMessage) error

type MyWorker struct {
	brokers,zkAddr   NameSlice
	instanceName,workerName,distributorInstanceName string
	processLock sync.Mutex
	distributorStatus ConStatus
	allotMsgS AllotMsgSlice
	myStore *store.Store
	timeTickerSign,registrySign,distributorSign,processSign ControlSign
	dealDataFunc DealFunc
	errDataFunc DealFunc
}

func (p *MyWorker)RegistryDealFunc(fn DealFunc){
	p.dealDataFunc = fn
}

func (p *MyWorker)RegistryErrFunc(fn DealFunc){
	p.errDataFunc = fn
}

func (p *MyWorker)getOffsetPath() string{
	path:= zkBaseDirectory + zkPathSplit + p.workerName+ zkPathSplit + queueDirectory + zkPathSplit + offsetDirectory
	return path
}

func (p *MyWorker)getDistributorPath() string{
	path:= zkBaseDirectory + zkPathSplit + p.workerName+ zkPathSplit + distributorDirectory +
		zkPathSplit + p.distributorInstanceName + zkPathSplit + p.instanceName
	return path
}

func (p *MyWorker)getInstanceRegistryPath() string{
	path:= zkBaseDirectory + zkPathSplit + p.workerName+ zkPathSplit + registryDirectory + zkPathSplit +workerDirectory +zkPathSplit+p.instanceName
	return path
}

func (p *MyWorker)getDistributorRegistryPath() string{
	path:= zkBaseDirectory + zkPathSplit + p.workerName+ zkPathSplit + registryDirectory + zkPathSplit +distributorDirectory
	return path
}

func (p *MyWorker)InitParam(configFile string,logFile string) {

	err := CheckFile(configFile)
	CheckErr(err)
	err = ReadConfig(InstanceTypeWorker,configFile)
	CheckErr(err)

	err = CheckFile(logFile)
	CheckErr(err)
	contentStr,err:=ReSetLogFileName(logFile,configContent.instanceName)
	CheckErr(err)
	err = logger.SetLogger(contentStr)
	CheckErr(err)

	PrintConfig()

	p.zkAddr = configContent.zk.zkAddr
	p.brokers = configContent.kafka.brokers
	p.workerName = configContent.workerName
	p.instanceName = configContent.instanceName

	p.distributorStatus.updateStatus(workStatusInit)

	p.timeTickerSign.init()
	p.registrySign.init()
	p.distributorSign.init()
	p.processSign.init()
}

func (p *MyWorker)Start() {

	//获取zk的连接
	kv, err := libkv.NewStore(store.ZK, p.zkAddr, nil)
	LoggerErr("init store",err)
	p.myStore = &kv
	defer (*p.myStore).Close()

	//检测中止信号
	p.checkSignal()

	//注册
	if err:=p.registryWorker();err!=nil{
		logger.Error(fmt.Sprintf("registryWorker err: %v",err))
		panic(err)
	}

	//获取分配的任务
	p.watchDistributorInfo()

	p.LoopProcess()

	for {
		 if p.timeTickerSign.getStatus() == routineStop && p.registrySign.getStatus()== routineStop &&
			p.distributorSign.getStatus()== routineStop && p.processSign.getStatus() == routineStop{
			 logger.Warn(fmt.Sprintf("worker instance : %s stoped ",p.instanceName))
			break
		}
		time.Sleep(time.Second)
	}
}

func (p *MyWorker)checkSignal(){

	//获取kill信号
	signals := make(chan os.Signal,1)
	signal.Notify(signals,
				syscall.SIGHUP,
				syscall.SIGINT,
				syscall.SIGTERM,
				syscall.SIGQUIT,
				os.Interrupt,
				os.Kill)

	go func(){
		for {
			select {
				case <-signals:
					p.timeTickerSign.quit <- struct{}{}
					p.registrySign.quit <- struct{}{}
					p.distributorSign.quit <- struct{}{}
					p.processSign.quit <- struct{}{}
					logger.Warn("get kill signal ,will stop process ... ")
					return
			}
		}
	}()
}

// 注册工作进程
func (p *MyWorker) registryWorker() error{

	logger.Warn("registryWorker begin ... ")
	ip,err:=getIP()
	LoggerErr("getIP",err)

	registryInfo:=RegistryValue{InstanceName:p.instanceName,Host:ip,RegistryTime:GetCurrentTime()}
	value,err:=msgpack.Marshal(registryInfo)
	LoggerErr("Marshal registryInfo",err)

	writeTempNodeValue(p.myStore,p.getInstanceRegistryPath(), value,&p.registrySign)
	return nil
}

// 获取被分配的 partition
func (p *MyWorker) watchDistributorInfo() {

	logger.Info("watchDistributorInfo begin ... ")

	stopWatchCh := make(chan struct{},1)
	stopCheckCh := make(chan struct{},1)

	//等待 分配器注册信息
	path:=p.getDistributorRegistryPath()
	ticker:= time.NewTicker(3*time.Second)
	p.distributorSign.updateStart()
	for {
		select {
		case <-ticker.C:
			if ok,err:=(*p.myStore).Exists(path);err!=nil{
				logger.Error(fmt.Sprintf("judge DistributorRegistryPath err: %v",err))
				panic(err)
			}else {
				if ok{
					kvPairs,err:=(*p.myStore).List(path)
					LoggerErr("get DistributorRegistryPath child value",err)
					if len(kvPairs) == 1{

						var registryInfo RegistryValue
						for _, pair := range kvPairs {
							err:=msgpack.Unmarshal(pair.Value,&registryInfo)
							LoggerErr("Unmarshal distributor registryInfo",err)
						}
						p.distributorInstanceName = registryInfo.InstanceName
						logger.Info(fmt.Sprintf("get  distributorInstanceName sucess , name : %v ",registryInfo.InstanceName))
						goto label1
					}
				}
			}
		case <-p.distributorSign.quit:
			p.distributorSign.updateStop()
			logger.Warn("distributorSign.updateStop() 1 ... ")
			return
		}
	}
	label1:

	//等待分配结果
	path= p.getDistributorPath()
	ticker = time.NewTicker(3*time.Second)
	for {
		select {
		case <-ticker.C:
			if ok,err:=(*p.myStore).Exists(path);err!=nil{
				LoggerErr("judge DistributorPath ",err)
			}else {
				if ok{
					logger.Info(fmt.Sprintf("path : %v is exists",path))
					goto label2
				}
			}
		case <-p.distributorSign.quit:
			p.distributorSign.updateStop()
			logger.Warn("distributorSign.updateStop() 2 ... ")
			return
		}
	}
	label2:

	//获取分配结果
	{
		kv,err:=(*p.myStore).Get(path)
		LoggerErr("get Distributor value",err)
		var msg AllotMsgSlice
		err=msgpack.Unmarshal(kv.Value,&msg)
		LoggerErr("Unmarshal DistributorPath value",err)

		p.allotMsgS = msg
		p.distributorStatus.updateStatus(workStatusGetTask)

		logger.Info(fmt.Sprintf("first allotMsgS: %v ",p.allotMsgS))
	}

	//监听分配结果
	kvCh, err := (*p.myStore).Watch(path,stopWatchCh)
	LoggerErr("watch distributor info",err)

	go func(){
		for {
			select {
				case pair := <-kvCh:
					if pair==nil{
						p.allotMsgS = AllotMsgSlice{}
						if p.distributorStatus.getStatus() != workStatusInit{
							p.distributorStatus.updateStatus(workStatusLostWatch)
						}
					}else{
						var msg AllotMsgSlice
						if err:=msgpack.Unmarshal(pair.Value,&msg);err!=nil{
							panic(err)
						}
						if !msg.equal(&p.allotMsgS){
							p.allotMsgS = msg
							p.distributorStatus.updateStatus(workStatusGetTask)
							logger.Warn(fmt.Sprintf("get allotMsgS: %v ",p.allotMsgS))
						}
					}
				case <-p.distributorSign.quit:
					stopWatchCh <- struct{}{}
					stopCheckCh <- struct{}{}
				case <-stopCheckCh:
					p.distributorSign.updateStop()
					logger.Warn("distributorSign.updateStop() 3 ... ")
					return
			}
		}
	}()
}


// 主体处理进程
func (p *MyWorker)process() {

	logger.Warn("process begin ... ")

	var offsetList []*OffsetMsg // 传递偏移量
	var signList []*ControlSign // 传递信号
	timeQuit:=make(chan struct{},1)
	for i:=0;i<p.allotMsgS.Len();i++{
		var w ControlSign
		w.init()
		signList = append(signList,&w)

		offsetMsg:=OffsetMsg{OffsetCommit:0}
		offsetList = append(offsetList,&offsetMsg)
	}
	// 外部信号 传递到内部
	p.processSign.updateStart()
	go func(){
		for {
			select {
			case <-p.processSign.quit:
				for _, sign := range signList {
					sign.quit <- struct{}{}
				}
				timeQuit <- struct{}{}
				p.processSign.updateStop()
				logger.Warn("processSign.updateStop() ... ")
				return
			}
		}
	}()

	var writeOffset = func(){
		for _, msg := range offsetList {
			key:=p.getOffsetPath()+zkPathSplit+msg.Topic+ strconv.FormatInt(int64(msg.Partition),10)

			value,err:=msgpack.Marshal(*msg)
			LoggerErr("Marshal offsetMsg",err)

			err=(*p.myStore).Put(key, value, nil)
			LoggerErr("write offset",err)
		}
	}
	//定时同步 偏移量
	ticker := time.NewTicker(3*time.Second)
	go func() {
		for {
			select {
				case <-ticker.C:
					writeOffset()
				case <-timeQuit:
					writeOffset()
					logger.Warn("stop writeOffset ... ")
					return
			}
		}
	}()

	// init consumer
	config:= sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(p.brokers, config)
	LoggerErr("init consumer",err)
	logger.Warn("init consumer success ... ")

	for i, msg := range p.allotMsgS {
		// consume partitions
		logger.Warn(fmt.Sprintf("topic: %v ,partition: %v",msg.Topic, msg.Partition))
		index:=i
		go func(topic string ,partition int32){
			pc, err :=consumer.ConsumePartition(topic,partition, sarama.OffsetOldest)
			LoggerErr("get ConsumePartition",err)
			defer pc.AsyncClose()

			for {
				select {
					case part, ok := <-pc.Messages():
						if !ok {
							logger.Error(fmt.Sprintf("get kafka msg err: %v",err))
							return
						}
						//错误信息的处理
						if err:=p.dealDataFunc(part);err!=nil{
							logger.Warn(fmt.Sprintf("dealDataFunc err: %v",err))
							if subErr:=p.errDataFunc(part);subErr!=nil{
								logger.Error(fmt.Sprintf("errDataFunc err: %v",subErr))
							}
						} else{
							offsetList[index].Topic 		= part.Topic
							offsetList[index].Partition 	= part.Partition
							offsetList[index].OffsetCommit 	= part.Offset
							offsetList[index].OffsetNewest 	= -1
						}
					case <- signList[index].quit:
						logger.Warn(fmt.Sprintf("stop deal data [topic : %v , partition : %v ]",topic,partition))
						return
				}
			}
		}(msg.Topic, msg.Partition)
	}
}

func (p *MyWorker)LoopProcess(){

	logger.Warn("LoopProcess begin ... ")
	//没有获取到任务，就走到这里，说明是接到了退出信号
	if p.distributorStatus.getStatus() == workStatusInit{
		p.timeTickerSign.updateStop()
		p.processSign.updateStop()
		return
	}

	p.processLock.Lock()
	p.process()
	p.distributorStatus.updateStatus(workStatusStartTask)
	p.processLock.Unlock()

	//定时检测分配的任务是否有变动
	ticker := time.NewTicker(6*time.Second)
	p.timeTickerSign.updateStart()
	go func(){
		for {
			select {
				case <-ticker.C:
					p.processLock.Lock()
					if p.distributorStatus.getStatus() == workStatusLostWatch {

						p.distributorStatus.updateStatus(workStatusInit)

						p.processSign.start2Stop()
						p.distributorSign.start2Stop()
						p.processSign.waitIdle()
						p.distributorSign.waitIdle()

						p.watchDistributorInfo()
						p.process()
						p.distributorStatus.updateStatus(workStatusStartTask)

					}else if p.distributorStatus.getStatus() == workStatusGetTask{
						p.processSign.start2Stop()
						p.processSign.waitIdle()
						if len(p.allotMsgS)>0{
							p.process()
						}else{
							logger.Warn(" sleep ... ")
						}
						p.distributorStatus.updateStatus(workStatusStartTask)
					}
					p.processLock.Unlock()
				case <-p.timeTickerSign.quit:
					p.timeTickerSign.updateStop()
					logger.Warn("LoopProcess timeTickerSign.updateStop() ... ")
					return
			}
		}
	}()

}


