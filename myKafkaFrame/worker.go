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
	instanceName,workerName string
	lock sync.Mutex
	distributorTag int  // -1 初始化  0 开始工作  >0 分配任务
	allotMsgS AllotMsgSlice
	myStore *store.Store
	timeTickerCh,registryCh,distributorCh,processCh chan struct{}
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
	path:= zkBaseDirectory + zkPathSplit + p.workerName+ zkPathSplit + distributorDirectory + zkPathSplit + p.instanceName
	return path
}

func (p *MyWorker)getRegistryPath() string{
	path:= zkBaseDirectory + zkPathSplit + p.workerName+ zkPathSplit + registryDirectory + zkPathSplit +workerDirectory +zkPathSplit+p.instanceName
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

	p.timeTickerCh = make(chan struct{})
	p.registryCh = make(chan struct{})
	p.distributorCh = make(chan struct{})
	p.processCh = make(chan struct{})
	p.distributorTag = -1
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

	//处理任务
	p.mainLoop()

}

func (p *MyWorker)checkSignal(){

	//获取kill信号
	signals := make(chan os.Signal, 1)
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
					p.registryCh <- struct{}{}
					p.distributorCh <- struct{}{}
					p.processCh <- struct{}{}
					p.timeTickerCh <- struct{}{}
					time.Sleep(time.Second)
					logger.Warn("consumer end ... ")
					return
			}
		}
	}()
}

// 注册工作进程
func (p *MyWorker) registryWorker() error{

	ip,err:=getIP()
	LoggerErr("getIP",err)

	registryInfo:=RegistryValue{InstanceName:p.instanceName,Host:ip,RegistryTime:GetCurrentTime()}
	value,err:=msgpack.Marshal(registryInfo)
	LoggerErr("Marshal registryInfo",err)

	writeNodeValue(p.myStore,p.getRegistryPath(), value,p.registryCh)
	return nil
}

// 获取被分配的 partition
func (p *MyWorker) watchDistributorInfo() {
	stopWatchCh := make(chan struct{})
	stopCheckCh := make(chan struct{})

	path:= p.getDistributorPath()
	//等待分配结果
	ticker := time.NewTicker(3*time.Second)
	for {
		select {
		case <-ticker.C:
			if ok,err:=(*p.myStore).Exists(path);err!=nil{
				logger.Error(fmt.Sprintf("init consumer err: %v",err))
				panic(err)
			}else {
				if ok{
					goto label
				}
			}
		case <-p.distributorCh:
			return
		}
	}
	label:

	//获取分配结果
	{
		kv,err:=(*p.myStore).Get(path)
		LoggerErr("get Distributor value",err)
		var msg AllotMsgSlice
		err=msgpack.Unmarshal(kv.Value,&msg)
		LoggerErr("Unmarshal DistributorPath value",err)

		p.lock.Lock()
		p.allotMsgS = msg
		p.distributorTag = 1
		p.lock.Unlock()
	}

	//监听分配结果
	kvCh, err := (*p.myStore).Watch(path,stopWatchCh)
	LoggerErr("watch distributor info",err)

	go func(){
		for {
			select {
				case pair := <-kvCh:
					var msg AllotMsgSlice
					if err:=msgpack.Unmarshal(pair.Value,&msg);err!=nil{
						panic(err)
					}
					p.lock.Lock()
					p.allotMsgS = msg
					p.distributorTag = 1
					p.lock.Unlock()
				case <-p.distributorCh:
					stopWatchCh <- struct{}{}
					stopCheckCh <- struct{}{}
				case <-stopCheckCh:
					return
			}
		}
	}()
}


// 主体处理进程
func (p *MyWorker)process() {

	var offsetList []*OffsetMsg // 传递偏移量
	var signList []*ControlSign // 传递信号
	var timeQuit chan struct{}
	for i:=0;i<p.allotMsgS.Len();i++{
		var w ControlSign
		w.init()
		signList = append(signList,&w)

		offsetMsg:=OffsetMsg{OffsetCommit:0}
		offsetList = append(offsetList,&offsetMsg)
	}
	// 外部信号 传递到内部
	go func(){
		for {
			select {
			case <-p.processCh:
				for _, sign := range signList {
					sign.quit <- struct{}{}
				}
				timeQuit <- struct{}{}
				time.Sleep(time.Second)
				logger.Warn("get process quit signal ... ")
				return
			}
		}
	}()

	//定时同步 偏移量
	ticker := time.NewTicker(3*time.Second)
	go func() {
		for {
			select {
				case <-ticker.C:
					for _, msg := range offsetList {
						key:=p.getOffsetPath()+zkPathSplit+msg.Topic+ strconv.FormatInt(int64(msg.Partition),10)

						value,err:=msgpack.Marshal(*msg)
						LoggerErr("Marshal offsetMsg",err)

						err=(*p.myStore).Put(key, value, nil)
						LoggerErr("write offset",err)
					}
				case <-timeQuit:
					return
			}
		}
	}()

	// init consumer
	consumer, err := sarama.NewConsumer(p.brokers, nil)
	LoggerErr("init consumer",err)
	logger.Warn("init consumer success ... ")


	for i, msg := range p.allotMsgS {
		// consume partitions
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
							offsetList[index].Topic = part.Topic
							offsetList[index].Partition = part.Partition
							offsetList[index].OffsetCommit = part.Offset
							offsetList[index].OffsetNewest = -1
						}
					case <- signList[index].quit:




						logger.Warn(fmt.Sprintf("get kafka msg err: %v",err))
						return
				}
			}
		}(msg.Topic, msg.Partition)
	}

}

func (p *MyWorker)mainLoop(){

	p.lock.Lock()
	p.process()
	p.distributorTag = 0
	p.lock.Unlock()

	ticker := time.NewTicker(30*time.Second)

	for {
		select {
			case <-ticker.C:
				p.lock.Lock()
				if p.distributorTag >0{
					p.processCh <- struct{}{}
					time.Sleep(time.Second*3)
					p.process()
					p.distributorTag = 0
				}
				p.lock.Unlock()
			case <-p.timeTickerCh:
				return
		}
	}

}


