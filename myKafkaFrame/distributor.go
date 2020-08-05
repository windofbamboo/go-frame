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
	"syscall"
	"time"
)

type MyDistributor struct{
	brokers, topics, zkAddr NameSlice
	instanceName,workerName string
	myStore *store.Store
	workers NameSlice
	allotMsgS map[string]AllotMsgSlice
	offsetMap map[AllotMsg]*OffsetMsg
	partitions map[string][]int32

	totalStopCh,lockStopCh,writeStopCh,watchStopCh,watchOffsetCh,checkOffsetCh chan struct{}
}

func (c *MyDistributor)getOffsetPath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + queueDirectory + zkPathSplit + offsetDirectory
	return path
}

func (c *MyDistributor)getDistributorBasePath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + distributorDirectory
	return path
}

func (c *MyDistributor)getDistributorRegistryPath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + registryDirectory + zkPathSplit +distributorDirectory
	return path
}

func (c *MyDistributor)getWorkerRegistryPath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + registryDirectory + zkPathSplit +workerDirectory
	return path
}

func (c *MyDistributor)InitParam(configFile string,logFile string) {

	err := CheckFile(configFile)
	CheckErr(err)
	err = ReadConfig(InstanceTypeDistributor,configFile)
	CheckErr(err)

	err = CheckFile(logFile)
	CheckErr(err)
	contentStr,err:=ReSetLogFileName(logFile,configContent.instanceName)
	CheckErr(err)
	err = logger.SetLogger(contentStr)
	CheckErr(err)

	PrintConfig()

	c.zkAddr = configContent.zk.zkAddr
	c.brokers = configContent.kafka.brokers
	c.topics = configContent.kafka.topics
	c.workerName = configContent.workerName
	c.instanceName = configContent.instanceName

	c.allotMsgS = make(map[string]AllotMsgSlice)
	c.offsetMap = make(map[AllotMsg]*OffsetMsg)
	c.partitions = make(map[string][]int32)

	c.totalStopCh = make(chan struct{})
	c.lockStopCh = make(chan struct{})
	c.writeStopCh = make(chan struct{})
	c.watchStopCh = make(chan struct{})
	c.watchOffsetCh = make(chan struct{})
	c.checkOffsetCh = make(chan struct{})
}

func (c *MyDistributor)Start() {

	kv, err := libkv.NewStore(store.ZK, c.zkAddr, nil)
	if err!=nil{
		logger.Error(fmt.Sprintf("init store err: %v",err))
		panic(err)
	}
	c.myStore = &kv
	defer (*c.myStore).Close()

	c.checkSignal()

	c.getExecutePermission()
	c.getTopics()
	c.firstAllot()

	// 监听offset 信息
	c.WatchOffset()
	// 定时检查offset
	c.checkOffset()
	// 监听 worker 注册节点
	c.watchWorker()

	//等待接收停止信号
	for {
		select {
		case <-c.totalStopCh:
			logger.Warn("distributor end ... ")
			return
		}
	}
}


func (c *MyDistributor)getExecutePermission(){
	logger.Warn(fmt.Sprintf("%s try lock ...",c.instanceName))
	path:= c.getDistributorRegistryPath()

	if ok,err:=getQueueLock(c.myStore,path,c.workerName,c.lockStopCh);err!=nil{
		logger.Error(fmt.Sprintf("get lock err: %v",err))
		panic(err)
	}else{
		if !ok{
			logger.Error("can not get lock ...")
		}
	}
	logger.Warn(fmt.Sprintf("%s get lock success , start deal data ...",c.instanceName))
}

func (c *MyDistributor)getTopics(){
	client,err:= sarama.NewClient(c.brokers,nil)
	if err!=nil{
		logger.Error("get kafka client err")
		panic(err)
	}
	defer client.Close()

	for _, topic := range c.topics {
		partitions,err:= client.Partitions(topic)
		if err!=nil {
			panic(err)
		}
		c.partitions[topic] = partitions
	}
}

func (c *MyDistributor)firstAllot(){
	// 获取 worker 的注册信息
	instances,err:=c.getWorkers()
	if err!=nil{
		logger.Error(fmt.Sprintf("get worker instance information err: %v",err))
		panic(err)
	}else{
		logger.Warn(fmt.Sprintf("worker instances : %v",instances))
	}
	// 获得分配方式
	allotMsgSlice,err:= simpleAllot(&instances,&(c.partitions))
	logger.Warn(fmt.Sprintf("allotMsgSlice : %v",allotMsgSlice))
	// 把分配方式写入 zk 中
	c.writeAllotMsg(allotMsgSlice)
	logger.Warn("writeAllotMsg finish")
	c.allotMsgS = allotMsgSlice
}

func (c *MyDistributor)checkOffset(){

	ticker := time.NewTicker(5*time.Minute)
	go func() {
		for {
			select {
				case <-ticker.C:
					client,err:= sarama.NewClient(c.brokers,nil)
					if err!=nil{
						logger.Error("get kafka client err")
						panic(err)
					}
					defer client.Close()

					// offsetMap map[AllotMsg]OffsetMsg
					var offsetSlice OffsetMsgSlice
					for msg, offsetMsg := range c.offsetMap {
						offsetNewest,err:=client.GetOffset(msg.Topic,msg.Partition,sarama.OffsetNewest)
						if err!=nil{
							logger.Error("get kafka offset err")
							panic(err)
						}
						offsetMsg.OffsetNewest = offsetNewest
						offsetSlice = append(offsetSlice,*offsetMsg)
					}

					isNeed,thisAllotMsgS,err:= reAllotByOffset(&offsetSlice,100,&c.allotMsgS)
					if isNeed{
						c.writeStopCh <-struct{}{}
						// 写入 注册信息
						c.writeAllotMsg(thisAllotMsgS)
						//
						c.allotMsgS = thisAllotMsgS
					}
				case <-c.checkOffsetCh:
					return
			}
		}
	}()
}


func (c *MyDistributor)checkSignal(){
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals,
				syscall.SIGHUP,
				syscall.SIGINT,
				syscall.SIGTERM,
				syscall.SIGQUIT,
				os.Interrupt,
				os.Kill)

	go func() {
		for {
			select {
				case <-signals:
					c.totalStopCh <- struct{}{}
					c.lockStopCh <- struct{}{}
					c.writeStopCh <- struct{}{}
					c.watchStopCh <- struct{}{}
					c.watchOffsetCh <- struct{}{}
					c.checkOffsetCh <- struct{}{}
					logger.Warn("distributor signal end ... ")
					return
			}
		}
	}()

}



// 监听 topic 的 offset 信息
func (c *MyDistributor)WatchOffset(){

	//初始化 信号
	var signMap = make(map[string][]*ControlSign)
	for topicName, partitions := range c.partitions {
		var signList []*ControlSign
		for range partitions {
			var w ControlSign
			w.init()
			signList = append(signList,&w)
		}
		signMap[topicName] = signList
	}

	// 信号传递
	go func(){
		for {
			select {
			case <-c.watchOffsetCh:
				for _, signs := range signMap {
					for _, sign := range signs {
						sign.quit <- struct{}{}
					}
				}
				time.Sleep(time.Second)
				logger.Warn("get WatchOffset quit signal ... ")
				return
			}
		}
	}()

	// path: /myKafkaFrame/workerName/kafka/Offset
	// 监听每一个 partition 的 offset, 并更新到 offsetMap 对象中
	path:= c.getOffsetPath()
	for topic, partitions := range c.partitions {
		signList := signMap[topic]

		for i, partition := range partitions {
			key:= path + zkPathSplit + topic+ strconv.FormatInt(int64(partition),10)

			go func(key string,sign *ControlSign){
				for{
					if ok,err:=(*c.myStore).Exists(key);err!=nil{
						logger.Warn(fmt.Sprintf("check node err : %v ",err))
					}else{
						if ok{
							break
						}
						time.Sleep(3*time.Second)
					}
				}
				kvCh, err := (*c.myStore).Watch(key,sign.quit)
				if err!=nil {
					panic(err)
				}
				for {
					select {
						case pair := <-kvCh:
							var msg OffsetMsg
							if err:=msgpack.Unmarshal(pair.Value,&msg);err!=nil{
								panic(err)
							}
							allotMsg:= AllotMsg{Topic:msg.Topic,Partition:msg.Partition}
							c.offsetMap[allotMsg]=&msg
					}
				}
			}(key,signList[i])
		}
	}

}

// 监听 worker 进程 注册信息
// 当节点数量变化时，重新分配
func (c *MyDistributor)watchWorker(){
	// path: /myKafkaFrame/workerName/registry/worker/

	stopWatchCh := make(chan struct{})
	stopCheckCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-c.watchStopCh:
				stopWatchCh <- struct{}{}
				stopCheckCh <- struct{}{}
				return
			}
		}
	}()

	path:= c.getWorkerRegistryPath()
	kvCh, err := (*c.myStore).WatchTree(path,stopWatchCh)
	CheckErr(err)

	go func(){
		for {
			select {
				case child := <-kvCh:
					c.workers = NameSlice{}
					for _, pair := range child {
						nodeName := pair.Key
						c.workers = append(c.workers,nodeName)
					}
					//重新分配
					c.writeStopCh <-struct{}{}
					res,err:=simpleAllot(&c.workers,&c.partitions)
					CheckErr(err)
					// 写入 注册信息
					c.writeAllotMsg(res)
					//
					c.allotMsgS = res

				case <-stopCheckCh:
					return
			}
		}
	}()
}


// 写入worker 分配信息
func (c *MyDistributor) writeAllotMsg(allotMap map[string] AllotMsgSlice){

	var signList []*ControlSign
	for i:=0;i< len(allotMap);i++{
		var w ControlSign
		w.init()
		signList = append(signList,&w)
	}

	go func(){
		for {
			select {
			case <-c.writeStopCh:
				for _, sign := range signList {
					sign.quit <- struct{}{}
				}
				time.Sleep(time.Second)
				logger.Warn("writeAllotMsg end ... ")
				return
			}
		}
	}()

	//  path /myKafkaFrame/workerName/distributor/{work-instance}
	var index = 0
	for nodeName, slice := range allotMap {
		logger.Error(fmt.Sprintf("nodeName : %v",nodeName))

		path:= c.getDistributorBasePath() + zkPathSplit + nodeName
		value,err:= msgpack.Marshal(slice)
		CheckErr(err)

		writeNodeValue(c.myStore,path, value,signList[index].quit)
		index++
	}
}

// 获取 worker 进程 注册信息
func (c *MyDistributor)getWorkers() (NameSlice,error){
	// path: /myKafkaFrame/workerName/registry/worker/
	path:= c.getWorkerRegistryPath()
	kvS, err:= (*c.myStore).List(path)
	if err!=nil{
		logger.Error(fmt.Sprintf("get list err: %v",err))
		return nil,err
	}

	var instanceNames NameSlice
	for _, kv := range kvS {
		nodeName:=kv.Key
		var msg RegistryValue
		if err:=msgpack.Unmarshal(kv.Value,&msg);err!=nil{
			return nil,err
		}
		instanceNames= append(instanceNames,nodeName)
	}

	return instanceNames,nil
}








