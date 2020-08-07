package myKafkaFrame

import (
	"errors"
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

type MyDistributor struct{
	brokers, topics, zkAddr NameSlice
	instanceName,workerName string
	myStore *store.Store
	workers NameSlice
	allotMsgS map[string]AllotMsgSlice
	offsetMap map[AllotMsg]*OffsetMsg
	partitions map[string][]int32
	registrySign,lockStopSign,writeAllotSign,getWorkStopSign,watchStopSign,watchOffsetSign,checkOffsetSign ControlSign

	lockName string
	finishStatus ConStatus
	lock sync.Mutex
}

func (c *MyDistributor)getOffsetPath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + queueDirectory + zkPathSplit + offsetDirectory
	return path
}

func (c *MyDistributor)getDistributorBasePath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + distributorDirectory + zkPathSplit + c.instanceName
	return path
}

func (c *MyDistributor)getDistributorRegistryLockPath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + registryDirectory + zkPathSplit +lockDirectory
	return path
}

func (c *MyDistributor)getInstanceRegistryPath() string{
	path:= zkBaseDirectory + zkPathSplit + c.workerName+ zkPathSplit + registryDirectory + zkPathSplit +distributorDirectory + zkPathSplit + c.instanceName
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

	c.registrySign.init()
	c.lockStopSign.init()
	c.writeAllotSign.init()
	c.getWorkStopSign.init()
	c.watchStopSign.init()
	c.watchOffsetSign.init()
	c.checkOffsetSign.init()

	c.finishStatus.updateStatus(distributorInit)
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
	//获取执行权限
	err=c.getExecutePermission()
	if err != nil{
		c.clearWorker()
		logger.Warn(fmt.Sprintf("distributor instance : %s stoped ",c.instanceName))
		return
	}
	//注册节点
	if err:=c.registryDistributor();err!=nil{
		logger.Error(fmt.Sprintf("registryDistributor err: %v",err))
		panic(err)
	}
	//获取所有的分区
	c.getTopics()
	//首次分配任务
	c.firstAllot()
	// 监听offset 信息
	c.WatchOffset()
	// 定时检查offset
	c.checkOffset()
	// 监听 worker 注册节点
	c.watchWorker()

	for {
		if  c.registrySign.getStatus() == routineStop && c.lockStopSign.getStatus() == routineStop &&
			c.getWorkStopSign.getStatus() == routineStop && c.watchStopSign.getStatus()  == routineStop &&
			c.watchOffsetSign.getStatus() == routineStop && c.checkOffsetSign.getStatus()== routineStop &&
			c.writeAllotSign.getStatus() == routineStop{
			c.clearWorker()
			logger.Warn(fmt.Sprintf("distributor instance : %s stoped ",c.instanceName))
			break
		}
	}

}

func (c *MyDistributor) clearWorker() {

	close(c.registrySign.quit)
	close(c.lockStopSign.quit)
	close(c.getWorkStopSign.quit)
	close(c.watchStopSign.quit)
	close(c.watchOffsetSign.quit)
	close(c.checkOffsetSign.quit)
	close(c.writeAllotSign.quit)

	//删除分配的信息
	if c.finishStatus.getStatus() >=distributorFirstAllot {
		for nodeName := range c.allotMsgS {
			path:= c.getDistributorBasePath() + zkPathSplit + nodeName
			if err:=(*c.myStore).Delete(path);err!=nil{
				LoggerErr("delete worker instance registry node ",err)
			}
		}
	}
}

func (c *MyDistributor)getExecutePermission() error{

	nodeName,ok,err:=getQueueLock(c.myStore,c.getDistributorRegistryLockPath(),c.workerName,&c.lockStopSign)
	if err!=nil {
		logger.Error(fmt.Sprintf("get lock err: %v", err))
		return err
	}
	if nodeName !=""{
		c.lockName = nodeName
		c.finishStatus.updateStatus(distributorLockNode)
	}
	if !ok{
		return errors.New("get stop signal ")
	}
	return nil
}

// 注册工作进程
func (c *MyDistributor) registryDistributor() error{
	ip,err:=getIP()
	LoggerErr("getIP",err)

	registryInfo:=RegistryValue{InstanceName:c.instanceName,Host:ip,RegistryTime:GetCurrentTime()}
	value,err:=msgpack.Marshal(registryInfo)
	LoggerErr("Marshal registryInfo",err)

	writeTempNodeValue(c.myStore,c.getInstanceRegistryPath(), value,&c.registrySign)
	return nil
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

	c.allotMsgS = allotMsgSlice
	c.workers = instances
	c.finishStatus.updateStatus(distributorFirstAllot)
}

func (c *MyDistributor)checkOffset(){

	reAllot :=func(){
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
			c.writeAllotSign.start2Stop()
			c.writeAllotSign.waitIdle()
			// 写入 注册信息
			c.writeAllotMsg(thisAllotMsgS)
			c.allotMsgS = thisAllotMsgS

			logger.Warn("checkOffset reWrite allotMsgS")
		}
	}

	go func() {
		ticker := time.NewTicker(5*time.Minute)
		defer ticker.Stop()

		c.checkOffsetSign.updateStart()
		for {
			select {
				case <-ticker.C:
					reAllot()
				case <-c.checkOffsetSign.quit:
					c.checkOffsetSign.updateStop()
					return
			}
		}
	}()
	c.finishStatus.updateStatus(distributorCheckOffset)
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
					c.registrySign.quit <- struct{}{}
					c.lockStopSign.quit <- struct{}{}
					c.getWorkStopSign.quit <- struct{}{}
					c.watchStopSign.quit <- struct{}{}
					c.watchOffsetSign.quit <- struct{}{}
					c.checkOffsetSign.quit <- struct{}{}
					logger.Warn("get kill signal ,will stop process ... ")
					return
			}
		}
	}()

}


// 监听 topic 的 offset 信息
func (c *MyDistributor)WatchOffset(){
	//初始化 信号
	var size = 0
	for _, partitions := range c.partitions {
		size += len(partitions)
	}
	signList := c.watchOffsetSign.signalMultiplication(size*2)

	// 监听每一个 partition 的 offset, 并更新到 offsetMap 对象中
	// todo
	path:= c.getOffsetPath()
	var index = 0
	for topic, partitions := range c.partitions {
		for _, partition := range partitions {
			key:= path + zkPathSplit + topic+ strconv.FormatInt(int64(partition),10)

			go func(key string,watchSign *ControlSign,checkSign *ControlSign){
				for{
					if ok,err:=(*c.myStore).Exists(key);err!=nil{
						logger.Warn(fmt.Sprintf("check node err : %v ",err))
					}else{
						if ok{
							break
						}
						time.Sleep(time.Second)
					}
				}
				kvCh, err := (*c.myStore).Watch(key,watchSign.quit)
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
							c.lock.Lock()
							c.offsetMap[allotMsg]=&msg
							c.lock.Unlock()
						case <- checkSign.quit:
							return
					}
				}
			}(key,signList[index*2],signList[index*2+1])
			index++
		}
	}
	c.finishStatus.updateStatus(distributorWatchOffset)
}

// 监听 worker 进程 注册信息
// 当节点数量变化时，重新分配
func (c *MyDistributor)watchWorker(){

	signList := c.watchStopSign.signalMultiplication(2)

	path:= c.getWorkerRegistryPath()
	kvCh, err := (*c.myStore).WatchTree(path,signList[0].quit)
	CheckErr(err)

	go func(){
		c.watchStopSign.updateStart()
		c.finishStatus.updateStatus(distributorWatchWorker)
		for {
			select {
				case child := <-kvCh:
					for _, pair := range child {
						logger.Warn(pair.Key)
					}
					kvPairs,err:= (*c.myStore).List(path)
					CheckErr(err)

					var thisWorkers NameSlice
					for _, pair := range kvPairs {
						nodeName := pair.Key
						thisWorkers = append(thisWorkers,nodeName)
					}

					if len(thisWorkers) > 0 {
						if !thisWorkers.equal(&c.workers){
							c.writeAllotSign.start2Stop()
							c.workers = thisWorkers
							//重新分配
							res,err:=simpleAllot(&c.workers,&c.partitions)
							CheckErr(err)
							c.writeAllotSign.waitIdle()
							// 写入 注册信息
							c.writeAllotMsg(res)
							c.lock.Lock()
							c.allotMsgS = res
							c.lock.Unlock()
							logger.Warn("watchWorker reWrite allotMsgS")
						}
					}
				case <-signList[1].quit:
					c.watchStopSign.updateStop()
					return
			}
		}
	}()
}


// 写入worker 分配信息
func (c *MyDistributor) writeAllotMsg(allotMap map[string] AllotMsgSlice){

	signList:=c.writeAllotSign.signalMultiplication(len(allotMap))

	var index = 0
	for nodeName, slice := range allotMap {
		path:= c.getDistributorBasePath() + zkPathSplit + nodeName
		value,err:= msgpack.Marshal(slice)
		CheckErr(err)
		writeTempNodeValue(c.myStore,path,value,signList[index])
		logger.Warn(fmt.Sprintf("writeAllotMsg nodeName: %v ,slice: %v",nodeName, slice))
		index++
	}
}

// 获取 worker 进程 注册信息
func (c *MyDistributor)getWorkers() (NameSlice,error){
	path:= c.getWorkerRegistryPath()

	c.getWorkStopSign.updateStart()
	ticker := time.NewTicker(3*time.Second)
	defer ticker.Stop()
	for {
		select {
			case <-ticker.C:
				kvS, err:= (*c.myStore).List(path)
				if err!=nil{
					logger.Error(fmt.Sprintf("get list err: %v",err))
					c.getWorkStopSign.updateStop()
					return nil,err
				}
				if len(kvS) > 0{
					var instanceNames NameSlice
					for _, kv := range kvS {
						nodeName:=kv.Key
						var msg RegistryValue
						if err:=msgpack.Unmarshal(kv.Value,&msg);err!=nil{
							return nil,err
						}
						instanceNames= append(instanceNames,nodeName)
					}
					c.getWorkStopSign.updateStop()
					return instanceNames,nil
				}
			case <-c.getWorkStopSign.quit:
				c.getWorkStopSign.updateStop()
				return nil,errors.New("get signal stop")
		}
	}

}








