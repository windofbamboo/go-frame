package base

import (
	"context"
	"fmt"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/robfig/cron"
	myClient "github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
	"github.com/wonderivan/logger"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type MyConsumer struct {
	storeAddr    []string
	storeType,instanceName   string
	connectTimeout,backupLatency  int
	myStore     *store.Store
	xClient 	*myClient.XClient
	taskQueue,failQueue,successQueue chan Message
	lockStopCh,taskStopCh chan struct{}
	failFunc      func(msg *Message) error
	successFunc   func(msg *Message) error
}

func NewMyConsumer() *MyConsumer{

	m := &MyConsumer{}

	m.taskQueue = make(chan Message,3000)
	m.failQueue = make(chan Message,3)
	m.successQueue = make(chan Message,3)
	m.lockStopCh = make(chan struct{})
	m.taskStopCh = make(chan struct{})

	m.instanceName = ConsumerInstanceName
	m.storeType = configContent.server.storeType
	m.storeAddr = configContent.server.storeAddr
	m.connectTimeout = configContent.server.connectTimeout
	m.backupLatency = configContent.server.backupLatency

	return m
}


func (m *MyConsumer) RegistrySuccessFunc(fn DealFunc) {
	m.successFunc = fn
}

func (m *MyConsumer) RegistryFailFunc(fn DealFunc) {
	m.failFunc = fn
}

func (m *MyConsumer) Start() {

	m.initStore()
	defer (*m.myStore).Close()
	//get Permission
	m.getExecPermission()
	//get client
	m.initXClient()
	defer (*m.xClient).Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt,
		os.Kill)

	go m.getTaskList()

	// consumer
	for {
		select {
		case msg, ok := <-m.taskQueue:
			logger.Trace(fmt.Sprintf("get task msg { %v } ",msg.InPutParam.Info() ))
			if !ok {
				logger.Error("get taskQueue err ... ")
				return
			}
			go m.remoteCallProcedure(msg)

		case msg := <-m.failQueue:
			m.dealFail(&msg)
		case msg := <-m.successQueue:
			m.dealSuccess(&msg)
		case <-signals:
			m.lockStopCh <- struct{}{}
			m.taskStopCh <- struct{}{}
			time.Sleep(time.Second)
			logger.Warn("consumer end ... ")
			return
		}
	}
}

func (m *MyConsumer) remoteCallProcedure(msg Message) {

	(*m.xClient).Auth(DefaultToken)

	ctx := context.WithValue(context.Background(), share.ReqMetaDataKey, make(map[string]string))
	ctx = context.WithValue(ctx, share.ResMetaDataKey, make(map[string]string))
	ctx,_ = context.WithTimeout(ctx,GetSecondTime(msg.TimeOut))

	inData := &Message{InPutParam:msg.InPutParam}
	outData := &Message{}

	if res, err := (*m.xClient).Go(ctx, "callProcedure", inData, outData, nil); err != nil {
		logger.Error(fmt.Sprintf("xClient.Go err : %v", err))
		panic(err)
	} else {
		replyCall := <-res.Done
		if replyCall.Error != nil {
			logger.Warn(fmt.Sprintf("server return err : %v", replyCall.Error))
			if m.failFunc != nil {
				m.failQueue <- *outData
			}
		} else {
			if m.successFunc != nil {
				m.successQueue <- *outData
			}
		}
	}
}

func (m *MyConsumer) initStore(){

	var storeType store.Backend
	switch m.storeType {
	case StoreTypeEtcd:
		storeType = store.ETCD
	case StoreTypeConsul:
		storeType = store.CONSUL
	case StoreTypeZk:
		storeType = store.ZK
	}

	kv, err := libkv.NewStore(storeType, m.storeAddr, nil)
	if err != nil {
		logger.Error(fmt.Sprintf("init store err: %v", err))
		panic(err)
	}
	m.myStore = &kv

	logger.Trace("store init success ")
}

func (m *MyConsumer) initXClient() {

	var d myClient.ServiceDiscovery
	switch m.storeType {
	case StoreTypeEtcd:
		d = myClient.NewEtcdV3Discovery(basePath, ServicePath, m.storeAddr, nil)
	case StoreTypeConsul:
		d = myClient.NewConsulDiscovery(basePath, ServicePath, m.storeAddr, nil)
	case StoreTypeZk:
		d = myClient.NewZookeeperDiscovery(basePath, ServicePath, m.storeAddr, nil)
	}

	option := &myClient.DefaultOption
	option.CompressType = protocol.Gzip
	option.ConnectTimeout = GetSecondTime(m.connectTimeout)
	option.BackupLatency = GetSecondTime(m.backupLatency)
	option.Heartbeat = true
	option.HeartbeatInterval = time.Second
	option.Group = DefaultServerGroup

 	client := myClient.NewXClient(ServicePath, myClient.Failtry, myClient.RoundRobin, d, *option)
	m.xClient = &client

	logger.Trace("client init success ")
}

func (m *MyConsumer) getExecPermission(){

	logger.Warn(fmt.Sprintf("%s try lock ...", m.instanceName))
	for {
		if ok, err := m.getQueueLock(lockPath, lockNode, m.lockStopCh); err != nil {
			logger.Error(fmt.Sprintf("get lock err: %v", err))
			panic(err)
		} else {
			if ok {
				break
			}
		}
		time.Sleep(tickerTime)
	}
	logger.Warn(fmt.Sprintf("%s get lock success , start deal data ...", m.instanceName))
}

func(m *MyConsumer) getTaskList() {

	location,err:=time.LoadLocation("Asia/Shanghai")
	if err!=nil{
		panic(err)
	}
	j := cron.NewWithLocation(location)

	logger.Trace(fmt.Sprintf("Location is : %v",j.Location()))

	for _, taskJob := range configContent.TaskJobs {
		for _, job := range taskJob.Jobs {

			var msg Message
			msg.InPutParam.EnvId = job.EnvId
			msg.InPutParam.ProcedureName = job.ProcedureName
			msg.InPutParam.InParamStr = job.InParamStr
			msg.TimeOut = job.TimeOut

			logger.Trace(fmt.Sprintf("cron : %v , task_id : %v,envId : %v , procedureName : %v , InParamStr : %v",
				taskJob.Spec,taskJob.Id,job.EnvId,job.ProcedureName,job.InParamStr ))

			err=j.AddFunc(taskJob.Spec,func(){
				logger.Trace(fmt.Sprintf("taskQueue add msg { %v } ",msg.InPutParam.Info() ))
				m.taskQueue <- msg
			})
			if err!=nil{
				panic(err)
			}
		}
	}

	j.Start()
	defer j.Stop()

	select {
	case <- m.taskStopCh:
		return
	}
}


func (m *MyConsumer) dealSuccess(msg *Message) {
	if m.successFunc != nil {
		if err := m.successFunc(msg); err != nil {
			logger.Error(fmt.Sprintf("exec SuccessFunc err : %v", err))
		}
	}
}

func (m *MyConsumer) dealFail(msg *Message) {
	if m.failFunc != nil {
		if err := m.failFunc(msg); err != nil {
			logger.Error(fmt.Sprintf("FailFunc err : %v", err))
		}
	}
}

func (m *MyConsumer) createNode(node string) error {

	if ok, err := (*m.myStore).Exists(node); err != nil {
		return err
	} else {
		if ok {
			return nil
		}
	}
	if err := (*m.myStore).Put(node, []byte{1}, nil); err != nil {
		return err
	} else {
		return nil
	}
}

func (m *MyConsumer) getQueueLock(lockPath string, lockName string, lockStopCh <-chan struct{}) (bool, error) {

	if err := m.createNode(lockPath); err != nil {
		return false, err
	}
	mySequence := GetCurrentTime()
	tempPath := lockPath + "/" + lockName + mySequence

	if err := (*m.myStore).Put(tempPath, []byte(lockName), &store.WriteOptions{TTL: 2 * tickerTime}); err != nil {
		return false, err
	}

	stopWatchCh := make(chan struct{})
	stopCheckCh := make(chan struct{})

	ticker := time.NewTicker(tickerTime)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := (*m.myStore).Put(tempPath, []byte(lockName), &store.WriteOptions{TTL: 2 * tickerTime})
				if err != nil {
					fmt.Printf("set node value err: %v", err)
				}
			case <-lockStopCh:
				stopWatchCh <- struct{}{}
				stopCheckCh <- struct{}{}
				return
			}
		}
	}()

	kvCh, err := (*m.myStore).WatchTree(lockPath, stopWatchCh)
	if err != nil {
		return false, err
	}

	for {
		select {
		case child := <-kvCh:
			var minSequence string
			for _, pair := range child {
				sequenceName := strings.ReplaceAll(pair.Key, lockName, "")

				if minSequence > sequenceName || minSequence == "" {
					minSequence = sequenceName
				}
			}
			if mySequence == minSequence {
				return true, nil
			}
		case <-stopCheckCh:
			return false, nil
		}
	}

}
