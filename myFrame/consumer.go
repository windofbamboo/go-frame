package myFrame

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
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
	brokers, topics, zkAddr                []string
	appName,instanceName, groupId, model string
	batchNum                               int32
	batchWaitSecond                        int64
	connectTimeout, backupLatency          int
	myStore                                *store.Store
	xClient 								*myClient.XClient
	failFunc                               func(msg *Message) error
	successFunc                            func(msg *Message) error
	lockStopCh	chan struct{}
}

func NewMyConsumer(appName,instanceName string) *MyConsumer {

	m := &MyConsumer{}

	m.appName = appName
	m.instanceName = instanceName
	m.zkAddr = configContent.zk.zkAddr
	//m.basePath = configContent.zk.basePath

	m.brokers = configContent.kafka.brokers
	m.topics = configContent.kafka.topics
	m.groupId = configContent.kafka.consumer
	m.batchNum = int32(configContent.kafka.batchNum)
	m.batchWaitSecond = int64(configContent.kafka.waitSecond)

	m.model = configContent.server.model
	m.connectTimeout = configContent.server.connectTimeout
	m.backupLatency = configContent.server.backupLatency

	m.lockStopCh = make(chan struct{})

	return m
}

type ResponseFunc func(in *Message) error

func (m *MyConsumer) RegistrySuccessFunc(fn ResponseFunc) {
	m.successFunc = fn
}

func (m *MyConsumer) RegistryFailFunc(fn ResponseFunc) {
	m.failFunc = fn
}

func (m *MyConsumer) Start() {

	m.initStore()
	defer (*m.myStore).Close()
	//get lock
	m.getExecPermission()
	//get client
	m.initXClient()
	defer (*m.xClient).Close()

	//read kafka
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// init consumer
	consumer, err := cluster.NewConsumer(m.brokers, m.groupId, m.topics, config)
	if err != nil {
		logger.Error(fmt.Sprintf("init consumer err: %v", err))
		panic(err)
	} else {
		logger.Warn("init consumer success ... ")
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt,
		os.Kill)

	failQueue := make(chan Message, 300)
	successQueue := make(chan Message, 300)
	// consume partitions
	for {
		select {
		case part, ok := <-consumer.Partitions():
			if !ok {
				return
			}
			if m.model == "batch" {
				go m.batchDealPartition(consumer, &part, failQueue, successQueue)
			} else {
				go m.singleDealPartition(consumer, &part, failQueue, successQueue)
			}
		case msg := <-failQueue:
			m.dealFail(&msg)
		case msg := <-successQueue:
			m.dealSuccess(&msg)
		case <-signals:
			m.lockStopCh <- struct{}{}
			logger.Warn("consumer end ... ")
			return
		}
	}
}


func (m *MyConsumer) initStore(){

	storeType := store.ZK
	kv, err := libkv.NewStore(storeType, m.zkAddr, nil)
	if err != nil {
		logger.Error(fmt.Sprintf("init store err: %v", err))
		panic(err)
	}
	m.myStore = &kv

	logger.Trace("store init success ")
}

func (m *MyConsumer) initXClient() {

	d := myClient.NewZookeeperDiscovery(FrameName+"/"+m.appName, ServicePath, m.zkAddr, nil)

	option := &myClient.DefaultOption
	option.CompressType = protocol.Gzip
	option.ConnectTimeout = GetSecondTime(m.connectTimeout)
	option.BackupLatency = GetSecondTime(m.backupLatency)
	option.Heartbeat = true
	option.HeartbeatInterval = time.Second
	option.Group = DefaultServerGroup

	client := myClient.NewXClient(ServicePath, myClient.Failtry, myClient.RoundRobin, d, *option)
	m.xClient = &client
}


func (m *MyConsumer) singleDealPartition(consumer *cluster.Consumer,
										pc *cluster.PartitionConsumer,
										failQueue chan<- Message,
										successQueue chan<- Message) {


	(*m.xClient).Auth(DefaultToken)

	ctx := context.WithValue(context.Background(), share.ReqMetaDataKey, make(map[string]string))
	ctx = context.WithValue(ctx, share.ResMetaDataKey, make(map[string]string))

	for msg := range (*pc).Messages() {
		inData := Message{Key: msg.Key, Value: msg.Value, Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}
		outData := Message{}
		if res, err := (*m.xClient).Go(ctx, "singleDeal", &inData, &outData, nil); err != nil {
			logger.Error(fmt.Sprintf("xClient.Go err : %v", err))
			panic(err)
		} else {
			replyCall := <-res.Done
			if replyCall.Error != nil {
				logger.Warn(fmt.Sprintf("server return err : %v", replyCall.Error))
				if m.failFunc != nil {
					failQueue <- outData
				}
			} else {
				consumer.MarkOffset(msg, "")
				if m.successFunc != nil {
					successQueue <- outData
				}
			}
		}
	}
}

func (m *MyConsumer) batchDealPartition(consumer *cluster.Consumer, pc *cluster.PartitionConsumer,
	failQueue chan<- Message, successQueue chan<- Message) {

	var rowNum int32
	var values []Message
	var lastTime = GetUnixTime()

	for msg := range (*pc).Messages() {
		data := Message{Key: msg.Key, Value: msg.Value, Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}
		values = append(values, data)
		rowNum += 1
		currTime := GetUnixTime()
		if rowNum >= m.batchNum || currTime-lastTime >= m.batchWaitSecond {

			if err := m.dealBatch(&values, failQueue, successQueue); err != nil {
				logger.Warn(fmt.Sprintf("dealBatch return err : %v", err))
			} else {
				lastTime = GetUnixTime()
				rowNum = 0
				values = []Message{}

				consumer.MarkOffset(msg, "") // mark message as processed
			}
		}
	}
}


func (m *MyConsumer) dealBatch(values *[]Message, failQueue chan<- Message, successQueue chan<- Message) error {

	(*m.xClient).Auth(DefaultToken)

	ctx := context.WithValue(context.Background(), share.ReqMetaDataKey, make(map[string]string))
	ctx = context.WithValue(ctx, share.ResMetaDataKey, make(map[string]string))

	if res, err := (*m.xClient).Go(ctx, "batchDeal", values, values, nil); err != nil {
		if m.failFunc != nil {
			for _, message := range *values {
				failQueue <- message
			}
		}
		logger.Error(fmt.Sprintf("xClient.Go err : %v", err))
		return err
	} else {
		replyCall := <-res.Done
		if replyCall.Error != nil {
			if m.failFunc != nil {
				for _, message := range *values {
					failQueue <- message
				}
			}
			logger.Warn(fmt.Sprintf("batchDeal return err : %v", replyCall.Error))
			return replyCall.Error
		} else {
			if m.successFunc != nil {
				for _, message := range *values {
					successQueue <- message
				}
			}
			return nil
		}
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

func (m *MyConsumer) getExecPermission(){

	logger.Warn(fmt.Sprintf("%s try lock ...", m.instanceName))
	for {
		if ok, err := m.getQueueLock(m.lockStopCh); err != nil {
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

func (m *MyConsumer) getQueueLock(lockStopCh <-chan struct{}) (bool, error) {

	var node = FrameName
	if err := m.createNode(node); err != nil {
		return false, err
	}
	node = FrameName + "/" + m.appName
	if err := m.createNode(node); err != nil {
		return false, err
	}
	appLockPath := FrameName + "/" + m.appName + "/" +lockPath
	if err := m.createNode(node); err != nil {
		return false, err
	}

	mySequence := GetCurrentTime()
	tempPath := appLockPath + "/" + m.appName + mySequence

	if err := (*m.myStore).Put(tempPath, []byte(m.appName), &store.WriteOptions{TTL: 2 * tickerTime}); err != nil {
		return false, err
	}

	stopWatchCh := make(chan struct{})
	stopCheckCh := make(chan struct{})

	ticker := time.NewTicker(tickerTime)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := (*m.myStore).Put(tempPath, []byte(m.appName), &store.WriteOptions{TTL: 2 * tickerTime})
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

	kvCh, err := (*m.myStore).WatchTree(appLockPath, stopWatchCh)
	if err != nil {
		return false, err
	}

	for {
		select {
		case child := <-kvCh:
			var minSequence string
			for _, pair := range child {
				sequenceName := strings.ReplaceAll(pair.Key, m.appName, "")

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
