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
	"github.com/wonderivan/logger"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type MyConsumer struct {
	brokers, topics, zkAddr                []string
	instanceName, groupId, basePath, model string
	batchNum                               int32
	batchWaitSecond                        int64
	connectTimeout, backupLatency          int
	myStore                                *store.Store
	FailFunc                               func(msg *Message) error
	SuccessFunc                            func(msg *Message) error
}

func (c *MyConsumer) InitParam(instanceName string, configFile string, logFile string) {

	err := CheckFile(logFile)
	CheckErr(err)
	contentStr, err := ReSetLogFileName(logFile, instanceName)
	CheckErr(err)
	err = logger.SetLogger(contentStr)
	CheckErr(err)

	err = CheckFile(configFile)
	CheckErr(err)

	err = ReadConfig(configFile)
	CheckErr(err)

	err = CheckInstance(InstanceTypeConsumer, instanceName)
	CheckErr(err)

	c.instanceName = instanceName
	c.zkAddr = configContent.zk.zkAddr
	c.basePath = configContent.zk.basePath

	c.brokers = configContent.kafka.brokers
	c.topics = configContent.kafka.topics
	c.groupId = configContent.kafka.consumer
	c.batchNum = int32(configContent.kafka.batchNum)
	c.batchWaitSecond = int64(configContent.kafka.waitSecond)

	c.model = configContent.server.model
	c.connectTimeout = configContent.server.connectTimeout
	c.backupLatency = configContent.server.backupLatency
}

func (c *MyConsumer) Start() {

	storeType := store.ZK
	kv, err := libkv.NewStore(storeType, c.zkAddr, nil)
	if err != nil {
		logger.Error(fmt.Sprintf("init store err: %v", err))
		panic(err)
	}
	c.myStore = &kv
	defer (*c.myStore).Close()

	//get lock
	lockStopCh := make(chan struct{})
	logger.Warn(fmt.Sprintf("%s try lock ...", c.instanceName))
	for {
		if ok, err := c.getQueueLock(lockPath, lockNode, lockStopCh); err != nil {
			logger.Error(fmt.Sprintf("get lock err: %v", err))
			panic(err)
		} else {
			if ok {
				break
			}
		}
		time.Sleep(tickerTime)
	}
	logger.Warn(fmt.Sprintf("%s get lock success , start deal data ...", c.instanceName))

	//read kafka
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// init consumer
	consumer, err := cluster.NewConsumer(c.brokers, c.groupId, c.topics, config)
	if err != nil {
		logger.Error(fmt.Sprintf("init consumer err: %v", err))
		panic(err)
	} else {
		logger.Warn("init consumer success ... ")
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
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
			if c.model == "batch" {
				go c.batchDealPartition(consumer, &part, failQueue, successQueue)
			} else {
				go c.singleDealPartition(consumer, &part, failQueue, successQueue)
			}
		case msg := <-failQueue:
			c.dealFail(&msg)
		case msg := <-successQueue:
			c.dealSuccess(&msg)
		case <-signals:
			lockStopCh <- struct{}{}
			logger.Warn("consumer end ... ")
			return
		}
	}
}

func (c *MyConsumer) singleDealPartition(consumer *cluster.Consumer,
										pc *cluster.PartitionConsumer,
										failQueue chan<- Message,
										successQueue chan<- Message) {

	xClient := c.getXClient()
	defer xClient.Close()

	for msg := range (*pc).Messages() {
		data := Message{Key: msg.Key, Value: msg.Value, Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}

		if res, err := xClient.Go(context.Background(), "singleDeal", data, data, nil); err != nil {
			logger.Error(fmt.Sprintf("xClient.Go err : %v", err))
			panic(err)
		} else {
			replyCall := <-res.Done
			if replyCall.Error != nil {
				logger.Warn(fmt.Sprintf("server return err : %v", replyCall.Error))
				if c.FailFunc != nil {
					failQueue <- data
				}
			} else {
				consumer.MarkOffset(msg, "")
				if c.SuccessFunc != nil {
					successQueue <- data
				}
			}
		}
	}
}

func (c *MyConsumer) batchDealPartition(consumer *cluster.Consumer, pc *cluster.PartitionConsumer,
	failQueue chan<- Message, successQueue chan<- Message) {

	xClient := c.getXClient()
	defer xClient.Close()

	var rowNum int32
	var values []Message
	var lastTime = GetUnixTime()

	for msg := range (*pc).Messages() {
		data := Message{Key: msg.Key, Value: msg.Value, Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}
		values = append(values, data)
		rowNum += 1
		currTime := GetUnixTime()
		if rowNum >= c.batchNum || currTime-lastTime >= c.batchWaitSecond {

			if err := c.dealBatch(&xClient, &values, failQueue, successQueue); err != nil {
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

func (c *MyConsumer) getXClient() myClient.XClient {

	d := myClient.NewZookeeperDiscovery(c.basePath, ServicePath, c.zkAddr, nil)
	option := &myClient.DefaultOption
	option.CompressType = protocol.Gzip
	option.ConnectTimeout = GetSecondTime(c.connectTimeout)
	option.BackupLatency = GetSecondTime(c.backupLatency)

	return myClient.NewXClient(ServicePath, myClient.Failtry, myClient.RoundRobin, d, *option)
}

func (c *MyConsumer) dealBatch(xClient *myClient.XClient, values *[]Message, failQueue chan<- Message, successQueue chan<- Message) error {

	if res, err := (*xClient).Go(context.Background(), "batchDeal", values, values, nil); err != nil {
		if c.FailFunc != nil {
			for _, message := range *values {
				failQueue <- message
			}
		}
		logger.Error(fmt.Sprintf("xClient.Go err : %v", err))
		return err
	} else {
		replyCall := <-res.Done
		if replyCall.Error != nil {
			if c.FailFunc != nil {
				for _, message := range *values {
					failQueue <- message
				}
			}
			logger.Warn(fmt.Sprintf("batchDeal return err : %v", replyCall.Error))
			return replyCall.Error
		} else {
			if c.SuccessFunc != nil {
				for _, message := range *values {
					successQueue <- message
				}
			}
			return nil
		}
	}
}

func (c *MyConsumer) dealSuccess(msg *Message) {
	if c.SuccessFunc != nil {
		if err := c.SuccessFunc(msg); err != nil {
			logger.Error(fmt.Sprintf("exec SuccessFunc err : %v", err))
		}
	}
}

func (c *MyConsumer) dealFail(msg *Message) {
	if c.FailFunc != nil {
		if err := c.FailFunc(msg); err != nil {
			logger.Error(fmt.Sprintf("FailFunc err : %v", err))
		}
	}
}

func (c *MyConsumer) createNode(node string) error {

	if ok, err := (*c.myStore).Exists(node); err != nil {
		return err
	} else {
		if ok {
			return nil
		}
	}
	if err := (*c.myStore).Put(node, []byte{1}, nil); err != nil {
		return err
	} else {
		return nil
	}
}

func (c *MyConsumer) getQueueLock(lockPath string, lockName string, lockStopCh <-chan struct{}) (bool, error) {

	if err := c.createNode(lockPath); err != nil {
		return false, err
	}
	mySequence := GetCurrentTime()
	tempPath := lockPath + "/" + lockName + mySequence

	if err := (*c.myStore).Put(tempPath, []byte(lockName), &store.WriteOptions{TTL: 2 * tickerTime}); err != nil {
		return false, err
	}

	stopWatchCh := make(chan struct{})
	stopCheckCh := make(chan struct{})

	ticker := time.NewTicker(tickerTime)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := (*c.myStore).Put(tempPath, []byte(lockName), &store.WriteOptions{TTL: 2 * tickerTime})
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

	kvCh, err := (*c.myStore).WatchTree(lockPath, stopWatchCh)
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
