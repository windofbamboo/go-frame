package myKafkaFrame

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"github.com/wonderivan/logger"
	"strings"
)

type zkContent struct {
	zkAddr     []string
}

type kafkaContent struct {
	brokers,topics []string
	consumer string
}

// 配置文件 内容
type ConfigContent struct {
	zk zkContent
	kafka kafkaContent
	workerName,instanceName,host string
	instanceType string
}

var configContent ConfigContent

func ReadConfig(instanceType string,cfg string) error {

	if err := Init(cfg); err != nil {
		panic(err)
	}
	// zk
	var zk zkContent
	paramContent := viper.GetStringMap("zk")
	for k, v := range paramContent {
		var name = k
		switch name {
			case strings.ToLower("zkAddr"):
				var zkAddrStr = v.(string)
				zk.zkAddr = strings.Split(zkAddrStr, ",")
			default:
		}
	}
	// kafka
	var kafka kafkaContent
	paramContent = viper.GetStringMap("kafka")
	for k, v := range paramContent {
		var name = k
		switch name {
			case strings.ToLower("brokers"):
				var brokerStr = v.(string)
				kafka.brokers = strings.Split(brokerStr, ",")
			case strings.ToLower("topics"):
				var topicStr = v.(string)
				kafka.topics = strings.Split(topicStr, ",")
			case strings.ToLower("consumer"):
				kafka.consumer = v.(string)
			default:
		}
	}

	configContent.zk = zk
	configContent.kafka = kafka

	if instanceType == InstanceTypeDistributor{
		configContent.instanceType = InstanceTypeDistributor
		paramContent = viper.GetStringMap("distributor")
		for k, v := range paramContent {
			var name = k
			switch name {
			case strings.ToLower("instanceName"):
				configContent.instanceName = v.(string)
			case strings.ToLower("workerName"):
				configContent.workerName = v.(string)
			case strings.ToLower("host"):
				configContent.host = v.(string)
			default:
			}
		}
	}else if instanceType == InstanceTypeWorker{
		configContent.instanceType = InstanceTypeWorker
		paramContent = viper.GetStringMap("worker")
		for k, v := range paramContent {
			var name = k
			switch name {
			case strings.ToLower("instanceName"):
				configContent.instanceName = v.(string)
			case strings.ToLower("workerName"):
				configContent.workerName = v.(string)
			case strings.ToLower("host"):
				configContent.host = v.(string)
			default:
			}
		}
	}else{
		return errors.New("instanceType is err ")
	}

	err := CheckConfig()
	if err != nil {
		return err
	}

	return nil
}

func CheckConfig() error {

	if len(configContent.zk.zkAddr) == 0{
		return 	errors.New("zk addr is nil ")
	}

	if len(configContent.kafka.brokers) == 0{
		return 	errors.New("kafka brokers is nil ")
	}
	if len(configContent.kafka.topics) == 0{
		return 	errors.New("kafka brokers is nil ")
	}

	if configContent.instanceType == InstanceTypeWorker{
		if configContent.kafka.consumer == ""{
			return 	errors.New("kafka consumer is nil ")
		}
	}

	if len(configContent.workerName) == 0{
		return 	errors.New("workerName is nil ")
	}
	if len(configContent.instanceName) == 0{
		return 	errors.New("instanceName is nil ")
	}

	return nil
}


func PrintConfig() {

	logger.Info("==================== config file context ========================================= ")
	logger.Info(" instance type is : ",configContent.instanceType)
	//zk
	logMessage := fmt.Sprintf("zk : { zkAddr : ")
	for i, broker := range configContent.zk.zkAddr {
		if i == 0 {
			logMessage += fmt.Sprintf("[ %s", broker)
		} else {
			logMessage += fmt.Sprintf(",%s", broker)
		}
	}
	logMessage += fmt.Sprintf(" ] } ")
	logger.Info(logMessage)

	//kafka
	logMessage = fmt.Sprintf("kafka : { brokers : ")
	for i, broker := range configContent.kafka.brokers {
		if i == 0 {
			logMessage += fmt.Sprintf("[ %s", broker)
		} else {
			logMessage += fmt.Sprintf(",%s", broker)
		}
		logMessage += fmt.Sprintf(" ]")
	}
	logMessage += fmt.Sprintf(" , topics : ")
	for i, topic := range configContent.kafka.topics {
		if i == 0 {
			logMessage += fmt.Sprintf("[ %s", topic)
		} else {
			logMessage += fmt.Sprintf(",%s", topic)
		}
		logMessage += fmt.Sprintf(" ]")
	}
	if configContent.instanceType == InstanceTypeWorker{
		logMessage += fmt.Sprintf(" , consumer : %v",configContent.kafka.consumer)
	}
	logMessage += fmt.Sprintf(" } ")
	logger.Info(logMessage)

	//server
	if configContent.instanceType == InstanceTypeDistributor{
		logMessage = fmt.Sprintf("distributor : { ")
		logMessage += fmt.Sprintf(" workerName : %v",configContent.workerName)
		logMessage += fmt.Sprintf(" , instanceName : %v",configContent.instanceName)
		logMessage += fmt.Sprintf(" , host : %v } ",configContent.host)
		logger.Info(logMessage)
	}

	if configContent.instanceType == InstanceTypeWorker{
		logMessage = fmt.Sprintf("worker : { ")
		logMessage += fmt.Sprintf(" workerName : %v",configContent.workerName)
		logMessage += fmt.Sprintf(" , instanceName : %v",configContent.instanceName)
		logMessage += fmt.Sprintf(" , host : %v } ",configContent.host)
		logger.Info(logMessage)
	}

	logger.Info("================================================================================== ")
}
