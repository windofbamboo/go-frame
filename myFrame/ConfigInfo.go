package myFrame

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"github.com/wonderivan/logger"
	"strconv"
	"strings"
)

type zkContent struct {
	zkAddr     []string
	//basePath,servicePath string
}

type kafkaContent struct {
	brokers,topics []string
	consumer string
	batchNum int
	waitSecond int
}

type serverContent struct {
	model string
	connectTimeout,backupLatency int
}

type service struct {
	name,host string
	port int
}

type client struct {
	name,host,initStatus string
}

// 配置文件 内容
type ConfigContent struct {
	zk zkContent
	kafka kafkaContent
	server serverContent
	provider []service
	consumer []client
}

var configContent ConfigContent

func ReadConfig(cfg string) error {

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
			case strings.ToLower("batchNum"):
				a,err:=strconv.Atoi(v.(string))
				if err!=nil{}
				kafka.batchNum = a
			case strings.ToLower("waitSecond"):
				a,err:=strconv.Atoi(v.(string))
				if err!=nil{}
				kafka.batchNum = a
			default:
		}
	}

	var server serverContent
	paramContent = viper.GetStringMap("server")
	for k, v := range paramContent {
		var name = k
		switch name {
		case strings.ToLower("model"):
			server.model = v.(string)
		case strings.ToLower("ConnectTimeout"):
			a,err:=strconv.Atoi(v.(string))
			if err!=nil{}
			server.connectTimeout = a
			//server.connectTimeout = v.(int)
		case strings.ToLower("backupLatency"):
			a,err:=strconv.Atoi(v.(string))
			if err!=nil{}
			server.backupLatency = a
		default:
		}
	}

	var provider []service
	paramContent = viper.GetStringMap("provider")
	for k, vMap := range paramContent {
		var instance service
		instance.name = k
		contentMap := vMap.(map[string]interface{})
		if host, ok := contentMap[strings.ToLower("host")]; ok {
			instance.host = host.(string)
		}
		if port, ok := contentMap[strings.ToLower("port")]; ok {
			a,err:=strconv.Atoi(port.(string))
			if err!=nil{}
			instance.port = a
		}
		provider = append(provider,instance)
	}

	var consumer []client
	paramContent = viper.GetStringMap("consumer")
	for k, vMap := range paramContent {
		var instance client
		instance.name = k
		contentMap := vMap.(map[string]interface{})
		if host, ok := contentMap[strings.ToLower("host")]; ok {
			instance.host = host.(string)
		}
		if initStatus, ok := contentMap[strings.ToLower("initStatus")]; ok {
			instance.initStatus = initStatus.(string)
		}
		consumer = append(consumer,instance)
	}

	configContent.zk = zk
	configContent.kafka = kafka
	configContent.server = server
	configContent.provider = provider
	configContent.consumer = consumer

	err := CheckConfig()
	if err != nil {
		return err
	}

	PrintConfig()
	return nil
}

func CheckConfig() error {
	return nil
}

func CheckInstance(instanceType string, instanceName string) error{
	switch instanceType {
		case InstanceTypeProvider:
			for _, s := range configContent.provider {
				if s.name == instanceName{
					return nil
				}
			}
		case InstanceTypeConsumer:
			for _, c := range configContent.consumer {
				if c.name == instanceName{
					return nil
				}
			}
		default:
	}
	return errors.New("not found instanceName in configFile")
}



func PrintConfig() {

	logger.Info("==================== config file context ========================================= ")
	//zk
	logMessage := fmt.Sprintf("zk : { zkAddr : ")
	for i, broker := range configContent.zk.zkAddr {
		if i == 0 {
			logMessage += fmt.Sprintf("[ %s", broker)
		} else {
			logMessage += fmt.Sprintf(",%s", broker)
		}
		logMessage += fmt.Sprintf(" ]")
	}
	//logMessage += fmt.Sprintf(" , basePath : %v",configContent.zk.basePath)
	//logMessage += fmt.Sprintf(" , servicePath : %v }",configContent.zk.servicePath)
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
	logMessage += fmt.Sprintf(" , consumer : %v",configContent.kafka.consumer)
	logMessage += fmt.Sprintf(" , batchNum : %v",configContent.kafka.batchNum)
	logMessage += fmt.Sprintf(" , waitSecond : %v }",configContent.kafka.waitSecond)
	logger.Info(logMessage)

	//server
	logMessage = fmt.Sprintf("server : { ")
	logMessage += fmt.Sprintf(" , model : %v",configContent.server.model)
	logMessage += fmt.Sprintf(" , connectTimeout : %v",configContent.server.connectTimeout)
	logMessage += fmt.Sprintf(" , backupLatency : %v }",configContent.server.backupLatency)
	logger.Info(logMessage)

	//provider
	logMessage = fmt.Sprintf("provider : { \n")
	for _, s := range configContent.provider {
		logMessage +=fmt.Sprintf(" %s : { host: %s , port: %d }\n", s.name,s.host,s.port)
	}
	logMessage +="}"
	logger.Info(logMessage)

	//consumer
	logMessage = fmt.Sprintf("consumer : { \n")
	for _, c := range configContent.consumer {
		logMessage +=fmt.Sprintf(" %s : { host: %s , initStatus: %s }\n", c.name,c.host,c.initStatus)
	}
	logMessage +="}"
	logger.Info(logMessage)

	logger.Info("================================================================================== ")
}
