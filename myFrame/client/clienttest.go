package main

import (
	"flag"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"github.com/wonderivan/logger"
	"myFrame"
	"os"
	"path/filepath"
)

type Rectangle struct {
	Length int
	Height int
}

type Square struct {
	S int
}

var (
	DefaultInstanceName = "a"
	DefaultConfigPath     = "myFrame"
	DefaultConfigFileName = "myFrame.json"
	LogConfigFileName     = "myLog.json"
)

func main() {

	flag.Usage = func() {
		help()
	}

	instanceName,configFile,logFile:=flagInit()
	c:= myFrame.MyConsumer{}
	c.InitParam(instanceName,configFile,logFile)

	c.SuccessFunc = successDeal
	c.FailFunc = failDeal
	c.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   -i [instanceName] -f [configFile] ")
	fmt.Println("example : ")
	fmt.Println("             client -i p1 ")
	fmt.Println("====================================================")
}

func flagInit() (string,string,string) {

	exPath := os.Getenv("CONFIG_PATH")
	logFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	localFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)

	var instanceName string
	var configFile string

	flag.StringVar(&instanceName, "i", DefaultInstanceName, " instanceName ")
	flag.StringVar(&configFile, "f", localFile, "configFile for read")
	flag.Parse()

	return instanceName,configFile,logFile
}

func  successDeal (msg *myFrame.Message) error{

	var shape Rectangle
	if err:=msgpack.Unmarshal(msg.Value,&shape);err!=nil{
		logger.Error(fmt.Sprintf("Unmarshal value err : %v \n",err))
		return err
	}
	var res Square
	if err:=msgpack.Unmarshal(msg.Result,&res);err!=nil{
		logger.Error(fmt.Sprintf("Unmarshal result err : %v \n",err))
		return err
	}

	//logger.Warn(fmt.Sprintf( "topic: %s ,partition: %d ,offset: %d ,key: %s ,shape: %v ,square: %d \n",
	//		msg.Topic, msg.Partition, msg.Offset, msg.Key, shape,res.S))
	return nil
}

func  failDeal (msg *myFrame.Message) error{
	return nil
}

