package main

import (
	"flag"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"github.com/wonderivan/logger"
	"myFrame"
	"myFrame/example"
)

func main() {

	flag.Usage = func() {
		help()
	}

	instanceName:=flagInit()

	myFrame.ConsumerInit(example.AppName,instanceName,example.LogConfigFileName,example.DefaultConfigFileName)

	c:= myFrame.NewMyConsumer(example.AppName,instanceName)
	c.RegistrySuccessFunc(successDeal)
	c.RegistryFailFunc(failDeal)
	c.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   -i [instanceName] ")
	fmt.Println("example : ")
	fmt.Println("             client -i p1 ")
	fmt.Println("====================================================")
}

func flagInit() (instanceName string) {

	flag.StringVar(&instanceName, "i", example.DefaultInstanceName, " instanceName ")
	flag.Parse()

	return instanceName
}

func successDeal (msg *myFrame.Message) error{

	var shape example.Rectangle
	if err:=msgpack.Unmarshal(msg.Value,&shape);err!=nil{
		logger.Error(fmt.Sprintf("Unmarshal value err : %v \n",err))
		return err
	}
	var res example.Square
	if err:=msgpack.Unmarshal(msg.Result,&res);err!=nil{
		logger.Error(fmt.Sprintf("Unmarshal result err : %v \n",err))
		return err
	}

	logger.Warn(fmt.Sprintf( "topic: %s ,partition: %d ,offset: %d ,key: %s ,shape: %v ,square: %d \n",
			msg.Topic, msg.Partition, msg.Offset, msg.Key, shape,res.S))
	return nil
}

func  failDeal (msg *myFrame.Message) error{
	return nil
}

