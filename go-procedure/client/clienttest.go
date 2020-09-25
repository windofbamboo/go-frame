package main

import (
	"flag"
	"fmt"
	"github.com/wonderivan/logger"
	"go-procedure/base"
)

func main() {

	flag.Usage = func() {
		help()
	}

	base.ConsumerInit()

	c:= base.NewMyConsumer()
	c.RegistrySuccessFunc(successDeal)
	c.RegistryFailFunc(failDeal)

	c.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :  client ")
	fmt.Println("====================================================")
}

func  successDeal (msg *base.Message) error{

	info:=fmt.Sprintf("call {%v} success ",msg.InPutParam.Info())
	logger.Info(info)
	return nil
}

func  failDeal (msg *base.Message) error{

	logger.Error(fmt.Sprintf("call {%v} fail ,err: %v",msg.InPutParam.Info() ,msg.ResultInfo))
	return nil
}

