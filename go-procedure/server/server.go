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

	instanceName,err:=flagInit()
	if err!=nil{
		panic(err)
	}
	base.ProviderInit(instanceName)

	base.InitPoolMap()
	defer base.DestroyPoolMap()

	p:= base.NewMyProvider(instanceName)
	p.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :  -i [instanceName] ")
	fmt.Println("example : ")
	fmt.Println("             server -i c1 ")
	fmt.Println("====================================================")
}

func flagInit() (instanceName string,err error) {

	flag.StringVar(&instanceName, "i", base.NullStr, " instanceName ")
	flag.Parse()

	logger.Trace(fmt.Sprintf("instanceName = %v ",instanceName) )
	return instanceName,nil
}

