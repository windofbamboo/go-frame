package main

import (
	"flag"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"myFrame"
	"myFrame/example"
)

func main() {

	flag.Usage = func() {
		help()
	}

	instanceName:=flagInit()
	myFrame.ProviderInit(example.AppName,instanceName,example.LogConfigFileName,example.DefaultConfigFileName)

	p:= myFrame.NewMyProvider(example.AppName,instanceName)

	p.RegistryDealFunc(deal)
	p.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   -i [instanceName] ")
	fmt.Println("example : ")
	fmt.Println("             server -i c1 ")
	fmt.Println("====================================================")
}

func flagInit() (instanceName string) {

	flag.StringVar(&instanceName, "i", example.DefaultInstanceName, " instanceName ")
	flag.Parse()

	return instanceName
}

func deal(in *[]byte, out *[]byte) error{

	var rectangle example.Rectangle
	if err:=msgpack.Unmarshal(*in,&rectangle);err!=nil{
		return err
	}

	if res,err:= msgpack.Marshal(getSquare(&rectangle));err!=nil{
		return err
	}else{
		*out = res
	}

	return nil
}

func getSquare(rectangle *example.Rectangle) example.Square{
	return example.Square{S: rectangle.Height * rectangle.Length}
}



