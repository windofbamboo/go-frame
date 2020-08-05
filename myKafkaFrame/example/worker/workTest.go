package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/vmihailenco/msgpack"
	"myKafkaFrame"
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
	DefaultConfigPath     = "myKafkaFrame"
	DefaultConfigFileName = "worker.json"
	LogConfigFileName     = "myLog.json"
)

func main() {

	flag.Usage = func() {
		help()
	}

	configFile,logFile:=flagInit()
	p:= myKafkaFrame.MyWorker{}
	p.InitParam(configFile,logFile)

	p.RegistryDealFunc(dealDataFunc)
	p.RegistryErrFunc(errDataFunc)

	p.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :  -f [configFile] ")
	fmt.Println("example : ")
	fmt.Println("             worker -f worker.json ")
	fmt.Println("====================================================")
}

func flagInit() (string,string) {

	exPath := os.Getenv("CONFIG_PATH")
	logFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	localFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)

	var configFile string

	flag.StringVar(&configFile, "f", localFile, "configFile for read")
	flag.Parse()

	return configFile,logFile
}


func errDataFunc(in *sarama.ConsumerMessage) error{

	fmt.Printf("err msg : %v \n",in)
	return nil
}


func dealDataFunc(msg *sarama.ConsumerMessage) error{

	var rectangle Rectangle
	if err:=msgpack.Unmarshal(msg.Value,&rectangle);err!=nil{
		return err
	}

	if res,err:= msgpack.Marshal(getSquare(&rectangle));err!=nil{
		return err
	}else{
		fmt.Println(res)
	}

	return nil
}

func getSquare(rectangle *Rectangle) Square{
	return Square{rectangle.Height * rectangle.Length}
}



