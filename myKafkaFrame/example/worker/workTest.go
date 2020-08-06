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
	fmt.Println("command :  -c [configFile] -l [logFile] ")
	fmt.Println("example : ")
	fmt.Println("             worker -c worker.json -l myLog.json")
	fmt.Println("====================================================")
}

func flagInit() (string,string) {

	exPath := os.Getenv("CONFIG_PATH")
	defaultLogFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	defaultConfigFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)

	//defaultLogFile = "D:\\backup\\study\\go\\myKafkaFrame\\example\\conf\\myLog.json"
	//defaultConfigFile = "D:\\backup\\study\\go\\myKafkaFrame\\example\\conf\\worker.json"

	var configFile string
	var logFile string

	flag.StringVar(&configFile, "c", defaultConfigFile, "configFile for read")
	flag.StringVar(&logFile, "l", defaultLogFile, "logFile for read")
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

	//square:=getSquare(&rectangle)
	//fmt.Println("square: ",square)

	return nil
}

func getSquare(rectangle *Rectangle) Square{
	return Square{rectangle.Height * rectangle.Length}
}



