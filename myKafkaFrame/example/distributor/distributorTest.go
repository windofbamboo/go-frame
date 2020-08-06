package main

import (
	"flag"
	"fmt"
	"myKafkaFrame"
	"os"
	"path/filepath"
)

var (
	DefaultConfigPath     = "myKafkaFrame"
	DefaultConfigFileName = "distributor.json"
	LogConfigFileName     = "myLog.json"
)

func main() {

	flag.Usage = func() {
		help()
	}

	configFile,logFile:=flagInit()
	c:= myKafkaFrame.MyDistributor{}
	c.InitParam(configFile,logFile)
	c.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   -c [configFile] -l [logFile]")
	fmt.Println("example : ")
	fmt.Println("             distributor -c distributor.json -l myLog.json")
	fmt.Println("====================================================")
}

func flagInit() (string,string) {

	exPath := os.Getenv("CONFIG_PATH")
	defaultLogFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	defaultConfigFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)

	//defaultLogFile = "D:\\backup\\study\\go\\myKafkaFrame\\example\\conf\\myLog.json"
	//defaultConfigFile = "D:\\backup\\study\\go\\myKafkaFrame\\example\\conf\\distributor.json"

	var configFile string
	var logFile string

	flag.StringVar(&configFile, "c", defaultConfigFile, "configFile for read")
	flag.StringVar(&logFile, "l", defaultLogFile, "logFile for read")
	flag.Parse()

	return configFile,logFile
}


