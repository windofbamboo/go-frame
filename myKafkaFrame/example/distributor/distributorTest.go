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
	fmt.Println("command :   -f [configFile] ")
	fmt.Println("example : ")
	fmt.Println("             distributor -f distributor.json ")
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


