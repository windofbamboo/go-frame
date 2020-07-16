package main

import (
	"flag"
	"fmt"
	"github.com/vmihailenco/msgpack"
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
	p:= myFrame.MyProvider{}
	p.InitParam(instanceName,configFile,logFile)
	p.RegistryDealFunc(deal)
	p.Start()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   -i [instanceName] -f [configFile] ")
	fmt.Println("example : ")
	fmt.Println("             client -i c1 ")
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

func deal(in *[]byte, out *[]byte) error{

	var rectangle Rectangle
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

func getSquare(rectangle *Rectangle) Square{
	return Square{rectangle.Height * rectangle.Length}
}



