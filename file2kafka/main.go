package main

import (
	"file2kafka/entity"
	"flag"
	"fmt"
	"github.com/wonderivan/logger"
	"os"
	"path/filepath"
	"reflect"
)

var (
	myProcess = File2kafka{BaseProcess: BaseProcess{Name:"File2kafka"} }
	structMap = make(map[string] interface{})
	lineName string
)

const (
	DefaultConfigPath     string = "file2kafka"
	DefaultConfigFileName string = "config.yaml"
	LogConfigFileName     string = "log.json"
)

func main() {

	flag.Usage = func() {
		help()
	}

	registerStruct()

	lineName = initConfig()

	myProcess.execute()
}

func registerStruct(){

	register:=func(o interface{}){
		typ:=reflect.TypeOf(o)
		structMap[typ.Name()] = o
	}

	register(entity.SmsRecord{})
	register(entity.GsmRecord{})
	register(entity.GprsRecord{})

	typ:= reflect.TypeOf(myProcess)
	myProcess.Typ =  typ
}


func initConfig() string{

	var pipLine string

	flag.StringVar(&pipLine, "p", "", "pipLine for process")
	flag.Parse()

	exPath := os.Getenv("CONFIG_PATH")
	err := CheckPath(exPath)
	CheckErr(err)

	logFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	logFile = "D:\\backup\\study\\go\\file2kafka\\conf\\log.json"
	err = CheckFile(logFile)
	CheckErr(err)

	err = logger.SetLogger(logFile)
	CheckErr(err)

	configFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)
	configFile = "D:\\backup\\study\\go\\file2kafka\\conf\\config.yaml"
	err = CheckFile(configFile)
	CheckErr(err)

	err = ReadConfig(configFile)
	CheckErr(err)

	lineName = pipLine

	return pipLine
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   -p [pipLine] ")
	fmt.Println("example : ")
	fmt.Println("             file2kafka -p c1 ")
	fmt.Println("====================================================")
}

func CheckErr(err error) {
	if err != nil {
		logger.Error(fmt.Sprintf(" err : %s ", err.Error()))
		panic(err)
	}
}
