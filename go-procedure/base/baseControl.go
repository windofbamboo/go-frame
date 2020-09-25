package base

import (
	"fmt"
	"github.com/wonderivan/logger"
	"os"
	"path/filepath"
)

func CommandInit(environmentId,procedureName,paramStr string) (varMap map[string]interface{}) {

	configPath:= os.Getenv("CONFIG_PATH")
	if configPath == NullStr {
		panic(fmt.Errorf("CONFIG_PATH is not defind in environment variable "))
	}

	logFile := filepath.Join(configPath, DefaultConfigPath, LogConfigFileName)
	err:= CheckFile(logFile)
	if err!=nil{
		panic(err)
	}

	err = logger.SetLogger(logFile)
	if err!=nil{
		panic(fmt.Errorf("SetLogger err : %v",err))
	}

	err = ReadConfigXml(configPath)
	CheckErr(err)

	if _,ok:= configContent.DbInfos[environmentId];!ok{
		panic(fmt.Errorf("environmentId : %v  is not exists in config.xml ",environmentId))
	}

	varMap,err = ParseInParam(procedureName,paramStr)
	if err!=nil{
		panic(err)
	}
	return varMap
}

func ProviderInit(instanceName string){

	configPath:= os.Getenv("CONFIG_PATH")
	if configPath == NullStr {
		panic(fmt.Errorf("CONFIG_PATH is not defind in environment variable "))
	}

	logFile := filepath.Join(configPath, DefaultConfigPath,LogConfigFileName)
	err := CheckFile(logFile)
	if err!=nil{
		panic(err)
	}
	contentStr, err := ReSetLogFileName(logFile, ProviderInstanceName+"-"+instanceName)
	if err!=nil{
		panic(err)
	}

	err = logger.SetLogger(contentStr)
	CheckErr(err)

	err = ReadConfigXml(configPath)
	CheckErr(err)

	err= CheckInstance(InstanceTypeProvider,instanceName)
	CheckErr(err)
}

func ConsumerInit(){

	configPath:= os.Getenv("CONFIG_PATH")
	if configPath == NullStr {
		panic(fmt.Errorf("CONFIG_PATH is not defind in environment variable "))
	}

	logFile := filepath.Join(configPath, DefaultConfigPath,LogConfigFileName)
	err := CheckFile(logFile)
	if err!=nil{
		panic(err)
	}
	contentStr, err := ReSetLogFileName(logFile, ConsumerInstanceName)
	if err!=nil{
		panic(err)
	}
	err = logger.SetLogger(contentStr)
	CheckErr(err)

	err = ReadConfigXml(configPath)
	CheckErr(err)
}

func ExecProcedure(environmentId,procedureName string,varMap *map[string]interface{}) error{

	execInfo:=fmt.Sprintf("exec procedure %v(",procedureName)
	var i=0
	for paramName, paramValue := range *varMap {
		if i==0{
			execInfo +=fmt.Sprintf("%v:%v",paramName,paramValue)
		}else{
			execInfo +=fmt.Sprintf(",%v:%v",paramName,paramValue)
		}
		i++
	}
	execInfo +=fmt.Sprintf(")")
	logger.Trace(execInfo+" begin ... ")

	procedure,_:= configContent.Procedures[procedureName]

	instance:=Procedure{ProcedureHead:procedure.ProcedureHead,ProcedureBody:procedure.ProcedureBody}

	err:= instance.setExecEnvironment(environmentId,*varMap)
	CheckErr(err)

	err = instance.execProcedure()
	if err!=nil{
		return err
	}
	logger.Trace(execInfo+" success ... ")

	*varMap = instance.varMap

	instance.initEnvironment()
	return nil
}

