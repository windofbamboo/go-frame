package myFrame

import (
	"github.com/wonderivan/logger"
	"os"
	"path/filepath"
)

func ProviderInit(appName,instanceName,logFile,configFile string){

	exPath := os.Getenv(EnvConfigPath)
	localLogFile := filepath.Join(exPath, appName, logFile)
	err := CheckFile(localLogFile)
	if err != nil {
		panic(err)
	}

	contentStr, err := ReSetLogFileName(localLogFile, instanceName)
	if err != nil {
		panic(err)
	}

	err = logger.SetLogger(contentStr)
	if err != nil {
		panic(err)
	}

	localConfigFile := filepath.Join(exPath, appName, configFile)

	err = CheckFile(localConfigFile)
	CheckErr(err)

	err = ReadConfig(localConfigFile)
	CheckErr(err)

	err = CheckInstance(InstanceTypeProvider, instanceName)
	CheckErr(err)
}

func ConsumerInit(appName,instanceName,logFile,configFile string){

	exPath := os.Getenv(EnvConfigPath)
	localLogFile := filepath.Join(exPath, appName, logFile)
	err := CheckFile(localLogFile)
	if err != nil {
		panic(err)
	}

	contentStr, err := ReSetLogFileName(localLogFile, instanceName)
	if err != nil {
		panic(err)
	}

	err = logger.SetLogger(contentStr)
	if err != nil {
		panic(err)
	}

	localConfigFile := filepath.Join(exPath, appName, configFile)

	err = CheckFile(localConfigFile)
	CheckErr(err)

	err = ReadConfig(localConfigFile)
	CheckErr(err)

	err = CheckInstance(InstanceTypeConsumer, instanceName)
	CheckErr(err)
}