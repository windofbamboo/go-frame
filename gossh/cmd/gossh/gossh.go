package main

import (
	"errors"
	"flag"
	"fmt"
	"gossh/config"
	"gossh/help"
	"gossh/logs"
	"gossh/machine"
	"gossh/run"
	"gossh/tools"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	AppVersion = "gossh 0.7"
	logFile   = "gossh.log"
	cons      = 30
	pTimeOut  = 10
)

type InParam struct{

	pVersion bool
	completion bool

	runType string
	groupName string
	nodeName string
	password string
	logLevel string

	cmd string
	force,pSafe bool

	sourceFile string
	targetPath string

	newPSW string
}

var (
	defaultIpFile,defaultCmdFile,defaultLogPath string

	pVersion = flag.Bool("version", false, "gossh version")
	psw = flag.String("p", "", "ssh password")
	pLogLevel = flag.String("l", "info", "log level (debug|info|warn|error")
	groupName = flag.String("g", "", "group is the list of machine ")
	nodeName  = flag.String("node", "", "alias name of a machine ")
	pRunType  = flag.String("t", "cmd", "running mode: cmd|push|pull")
	force = flag.Bool("f", false, "force to run even if it is not safe")
	pSafe = flag.Bool("s", false, "if -s is setting, gossh will exit when error occurs")
	cmd   = flag.String("exec", "", "force to run even if it is not safe")
	cmdFile = flag.String("cmdFile", defaultCmdFile, "force to run even if it is not safe")

	log       = logs.NewLogger()
)

func main() {

	usage := func() {
		fmt.Println(help.Help)
	}
	flag.Usage = func() {
		usage()
	}
	setDefaultValue()

	var inParam InParam
	var err error
	inParam,err =initParam()
	if err!=nil{
		usage()
		panic(err)
	}

	if inParam.pVersion{
		fmt.Println(AppVersion)
	}

	var groupMap map[string][]config.LogicUser
	groupMap,err=config.ReadConfig(defaultIpFile)
	if err!=nil{
		panic(err)
	}

	nodeList,ok:=groupMap[inParam.groupName]
	if !ok{
		panic(errors.New("group is not exist in ip.json"))
	}

	var singleNode config.LogicUser
	if inParam.nodeName!=""{
		var exist = false
		for _, node := range nodeList {
			if node.AliasName == inParam.nodeName{
				singleNode = node
				exist = true
				break
			}
		}
		if !exist{
			panic(errors.New("node is not exist in group "))
		}
	}

	if inParam.password !=""{
		if inParam.nodeName!=""{
			singleNode.Password = inParam.password
		}else{
			for _, user := range nodeList {
				user.Password = inParam.password
			}
		}
	}

	err=initLog(inParam.logLevel)
	if err!=nil{
		panic(err)
	}

	//异步日志，需要最后刷新和关闭
	defer func() {
		log.Flush()
		log.Close()
	}()

	log.Debug("parse flag ok , init log setting ok.")

	switch inParam.runType {

	case "cmd":

		if inParam.nodeName !="" {
			log.Info("[servers]=%s", singleNode.Host)
			run.SingleRun(&singleNode, inParam.cmd, inParam.force, pTimeOut)

		} else {
			cr := make(chan machine.Result)
			cCons := make(chan struct{}, cons)
			wg := &sync.WaitGroup{}
			run.ServersRun(nodeList,inParam.cmd, wg, cr, cCons, inParam.pSafe, inParam.force, pTimeOut)
			wg.Wait()
		}
	case "psw":
		if inParam.nodeName !="" {

			log.Info("[servers]=%s", singleNode.Host)
			run.SinglePsw(&singleNode,inParam.newPSW,inParam.force, pTimeOut)

		} else {
			cr := make(chan machine.Result)
			cCons := make(chan struct{}, cons)
			wg := &sync.WaitGroup{}
			run.ServersPsw(nodeList,wg, cr, cCons, inParam.newPSW, inParam.pSafe, inParam.force, pTimeOut)
			wg.Wait()
		}
	//push file or dir  to remote server
	case "scp", "push":

		if inParam.nodeName !=""{
			run.SinglePush(inParam.sourceFile, inParam.targetPath,&singleNode, inParam.force, pTimeOut)
		}else{
			cr := make(chan machine.Result, 20)
			cCons := make(chan struct{}, cons)
			wg := &sync.WaitGroup{}
			run.ServersPush(inParam.sourceFile, inParam.targetPath, nodeList, wg, cCons, cr, inParam.force,pTimeOut)
			wg.Wait()
		}
	//pull file from remote server
	case "pull":

		if inParam.nodeName !=""{
			run.SinglePull(inParam.sourceFile, inParam.targetPath, &singleNode, inParam.force)
		}else{
			run.ServersPull(inParam.sourceFile, inParam.targetPath, nodeList, inParam.force)
		}

	}
}

func initParam() (InParam,error){

	flag.Parse()

	var inParam  InParam
	if *pVersion {
		inParam.pVersion = *pVersion
		inParam.completion = true
		return inParam,nil
	}

	inParam.password = *psw
	inParam.logLevel = *pLogLevel

	if *groupName ==""{
		inParam.completion = false
		return inParam,errors.New("group is not set ")
	}
	inParam.groupName = *groupName
	inParam.nodeName = *nodeName
	inParam.runType = *pRunType

	switch *pRunType {
	case "cmd":
		inParam.force = *force
		inParam.pSafe = *pSafe

		if *cmd =="" && *cmdFile ==""{
			inParam.completion = false
			return inParam,errors.New("in cmd type, not found exec or cmdFile ")
		}

		if *cmd !=""{
			inParam.cmd = *cmd
			if ok := tools.CheckSafe(inParam.cmd); !ok && inParam.force == false {
				fmt.Printf("Dangerous command in %v", inParam.cmd)
				fmt.Printf("You can use the `-f` option to force to excute")
				return inParam,errors.New("Dangerous command in : [ "+inParam.cmd +" ]")
			}
		}else {
			cmd,err:= tools.ReadCmdFile(*cmdFile,inParam.force)
			if err!=nil{
				inParam.completion = false
				return inParam,err
			}
			inParam.cmd = cmd
		}
		inParam.completion = true

	case "psw":
		if flag.NArg() != 1 {
			inParam.completion = false
			return inParam,nil
		}
		newPSW :=flag.Arg(0)
		inParam.newPSW = newPSW
		inParam.completion = true

	case "scp", "push","pull":
		if flag.NArg() != 2 {
			inParam.completion = false
			return inParam,nil
		}

		src := flag.Arg(0)
		dst := flag.Arg(1)

		inParam.sourceFile = src
		inParam.targetPath = dst
		inParam.completion = true

	default:
		inParam.completion = false
		return inParam,errors.New("unKnown runType")
	}

	return inParam,nil
}


//setting log
func initLog(logLevel string) error {

	switch logLevel {
	case "debug":
		log.SetLevel(logs.LevelDebug)
	case "error":
		log.SetLevel(logs.LevelError)
	case "info":
		log.SetLevel(logs.LevelInformational)
	case "warn":
		log.SetLevel(logs.LevelWarning)
	default:
		log.SetLevel(logs.LevelInformational)
	}

	logPath := defaultLogPath
	err := tools.MakePath(logPath)
	if err != nil {
		return err
	}

	logName := filepath.Join(logPath, logFile)
	logStr := `{"filename":"` + logName + `"}`

	//此处主要是处理windows下文件路径问题,不做转义，日志模块会报如下错误
	logStr = strings.Replace(logStr, `\`, `\\`, -1)

	err = log.SetLogger("file", logStr)
	if err != nil {
		return err
	}
	//开启日志异步提升性能
	log.Async()
	return nil
}


func setDefaultValue() {
	configPath := os.Getenv("CONFIG_PATH")
	logPath := os.Getenv("LOG_PATH")
	dir, _ := os.Executable()
	localPath := filepath.Dir(dir)

	if configPath !=""{
		defaultIpFile = filepath.Join(configPath, "gossh", "ip.json")
		defaultCmdFile = filepath.Join(configPath, "gossh", "cmd.txt")
	}else{
		defaultIpFile = filepath.Join(localPath,"ip.txt")
		defaultCmdFile = filepath.Join(localPath,"cmd.txt")
	}

	if logPath !=""{
		defaultLogPath = filepath.Join(logPath, "gossh")
	}else{
		defaultLogPath = filepath.Join(localPath,"gossh")
	}

}



