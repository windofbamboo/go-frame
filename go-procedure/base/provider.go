package base

import (
	"context"
	"flag"
	"fmt"
	"github.com/docker/libkv/store"
	myMetrics "github.com/rcrowley/go-metrics"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/serverplugin"
	"github.com/wonderivan/logger"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type MyProvider struct {
	instanceName string
	storeType 	string
	storeAddr   []string //store 地址
	addr     	*string  //store上的注册节点目录
	appPath 	string
}

func NewMyProvider(instanceName string) *MyProvider{

	m := &MyProvider{}

	m.instanceName = instanceName
	m.storeType = configContent.server.storeType
	m.storeAddr = configContent.server.storeAddr
	m.appPath = FrameName + "/" + AppName

	var addrStr string
	for _, s := range configContent.providers {
		if s.name == m.instanceName {
			addrStr = fmt.Sprintf("%s:%d", s.host, s.port)
			break
		}
	}
	m.addr = flag.String("addr", addrStr, "server address")

	return m
}


func (m *MyProvider) Start() {

	s := server.NewServer()

	//获取kill信号
	signals := make(chan os.Signal, 1)
	signal.Notify(signals,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt,
		os.Kill)
	go func() {
		for {
			select {
			case <-signals:
				if err := s.Close(); err != nil {
					logger.Error(fmt.Sprintf("close server err : %v", err))
					panic(err)
				} else {
					logger.Warn("close server ... ")
				}
				return
			}
		}
	}()

	m.addRegistryPlugin(s)

	meta := fmt.Sprintf("group=%v",DefaultServerGroup)
	if err := s.RegisterFunction(ServicePath, callProcedure, meta); err != nil {
		logger.Error(fmt.Sprintf("Register function singleDeal err : %v", err))
		panic(err)
	}

	s.AuthFunc = auth

	if err := s.Serve("tcp", *m.addr); err != nil {
		logger.Error(fmt.Sprintf("start server err : %v", err))
	}
}

func (m *MyProvider) addRegistryPlugin(s *server.Server) {

	switch m.storeType{

	case string(store.ZK):

		r := &serverplugin.ZooKeeperRegisterPlugin{
			ServiceAddress:   "tcp@" + *m.addr,
			ZooKeeperServers: m.storeAddr,
			BasePath:         m.appPath,
			Metrics:          myMetrics.NewRegistry(),
			UpdateInterval:   time.Minute,
		}
		err := r.Start()
		if err != nil {
			logger.Error(err)
			panic(err)
		}
		s.Plugins.Add(r)

	case string(store.ETCD):

		r := &serverplugin.EtcdV3RegisterPlugin{
			ServiceAddress: "tcp@" + *m.addr,
			EtcdServers:    m.storeAddr,
			BasePath:       m.appPath,
			Metrics:        myMetrics.NewRegistry(),
			UpdateInterval: time.Minute,
		}
		err := r.Start()
		if err != nil {
			logger.Error(err)
			panic(err)
		}
		s.Plugins.Add(r)

	case string(store.CONSUL):

		r := &serverplugin.ConsulRegisterPlugin{
			ServiceAddress: "tcp@" + *m.addr,
			ConsulServers:  m.storeAddr,
			BasePath:       m.appPath,
			Metrics:        myMetrics.NewRegistry(),
			UpdateInterval: time.Minute,
		}
		err := r.Start()
		if err != nil {
			logger.Error(err)
			panic(err)
		}
		s.Plugins.Add(r)

	default:
		panic(fmt.Errorf("unknown storeType"))
	}

}

func auth(ctx context.Context, req *protocol.Message, token string) error {
	if token == DefaultToken {
		return nil
	}
	return fmt.Errorf ("invalid token")
}

func callProcedure(ctx context.Context, inPut *Message, outPut *Message) error {

	varMap,err:=ParseInParam(inPut.InPutParam.ProcedureName,inPut.InPutParam.InParamStr)
	if err!=nil{
		inPut.ResultCode = ResultCodeErr
		inPut.ResultInfo = "fail"
		return err
	}

	err = ExecProcedure(inPut.InPutParam.EnvId,inPut.InPutParam.ProcedureName,&varMap)
	if err!=nil{
		inPut.ResultCode = ResultCodeErr
		inPut.ResultInfo = "fail"
		return err
	}

	procedure,_:= configContent.Procedures[inPut.InPutParam.ProcedureName]
	var i = 0
	for _, param := range procedure.ProcedureHead.OutParam {
		if value,ok := varMap[param.ParamName];ok{
			if i>0{
				inPut.OutParamStr +=","
			}
			inPut.OutParamStr += fmt.Sprintf("%v=%v",param.ParamName,value)
			i++
		}else{
			info:=fmt.Sprintf("call %v outParam [%v] is not be assigned a value",inPut.InPutParam.ProcedureName,param.ParamName)
			logger.Error(info)
		}
	}

	inPut.ResultCode = ResultCodeOk
	inPut.ResultInfo = "success"

	*outPut = *inPut
	return nil
}

