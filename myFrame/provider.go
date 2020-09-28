package myFrame

import (
	"context"
	"flag"
	"fmt"
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
	addr     *string
	zkAddr   []string
	appPath  string
}

func NewMyProvider(appName,instanceName string) *MyProvider {

	m:= &MyProvider{}
	m.instanceName = instanceName
	m.zkAddr = configContent.zk.zkAddr
	m.appPath = FrameName + "/" + appName

	var addrStr string
	for _, s := range configContent.provider {
		if s.name == instanceName {
			addrStr = fmt.Sprintf("%s:%d", s.host, s.port)
			break
		}
	}
	m.addr = flag.String("addr", addrStr, "server address")

	return m
}

type DealFunc func(in *[]byte, out *[]byte) error
var dealFunc DealFunc

func (m *MyProvider) RegistryDealFunc(fn DealFunc) {
	dealFunc = fn
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
	if err := s.RegisterFunction(ServicePath, singleDeal, meta); err != nil {
		logger.Error(fmt.Sprintf("Register function singleDeal err : %v", err))
		panic(err)
	}
	if err := s.RegisterFunction(ServicePath, batchDeal, meta); err != nil {
		logger.Error(fmt.Sprintf("Register function batchDeal err : %v", err))
		panic(err)
	}

	s.AuthFunc = auth

	if err := s.Serve("tcp", *m.addr); err != nil {
		logger.Error(fmt.Sprintf("start server err : %v", err))
		panic(err)
	}
}

func (m *MyProvider) addRegistryPlugin(s *server.Server) {

	r := &serverplugin.ZooKeeperRegisterPlugin{
		ServiceAddress:   "tcp@" + *m.addr,
		ZooKeeperServers: m.zkAddr,
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
}

func singleDeal(ctx context.Context, inPut *Message, outPut *Message) error {

	data := inPut.Value
	var res []byte
	if err := dealFunc(&data, &res); err != nil {
		logger.Warn(fmt.Sprintf("topic: %s ,partition: %d ,offset: %d ,key: %s ,value: %s ,deal err: %v ",
			inPut.Topic, inPut.Partition, inPut.Offset, inPut.Key, inPut.Value, err))
		inPut.DealTag = false
		return err
	} else {
		inPut.DealTag = true
		inPut.Result = res
	}
	outPut = inPut
	return nil
}

func batchDeal(ctx context.Context, inPut *[]Message, outPut *[]Message) error {

	for i := range *inPut {
		msg := (*inPut)[i]
		data := msg.Value
		var res []byte
		if err := dealFunc(&data, &res); err != nil {
			logger.Warn(fmt.Sprintf("topic: %s ,partition: %d ,offset: %d ,key: %s ,value: %s ,deal err: %v ",
				msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value, err))
			msg.DealTag = false
		} else {
			msg.DealTag = true
			msg.Result = res
		}
		*outPut = append(*outPut, msg)
	}
	return nil
}

func auth(ctx context.Context, req *protocol.Message, token string) error {

	if token == DefaultToken {
		return nil
	}
	return fmt.Errorf ("invalid token")
}