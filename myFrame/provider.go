package myFrame

import (
	"context"
	"flag"
	"fmt"
	myMetrics "github.com/rcrowley/go-metrics"
	"github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/serverplugin"
	"github.com/wonderivan/logger"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type MyProvider struct {
	addr     *string
	zkAddr   []string
	basePath string
}

type DealFunc func(in *[]byte, out *[]byte) error

var dealFunc DealFunc

func (m *MyProvider) InitParam(instanceName string, configFile string, logFile string) {

	err := CheckFile(logFile)
	CheckErr(err)

	contentStr, err := ReSetLogFileName(logFile, instanceName)
	CheckErr(err)

	err = logger.SetLogger(contentStr)
	CheckErr(err)

	err = CheckFile(configFile)
	CheckErr(err)

	err = ReadConfig(configFile)
	CheckErr(err)

	err = CheckInstance(InstanceTypeProvider, instanceName)
	CheckErr(err)

	m.zkAddr = configContent.zk.zkAddr
	m.basePath = configContent.zk.basePath

	var addrStr string
	for _, s := range configContent.provider {
		if s.name == instanceName {
			addrStr = fmt.Sprintf("%s:%d", s.host, s.port)
			break
		}
	}
	m.addr = flag.String("addr", addrStr, "server address")
}

func (p *MyProvider) RegistryDealFunc(fn DealFunc) {
	dealFunc = fn
}

func (p *MyProvider) Start() {

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

	p.addRegistryPlugin(s)
	if err := s.RegisterFunction(ServicePath, singleDeal, ""); err != nil {
		logger.Error(fmt.Sprintf("Register function singleDeal err : %v", err))
		panic(err)
	}
	if err := s.RegisterFunction(ServicePath, batchDeal, ""); err != nil {
		logger.Error(fmt.Sprintf("Register function batchDeal err : %v", err))
		panic(err)
	}

	//metricsPlugin :=serverplugin.NewMetricsPlugin(myMetrics.DefaultRegistry)
	//s.Plugins.Add(metricsPlugin)
	//p.startMetrics()

	if err := s.Serve("tcp", *p.addr); err != nil {
		logger.Error(fmt.Sprintf("start server err : %v", err))
	}
}

//func (p *MyProvider)startMetrics() {
//
//	myMetrics.RegisterRuntimeMemStats(myMetrics.DefaultRegistry)
//	go myMetrics.CaptureRuntimeMemStats(myMetrics.DefaultRegistry, time.Second)
//
//	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:2003")
//	go myGraphite.Graphite(myMetrics.DefaultRegistry, 1e9, "rpcx.services.host.127_0_0_1", addr)
//}

func (p *MyProvider) addRegistryPlugin(s *server.Server) {

	r := &serverplugin.ZooKeeperRegisterPlugin{
		ServiceAddress:   "tcp@" + *p.addr,
		ZooKeeperServers: p.zkAddr,
		BasePath:         p.basePath,
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
