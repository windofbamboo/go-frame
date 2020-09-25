package base

import (
	"fmt"
	"time"
)

const (
	//配置文件相关
	DefaultConfigPath		string = "go-procedure"
	ProcedureConfigPath		string = "procedures"
	DefaultConfigFileName	string = "config.xml"
	LogConfigFileName		string = "log.json"

	SeparatorCharacter 	string = ","
 	NullStr 			string = ""
	//实例类型
	InstanceTypeProvider string = "provider"
	InstanceTypeConsumer string = "consumer"
 	// store 类型
	StoreTypeEtcd	string = "etcd"
	StoreTypeConsul	string = "consul"
	StoreTypeZk	string = "zk"
	// store 目录相关
	basePath = "go-procedure" // server 注册根目录
	ServicePath = "service"   // 在basePath下,注册服务的子目录

	lockPath = "lock"		// consumer 获取权限时，lock根目录
	lockNode = "client"		// consumer 获取权限时，lock名称

	ProviderInstanceName string = "myProvider"
	ConsumerInstanceName string = "myConsumer"
	DefaultServerGroup string = "myGroup"
	DefaultToken string = "bearer tGzv3JOkF0XG5Qx2TlKWIA"

	// 数据库驱动类型
	DriverTypePostgres	string = "postgres"
	DriverTypeOracle	string = "oci8"
	DriverTypeMysql	string = "mysql"

	//数据类型
	ParamTypeInt	string = "int"
	ParamTypeString	string = "string"

	//执行结果
	ResultCodeOk	int = 0
	ResultCodeErr	int = -1

	tickerTime = 30 * time.Second
)

type CallParam struct{
	EnvId,ProcedureName,InParamStr string
}

func (m *CallParam)Info() string{
	return fmt.Sprintf("in EnvId[%v], call procedure %v(%v)",m.EnvId,m.ProcedureName,m.InParamStr)
}

type Message struct {
	InPutParam CallParam
	OutParamStr string
	ResultCode	int
	ResultInfo	string
	TimeOut int
}

type DealFunc func(msg *Message) error
