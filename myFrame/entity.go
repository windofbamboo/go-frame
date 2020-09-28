package myFrame

import "time"

type Message struct {
	Key, Value     []byte
	Topic          string
	Partition      int32
	Offset         int64
	DealTag			bool
	Result			[]byte
}

const (
	InstanceTypeProvider string = "provider"
	InstanceTypeConsumer string = "consumer"

	FrameName = "myFrame"  // 框架名
	ServicePath = "service"

	lockPath = "lock"
	tickerTime = 30 * time.Second


	DefaultServerGroup string = "myGroup"
	DefaultToken string = "tGzv3JOkF0XG5Qx2TlKWIA"
	EnvConfigPath string ="CONFIG_PATH"
)

