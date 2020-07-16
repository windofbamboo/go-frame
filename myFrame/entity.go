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

var (
	InstanceTypeProvider string = "provider"
	InstanceTypeConsumer string = "consumer"
	ServicePath = "myService"
	lockPath = "lock"
	lockNode = "client"
	tickerTime = 30 * time.Second
)

