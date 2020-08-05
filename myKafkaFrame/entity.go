package myKafkaFrame

import "time"

type NameSlice []string

func (s NameSlice)Len() int {
	return len(s)
}
func (s NameSlice)Swap(i, j int){
	s[i], s[j] = s[j], s[i]
}
func (s NameSlice)Less(i, j int) bool{
	return s[i] < s[j]
}

type AllotMsg struct{
	Topic          string
	Partition      int32
}

type AllotMsgSlice []AllotMsg

func (s AllotMsgSlice)Len() int {
	return len(s)
}
func (s AllotMsgSlice)Swap(i, j int){
	s[i], s[j] = s[j], s[i]
}
func (s AllotMsgSlice)Less(i, j int) bool{
	if s[i].Topic != s[j].Topic {
		return s[i].Topic < s[j].Topic
	}
	return s[i].Partition < s[j].Partition
}

type OffsetMsg struct {
	AllotMsg
	OffsetCommit   int64
	OffsetNewest   int64
}

type OffsetMsgSlice []OffsetMsg

func (s OffsetMsgSlice)Len() int {
	return len(s)
}
func (s OffsetMsgSlice)Swap(i, j int){
	s[i], s[j] = s[j], s[i]
}
func (s OffsetMsgSlice)Less(i, j int) bool{
	return s[i].OffsetNewest - s[i].OffsetCommit < s[j].OffsetNewest - s[j].OffsetCommit
}

//节点注册信息
type RegistryValue struct {
	InstanceName string
	Host string
	RegistryTime string
}

type ControlSign struct{
	quit chan struct{}
}
func (o *ControlSign)init(){
	o.quit = make(chan struct{})
}

var (
	InstanceTypeDistributor string = "distributor"
	InstanceTypeWorker string = "worker"
	tickerTime = 30 * time.Second

	zkBaseDirectory = "myKafkaFrame"
	registryDirectory = "registry"
	distributorDirectory = "distributor"
	workerDirectory = "worker"
	queueDirectory = "kafka"
	offsetDirectory = "offset"

	zkPathSplit = "/"

)

