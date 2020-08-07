package myKafkaFrame

import (
	"github.com/Shopify/sarama"
	"sort"
	"sync"
	"time"
)


type DealFunc func(in *sarama.ConsumerMessage) error

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
func (s *NameSlice)equal(o *NameSlice) bool{
	if s.Len() != o.Len(){
		return false
	}
	sort.Sort(s)
	sort.Sort(o)
	for i:=0;i<s.Len();i++{
		if (*s)[i] != (*o)[i]{
			return false
		}
	}
	return true
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
func (c *AllotMsgSlice)equal(o *AllotMsgSlice) bool{
	if c.Len() != o.Len(){
		return false
	}
	sort.Sort(c)
	sort.Sort(o)
	for i:=0;i<c.Len();i++{
		if (*c)[i].Topic != (*o)[i].Topic{
			return false
		}
		if (*c)[i].Partition != (*o)[i].Partition{
			return false
		}
	}
	return true
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

const (
	InstanceTypeDistributor string = "distributor"
	InstanceTypeWorker string = "worker"
	tickerTime = 30 * time.Second

	zkBaseDirectory = "myKafkaFrame"
	registryDirectory = "registry"
	distributorDirectory = "distributor"
	workerDirectory = "worker"
	lockDirectory = "lock"
	queueDirectory = "kafka"
	offsetDirectory = "offset"

	zkPathSplit = "/"
)

type ExecuteStatus int8
const (
	routineInit 	ExecuteStatus = -1
	routineStart 	ExecuteStatus = 0
	routineStop 	ExecuteStatus = 1

	workStatusInit		ExecuteStatus = -1
	workStatusGetTask	ExecuteStatus = 0
	workStatusStartTask	ExecuteStatus = 1
	workStatusLostWatch	ExecuteStatus = 2

	distributorInit			ExecuteStatus = -1
	distributorLockNode		ExecuteStatus = 1
	distributorFirstAllot	ExecuteStatus = 2
	distributorWatchOffset	ExecuteStatus = 3
	distributorCheckOffset	ExecuteStatus = 4
	distributorWatchWorker	ExecuteStatus = 5
)

type ControlSign struct{
	quit chan struct{}
	status ExecuteStatus
	rwLock sync.RWMutex
}
func (o *ControlSign)init(){
	o.quit = make(chan struct{},1)
	o.status = routineInit
}
func (o *ControlSign)updateStart(){
	o.rwLock.Lock()
	defer o.rwLock.Unlock()
	o.status = routineStart
}
func (o *ControlSign)updateStop(){
	o.rwLock.Lock()
	defer o.rwLock.Unlock()
	o.status = routineStop
}
func (o *ControlSign)getStatus() ExecuteStatus{
	o.rwLock.RLock()
	defer o.rwLock.RUnlock()
	return o.status
}
func (o *ControlSign)waitIdle() {
	for{
		if o.getStatus() == routineStop{
			break
		}
		time.Sleep(time.Millisecond)
	}
}
func (o *ControlSign)setStopSign() {
	o.rwLock.Lock()
	defer o.rwLock.Unlock()
	o.quit <- struct{}{}
}
func (o *ControlSign)start2Stop() {
	o.rwLock.Lock()
	defer o.rwLock.Unlock()
	if o.status == routineStart{
		o.quit <- struct{}{}
	}
}

func (o *ControlSign)signalMultiplication(size int) []*ControlSign{

	var signList []*ControlSign // 传递信号
	for i:=0;i<size;i++{
		var w ControlSign
		w.init()
		signList = append(signList,&w)
	}

	go func(){
		o.updateStart()
		for {
			select {
				case <-o.quit:
					for _, sign := range signList {
						sign.quit <- struct{}{}
					}
					o.updateStop()
					return
			}
		}
	}()

	return signList
}

type ConStatus struct{
	status ExecuteStatus
	rwLock sync.RWMutex
}
func (o *ConStatus)updateStatus(value ExecuteStatus){
	o.rwLock.Lock()
	defer o.rwLock.Unlock()
	o.status = value
}
func (o *ConStatus)getStatus() ExecuteStatus{
	o.rwLock.RLock()
	defer o.rwLock.RUnlock()
	return o.status
}