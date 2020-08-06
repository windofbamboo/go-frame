package myKafkaFrame

import (
	"errors"
	"fmt"
	"github.com/docker/libkv/store"
	"github.com/wonderivan/logger"
	"sort"
	"strings"
	"time"
)

// 简单分配, 按照 partition 数量，平均分配给 worker
func simpleAllot ( workers *NameSlice, topics *map[string] []int32) (map[string] AllotMsgSlice,error){

	sort.Sort(workers)
	var total AllotMsgSlice
	for topic, partitions := range *topics {
		for _, partition := range partitions {
			total = append(total,AllotMsg{Topic:topic,Partition:partition})
		}
	}

	var res =make(map[string]AllotMsgSlice)
	workerNum :=len(*workers)
	for i, msg := range total {
		j:=i%workerNum
		name:= (*workers)[j]

		if _,ok:= res[name];ok{
			res[name] = append(res[name],msg)
		}else{
			res[name] = AllotMsgSlice{msg}
		}
	}
	return res,nil
}

func reAllotByOffset(offsetSlice *OffsetMsgSlice, thresholdValue int64,
					lastAllotMsgS *map[string]AllotMsgSlice) (bool,map[string] AllotMsgSlice,error){

	// 如果 实例数量 =  分区数量 ,则无需重新分配
	if len(*lastAllotMsgS) == offsetSlice.Len(){
		return false,*lastAllotMsgS,nil
	}

	// 寻找存在积压的分区
	var overMap1 = make(map[AllotMsg]bool)
	var overPartitions OffsetMsgSlice
	for _, msg := range *offsetSlice {
		key:= AllotMsg{Topic:msg.Topic,Partition:msg.Partition}
		if msg.OffsetNewest - msg.OffsetCommit > thresholdValue{
			overMap1[key] = true
			overPartitions = append(overPartitions,msg)
		}else{
			overMap1[key] = false
		}
	}
	// 无积压， 无需重新分配
	if len(overPartitions) == 0{
		return false,*lastAllotMsgS,nil
	}
	// 重新分配
	// 按偏移量的差值，进行排序分配; 从大到小
	// 0 1 2
	// 5 4 3
	var workers NameSlice
	for s := range *lastAllotMsgS {
		workers = append(workers,s)
	}
	sort.Sort(workers)
	sort.Sort(offsetSlice)

	thisAllotMsgS := make(map[string]AllotMsgSlice)
	workerNum :=len(workers)
	for i, msg := range *offsetSlice {
		j:= i%(workerNum*2)
		k:= i%workerNum
		if k<j{
			k=2*workerNum-j-1
		}
		name:=workers[k]

		if msgs,ok:= thisAllotMsgS[name];ok{
			msgs = append(msgs,AllotMsg{Topic:msg.Topic,Partition:msg.Partition})
		}else{
			thisAllotMsgS[name] = AllotMsgSlice{AllotMsg{Topic:msg.Topic,Partition:msg.Partition}}
		}
	}

	return true,thisAllotMsgS,nil
}


func createNode(myStore *store.Store,node string) error{

	if ok,err:=(*myStore).Exists(node);err!=nil{
		return err
	}else{
		if ok{
			return nil
		}
	}
	if err:=(*myStore).Put(node,[]byte{1},nil);err!=nil{
		return err
	}else {
		return nil
	}
}

func getQueueLock(myStore *store.Store,lockPath string, lockName string,controlSign *ControlSign) (string,bool,error){

	if err:=createNode(myStore,lockPath);err!=nil{
		return "",false,errors.New("create lockPath err : "+ err.Error())
	}
	mySequence:=GetCurrentTime()
	tempPath:=lockPath+zkPathSplit+lockName+mySequence

	if err:=(*myStore).Put(tempPath, []byte(lockName), &store.WriteOptions{TTL: 2*tickerTime});err!=nil{
		return "",false,errors.New("create lockNode err : "+ err.Error())
	}

	stopWatchCh := make(chan struct{})
	stopCheckCh := make(chan struct{})

	ticker := time.NewTicker(tickerTime)
	controlSign.updateStart()
	go func() {
		for {
			select {
				case <-ticker.C:
					err:=(*myStore).Put(tempPath, []byte(lockName), &store.WriteOptions{TTL: 2*tickerTime})
					if err != nil {
						logger.Warn(fmt.Printf("set node value err: %v",err))
					}
				case <-controlSign.quit:
					stopWatchCh <- struct{}{}
					stopCheckCh <- struct{}{}
					controlSign.updateStop()
					return
			}
		}
	}()

	kvCh, err := (*myStore).WatchTree(lockPath,stopWatchCh)
	if err != nil {
		return lockName+mySequence,false,errors.New("WatchTree lockPath err : "+ err.Error())
	}

	for {
		select {
			case child := <-kvCh:
				var minSequence string
				for _, pair := range child {
					sequenceName := strings.ReplaceAll(pair.Key,lockName,"")
					if minSequence > sequenceName || minSequence == ""{
						minSequence = sequenceName
					}
				}
				if mySequence == minSequence{
					controlSign.updateStop()
					return lockName+mySequence,true,nil
				}
			case <-stopCheckCh:
				controlSign.updateStop()
				return lockName+mySequence,false,nil
		}
	}
}


func writeTempNodeValue(myStore *store.Store,node string,value []byte,controlSign *ControlSign) {

	if err:=(*myStore).Put(node, value, &store.WriteOptions{TTL: 2*tickerTime});err!=nil{
		CheckErr(err)
	}
	ticker := time.NewTicker(tickerTime)
	controlSign.updateStart()
	go func() {
		for {
			select {
				case <-ticker.C:
					err:=(*myStore).Put(node, value, &store.WriteOptions{TTL: 2*tickerTime})
					if err != nil {
						logger.Error(fmt.Sprintf("set node value err: %v",err))
					}
				case <-controlSign.quit:
					controlSign.updateStop()
					logger.Warn("controlSign.updateStop() ... ")
					return
			}
		}
	}()

}



