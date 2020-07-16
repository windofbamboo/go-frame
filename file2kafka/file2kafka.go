package main

import (
	"errors"
	"file2kafka/entity"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	"github.com/wonderivan/logger"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type File2kafka struct {
	BaseProcess
}


func (*File2kafka)LoopProcess() {

	pipLine:= configContent.Lines[lineName]

	inPath:= pipLine.In.SpoolDir
	rule:= pipLine.In.FileName
	recordType := pipLine.In.RecordType
	columns := pipLine.In.columns
	columnName :=strings.Split(columns,",")

	skipPath:=pipLine.Out.skipOut.Path
	failPath:=pipLine.Out.failOut.Path
	bootstrap:=pipLine.Out.kafka.bootstrap
	servers:=strings.Split(bootstrap,",")

	parseTail:="parse"
	sendTail:="send"
/**
 read
   |______lineQueue
             |
           parse_________ parseFailQueue  ->write
                   |______sendQueue
                            |
                            send _________ sendFailQueue ->write
 */
	fileQueue := make(chan string,30000)
	lineQueue := make(chan string,30000)
	parseFailQueue := make(chan string,30000)
	sendQueue := make(chan []byte,30000)
	sendFailQueue := make(chan string,30000)

	producer, err :=getProducer(&servers)
	if err!=nil{
		panic(err)
	}
	defer producer.AsyncClose()

	//kafka 返回信息
	go dealBackMsg (&recordType,&columnName,&producer,sendFailQueue)

	//对读取到内存中的记录，进行序列化
	go parseMsg(&recordType,&columnName,lineQueue,sendQueue,parseFailQueue)
	//发送数据到kafka
	go sendMsg(&producer,sendQueue)
	//序列化失败的记录，填写到文件中
	go writeFail(&parseTail,&failPath,parseFailQueue)
	//发送失败的记录，填写到文件中
	go writeFail(&sendTail,&failPath,sendFailQueue)

	for{
		fileNames,err := GetFiles(&inPath)
		if err!=nil{
			logger.Painc(err)
		}

		matchFiles,unMatchFiles:= groupFile(&fileNames,&rule)
		mvUnMatchFiles(&unMatchFiles,&skipPath)

		for i := range matchFiles {
			fileQueue <- matchFiles[i]
		}
		dealFile(10,fileQueue,lineQueue)

		time.Sleep(time.Millisecond)
	}
}

//多携程处理文件
func dealFile(num int, fileNames <-chan string, out chan<- string){

	var wg sync.WaitGroup
	wg.Add(len(fileNames))

	for i := 0; i < num; i++ {
		go func(){
			defer wg.Done()

			for filename := range fileNames {
				lines,err:= ReadFile(filename)
				if err !=nil{
					logger.Error(fmt.Sprintf("read file : %s ,err : %s ",filename,err.Error()))
					panic(err)
				}

				baseName:= filepath.Base(filename)
				for i := range lines {
					out <- fmt.Sprintf("%s,%d,%s",baseName,i,lines[i])
				}

				if err:=RmFile(filename);err!=nil{
					logger.Error(fmt.Sprintf("rm file : %s ,err : %s ",filename,err.Error()))
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}

func dealBackMsg (recordType *string,columnName *[]string,p *sarama.AsyncProducer,sendFailQueue chan<- string) {
	for{
		select {
		case  <-(*p).Successes():
			//if data,err:=success.Value.Encode();err!=nil{
			//	logger.Error("get kafka success value err: ", err)
			//}else{
			//
			//}
		case fail := <-(*p).Errors():
			if data,err:=fail.Msg.Value.Encode(); err!=nil{
				logger.Error("get kafka fail value err: ", err)
			}else{
				if columnStr,err2:= kafkaMsgValue2str(recordType,columnName,data) ; err2!=nil{
					sendFailQueue <- columnStr
				}else{
					logger.Error("kafka fail value cannot parse ,err: ", err2)
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
}


func parseMsg(recordType *string,columnName *[]string,
			  lineQueue<- chan string,sendQueue chan<- []byte,parseFailQueue chan<- string){

	for{
		for line := range lineQueue {
			if buffer,err:= str2kafkaMsgValue(recordType,columnName,&line); err!=nil{
				parseFailQueue <- line
			}else{
				sendQueue <- buffer
			}
		}
		time.Sleep(time.Millisecond)
	}
}

func sendMsg(producer *sarama.AsyncProducer,sendQueue<- chan []byte){

	for data := range sendQueue {
		msg := &sarama.ProducerMessage{
			Topic: "sms",
			Value : sarama.ByteEncoder(data),
		}
		(*producer).Input() <- msg
	}
	time.Sleep(time.Millisecond)
}

func writeFail(tail *string,failPath *string,sendFailQueue<- chan string ){

	var context []string
	lastTime := GetUnixTime()
	thisTime := lastTime

	for{
		thisTime = GetUnixTime()
		if thisTime - lastTime > 60 || len(context) >3000{

			baseName:= "err_"+lineName+"_"+GetCurrentTime()+"."+*tail
			filename:= path.Join(*failPath,baseName)

			WriteFile(filename,&context)

			context = []string{}
			lastTime = GetUnixTime()
		}

		for data := range sendFailQueue {
			context = append(context,data)
		}
		time.Sleep(time.Millisecond)
	}
}

func str2kafkaMsgValue(recordType *string,columnName *[]string,lineStr *string) ([]byte,error){

	switch *recordType {
	case "sms":
		record :=entity.SmsRecord{}
		if err:=SetValue(&record,columnName,lineStr); err!=nil{
			return nil, err
		}else{
			if buffer, err2 := proto.Marshal(&record); err2!=nil{
				return nil, err
			}else{
				return buffer,nil
			}
		}
	case "gsm":
		record :=entity.GsmRecord{}
		if err:=SetValue(&record,columnName,lineStr); err!=nil{
			return nil, err
		}else{
			if buffer, err2 := proto.Marshal(&record); err2!=nil{
				return nil, err
			}else{
				return buffer,nil
			}
		}
	case "gprs":
		record :=entity.GprsRecord{}
		if err:=SetValue(&record,columnName,lineStr); err!=nil{
			return nil, err
		}else{
			if buffer, err2 := proto.Marshal(&record); err2!=nil{
				return nil, err
			}else{
				return buffer,nil
			}
		}
	}
	return nil,errors.New("unKnown recordType :"+ *recordType)
}


func kafkaMsgValue2str(recordType *string,columnName *[]string,data []byte) (string,error){

	switch *recordType {
	case "sms":
		var record entity.GsmRecord
		if err := proto.Unmarshal(data, &record) ; err==nil{
			return GetStr(&record,columnName),nil
		}else{
			return "",err
		}
	case "gsm":
		var record entity.GsmRecord
		if err := proto.Unmarshal(data, &record) ; err==nil{
			return GetStr(&record,columnName),nil
		}else{
			return "",err
		}
	case "gprs":
		var record entity.GsmRecord
		if err := proto.Unmarshal(data, &record) ; err==nil{
			return GetStr(&record,columnName),nil
		}else{
			return "",err
		}
	}
	return "",errors.New("unKnown recordType :"+ *recordType)
}

