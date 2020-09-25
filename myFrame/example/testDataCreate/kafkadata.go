package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/vmihailenco/msgpack"
	"myFrame/example"
	"time"
)

var(
	brokers = []string{"192.168.190.50:9092","192.168.190.51:9092","192.168.190.52:9092"}
	topic = "test1"
)

func main() {
	syncProducer()
}

func syncProducer(){
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回
	config.Producer.Return.Errors = true
	config.Version = sarama.V1_1_0_0

	// 连接kafka
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	defer producer.Close()

	var msgs []*sarama.ProducerMessage
	for i:=2000;i<10000;i++{
		shape:=example.Rectangle{Length: i, Height: i}
		if value,err:=msgpack.Marshal(shape);err==nil{
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder("test"),
			}
			str:= string(value)
			msg.Value = sarama.ByteEncoder(str)
			msgs = append(msgs,msg)
		}
	}

	err = producer.SendMessages(msgs)
	if err!=nil{
		fmt.Printf("err: %v\n", err)
	}
}


func AsyncProducer(){
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回
	config.Producer.Return.Errors = true
	config.Version = sarama.V1_1_0_0

	// 连接kafka
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	defer producer.AsyncClose()

	go func(){
		for {
			select {
			case suc := <-producer.Successes():
				fmt.Printf("partition: %d, offset: %d,  timestamp: %s \n",
					suc.Partition, suc.Offset, suc.Timestamp.String())
			case fail := <-producer.Errors():
				fmt.Printf("err: %v \n", fail.Err)
			}
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("test"),
	}

	for i:=1;i<100;i++{
		shape:=example.Rectangle{Length: i, Height: i}
		if value,err:=msgpack.Marshal(shape);err==nil{
			str:= string(value)
			msg.Value = sarama.ByteEncoder(str)

			producer.Input() <- msg
		}
	}

	time.Sleep(time.Minute)
}


