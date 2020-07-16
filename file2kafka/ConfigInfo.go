package main

import (
	"errors"
	"github.com/spf13/viper"
	"strings"
)

type Source struct {
	RecordType	string
	SpoolDir    string
	DeletePolicy	string
	FileName	string
	columns string
	Split	string
}


type OutInfo struct {
	Path string
	FilePrefix string
	TempPrefix string
	RollInterval int
	BatchSize int
	Delim string
}

type KafkaInfo struct{
	bootstrap string
	topic string
	partition int
	consumer string
}

type Sink struct{
	kafka KafkaInfo
	skipOut OutInfo
	failOut OutInfo
}

type PipLine struct{
	In Source
	Out Sink
}

// 配置文件 内容
type ConfigContent struct {
	Sources   map[string]Source
	Sinks	map[string]Sink
	Lines map[string]PipLine
}

var configContent ConfigContent

func ReadConfig(cfg string) error {

	if err := Init(cfg); err != nil {
		panic(err)
	}

	// sources
	var sources = make(map[string]Source)
	sourceContent := viper.GetStringMap("sources")
	for k, v := range sourceContent {
		name := k
		contentMap := v.(map[string]interface{})

		var source Source
		if recordType, ok := contentMap[strings.ToLower("recordType")];ok{
			source.RecordType = recordType.(string)
		}
		if spoolDir, ok := contentMap[strings.ToLower("spoolDir")];ok{
			source.SpoolDir = spoolDir.(string)
		}
		if deletePolicy, ok := contentMap[strings.ToLower("deletePolicy")];ok{
			source.DeletePolicy = deletePolicy.(string)
		}
		if fileName, ok := contentMap[strings.ToLower("fileName")];ok{
			source.FileName = fileName.(string)
		}
		if columns, ok := contentMap[strings.ToLower("columns")];ok{
			source.columns = columns.(string)
		}
		if split, ok := contentMap[strings.ToLower("split")];ok{
			source.Split = split.(string)
		}

		if  source.RecordType != "" && source.SpoolDir != "" && source.DeletePolicy != "" &&
			source.FileName != "" && source.Split != ""{
			sources[name] = source
		}
	}

	if len(sources) == 0 {
		return errors.New("source is empty ")
	}

	var sinks = make(map[string]Sink)
	sinkContent := viper.GetStringMap("sinks")
	for k, v := range sinkContent {
		name := k
		var kafka KafkaInfo
		var skipOut OutInfo
		var failOut OutInfo

		contentMap := v.(map[string]interface{})
		for k1, v1 := range contentMap {
			map1 := v1.(map[string]interface{})
			switch k1 {
			case strings.ToLower("kafka"):
				if bootstrap, ok := map1[strings.ToLower("bootstrap.servers")];ok{
					kafka.bootstrap = bootstrap.(string)
				}
				if topic, ok := map1[strings.ToLower("topic")];ok{
					kafka.topic = topic.(string)
				}
				if partition, ok := map1[strings.ToLower("partition")];ok{
					kafka.partition = partition.(int)
				}
				if consumer, ok := map1[strings.ToLower("consumer.group.id")];ok{
					kafka.consumer = consumer.(string)
				}
			case strings.ToLower("skipOut"):
				if path, ok := map1[strings.ToLower("path")];ok{
					skipOut.Path = path.(string)
				}
				if filePrefix, ok := map1[strings.ToLower("filePrefix")];ok{
					skipOut.FilePrefix = filePrefix.(string)
				}
				if tempPrefix, ok := map1[strings.ToLower("tempPrefix")];ok{
					skipOut.TempPrefix = tempPrefix.(string)
				}
				if rollInterval, ok := map1[strings.ToLower("rollInterval")];ok{
					skipOut.RollInterval = rollInterval.(int)
				}
				if batchSize, ok := map1[strings.ToLower("batchSize")];ok{
					skipOut.BatchSize = batchSize.(int)
				}
				if delIm, ok := map1[strings.ToLower("delIm")];ok{
					skipOut.Delim = delIm.(string)
				}
			case strings.ToLower("failOut"):
				if path, ok := map1[strings.ToLower("path")];ok{
					failOut.Path = path.(string)
				}
				if filePrefix, ok := map1[strings.ToLower("filePrefix")];ok{
					failOut.FilePrefix = filePrefix.(string)
				}
				if tempPrefix, ok := map1[strings.ToLower("tempPrefix")];ok{
					failOut.TempPrefix = tempPrefix.(string)
				}
				if rollInterval, ok := map1[strings.ToLower("rollInterval")];ok{
					failOut.RollInterval = rollInterval.(int)
				}
				if batchSize, ok := map1[strings.ToLower("batchSize")];ok{
					failOut.BatchSize = batchSize.(int)
				}
				if delIm, ok := map1[strings.ToLower("delIm")];ok{
					failOut.Delim = delIm.(string)
				}
			}
		}
		sinks[name] = Sink{kafka,skipOut,failOut}
	}
	if len(sinks) == 0 {
		return errors.New("sink is empty ")
	}

	// dbConnect
	var pipLines = make(map[string]PipLine)

	pipLineContent := viper.GetStringMap("channels")
	for k, v := range pipLineContent {
		name:=k
		contentMap := v.(map[string]interface{})

		var pipLine PipLine
		if source, ok := contentMap[strings.ToLower("source")];ok{
			sName:= source.(string)
			if in,ok:= sources[sName];ok{
				pipLine.In = in
			}
		}
		if sink, ok := contentMap[strings.ToLower("sink")];ok{
			sName:= sink.(string)
			if out,ok:= sinks[sName];ok{
				pipLine.Out = out
			}
		}
		pipLines[name] = pipLine
	}

	if len(pipLines) == 0 {
		return errors.New("pipLine is empty ")
	}

	configContent.Sources = sources
	configContent.Sinks = sinks
	configContent.Lines = pipLines

	err := CheckConfig()
	if err != nil {
		return err
	}

	return nil
}

func CheckConfig() error {

	return nil
}


