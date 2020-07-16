package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/wonderivan/logger"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
)


func GetFiles(path *string) ([]string,error){

	files, err := ioutil.ReadDir(*path)
	if err != nil {
		return nil, err
	}
	// 获取文件，并输出它们的名字
	var fileNames []string
	for _, file := range files {
		fileNames = append(fileNames,file.Name())
	}
	return fileNames,nil
}

func ReadFile(fileName string) ([]string,error){
	f, err := os.Open(fileName)
	if err != nil {
		logger.Error("Cannot open file: %s, err: [%v] ", fileName, err)
		return []string{},err
	}
	defer f.Close()

	var lines []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()  // or
		lines = append(lines,line)
	}

	if err := scanner.Err(); err != nil {
		logger.Error("Cannot scanner text file: %s, err: [%v] ", fileName, err)
		return []string{},err
	}

	return lines,nil
}


func WriteFile(fileName string,context *[]string)  {

	lineNum:= len(*context)

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return
	}
	defer file.Close()
	// 获取writer对象
	writer := bufio.NewWriter(file)
	for i := 0; i < lineNum; i++ {
		writer.WriteString ((*context)[i])
	}
	// 刷新缓冲区，强制写出
	writer.Flush()
}


func RmFile(fileName string) error{

	if err:=os.Remove(fileName);err!=nil{
		return err
	}
	return nil
}

func MvFile(oldName string,newName string) error{

	if err:=os.Link(oldName,newName);err!=nil{
		return err
	}
	if err:=os.Remove(oldName);err!=nil{
		return err
	}
	return nil
}

func CheckFile(fileName string) error {

	fi, err := os.Stat(fileName)
	if err != nil || os.IsNotExist(err) {
		return errors.New("File : " + fileName + " is not exists ")
	}
	if fi.IsDir() {
		return errors.New("File : " + fileName + " is path, not file ")
	}
	return nil
}

func CheckPath(path string) error {

	fi, err := os.Stat(path)
	if err != nil || os.IsNotExist(err) {
		return errors.New("path : " + path + " is not exists ")
	}
	if !fi.IsDir() {
		return errors.New("path : " + path + " is file, not path ")
	}
	return nil
}


func groupFile(fileNames *[]string,rule *string)([]string,[]string){

	reg := regexp.MustCompile(*rule)

	var matchFiles []string
	var unMatchFiles []string
	for i := range *fileNames {
		if reg.Find([]byte((*fileNames)[i])) == nil{
			unMatchFiles = append(unMatchFiles,(*fileNames)[i])
		}else{
			matchFiles = append(matchFiles,(*fileNames)[i])
		}
	}
	return matchFiles,unMatchFiles
}

func mvUnMatchFiles(files *[]string,path *string)  {

	if len(*files) == 0{
		return
	}
	for i := range *files {
		skipName:= filepath.Join(*path, filepath.Base((*files)[i]))
		if err:=MvFile((*files)[i],skipName);err!=nil{
			logger.Error(fmt.Sprintf("mv %v  to  %v ,err: %v",(*files)[i],skipName,err))
		}
	}
}
