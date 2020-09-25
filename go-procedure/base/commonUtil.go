package base

import (
	"errors"
	"fmt"
	"github.com/wonderivan/logger"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var (
	CstZone = time.FixedZone("CST", 8*3600) // 东八
	OraTimeLayout = "20060102150405000000"
)

func GetCurrentTime() string {
	return time.Now().In(CstZone).Format(OraTimeLayout)
}

func GetUnixTime() int64 {
	return time.Now().In(CstZone).Unix()
}

func GetSecondTime(seconds int) time.Duration{
	
	secondTime:=time.Second
	for a:=0;a<seconds;a++{
		secondTime += time.Second
	}
	return secondTime
}

func CheckErr(err error) {
	if err != nil {
		logger.Error(fmt.Sprintf(" err : %s ", err.Error()))
		panic(err)
	}
}

func CheckFile(configFile string) error {

	fi, err := os.Stat(configFile)
	if err != nil || os.IsNotExist(err) {
		return errors.New("File : " + configFile + " is not exists ")
	}
	if fi.IsDir() {
		return errors.New("File : " + configFile + " is path, not file ")
	}
	return nil
}

func GetAllFiles(dirPath string)(files []string, err error){

	var dirs []string
	dir, err := ioutil.ReadDir(dirPath)

	if err != nil {
		return nil, err
	}
	PthSep := string(os.PathSeparator)

	for _, fi := range dir {
		if fi.IsDir() {
			dirs = append(dirs, dirPath+PthSep+fi.Name())
			newFiles, _ := GetAllFiles(dirPath + PthSep + fi.Name())
			files = append(files, newFiles...)
		} else {
			files = append(files, dirPath+PthSep+fi.Name())
		}
	}
	return files, nil
}

func ReSetLogFileName(logFile string,instanceName string) (string,error){

	fd, err := os.Open(logFile)
	if err != nil {
		return "",err
	}

	contents, err := ioutil.ReadAll(fd)
	if err != nil {
		return "",errors.New(fmt.Sprintf( "Could not read %s : %v", logFile, err))
	}
	strOld:= string(contents)

	if ok:=strings.Contains(strOld,"filename");!ok{
		return logFile,nil
	}

	a:= strings.Index(strOld,"filename")
	str2:=strOld[a:]
	b:= strings.Index(str2,",")
	str3:=str2[:b]

	if ok:=strings.Contains(str3,".log");ok{
		str4:=strings.Replace(str3,".log","-"+instanceName+".log",-1)
		strNew:=strings.Replace(strOld,str3,str4,-1)
		return strNew,nil
	}else{
		return logFile,nil
	}
}




