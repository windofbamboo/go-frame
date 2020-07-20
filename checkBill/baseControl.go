package checkBill

import (
	"errors"
	"flag"
	"fmt"
	"github.com/wonderivan/logger"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func TaskInit() ([]string, int) {

	var regionCodes string
	var channel int

	flag.StringVar(&regionCodes, "r", DefaultRegionCode, "region_code for check")
	flag.IntVar(&channel, "c", DefaultChannel, "channel for check")
	flag.Parse()

	exPath := os.Getenv("CONFIG_PATH")
	logFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	//logFile = "D:\\backup\\GoPath\\src\\checkBill\\conf\\log.json"
	err := CheckFile(logFile)
	CheckErr(err)
	err = logger.SetLogger(logFile)
	CheckErr(err)

	configFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)
	//configFile = "D:\\backup\\GoPath\\src\\checkBill\\conf\\config.yaml"
	err = CheckFile(configFile)
	CheckErr(err)

	err = ReadConfig(configFile)
	CheckErr(err)

	regionCodeList := strings.Split(regionCodes, SeparatorCharacter)
	validRegionCodes, err := CheckRegionCode(regionCodeList)
	CheckErr(err)

	err = CheckChannel(validRegionCodes, channel)
	CheckErr(err)

	return validRegionCodes, channel
}

func CommandInit() (string, []string, int, int, int64) {

	var scope string
	var regionCodes string
	var auditDay int
	var channel int
	var configFile string
	var acctId int64

	exPath := os.Getenv("CONFIG_PATH")
	logFile := filepath.Join(exPath, DefaultConfigPath, LogConfigFileName)
	//logFile = "D:\\backup\\GoPath\\src\\checkBill\\conf\\log.json"
	err := CheckFile(logFile)
	CheckErr(err)

	err = logger.SetLogger(logFile)
	CheckErr(err)

	localFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)
	//localFile = "D:\\backup\\GoPath\\src\\checkBill\\conf\\config.yaml"

	var lastDay = GetNDayBefore(1)

	flag.StringVar(&scope, "s", AuditScopeRegion, " scope of data ")
	flag.StringVar(&regionCodes, "r", DefaultRegionCode, "region_code for check")
	flag.IntVar(&auditDay, "d", lastDay, "auditDay for check")
	flag.IntVar(&channel, "c", DefaultChannel, "channel for check")
	flag.StringVar(&configFile, "f", localFile, "configFile for read")
	flag.Int64Var(&acctId, "a", DefaultId, "configFile for read")
	flag.Parse()

	err = CheckScope(scope)
	CheckErr(err)
	err = CheckAuditDay(auditDay, lastDay)
	CheckErr(err)
	err = CheckFile(configFile)
	CheckErr(err)

	err = ReadConfig(configFile)
	CheckErr(err)

	regionCodeList := strings.Split(regionCodes, SeparatorCharacter)
	validRegionCodes, err := CheckRegionCode(regionCodeList)
	CheckErr(err)

	err = CheckChannel(validRegionCodes, channel)
	CheckErr(err)

	if scope == AuditScopeAcct {
		if acctId == DefaultId || acctId < 100000 {
			panic(errors.New("scope is acct, but acct_id is invalid "))
		}
		if len(validRegionCodes) > 1 {
			panic(errors.New("scope is acct, but region_code numbers is more than 1 "))
		}
		if len(validRegionCodes) > 1 {
			panic(errors.New("scope is acct, but region_code is all "))
		}
	}

	return scope, validRegionCodes, auditDay, channel, acctId
}

func CheckScope(scope string) error {

	if scope != AuditScopeRegion && scope != AuditScopeTable && scope != AuditScopeAcct {
		return errors.New("scope : " + scope + " is error ! ")
	}
	return nil
}

func CheckAuditDay(auditDay int, lastDay int) error {

	if !CheckDay(auditDay) {
		return errors.New("auditDay : " + strconv.Itoa(auditDay) + " is a wrong date ")
	}
	if auditDay > lastDay {
		return errors.New("data of " + strconv.Itoa(auditDay) + " can not be audited ")
	}
	return nil
}

func CheckChannel(regionCodes []string, channel int) error {

	if channel == DefaultChannel {
		return nil
	}

	for _, regionCode := range regionCodes {
		task := configContent.Tasks[regionCode]
		if task.TaskNum < channel {
			return errors.New(" channel[ " + strconv.Itoa(channel) + " ] of regionCode[ " + regionCode + " ] is wrong" +
				", it can not be bigger than " + strconv.Itoa(task.TaskNum))
		}
	}
	return nil
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

func CheckRegionCode(regionCodeList []string) ([]string, error) {

	var validList []string

	for _, regionCode := range regionCodeList {
		if regionCode == DefaultRegionCode {
			validList = []string{}
			for _, task := range configContent.Tasks {
				validList = append(validList, task.RegionCode)
			}
			break
		}

		if _, ok := configContent.Tasks[regionCode]; !ok {
			fmt.Println("regionCode : " + regionCode + " is not exists in configFile ")
		} else {
			validList = append(validList, regionCode)
		}
	}

	if len(regionCodeList) == 0 {
		return validList, errors.New("valid regionCode is empty ")
	}
	return validList, nil
}

func GetFullLockFile(scope string, validRegionCodes []string, channel int, acctId int64) string {

	lockPath := os.Getenv("LOCK_PATH")
	fileName := "lock_checkBill"
	switch scope {
	case AuditScopeProvince, AuditScopeRegion:
		fileName += "_" + scope + "("
		for i, regionCode := range validRegionCodes {
			if i == 0 {
				fileName += regionCode
			} else {
				fileName += "," + regionCode
			}
		}
		if channel == DefaultChannel {
			fileName += ")" + ".pid"
		} else {
			fileName += ")_" + strconv.Itoa(channel) + ".pid"
		}
	case AuditScopeAcct:
		fileName += "_" + AuditScopeAcct + "_" + strconv.FormatInt(acctId, 10) + ".pid"
	case AuditScopeTable:
		fileName += "_" + AuditScopeTable + ".pid"
	default:
		fileName += ".pid"
	}
	lockFile := filepath.Join(lockPath, fileName)
	return lockFile
}

func GetAllTask(validRegionCodes []string, auditDay int, channel int) CheckTaskSlice {

	tasks := configContent.Tasks
	var allTasks CheckTaskSlice
	for _, regionCode := range validRegionCodes {
		if channel == DefaultChannel {
			for i := 0; i < tasks[regionCode].TaskNum; i++ {
				allTasks = append(allTasks, CheckTask{RegionCode: regionCode, AuditDay: auditDay, Channel: i})
			}
		} else {
			allTasks = append(allTasks, CheckTask{RegionCode: regionCode, AuditDay: auditDay, Channel: channel})
		}
	}
	return allTasks
}

func GetValidTask(allTasks *CheckTaskSlice, finishTasks *CheckTaskSlice) CheckTaskSlice {

	isFinish := func(task CheckTask) bool {
		for _, finished := range *finishTasks {
			if finished.RegionCode == task.RegionCode &&
				finished.AuditDay == task.AuditDay &&
				finished.Channel == task.Channel {
				return true
			}
		}
		return false
	}

	var validTasks CheckTaskSlice
	for _, task := range *allTasks {
		if !isFinish(task) {
			validTasks = append(validTasks, task)
		}
	}

	return validTasks
}
