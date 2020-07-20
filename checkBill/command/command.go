package main

import (
	"checkBill"
	"errors"
	"flag"
	"fmt"
	"github.com/wonderivan/logger"
	"os"
	"sort"
)

func main() {

	flag.Usage = func() {
		help()
	}

	scope, validRegionCodes, auditDay, channel, acctId := checkBill.CommandInit()

	lockFile := checkBill.GetFullLockFile(scope, validRegionCodes, channel, acctId)
	logger.Trace("lockFile :", lockFile)

	lock, err := os.Create(lockFile)
	if err != nil {
		panic(errors.New("create lockFile err"))
	}
	defer os.Remove(lockFile)
	defer lock.Close()

	err = checkBill.InitPoolMap()
	checkBill.CheckErr(err)

	if scope == checkBill.AuditScopeRegion {
		taskMap := getValidTask(validRegionCodes, auditDay, channel)
		checkBill.AuditRegion(&taskMap)
	} else if scope == checkBill.AuditScopeAcct {
		regionCode := validRegionCodes[0]
		checkBill.AuditAcct(regionCode, acctId, auditDay)
	} else if scope == checkBill.AuditScopeTable {
		checkBill.AuditTable(auditDay)
	}
	checkBill.DestroyPoolMap()
}

var help = func() {
	fmt.Println("====================================================")
	fmt.Println("command :   checkBill -d [auditDay] -f [configFile] -s region -r [regionCode] -c [channel] ")
	fmt.Println("                                                    -s acct -r [regionCode] -a [acctId] ")
	fmt.Println("                                                    -s table ")
	fmt.Println("                      -h ")
	fmt.Println("param description : ")
	fmt.Println("           -s   scope      [ region|acct|table , default : region ] ")
	fmt.Println("           -d   auditDay   [ format : yyyymmdd , default : yesterday  ] ")
	fmt.Println("           -f   configFile [ default : ${CONFIG_PATH}/checkBill/config.yaml ] ")
	fmt.Println("           -r   regionCode [ it is significance when scope is region , default : all ] ")
	fmt.Println("           -c   channel    [ it is significance when scope is region , default : -1 ] ")
	fmt.Println("           -a   acctId     [ it is significance when scope is acct ] ")
	fmt.Println("data description :")
	fmt.Println("             when scope is table , source data is check_acct in interfaceTableDb,result data is check_acct_result in auditInfoDb ")
	fmt.Println("             when scope is region , result data : check_region_schedule ,check_region_result_total,check_region_result_sub ")
	fmt.Println("example : ")
	fmt.Println("     scope - regionCode   ")
	fmt.Println("             checkBill -s region -r 0731 -c 0 -d 20200607 -f ${CONFIG_PATH}/conf/checkBill.yaml ")
	fmt.Println("             checkBill -r 0731 -c 0 -d 20200607 ")
	fmt.Println("             checkBill -r 0731,0732 -d 20200607 ")
	fmt.Println("             checkBill -d 20200607 ")
	fmt.Println("     scope - acct   ")
	fmt.Println("             checkBill -s acct -r 0731 -a 1234567890123450 -d 20200607 -f ${CONFIG_PATH}/conf/checkBill.yaml ")
	fmt.Println("             checkBill -s acct -r 0731 -a 1234567890123450 -d 20200607")
	fmt.Println("     scope - table   ")
	fmt.Println("             checkBill -s table -d 20200607 -f ${CONFIG_PATH}/conf/checkBill.yaml ")
	fmt.Println("             checkBill -s table -d 20200607 ")
	fmt.Println("====================================================")
}

func getValidTask(validRegionCodes []string, auditDay int, channel int) map[string]checkBill.CheckTaskSlice {

	allTasks := checkBill.GetAllTask(validRegionCodes, auditDay, channel)
	sort.Sort(allTasks)

	taskMap := make(map[string]checkBill.CheckTaskSlice)
	for _, task := range allTasks {
		if _, ok := taskMap[task.RegionCode]; !ok {
			ctSlice := checkBill.CheckTaskSlice{task}
			taskMap[task.RegionCode] = ctSlice
		} else {
			taskMap[task.RegionCode] = append(taskMap[task.RegionCode], task)
		}
	}
	return taskMap
}
