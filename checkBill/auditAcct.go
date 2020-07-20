package checkBill

import (
	"fmt"
	"github.com/wonderivan/logger"
	"strconv"
)

func AuditAcct(regionCode string, acctId int64, auditDay int) {

	logger.Info(fmt.Sprintf(" audit acctId : %d  auditDay : %d", acctId, auditDay))
	res := AuditSingleAcct(regionCode, acctId, auditDay)
	if len(res) > 0 {
		logger.Info(fmt.Sprintf("..................audit result........................................"))
		logger.Info(fmt.Sprintf("acctId : %d ,auditDay : %d , user bill detail:  ", acctId, auditDay))
		var num = 0
		for _, record := range res {
			logger.Info(fmt.Sprintf("[%d]\t{ user_id : %d\titem_code : %d\tbill_fee : %d\t cdr_fee : %d } ",
				num, record.UserId, record.ItemCode, record.BillFee, record.CdrFee))
			num++
		}
		logger.Info(fmt.Sprintf("......................................................................"))
	}
}

func AuditSingleAcct(regionCode string, acctId int64, auditDay int) AuditDetailResultSlice {

	billMap := getAcctBill(regionCode, acctId, auditDay)
	cdrMap := getAcctCdr(regionCode, acctId, auditDay)

	var res AuditDetailResultSlice
	if len(billMap) > 0 || len(cdrMap) > 0 {
		diffRecords := DiffAcctBillMap(&billMap, &cdrMap)
		for _, diffRecord := range diffRecords {
			var record = AuditDetailResult{auditDay, regionCode, acctId,
				diffRecord.UserId, diffRecord.ItemCode, diffRecord.BillFee, diffRecord.CdrFee}
			res = append(res, record)
		}
	}
	return res
}

func getAcctBill(regionCode string, acctId int64, auditDay int) map[int64]CdrRecordSlice {

	channel := int(acctId % 10)

	tMap1 := getAcctBill1Day(regionCode, acctId, auditDay, channel)
	if auditDay%100 != 1 {
		var lastDay = GetLastDay(auditDay)
		tMap2 := getAcctBill1Day(regionCode, acctId, lastDay, channel)
		tMap1 = GetBillMinus(&tMap1, &tMap2)
	}
	return tMap1
}

func getAcctBill1Day(regionCode string, acctId int64, auditDay int, channel int) map[int64]CdrRecordSlice {

	totalRecords, err := QueryAcctTotalBill(acctId, regionCode, auditDay, channel)
	CheckErr(err)

	var usageRecords SubBillSlice
	var spRecords SubBillSlice

	for _, totalBill := range totalRecords {
		sbRecords1, err := QueryAcctUsageBill(totalBill.BillId, regionCode, auditDay, channel)
		CheckErr(err)
		sbRecords2, err := QueryAcctSpBill(totalBill.BillId, regionCode, auditDay, channel)
		CheckErr(err)
		for _, record := range sbRecords1 {
			usageRecords = append(usageRecords, record)
		}
		for _, record := range sbRecords2 {
			spRecords = append(spRecords, record)
		}
	}
	return BillParseMap(&totalRecords, &usageRecords, &spRecords)
}

func getAcctCdr(regionCode string, acctId int64, auditDay int) map[int64]CdrRecordSlice {

	var cdrMap = make(map[int64]CdrRecordSlice)
	c1 := make(chan map[int64]CdrRecordSlice)

	channel := int(acctId % 10)
	heads, ok := configContent.CdrTables[BillTabPrefix]
	if ok {
		for _, head := range heads {
			var tableName = head + "_" + regionCode + "_" + strconv.Itoa(auditDay)
			var condSql = "and mod(acc_id,10) = " + strconv.Itoa(channel)
			go getSingleAcctCdr(regionCode, acctId, tableName, condSql, c1)
		}
	}

	heads, ok = configContent.CdrTables[BillTabPrefixNoRegion]
	if ok {
		for _, head := range heads {
			var tableName = head + "_" + strconv.Itoa(auditDay)
			var condSql = " and hplmn2 = '" + regionCode + "' and mod(acc_id,10) = " + strconv.Itoa(channel)
			go getSingleAcctCdr(regionCode, acctId, tableName, condSql, c1)
		}
	}

	heads, ok = configContent.CdrTables[BillTabPrefixMod]
	if ok {
		for _, head := range heads {
			var tableName = head + "_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay)
			var condSql = ""
			go getSingleAcctCdr(regionCode, acctId, tableName, condSql, c1)
		}
	}

	var tableNum = len(configContent.CdrTables[BillTabPrefix]) +
		len(configContent.CdrTables[BillTabPrefixNoRegion]) +
		len(configContent.CdrTables[BillTabPrefixMod])

	for i := 0; i < tableNum; i++ {
		tMap := <-c1
		cdrMap = CdrMapAppend(&cdrMap, &tMap)
	}
	close(c1)
	return cdrMap
}

func getSingleAcctCdr(regionCode string, acctId int64, tableName string, condSql string, c chan map[int64]CdrRecordSlice) {

	cdrRecords1, err := QueryAcctCdrFee(regionCode, acctId, tableName, condSql)
	CheckErr(err)

	cdrMap := CdrParseMap(&cdrRecords1)
	c <- cdrMap
}
