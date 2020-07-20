package checkBill

import (
	"fmt"
	"github.com/wonderivan/logger"
	"strconv"
	"strings"
)

// 任务信息
type CheckTask struct {
	AuditDay   int
	RegionCode string
	Channel    int
}

type CheckTaskSlice []CheckTask

func (s CheckTaskSlice) Len() int      { return len(s) }
func (s CheckTaskSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s CheckTaskSlice) Less(i, j int) bool {

	if s[i].AuditDay != s[j].AuditDay {
		return s[i].AuditDay < s[j].AuditDay
	}
	if s[i].RegionCode != s[j].RegionCode {
		return s[i].RegionCode < s[j].RegionCode
	}
	return s[i].Channel < s[j].Channel
}

func (task *CheckTask) dealChannel() error {

	regionCode := task.RegionCode
	auditDay := task.AuditDay
	channel := task.Channel

	logger.Info(fmt.Sprintf("\t\tRegionCode : %s channel: %d deal begin ... ", regionCode, channel))
	//删除原有的数据
	if err := DeleteCheckSchedule(regionCode, auditDay, channel); err != nil {
		return err
	}
	if err := DeleteCheckTotal(regionCode, auditDay, channel); err != nil {
		return err
	}
	if err := DeleteCheckSub(regionCode, auditDay, channel); err != nil {
		return err
	}
	//初始化进度信息
	if err := InitCheckSchedule(regionCode, auditDay, channel); err != nil {
		return err
	}

	// 查找清单记录
	c := make(chan DataRes, 2)
	// 查找清单记录
	go getCdr(regionCode, auditDay, channel, c)
	//账单数据
	go getBill(regionCode, auditDay, channel, c)

	dataRes1, dataRes2 := <-c, <-c
	close(c)

	diffRecords, totalMap := DiffCdrMap(&dataRes1, &dataRes2)

	if err := InsertCheckTotal(regionCode, auditDay, channel, &totalMap); err != nil {
		return err
	}
	if err := InsertCheckSub(regionCode, auditDay, channel, &diffRecords); err != nil {
		return err
	}
	if err := UpdateCheckSchedule(regionCode, auditDay, channel); err != nil {
		return err
	}
	return nil
}

func getCdr(regionCode string, auditDay int, channel int, c chan<- DataRes) {

	var tableNum = len(configContent.CdrTables[BillTabPrefix]) +
		len(configContent.CdrTables[BillTabPrefixNoRegion]) +
		len(configContent.CdrTables[BillTabPrefixMod])

	c1 := make(chan map[int64]CdrRecordSlice, tableNum)
	if heads, ok := configContent.CdrTables[BillTabPrefix]; ok {
		for _, head := range heads {
			var tableName = head + "_" + regionCode + "_" + strconv.Itoa(auditDay)
			var condSql = "and mod(acc_id,10) = " + strconv.Itoa(channel)
			go getRegionCdr(regionCode, tableName, condSql, c1)
		}
	}
	if heads, ok := configContent.CdrTables[BillTabPrefixNoRegion]; ok {
		for _, head := range heads {
			var tableName = head + "_" + strconv.Itoa(auditDay)
			var condSql = " and hplmn2 = '" + regionCode + "' and mod(acc_id,10) = " + strconv.Itoa(channel)
			go getRegionCdr(regionCode, tableName, condSql, c1)
		}
	}
	if heads, ok := configContent.CdrTables[BillTabPrefixMod]; ok {
		for _, head := range heads {
			var tableName = head + "_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay)
			var condSql = ""
			go getRegionCdr(regionCode, tableName, condSql, c1)
		}
	}

	var cdrMap = make(map[int64]CdrRecordSlice)
	for i := 0; i < tableNum; i++ {
		tMap := <-c1
		cdrMap = CdrMapAppend(&cdrMap, &tMap)
	}
	close(c1)

	var res = DataRes{DataSource: DataSourceCdr, DataMap: cdrMap}
	c <- res
}

func getRegionCdr(regionCode string, tableName string, condSql string, c chan<- map[int64]CdrRecordSlice) {

	var cdrRecords CdrRecordSlice
	var err error

	if isAuditUser := configContent.CommonAttributes[strings.ToLower(AttributeRegionAuditUser)].(bool); isAuditUser {
		cdrRecords, err = QueryCdrDetail(regionCode, tableName, condSql)
	} else {
		cdrRecords, err = QueryCdrTotal(regionCode, tableName, condSql)
	}

	CheckErr(err)
	cdrMap := CdrParseMap(&cdrRecords)
	c <- cdrMap
}

func getBill(regionCode string, auditDay int, channel int, c chan<- DataRes) {

	billMap := getRegionBill(regionCode, auditDay, channel)

	var res = DataRes{DataSource: DataSourceBill, DataMap: billMap}
	c <- res
}

func getRegionBill(regionCode string, auditDay int, channel int) map[int64]CdrRecordSlice {

	isAuditUser := configContent.CommonAttributes[strings.ToLower(AttributeRegionAuditUser)].(bool)

	get1DayBillDetail := func(auditDay int) map[int64]CdrRecordSlice {
		totalRecords, err := QueryTotalBillDetail(regionCode, auditDay, channel)
		CheckErr(err)
		usageRecords, err := QueryUsageBillDetail(regionCode, auditDay, channel)
		CheckErr(err)
		spRecords, err := QuerySpBillDetail(regionCode, auditDay, channel)
		CheckErr(err)
		return BillParseMap(&totalRecords, &usageRecords, &spRecords)
	}

	get1DayBillTotal := func(auditDay int) map[int64]CdrRecordSlice {
		var totalRecords TotalBillSlice
		totalRecords = append(totalRecords, TotalBillRecord{DefaultId, DefaultId})

		usageRecords, err := QueryUsageBillTotal(regionCode, auditDay, channel)
		CheckErr(err)
		spRecords, err := QuerySpBillTotal(regionCode, auditDay, channel)
		CheckErr(err)
		return BillParseMap(&totalRecords, &usageRecords, &spRecords)
	}

	get1DayBill := func(auditDay int) map[int64]CdrRecordSlice {
		if isAuditUser {
			return get1DayBillDetail(auditDay)
		} else {
			return get1DayBillTotal(auditDay)
		}
	}

	tMap1 := get1DayBill(auditDay)

	// 查找前一天账单记录
	if auditDay%100 != 1 {
		var lastDay = GetLastDay(auditDay)
		tMap2 := get1DayBill(lastDay)
		tMap1 = GetBillMinus(&tMap1, &tMap2)
	}

	return tMap1
}
