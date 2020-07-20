package checkBill

import (
	"fmt"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"
	"strconv"
)

/**
查询账单 CRasUsageBill CRasSpBill
*/
func QueryTotalBillDetail(regionCode string, auditDay int, channel int) (TotalBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT object_id ,bill_no " +
		" FROM CA_DAILY_BILL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" WHERE object_type = 0 "
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records TotalBillSlice
	var userId int64
	var billId int64
	for rows.Next() {
		err = rows.Scan(&userId, &billId)
		CheckErr(err)
		var record = TotalBillRecord{UserId: userId, BillId: billId}
		records = append(records, record)
	}
	return records, err
}

/**
查询账单 CRasUsageBill
*/
func QueryUsageBillDetail(regionCode string, auditDay int, channel int) (SubBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT bill_no,item_code,primal_fee " +
		" FROM CA_USAGE_DTL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" WHERE product_id not in(487100010,487100011,487100012) and item_code !=800000"
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records SubBillSlice
	var billId int64
	var itemCode int16
	var fee int32
	for rows.Next() {
		err = rows.Scan(&billId, &itemCode, &fee)
		CheckErr(err)
		var record = SubBillRecord{BillId: billId, ItemCode: itemCode, Fee: fee}
		records = append(records, record)
	}
	return records, err
}

func QuerySpBillDetail(regionCode string, auditDay int, channel int) (SubBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT bill_no,item_code,primal_fee " +
		" FROM CA_SP_DTL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay)
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records SubBillSlice
	var billId int64
	var itemCode int16
	var fee int32
	for rows.Next() {
		err = rows.Scan(&billId, &itemCode, &fee)
		CheckErr(err)
		var record = SubBillRecord{BillId: billId, ItemCode: itemCode, Fee: fee}
		records = append(records, record)
	}
	return records, err
}

func QueryUsageBillTotal(regionCode string, auditDay int, channel int) (SubBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT item_code,primal_fee " +
		" FROM CA_USAGE_DTL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" WHERE product_id not in(487100010,487100011,487100012) and item_code !=800000" +
		" group by item_code"
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records SubBillSlice
	var billId = DefaultId
	var itemCode int16
	var fee int32
	for rows.Next() {
		err = rows.Scan(&itemCode, &fee)
		CheckErr(err)
		var record = SubBillRecord{BillId: billId, ItemCode: itemCode, Fee: fee}
		records = append(records, record)
	}
	return records, err
}

func QuerySpBillTotal(regionCode string, auditDay int, channel int) (SubBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT item_code,sum(primal_fee) " +
		" FROM CA_SP_DTL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" group by item_code"
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records SubBillSlice
	var billId = DefaultId
	var itemCode int16
	var fee int32
	for rows.Next() {
		err = rows.Scan(&itemCode, &fee)
		CheckErr(err)
		var record = SubBillRecord{BillId: billId, ItemCode: itemCode, Fee: fee}
		records = append(records, record)
	}
	return records, err
}

func QueryAcctTotalBill(acctId int64, regionCode string, auditDay int, channel int) (TotalBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT object_id,bill_no " +
		" FROM CA_DAILY_BILL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" WHERE default_acct_id = :1 "
	// 查询数据
	rows, err := QuerySql(db, &sqlStr, acctId)
	CheckErr(err)

	// 装载数据
	var records TotalBillSlice
	var userId int64
	var billId int64
	for rows.Next() {
		err = rows.Scan(&userId, &billId)
		CheckErr(err)
		var record = TotalBillRecord{UserId: userId, BillId: billId}
		records = append(records, record)
	}
	return records, err
}

func QueryAcctUsageBill(billId int64, regionCode string, auditDay int, channel int) (SubBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT item_code,primal_fee " +
		" FROM CA_USAGE_DTL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" WHERE product_id not in(487100010,487100011,487100012) and item_code !=800000 " +
		" and bill_no = :1 "
	// 查询数据
	rows, err := QuerySql(db, &sqlStr, billId)
	CheckErr(err)

	// 装载数据
	var records SubBillSlice
	var itemCode int16
	var fee int32
	for rows.Next() {
		err = rows.Scan(&itemCode, &fee)
		CheckErr(err)
		var record = SubBillRecord{BillId: billId, ItemCode: itemCode, Fee: fee}
		records = append(records, record)
	}
	return records, err
}

func QueryAcctSpBill(billId int64, regionCode string, auditDay int, channel int) (SubBillSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceBill, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT item_code,sum(primal_fee) fee " +
		" FROM CA_SP_DTL_" + regionCode + "_" + strconv.Itoa(channel) + "_" + strconv.Itoa(auditDay) +
		" where bill_no = :1 group by item_code "
	// 查询数据
	rows, err := QuerySql(db, &sqlStr, billId)
	CheckErr(err)

	// 装载数据
	var records SubBillSlice
	var itemCode int16
	var fee int32
	for rows.Next() {
		err = rows.Scan(&itemCode, &fee)
		CheckErr(err)
		var record = SubBillRecord{BillId: billId, ItemCode: itemCode, Fee: fee}
		records = append(records, record)
	}
	return records, err
}

func QueryAcctList() (AuditDetailSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceITable)
	CheckErr(err)

	var sqlStr = " SELECT region_code,acct_id FROM check_acct"
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records AuditDetailSlice
	var regionCode string
	var acctId int64
	for rows.Next() {
		err = rows.Scan(&regionCode, &acctId)
		CheckErr(err)
		var record = AuditDetail{RegionCode: regionCode, AcctId: acctId}
		records = append(records, record)
	}
	return records, err
}

/**
插入检查结果
*/
func InsertCheckSub(regionCode string, auditDay int, channel int, records *DiffRecordSlice) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	currentTime := GetCurrentTime()
	var sqlStr = " INSERT INTO check_region_result_sub" +
		" (check_day,region_code,channel_no,user_id,item_code,bill_fee,cdr_fee,update_time) " +
		" values(:1,:2,:3,:4,:5,:6,:7,to_date(:8,'yyyy-mm-dd hh24:mi:ss')) "
	// 获取 tx
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 获取 stmt
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	defer stmt.Close()
	// 提交数据
	for _, record := range *records {
		_, err := stmt.Exec(auditDay, regionCode, channel, record.UserId, record.ItemCode, record.BillFee, record.CdrFee, currentTime)
		CheckErr(err)
	}

	err = tx.Commit()
	CheckErr(err)

	return err
}

/**
插入检查结果
*/
func InsertCheckTotal(regionCode string, auditDay int, channel int, totalMap *map[int16]*TotalRecord) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	currentTime := GetCurrentTime()
	var sqlStr = " INSERT INTO check_region_result_total" +
		"(check_day,region_code,channel_no,item_code,bill_fee,cdr_fee,diff_fee,update_time) " +
		" values(:1,:2,:3,:4,:5,:6,:7,to_date(:8,'yyyy-mm-dd hh24:mi:ss')) "

	// 获取 tx
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 获取 stmt
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	defer stmt.Close()
	// 提交数据
	for itemCode, record := range *totalMap {
		_, err := stmt.Exec(auditDay, regionCode, channel, itemCode, record.BillFee, record.CdrFee, record.DiffFee, currentTime)
		CheckErr(err)
	}

	err = tx.Commit()
	CheckErr(err)

	return err
}

func QueryCheckSchedule(regionCodes []string, auditDay int, channel int) (CheckTaskSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	sqlStr := fmt.Sprintf("select region_code,channel_no from check_region_schedule "+
		"where schedule_tag ='%s' and check_day = %d ", FinishTag, auditDay)
	if channel != DefaultChannel {
		sqlStr += fmt.Sprintf(" and channel_no = %d ", channel)
	}
	sqlStr += fmt.Sprintf(" and region_code in (")
	for i, regionCode := range regionCodes {
		if i > 0 {
			sqlStr += ","
		}
		sqlStr += fmt.Sprintf("'%s'", regionCode)
	}
	sqlStr += fmt.Sprintf(")")
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var ctSlice CheckTaskSlice
	var regionCode string
	var channelNo int
	for rows.Next() {
		err = rows.Scan(&regionCode, &channelNo)
		CheckErr(err)
		var record = CheckTask{AuditDay: auditDay, RegionCode: regionCode, Channel: channelNo}
		ctSlice = append(ctSlice, record)
	}
	return ctSlice, nil
}

/**
初始化 检查进度
*/
func InitCheckSchedule(regionCode string, auditDay int, channel int) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	currentTime := GetCurrentTime()
	var sqlStr = " INSERT INTO check_region_schedule" +
		" (check_day,region_code,channel_no,schedule_tag,start_time,update_time) " +
		" values(:1,:2,:3,:4,to_date(:5,'yyyy-mm-dd hh24:mi:ss'),to_date(:6,'yyyy-mm-dd hh24:mi:ss')) "
	// 插入数据
	return InsertSql(db, &sqlStr, auditDay, regionCode, channel, InitTag, currentTime, currentTime)
}

/**
初始化 检查进度
*/
func UpdateCheckSchedule(regionCode string, auditDay int, channel int) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	currentTime := GetCurrentTime()
	var sqlStr = " update check_region_schedule " +
		" set schedule_tag = :1 , end_time = to_date(:2,'yyyy-mm-dd hh24:mi:ss'), " +
		" update_time = to_date(:3,'yyyy-mm-dd hh24:mi:ss')" +
		" where check_day= :4 and region_code= :5 and channel_no = :6"
	// 插入数据
	return UpdateSql(db, &sqlStr, FinishTag, currentTime, currentTime, auditDay, regionCode, channel)
}

/**
删除检查结果
*/
func DeleteCheckSchedule(regionCode string, auditDay int, channel int) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	var sqlStr = " delete from check_region_schedule " +
		" where check_day= :1 and region_code= :2 and channel_no= :3 "
	// 插入数据
	return DeleteSql(db, &sqlStr, auditDay, regionCode, channel)
}

func DeleteCheckTotal(regionCode string, auditDay int, channel int) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	var sqlStr = " delete from check_region_result_total " +
		" where check_day= :1 and region_code= :2 and channel_no= :3"
	// 插入数据
	return DeleteSql(db, &sqlStr, auditDay, regionCode, channel)
}

func DeleteCheckSub(regionCode string, auditDay int, channel int) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	var sqlStr = " delete from check_region_result_sub " +
		" where check_day= :1 and region_code= :2 and channel_no= :3"
	// 插入数据
	return DeleteSql(db, &sqlStr, auditDay, regionCode, channel)
}

/**
插入检查结果
*/
func InsertCheckAcct(records *AuditDetailResultSlice) error {
	// 连接数据库
	db, err := GetConnPool(DataSourceResult)
	CheckErr(err)

	currentTime := GetCurrentTime()
	var sqlStr = " INSERT INTO check_acct_result" +
		" (check_day,region_code,acct_id,user_id,item_code,bill_fee,cdr_fee,update_time) " +
		" values(:1,:2,:3,:4,:5,:6,:7,to_date(:8,'yyyy-mm-dd hh24:mi:ss')) "
	// 获取 tx
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 获取 stmt
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	defer stmt.Close()
	// 提交数据
	for _, record := range *records {
		_, err := stmt.Exec(record.AuditDay, record.RegionCode, record.AcctId, record.UserId,
			record.ItemCode, record.BillFee, record.CdrFee, currentTime)
		CheckErr(err)
	}

	err = tx.Commit()
	CheckErr(err)

	return err
}
