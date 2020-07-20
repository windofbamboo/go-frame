package checkBill

import (
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"
)

func QueryCdrDetail(regionCode string, tableName string, condSql string) (CdrRecordSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceCdr, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT user_id,coalesce(billing_item1,0),coalesce(billing_item2,0),coalesce(billing_item3,0)," +
		" coalesce(billing_item4,0),coalesce(fee1,0),coalesce(fee2,0),coalesce(fee3,0),coalesce(fee4,0)" +
		" FROM " + tableName +
		" where user_id <> -1 and redo_flag >= 0 " + condSql
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records CdrRecordSlice
	var userId int64
	var billingItem1 int16
	var billingItem2 int16
	var billingItem3 int16
	var billingItem4 int16
	var fee1 int32
	var fee2 int32
	var fee3 int32
	var fee4 int32

	for rows.Next() {
		err = rows.Scan(&userId, &billingItem1, &billingItem2, &billingItem3, &billingItem4, &fee1, &fee2, &fee3, &fee4)
		CheckErr(err)

		if billingItem1 > 0 && fee1 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem1, Fee: fee1}
			records = append(records, record)
		}
		if billingItem2 > 0 && fee2 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem2, Fee: fee2}
			records = append(records, record)
		}
		if billingItem3 > 0 && fee3 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem3, Fee: fee3}
			records = append(records, record)
		}
		if billingItem4 > 0 && fee4 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem4, Fee: fee4}
			records = append(records, record)
		}
	}
	return records, err
}

func QueryCdrTotal(regionCode string, tableName string, condSql string) (CdrRecordSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceCdr, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT coalesce(billing_item1,0),coalesce(billing_item2,0),coalesce(billing_item3,0)," +
		" coalesce(billing_item4,0),coalesce(fee1,0),coalesce(fee2,0),coalesce(fee3,0),coalesce(fee4,0)" +
		" FROM " + tableName +
		" where user_id <> -1 and redo_flag >= 0 " + condSql +
		" group by coalesce(billing_item1,0),coalesce(billing_item2,0)," +
		" coalesce(billing_item3,0),coalesce(billing_item4,0)"
	// 查询数据
	rows, err := QuerySql(db, &sqlStr)
	CheckErr(err)

	// 装载数据
	var records CdrRecordSlice
	var userId = DefaultId
	var billingItem1 int16
	var billingItem2 int16
	var billingItem3 int16
	var billingItem4 int16
	var fee1 int32
	var fee2 int32
	var fee3 int32
	var fee4 int32

	for rows.Next() {
		err = rows.Scan(&billingItem1, &billingItem2, &billingItem3, &billingItem4, &fee1, &fee2, &fee3, &fee4)
		CheckErr(err)

		if billingItem1 > 0 && fee1 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem1, Fee: fee1}
			records = append(records, record)
		}
		if billingItem2 > 0 && fee2 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem2, Fee: fee2}
			records = append(records, record)
		}
		if billingItem3 > 0 && fee3 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem3, Fee: fee3}
			records = append(records, record)
		}
		if billingItem4 > 0 && fee4 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem4, Fee: fee4}
			records = append(records, record)
		}
	}
	return records, err
}

func QueryAcctCdrFee(regionCode string, acctId int64, tableName string, condSql string) (CdrRecordSlice, error) {
	// 连接数据库
	db, err := GetConnPool(DataSourceCdr, regionCode)
	CheckErr(err)

	var sqlStr = " SELECT user_id,coalesce(billing_item1,0),coalesce(billing_item2,0),coalesce(billing_item3,0)," +
		" coalesce(billing_item4,0),sum(coalesce(fee1,0)),sum(coalesce(fee2,0)),sum(coalesce(fee3,0)),sum(coalesce(fee4,0))" +
		" FROM " + tableName +
		" where acc_id = $1 and redo_flag >= 0 " + condSql +
		" group by user_id,coalesce(billing_item1,0),coalesce(billing_item2,0)," +
		" coalesce(billing_item3,0),coalesce(billing_item4,0)"
	// 查询数据
	rows, err := QuerySql(db, &sqlStr, acctId)
	CheckErr(err)
	// 装载数据
	var records CdrRecordSlice
	var userId int64
	var billingItem1 int16
	var billingItem2 int16
	var billingItem3 int16
	var billingItem4 int16
	var fee1 int32
	var fee2 int32
	var fee3 int32
	var fee4 int32

	for rows.Next() {
		err = rows.Scan(&userId, &billingItem1, &billingItem2, &billingItem3, &billingItem4, &fee1, &fee2, &fee3, &fee4)
		CheckErr(err)

		if billingItem1 > 0 && fee1 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem1, Fee: fee1}
			records = append(records, record)
		}
		if billingItem2 > 0 && fee2 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem2, Fee: fee2}
			records = append(records, record)
		}
		if billingItem3 > 0 && fee3 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem3, Fee: fee3}
			records = append(records, record)
		}
		if billingItem4 > 0 && fee4 > 0 {
			var record = CdrRecord{UserId: userId, ItemCode: billingItem4, Fee: fee4}
			records = append(records, record)
		}
	}
	return records, err
}
