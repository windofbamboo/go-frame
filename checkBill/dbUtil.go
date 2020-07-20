package checkBill

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"github.com/wonderivan/logger"
	"strings"
	"time"
)

var poolMap = make(map[string]*sql.DB)

func InitPoolMap() error {

	var durationTime = 1 * time.Second

	for _, dbInfo := range configContent.DbInfos {
		db, err := sql.Open(dbInfo.Driver, dbInfo.ConnStr)
		if err != nil {
			return err
		}

		for i := 0; i < dbInfo.ConnMaxLifeTime; i++ {
			durationTime += durationTime
		}

		db.SetMaxIdleConns(dbInfo.MaxIdleConnS)
		db.SetMaxOpenConns(dbInfo.MaxOpenConnS)
		db.SetConnMaxLifetime(durationTime)

		poolMap[dbInfo.ConnectName] = db
	}
	return nil
}

func DestroyPoolMap() {
	for _, db := range poolMap {
		db.Close()
	}
}

func GetConnPool(dataSource string, args ...string) (*sql.DB, error) {

	if dataSource != DataSourceCdr && dataSource != DataSourceBill &&
		dataSource != DataSourceResult && dataSource != DataSourceITable {
		return nil, errors.New("dataSource is unKnown ")
	}

	var regionCode = DefaultRegionCode
	if len(args) > 0 {
		regionCode = args[0]
	}

	var dbName string
	switch dataSource {
	case DataSourceCdr:
		if task, ok := configContent.Tasks[regionCode]; ok {
			dbName = task.CdrDb
		} else {
			return nil, errors.New("regionCode is unKnown ")
		}
	case DataSourceBill:
		if task, ok := configContent.Tasks[regionCode]; ok {
			dbName = task.BillDb
		} else {
			return nil, errors.New("regionCode is unKnown ")
		}
	case DataSourceResult:
		dbName = configContent.CommonAttributes[strings.ToLower(AttributeAuditInfoDb)].(string)
	case DataSourceITable:
		dbName = configContent.CommonAttributes[strings.ToLower(AttributeInterfaceTableDb)].(string)
	default:
		return nil, errors.New("dataSource : " + dataSource + " is unKnown ")
	}

	db, ok := poolMap[dbName]
	if !ok {
		return nil, errors.New("dataSource : " + dataSource + " , dbName : " + dbName + " is unKnown ")
	}
	return db, nil
}

func QuerySql(db *sql.DB, Sql *string, args ...interface{}) (*sql.Rows, error) {

	stmt, err := db.Prepare(*Sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func DeleteSql(db *sql.DB, Sql *string, args ...interface{}) error {

	// 获取 tx
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 获取 stmt
	stmt, err := db.Prepare(*Sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	result, err := stmt.Exec(args...)
	if err != nil {
		return err
	}
	err = tx.Commit()

	affect, err := result.RowsAffected()
	if err == nil {
		paramStr := "param.{ "
		for _, arg := range args {
			paramStr = paramStr + fmt.Sprint(arg) + " "
		}
		paramStr = paramStr + "}"

		logger.Trace(fmt.Sprintf("sql : [%s] %s delete %d rows ", *Sql, paramStr, affect))
		return err
	}
	return err
}

func UpdateSql(db *sql.DB, Sql *string, args ...interface{}) error {

	// 获取 tx
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 获取 stmt
	stmt, err := db.Prepare(*Sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	result, err := stmt.Exec(args...)
	if err != nil {
		return err
	}
	err = tx.Commit()

	affect, err := result.RowsAffected()
	if err == nil {
		paramStr := "param.{ "
		for _, arg := range args {
			paramStr = paramStr + fmt.Sprint(arg) + " "
		}
		paramStr = paramStr + "}"

		logger.Trace(fmt.Sprintf("sql : [ %s ] %s update %d rows ", *Sql, paramStr, affect))
		return err
	}
	return err
}

func InsertSql(db *sql.DB, Sql *string, args ...interface{}) error {

	paramStr := "param.{ "
	for _, arg := range args {
		paramStr = paramStr + fmt.Sprint(arg) + " "
	}
	paramStr = paramStr + "}"
	logger.Trace(fmt.Sprintf("sql : [ %s ] %s", *Sql, paramStr))

	// 获取 tx
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 获取 stmt
	stmt, err := db.Prepare(*Sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(args...)
	if err != nil {
		return err
	}
	err = tx.Commit()
	return err
}
