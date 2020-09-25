package base

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"
	"github.com/wonderivan/logger"
	"strconv"
	"strings"
	"time"
)

type DbInfo struct {
	ConnectName     string
	Driver          string
	ConnStr         string
	MaxIdleConnS    int
	MaxOpenConnS    int
	ConnMaxLifeTime int
}

var poolMap = make(map[string]*sqlx.DB)

func InitPoolMap() {

	logger.Trace("init dbPool begin ...")
	dbInfos:= configContent.DbInfos

	var durationTime time.Duration

	for _, dbInfo := range dbInfos {
		logger.Trace( fmt.Sprintf("driver: %v , connStr : %v",dbInfo.Driver, dbInfo.ConnStr))

		db, err := sqlx.Open(dbInfo.Driver, dbInfo.ConnStr)
		if err != nil {
			panic(err)
		}

		durationTime = GetSecondTime(dbInfo.ConnMaxLifeTime)

		db.SetMaxIdleConns(dbInfo.MaxIdleConnS)
		db.SetMaxOpenConns(dbInfo.MaxOpenConnS)
		db.SetConnMaxLifetime(durationTime)

		poolMap[dbInfo.ConnectName] = db
	}
	logger.Trace("init dbPool end ...")
}

func DestroyPoolMap() {
	for _, db := range poolMap {
		if err:=db.Close();err!=nil{
			logger.Error(fmt.Sprintf("close db err : %v ", err))
		}
	}
}

func GetConnPool(dataSource string) (*sqlx.DB, error) {

	db, ok := poolMap[dataSource]
	if !ok {
		logger.Error(fmt.Sprintf("dataSource : %v is unKnown ", dataSource))
		return nil, errors.New("dataSource : " + dataSource + " is unKnown ")
	}
	return db, nil
}

// 查询
func QuerySql(db *sqlx.DB, Sql *string, args ...interface{}) (*sqlx.Rows, error) {

	rows, err := db.Queryx(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("stmt Query err : [ %v ] ", err))
		return nil, err
	}
	return rows, nil
}

//执行sql 自身维护事务
func InsertSql(db *sqlx.DB, Sql *string, args ...interface{}) error {

	conn, err := db.Beginx()
	if err != nil {
		logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
		return err
	}

	result, err := conn.Exec(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("insert sql exec err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		logger.Error(fmt.Sprintf("insert sql exec err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	err = conn.Commit()
	logger.Trace(fmt.Sprintf("insert success: %v", id))
	return err
}

//执行sql 自身维护事务
func UpdateSql(db *sqlx.DB, Sql *string, args ...interface{}) error {

	conn, err := db.Beginx()
	if err != nil {
		logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
		return err
	}

	result, err := conn.Exec(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("update sql exec err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	num, err := result.RowsAffected()
	if err != nil {
		logger.Error(fmt.Sprintf("update sql exec err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	err = conn.Commit()
	logger.Trace(fmt.Sprintf("update success: %v", num))
	return err
}

//执行sql 自身维护事务
func DeleteSql(db *sqlx.DB, Sql *string, args ...interface{}) error {

	conn, err := db.Beginx()
	if err != nil {
		logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
		return err
	}

	result, err := conn.Exec(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("delete sql exec err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	num, err := result.RowsAffected()
	if err != nil {
		logger.Error(fmt.Sprintf("delete sql exec err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	err = conn.Commit()
	logger.Trace(fmt.Sprintf("delete success: %v", num))
	return err
}


func TxQuerySql(conn *sqlx.Tx, Sql *string, params *[]Param, args ...interface{}) ([][]interface{}, error) {

	var columns []interface{}
	for i:=0;i<len(*params);i++{
		var column string
		columns = append(columns, &column)
	}

	stmt, err := conn.Preparex(*Sql)
	if err != nil {
		logger.Error(fmt.Sprintf("get stmt err : [ %v ] ", err))
		return nil, err
	}
	defer stmt.Close()

	var rows *sql.Rows
	rows, err = stmt.Query(args...)
	if err != nil {
		logger.Error(fmt.Sprintf("stmt Query err : [ %v ] ", err))
		return nil, err
	}

	var res [][]interface{}
	for rows.Next(){

		err := rows.Scan(columns...)
		if err != nil {
			return nil,fmt.Errorf( "get value err : %v",err)
		}

		var singleRow []interface{}
		for i, param := range *params {
			value:=columns[i]
			sValue:= value.(*string)
			if strings.ToLower(param.ParamType) == ParamTypeInt {
				i64, err := strconv.ParseInt(*sValue, 10, 64)
				if err != nil {
					return nil,fmt.Errorf("converting %v string to int: %v", *sValue,  err)
				}
				singleRow = append(singleRow, i64)
			}else if strings.ToLower(param.ParamType) == ParamTypeString {
				singleRow = append(singleRow, *sValue)
			}
		}
		res = append(res,singleRow)
	}

	return res, nil
}

//执行sql 自身不维护事务 ，事务在外部维护
func TxInsertSql(conn *sqlx.Tx, Sql *string, args ...interface{}) error {

	var result sql.Result
	var affect int64
	var err error

	result, err = conn.Exec(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("insert sql exec err : [ %v ] ", err))
		return err
	}

	affect, err = result.RowsAffected()
	if err != nil {
		logger.Error(fmt.Sprintf("insert sql exec err : [ %v ] ", err))
		return err
	}

	logger.Trace(fmt.Sprintf("insert %d rows success ", affect))
	return nil
}

//执行sql 自身不维护事务 ，事务在外部维护
func TxUpdateSql(conn *sqlx.Tx, Sql *string, args ...interface{}) error {

	var result sql.Result
	var affect int64
	var err error

	result, err = conn.Exec(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("update sql exec err : [ %v ] ", err))
		return err
	}

	affect, err = result.RowsAffected()
	if err != nil {
		logger.Error(fmt.Sprintf("update sql exec err : [ %v ] ", err))
		return err
	}

	paramStr := "param.{ "
	for _, arg := range args {
		paramStr = paramStr + fmt.Sprint(arg) + " "
	}
	paramStr = paramStr + "}"
	logger.Trace(fmt.Sprintf("sql : [%s] %s update %d rows ", *Sql, paramStr, affect))
	return nil
}

//执行sql 自身不维护事务 ，事务在外部维护
func TxDeleteSql(conn *sqlx.Tx, Sql *string, args ...interface{}) error {

	var result sql.Result
	var affect int64
	var err error

	result, err = conn.Exec(*Sql,args...)
	if err != nil {
		logger.Error(fmt.Sprintf("delete sql exec err : [ %v ] ", err))
		return err
	}

	affect, err = result.RowsAffected()
	if err != nil {
		logger.Error(fmt.Sprintf("delete sql exec err : [ %v ] ", err))
		return err
	}

	paramStr := "param.{ "
	for _, arg := range args {
		paramStr = paramStr + fmt.Sprint(arg) + " "
	}
	paramStr = paramStr + "}"
	logger.Trace(fmt.Sprintf("sql : [%s] %s delete %d rows ", *Sql, paramStr, affect))
	return nil
}

/**
	sql语句的替换规则
	把语句中的变量，进行替换
	#{} , 替换成绑定变量; 并在varMap中找到对应的值, 存放到 args中
	${} , 替换字符串
*/
func changeSql(driver, sqlStr string, varMap map[string]interface{}) (string,[]interface{},error) {

	var replaceSqlVar = func(sqlOld string,varMap map[string]interface{}) (string,error){

		index:=strings.Index(sqlOld,"$")
		if index==-1{
			return sqlOld,nil
		}
		var sqlCharNew string = ""
		var st int = 0

		isVariable:=false
		for i := range sqlOld {
			if sqlOld[i:i+1]=="$"{
				if sqlOld[i+1:i+2]!="{" {
					return sqlOld,errors.New(fmt.Sprintf("第%v位分隔符错误 err!",i))
				}
				st=i+2
				isVariable=true
				continue
			}
			if sqlOld[i:i+1]=="}"&&isVariable{
				varName:=sqlOld[st:i]
				if value,ok:=varMap[varName];!ok{
					return sqlOld,errors.New(fmt.Sprintf("变量 %v 没有找到 ",varName))
				}else{
					substr:=fmt.Sprintf("%v",value)
					sqlCharNew += substr
				}
				isVariable=false
				continue
			}

			if !isVariable{
				sqlCharNew += sqlOld[i:i+1]
			}
		}
		return sqlCharNew,nil
	}

	var SqlBindVar = func(driver,sqlOld string,varMap map[string]interface{}) (string,[]interface{},error){

		var bindVar string
		switch driver{
		case "postgres":
			bindVar = "$"
		case "oci8":
			bindVar = ":"
		case "mysql":
			bindVar = "?"
		default:
			bindVar = "?"
		}

		index:=strings.Index(sqlOld,"#")
		if index==-1{
			return sqlOld,[]interface{}{},nil
		}
		var sqlCharNew string = ""
		var bindVars []interface{}
		var st int = 0
		var bindNo int = 1

		isVariable:=false
		for i := range sqlOld {
			if sqlOld[i:i+1]=="#"{
				if sqlOld[i+1:i+2]!="{" {
					return sqlOld,[]interface{}{},errors.New(fmt.Sprintf("第%v位分隔符错误 err!",i))
				}
				st=i+2
				isVariable=true
				continue
			}
			if sqlOld[i:i+1]=="}"&&isVariable{
				varName:=sqlOld[st:i]
				if value,ok:=varMap[varName];!ok{
					return sqlOld,[]interface{}{},errors.New(fmt.Sprintf("变量 %v 没有找到 ",varName))
				}else{
					if driver == "postgres" || driver == "oci8"{
						sqlCharNew += " "+bindVar+strconv.Itoa(bindNo)+" "
					}else{
						sqlCharNew += " "+bindVar+" "
					}
					bindVars = append(bindVars,value)
					bindNo++
				}
				isVariable=false
				continue
			}
			if !isVariable{
				sqlCharNew += sqlOld[i:i+1]
			}
		}
		return sqlCharNew,bindVars,nil
	}

	//替换 ${}
	sql1,err:=replaceSqlVar(sqlStr,varMap)
	if err!=nil{
		return sqlStr,[]interface{}{},nil
	}
	return SqlBindVar(driver,sql1,varMap)
}


