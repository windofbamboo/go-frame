package base

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"
	"github.com/wonderivan/logger"
	"sort"
)

type Procedure struct {
	ProcedureHead Head
	ProcedureBody Body

	driverName string
	db *sqlx.DB
	varMap map[string]interface{}
}

type Head struct {
	ProcedureName	string
	InParam		[]Param
	OutParam	[]Param
}

type Body struct{
	Blocks map[int]SqlBlock // cursor
}

type SqlBlock struct{
	StatementType string
	Context interface{}
}

type Loop struct{
	VarName string
	VarType	string
	EnumValue	[]interface{}
	SqlLoop	*SessionContext
}

type Cursor struct{
	CursorSql	*SqlContext
	CursorLoop	*SessionContext
}

type SessionContext struct{
	CommitNum int
	Sqls map[int]*SqlContext
}

type SqlContext struct{
	orderNo	int
	SqlType string
	SqlStr	string
	ResultParams []Param
}

type Param struct{
	ParamName,ParamType string
}

const(
	StatementTypeCursor = "cursor"
	StatementTypeLoop 	= "loop"
	StatementTypeCommon = "common"
)

func (p *Procedure)setExecEnvironment(environmentId string, varMap map[string]interface{}) error{

	if db, err := GetConnPool(environmentId); err!=nil{
		return fmt.Errorf("GetConnPool : %v",err)
	}else{
		p.db = db
		p.driverName = configContent.DbInfos[environmentId].Driver
	}

	p.varMap = varMap
	return nil
}

func (p *Procedure)initEnvironment(){

	p.driverName = NullStr
	p.varMap = make(map[string]interface{})
	p.db = nil
}

func (p *Procedure)execProcedure() error{

	sqlBlocks:=	p.ProcedureBody.Blocks

	var orderSet []int
	for i := range sqlBlocks {
		orderSet = append(orderSet, i)
	}
	sort.Ints(orderSet)

	for _,orderNo := range orderSet {

		block := sqlBlocks[orderNo]

		switch block.StatementType {
		case StatementTypeCursor:
			cursor := block.Context.(Cursor)
			if err:= p.execCursor(&cursor);err!=nil{
				return fmt.Errorf("execCursor : %v",err)
			}
		case StatementTypeLoop:
			loop := block.Context.(Loop)
			if err:= p.execLoop(&loop);err!=nil{
				return fmt.Errorf("execLoop : %v",err)
			}
		case StatementTypeCommon:
			sessionContext := block.Context.(SessionContext)
			if err:= p.execCommonSession(&sessionContext);err!=nil{
				return fmt.Errorf("execCommonSession : %v",err)
			}
		default:
			err:=fmt.Errorf("unknown StatementType : %v",block.StatementType)
			logger.Error(err)
			return fmt.Errorf("unknown StatementType : %v",block.StatementType)
		}
	}
	return nil
}


func (p *Procedure)execCursor(cursor *Cursor) error {

	sqlNew,bindVar,err:= changeSql(p.driverName,cursor.CursorSql.SqlStr,p.varMap)
	if err != nil {
		logger.Error(fmt.Sprintf("parseSql err : [ %v ] ", err))
		return err
	}
	conn, err := p.db.Beginx()
	if err != nil {
		logger.Error(fmt.Sprintf("get conn err : [ %v ] ", err))
		return err
	}
	params := cursor.CursorSql.ResultParams
	var res [][]interface{}
	if len(bindVar)>0{
		res, err= TxQuerySql(conn,&sqlNew,&params,bindVar)
	}else{
		res, err= TxQuerySql(conn,&sqlNew,&params)
	}

	if err != nil {
		logger.Error(fmt.Sprintf("QuerySql err : [ %v ] ", err))
		return err
	}

	commitNum:= cursor.CursorLoop.CommitNum

	var num = 0
	for _, singleRow := range res {
		for i, param := range params {
			paramName:= param.ParamName
			p.varMap[paramName] = singleRow[i]
		}
		err = p.execSessionSql(conn,cursor.CursorLoop.Sqls)
		if err != nil {
			logger.Error(fmt.Sprintf("execSession err : [ %v ] ", err))
			conn.Rollback()
			return err
		}
		num++
		if num>=commitNum{
			err = conn.Commit()
			if err != nil {
				logger.Error(fmt.Sprintf("commit tx err : [ %v ] ", err))
				return err
			}
			conn, err = p.db.Beginx()
			if err != nil {
				logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
				return err
			}
			num = 0
		}
	}
	//提交事务
	if num>=0{
		err = conn.Commit()
		if err != nil {
			logger.Error(fmt.Sprintf("commit tx err : [ %v ] ", err))
			return err
		}
	}
	return nil
}

func (p *Procedure)execLoop(loop *Loop) error {

	conn, err := p.db.Beginx()
	if err != nil {
		logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
		return err
	}

	commitNum:= loop.SqlLoop.CommitNum
	var num = 0
	for _, value := range loop.EnumValue {
		p.varMap[loop.VarName] = value

		err = p.execSessionSql(conn,loop.SqlLoop.Sqls)
		if err != nil {
			logger.Error(fmt.Sprintf("execSession err : [ %v ] ", err))
			conn.Rollback()
			return err
		}
		num++
		if num>=commitNum{
			err = conn.Commit()
			if err != nil {
				logger.Error(fmt.Sprintf("commit tx err : [ %v ] ", err))
				return err
			}
			// 重新获取事务
			conn, err = p.db.Beginx()
			if err != nil {
				logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
				return err
			}
			num = 0
		}
	}
	//提交事务
	if num>=0{
		err = conn.Commit()
		if err != nil {
			logger.Error(fmt.Sprintf("commit tx err : [ %v ] ", err))
			return err
		}
	}
	return nil
}


func (p *Procedure)execCommonSession(sessionContext *SessionContext) error {

	// 获取 tx
	conn, err := p.db.Beginx()
	if err != nil {
		logger.Error(fmt.Sprintf("get tx err : [ %v ] ", err))
		return err
	}

	err= p.execSessionSql(conn,sessionContext.Sqls)
	if err != nil {
		logger.Error(fmt.Sprintf("execSession err : [ %v ] ", err))
		conn.Rollback()
		return err
	}

	err = conn.Commit()
	if err != nil {
		logger.Error(fmt.Sprintf("commit tx err : [ %v ] ", err))
		return err
	}
	return nil
}

//参数含义  数据库连接  session内容  上下文参数
func (p *Procedure)execSessionSql(conn *sqlx.Tx, sqls map[int]*SqlContext) error {

	var orderSet []int
	for i := range sqls {
		orderSet = append(orderSet, i)
	}
	sort.Ints(orderSet)
	//开始执行语句
	for _, orderNo := range orderSet {
		sqlContext := sqls[orderNo]
		sqlNew,bindVar,err:= changeSql(p.driverName,sqlContext.SqlStr,p.varMap)
		if err != nil {
			logger.Error(fmt.Sprintf("parseSql err : [ %v ] ", err))
			return err
		}

		switch sqlContext.SqlType {
		case "select":
			params := sqlContext.ResultParams
			var res [][]interface{}
			var err error
			if len(bindVar)>0{
				res, err= TxQuerySql(conn,&sqlNew,&params,bindVar)
			}else{
				res, err= TxQuerySql(conn,&sqlNew,&params)
			}

			if err != nil {
				logger.Error(fmt.Sprintf("QuerySql err : [ %v ] ", err))
				return err
			}

			for i, param := range params {
				paramName:= param.ParamName
				p.varMap[paramName] = res[0][i]
			}
		case "insert":
			if err = TxInsertSql(conn,&sqlNew,bindVar...); err != nil {
				return err
			}
		case "update":
			if err = TxUpdateSql(conn,&sqlNew,bindVar...); err != nil {
				return err
			}
		case "delete":
			if err = TxDeleteSql(conn,&sqlNew,bindVar...); err != nil {
				return err
			}
		default:
			return fmt.Errorf("SqlType is Wrong ")
		}
	}
	return nil
}

