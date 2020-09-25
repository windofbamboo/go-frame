package base

import (
	"fmt"
	"github.com/beevik/etree"
	"github.com/wonderivan/logger"
	"path/filepath"
	"strconv"
	"strings"
)


type ServerContent struct {
	storeType string
	storeAddr  []string
	connectTimeout,backupLatency int
}

type ProviderContent struct {
	name,host string
	port int
}

type ConsumerContent struct {
	name,host,initStatus string
}

type Job struct {
	EnvId,ProcedureName,InParamStr string
	TimeOut int
}

type TaskJob struct {
	Id string
	Spec string
	Jobs []*Job
}

type ConfigContent struct {
	server ServerContent
	providers []ProviderContent
	consumers []ConsumerContent
	TaskJobs  []*TaskJob
	DbInfos          map[string]*DbInfo
	Procedures		 map[string]*Procedure
}

var configContent ConfigContent

func ReadConfigXml(exPath string) error{

	configFile := filepath.Join(exPath, DefaultConfigPath, DefaultConfigFileName)

	if err := CheckFile(configFile);err!=nil{
		logger.Error(err)
		return err
	}

	doc := etree.NewDocument()
	if err:= doc.ReadFromFile(configFile); err != nil {
		err = fmt.Errorf("read xml file err : %v", err)
		logger.Error(err)
		return err
	}
	root := doc.SelectElement("configuration")
	//服务注册
	server := root.SelectElement("server")
	serverContent,err:=readServerContent(server)
	//provider
	provider:=root.SelectElement("provider")
	providers,err:=readProviderContent(provider)
	if err != nil {
		return fmt.Errorf("readProviderContent err : %v", err)
	}
	//consumer
	consumer:=root.SelectElement("consumer")
	consumers,err:=readConsumerContent(consumer)
	if err != nil {
		return fmt.Errorf("readConsumerContent err : %v", err)
	}

	// 数据库信息
	environments := root.SelectElement("environments")
	dbInfos,err:= readDbInfos(environments)
	if err != nil {
		return fmt.Errorf("get dbInfo err : %v", err)
	}
	// 过程文件
	var procedureMap = make(map[string]*Procedure)
	procedures := root.SelectElement("procedures")
	//指定文件
	for _, procedure := range procedures.SelectElements("procedure") {
		fileName:= procedure.SelectAttr("resource").Value

		procedureFile := filepath.Join(exPath, DefaultConfigPath, ProcedureConfigPath,fileName)
		err = CheckFile(procedureFile)
		if err !=nil{
			continue
		}

		procedure,err:= readProcedureXml(procedureFile)
		if err!=nil{
			continue
		}
		procedureMap[procedure.ProcedureHead.ProcedureName] = procedure
	}
	//指定目录
	for _, procedurePath := range procedures.SelectElements("package") {
		tempPath:= procedurePath.SelectAttr("resource").Value
		filePath := filepath.Join(exPath, DefaultConfigPath, tempPath)
		files,err:= GetAllFiles(filePath)
		if err!=nil{
			return fmt.Errorf("path : %v , get files err : %v",filePath,err)
		}
		for _, file := range files {
			procedure,err:= readProcedureXml(file)
			if err!=nil{
				continue
			}
			procedureMap[procedure.ProcedureHead.ProcedureName] = procedure
		}
	}
	if len(procedureMap) == 0{
		return fmt.Errorf("can not get procedure config info ")
	}

	// 过程文件
	var taskJobs []*TaskJob
	tasks := root.SelectElement("tasks")
	for _, procedure := range tasks.SelectElements("taskFile") {
		fileName:= procedure.SelectAttr("resource").Value

		taskFile := filepath.Join(exPath, DefaultConfigPath, fileName)
		err = CheckFile(taskFile)
		if err !=nil{
			continue
		}

		taskList,err:= readTaskXml(taskFile)
		if err!=nil{
			continue
		}

		for _, taskPtr := range taskList {
			taskJobs = append(taskJobs, taskPtr)
		}
	}

	configContent.server	= serverContent
	configContent.providers = providers
	configContent.consumers = consumers
	configContent.TaskJobs  = taskJobs
	configContent.DbInfos    = dbInfos
	configContent.Procedures = procedureMap

	return nil
}

func readServerContent(server *etree.Element) (ServerContent,error){

	var serverContent ServerContent
	var err error
	if server == nil{
		return serverContent,fmt.Errorf("not found Element <server> ")
	}

	connectTimeout:=server.SelectAttr("connectTimeout")
	if connectTimeout == nil{
		return serverContent,fmt.Errorf("not found Attr [connectTimeout] in Element <server> ")
	}
	serverContent.connectTimeout,err= strconv.Atoi(connectTimeout.Value)
	if err !=nil{
		return serverContent,fmt.Errorf("connectTimeout : %v convert to int err : %v ",connectTimeout.Value,err)
	}

	backupLatency:=server.SelectAttr("backupLatency")
	if backupLatency == nil{
		return serverContent,fmt.Errorf("not found Attr [backupLatency] in Element <server> ")
	}
	serverContent.backupLatency,err= strconv.Atoi(backupLatency.Value)
	if err !=nil{
		return serverContent,fmt.Errorf("backupLatency : %v convert to int err : %v ",backupLatency.Value,err)
	}

	servRegistry:=server.SelectElement("servRegistry")
	if servRegistry == nil{
		return serverContent,fmt.Errorf("not found sub Element [servRegistry] in Element <server> ")
	}
	storeType:=servRegistry.SelectAttr("storeType")
	if storeType == nil{
		return serverContent,fmt.Errorf("not found Attr [storeType] in Element <servRegistry> ")
	}
	serverContent.storeType = storeType.Value

	storeAddr:=servRegistry.SelectAttr("storeAddr")
	if storeAddr == nil{
		return serverContent,fmt.Errorf("not found Attr [storeAddr] in Element <servRegistry> ")
	}
	serverContent.storeAddr = strings.Split(storeAddr.Value,",")

	return serverContent,nil
}

func readProviderContent(provider *etree.Element) ([]ProviderContent,error){

	var providerContents []ProviderContent
	var err error
	if provider == nil{
		return providerContents,fmt.Errorf("not found Element <provider> ")
	}

	instanceList:=provider.SelectElements("instance")
	if len(instanceList)==0{
		return providerContents,fmt.Errorf("not found subElement <instance> in Element <provider> ")
	}

	for _, element := range instanceList {

		var providerContent ProviderContent

		name:=element.SelectAttr("name")
		if name == nil{
			return providerContents,fmt.Errorf("not found Attr [name] in Element <provider> ")
		}
		providerContent.name = name.Value

		host:=element.SelectAttr("host")
		if host == nil{
			return providerContents,fmt.Errorf("not found Attr [host] in Element <provider> ")
		}
		providerContent.host = host.Value

		port:=element.SelectAttr("port")
		if port == nil{
			return providerContents,fmt.Errorf("not found Attr [port] in Element <provider> ")
		}
		providerContent.port,err= strconv.Atoi(port.Value)
		if err !=nil{
			return providerContents,fmt.Errorf("port : %v convert to int err : %v ",port.Value,err)
		}

		providerContents = append(providerContents, providerContent)
	}

	return providerContents, nil
}

func readConsumerContent(consumer *etree.Element) ([]ConsumerContent,error){

	var consumerContents []ConsumerContent
	if consumer == nil{
		return consumerContents,fmt.Errorf("not found Element <consumer> ")
	}

	instanceList:=consumer.SelectElements("instance")
	if len(instanceList)==0{
		return consumerContents,fmt.Errorf("not found subElement <instance> in Element <consumer> ")
	}

	for _, element := range instanceList {

		var consumerContent ConsumerContent

		name:=element.SelectAttr("name")
		if name == nil{
			return consumerContents,fmt.Errorf("not found Attr [name] in Element <consumer> ")
		}
		consumerContent.name = name.Value

		host:=element.SelectAttr("host")
		if host == nil{
			return consumerContents,fmt.Errorf("not found Attr [host] in Element <consumer> ")
		}
		consumerContent.host = host.Value

		initStatus:=element.SelectAttr("initStatus")
		if initStatus == nil{
			return consumerContents,fmt.Errorf("not found Attr [initStatus] in Element <consumer> ")
		}
		consumerContent.initStatus = initStatus.Value

		consumerContents = append(consumerContents, consumerContent)
	}

	return consumerContents, nil
}

func readDbInfos(environments *etree.Element) (map[string]*DbInfo,error){

	var dbInfos = make(map[string]*DbInfo)
	if environments == nil{
		return dbInfos,fmt.Errorf("not found Element <environments> ")
	}

	environmentList:=environments.SelectElements("environment")
	if len(environmentList) == 0{
		return dbInfos,fmt.Errorf("not found sub Element <environment> in Element <environment> ")
	}

	for i, environment := range environmentList {

		var dbInfo DbInfo
		var err error

		id:=environment.SelectAttrValue("id","none")
		if id == "none"{
			return nil, fmt.Errorf("not found Attr [id] in Element <environment>, orderNo is %v",i)
		}
		dbInfo.ConnectName = id

		if poolSize := environment.SelectElement("poolSize"); poolSize != nil {

			maxIdleConnS:=poolSize.SelectAttr("maxIdleConnS")
			if maxIdleConnS == nil{
				return nil,fmt.Errorf("not found Attr [maxIdleConnS] in sub element <poolSize> environment.id = %v",id)
			}
			dbInfo.MaxIdleConnS,err= strconv.Atoi(maxIdleConnS.Value)
			if err!=nil{
				return nil, fmt.Errorf("backupLatency : %v convert to int err : %v ",maxIdleConnS.Value,err)
			}

			maxOpenConnS:=poolSize.SelectAttr("maxOpenConnS")
			if maxOpenConnS == nil{
				return nil,fmt.Errorf("not found Attr [maxOpenConnS] in sub element <poolSize> environment.id = %v",id)
			}
			dbInfo.MaxOpenConnS,err= strconv.Atoi(maxOpenConnS.Value)
			if err!=nil{
				return nil, fmt.Errorf("backupLatency : %v convert to int err : %v ",maxOpenConnS.Value,err)
			}

			connMaxLifeTime:=poolSize.SelectAttr("connMaxLifeTime")
			if connMaxLifeTime == nil{
				return nil,fmt.Errorf("not found Attr [connMaxLifeTime] in sub element <poolSize> environment.id = %v",id)
			}
			dbInfo.ConnMaxLifeTime,err= strconv.Atoi(connMaxLifeTime.Value)
			if err!=nil{
				return nil, fmt.Errorf("backupLatency : %v convert to int err : %v ",connMaxLifeTime.Value,err)
			}
		}else{
			return nil,fmt.Errorf("not found sub Element <poolSize> in Element environment.id = %v ",id)
		}

		if driver:=environment.SelectElement("driver"); driver !=nil{
			driverType:=driver.SelectAttr("type").Value
			dbInfo.Driver = driverType

			var propertys = make(map[string]string)
			for _,property:= range driver.SelectElements("property"){
				key:=strings.ToLower(property.SelectAttr("name").Value)
				value:=property.SelectAttr("value").Value
				propertys[key]=value
			}

			if driverType == DriverTypePostgres{
				user,ok:=propertys["user"]
				if !ok{return nil,fmt.Errorf("can't find [user] in propertys, environment id = %v",id) }
				password,ok:=propertys["password"]
				if !ok{return nil,fmt.Errorf("can't find [password] in propertys, environment id = %v",id) }
				host,ok:=propertys["host"]
				if !ok{return nil,fmt.Errorf("can't find [host] in propertys, environment id = %v",id) }
				port,ok:=propertys["port"]
				if !ok{return nil,fmt.Errorf("can't find [port] in propertys, environment id = %v",id) }
				dbname,ok:=propertys["dbname"]
				if !ok{return nil,fmt.Errorf("can't find [dbname] in propertys, environment id = %v",id) }
				sslmode,ok:=propertys["sslmode"]
				if !ok{return nil,fmt.Errorf("can't find [sslmode] in propertys, environment id = %v",id) }

				connStr := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v sslmode=%v ",
					user,password,host,port,dbname,sslmode)
				dbInfo.ConnStr = connStr
			}else if driverType == DriverTypeOracle {

				user,ok:=propertys["user"]
				if !ok{return nil,fmt.Errorf("can't find [user] in propertys, environment id = %v",id) }
				password,ok:=propertys["password"]
				if !ok{return nil,fmt.Errorf("can't find [password] in propertys, environment id = %v",id) }
				tns,ok:=propertys["tns"]
				if !ok{return nil,fmt.Errorf("can't find [tns] in propertys, environment id = %v",id) }

				connStr := fmt.Sprintf("%v/%v@%v",user,password,tns)
				dbInfo.ConnStr = connStr
			}else if driverType == DriverTypeMysql {

				user,ok:=propertys["user"]
				if !ok{return nil,fmt.Errorf("can't find [user] in propertys, environment id = %v",id) }
				password,ok:=propertys["password"]
				if !ok{return nil,fmt.Errorf("can't find [password] in propertys, environment id = %v",id) }
				host,ok:=propertys["host"]
				if !ok{return nil,fmt.Errorf("can't find [host] in propertys, environment id = %v",id) }
				port,ok:=propertys["port"]
				if !ok{return nil,fmt.Errorf("can't find [port] in propertys, environment id = %v",id) }
				dbname,ok:=propertys["dbname"]
				if !ok{return nil,fmt.Errorf("can't find [dbname] in propertys, environment id = %v",id) }

				connStr := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", user, password, host, port, dbname)
				dbInfo.ConnStr = connStr
			}else{
				return nil,fmt.Errorf("database id : %v , driverType is unknown ",dbInfo.ConnectName)
			}
		}else{
			return nil,fmt.Errorf("not found sub Element <driver> in Element environment.id = %v ",id)
		}

		dbInfos[id] = &dbInfo
	}
	return dbInfos, nil
}

func readTaskXml(fileName string)(taskJobs []*TaskJob,err error){


	doc := etree.NewDocument()
	if err = doc.ReadFromFile(fileName); err != nil {
		return nil, err
	}

	root := doc.SelectElement("tasks")
	if root ==nil{
		return nil,fmt.Errorf("not found Element <tasks> in %v ",fileName)
	}

	tasks:=root.SelectElements("task")
	if tasks==nil{
		return nil,fmt.Errorf("not found Element [tasks] in %v ",fileName)
	}

	for i, task := range tasks {
		var taskJob TaskJob
		taskJob.Id 	 = task.SelectAttrValue("id",NullStr)
		taskJob.Spec = task.SelectAttrValue("cron",NullStr)

		if taskJob.Id == NullStr{
			return nil,fmt.Errorf("the %v task not found attr [id] in file %v ",i,fileName)
		}
		if taskJob.Spec== NullStr{
			return nil,fmt.Errorf("not found attr [cron] of task.id= %v in file %v ",taskJob.Id,fileName)
		}

		jobs:=task.SelectElements("job")
		if jobs==nil{
			return nil,fmt.Errorf("not found sub Element [job] of task.id= %v in file %v ",taskJob.Id,fileName)
		}
		for _, job := range jobs {
			var jobInstance Job
			jobInstance.EnvId = job.SelectAttrValue("envId",NullStr)
			jobInstance.ProcedureName = job.SelectAttrValue("name",NullStr)
			jobInstance.InParamStr = job.SelectAttrValue("inParam",NullStr)
			timeOut:= job.SelectAttrValue("timeOut","-1")
			jobInstance.TimeOut,err = strconv.Atoi(timeOut)
			if err!=nil{
				return nil, fmt.Errorf("timeOut : %v convert to int err : %v ",timeOut,err)
			}

			if jobInstance.EnvId == NullStr{
				return nil,fmt.Errorf("not found attr [envId] in sub Element <job> of task.id= %v in file %v ",taskJob.Id,fileName)
			}
			if jobInstance.ProcedureName == NullStr{
				return nil,fmt.Errorf("not found attr [name] in sub Element <job> of task.id= %v in file %v ",taskJob.Id,fileName)
			}
			taskJob.Jobs = append(taskJob.Jobs, &jobInstance)
		}
		taskJobs = append(taskJobs, &taskJob)
	}
	return taskJobs,nil
}


func readProcedureXml(fileName string ) (*Procedure,error){

	doc := etree.NewDocument()
	if err := doc.ReadFromFile(fileName); err != nil {
		return nil, err
	}

	root := doc.SelectElement("procedure")
	if root ==nil{
		return nil,fmt.Errorf("not found Element <procedure> ")
	}

	var procedure Procedure
	//Head
	name:=root.SelectAttr("name")
	if name==nil{
		return nil,fmt.Errorf("not found attr [name] in %v ",fileName)
	}

	procedure.ProcedureHead.ProcedureName = name.Value

	params := root.SelectElement("params")
	if params!=nil{
		var inParams []Param
		for _,param:= range params.SelectElements("inParam"){
			inParam:= Param{ParamName: strings.ToLower(param.SelectAttr("name").Value),
				ParamType:param.SelectAttr("type").Value}
			inParams = append(inParams,inParam)
		}
		procedure.ProcedureHead.InParam = inParams

		var outParams []Param
		for _,param:= range params.SelectElements("outParam"){
			outParam:= Param{ParamName: strings.ToLower(param.SelectAttr("name").Value),
				ParamType:param.SelectAttr("type").Value}
			outParams = append(outParams,outParam)
		}
		procedure.ProcedureHead.OutParam = outParams
	}

	//Body
	blocks := root.SelectElement("blocks")
	if blocks ==nil{
		return nil,fmt.Errorf("not found Element <blocks> in %v",fileName)
	}

	var blockMap = make(map[int]SqlBlock)
	for _, block := range blocks.SelectElements("block") {
		value := block.SelectAttrValue("orderNo","1")
		orderNo,err := strconv.Atoi(value)
		if err!=nil{
			return nil, fmt.Errorf("orderNo [%v] is err in %v ",value,fileName)
		}

		if _,ok:= blockMap[orderNo];ok{
			return nil, fmt.Errorf("block orderNo : %v appears many times in %v",orderNo,fileName)
		}

		var sqlBlock SqlBlock
		sqlBlock.StatementType = block.SelectAttrValue("statementType","common")

		switch sqlBlock.StatementType {
		case StatementTypeCursor:
			cursorElement:= block.SelectElement(StatementTypeCursor)
			cursor,err:= parseCursor(cursorElement)
			if err!=nil{
				return nil, fmt.Errorf("parseCursor err : %v", err.Error())
			}else{
				sqlBlock.Context = *cursor
			}
		case StatementTypeLoop:
			loopElement:= block.SelectElement(StatementTypeLoop)
			loop,err:= parseLoop(loopElement)
			if err!=nil{
				return nil, fmt.Errorf("parseLoop err : %v" , err.Error())
			}else{
				sqlBlock.Context = *loop
			}
		default:
			//commElement:= block.SelectElement(db.StatementTypeCommon)
			comm,err:= parseCommon(block)
			if err!=nil{
				return nil, fmt.Errorf("parseCommon err : %v" , err.Error())
			}else{
				sqlBlock.Context = *comm
			}
		}
		blockMap[orderNo] = sqlBlock
	}
	procedure.ProcedureBody.Blocks = blockMap
	return &procedure, nil
}

func parseCursor(element *etree.Element) (*Cursor,error){

	var cursor Cursor
	//cursorSql
	cursorSql:= element.SelectElement("cursorSql")
	sqlContext,err:= parseSql(cursorSql)
	if err!=nil{
		return nil, err
	}
	cursor.CursorSql = sqlContext
	//cursorLoop
	cursorLoop:=element.SelectElement("cursorLoop")
	sessionElement:= cursorLoop.SelectElement("session")
	if sessionElement!=nil{
		sessionContext,err:= parseSession(sessionElement)
		if err==nil{
			cursor.CursorLoop = sessionContext
		}else{
			return nil, err
		}
	}
	return &cursor,nil
}

func parseLoop(element *etree.Element) (*Loop,error){

	var context Loop
	varName := element.SelectAttr("varName")
	if varName ==nil{
		return nil, fmt.Errorf("not found varName in loop")
	}
	varType := element.SelectAttr("varType")
	if varType ==nil{
		return nil, fmt.Errorf("not found varType in loop")
	}
	enumValue:= element.SelectAttr("enumValue")
	if enumValue ==nil{
		return nil, fmt.Errorf("not found enumValue in loop")
	}
	context.VarName = varName.Value
	context.VarType = varType.Value

	for _, subStr := range strings.Split(enumValue.Value, ",") {
		if strings.ToLower(context.VarType)  == ParamTypeInt {
			value,err := strconv.Atoi(subStr)
			if err!=nil{
				return nil, err
			}
			context.EnumValue = append(context.EnumValue, value)
		}else if strings.ToLower(context.VarType) == ParamTypeString {
			context.EnumValue = append(context.EnumValue, subStr)
		}else{
			return nil, fmt.Errorf(" type : %v is unsupported",context.VarType)
		}
	}

	//sql
	sessionElement:= element.SelectElement("session")
	if sessionElement==nil{
		return nil, fmt.Errorf("not exists session Element ")
	}
	sessionContext,err:= parseSession(sessionElement)
	if err==nil{
		context.SqlLoop = sessionContext
	}else{
		return nil, err
	}

	return &context,nil
}

func parseCommon(element *etree.Element) (*SessionContext,error){

	sessionElement:= element.SelectElement("session")
	if sessionElement==nil{
		return nil, fmt.Errorf("not exists session Element ")
	}
	
	sessionContext,err:= parseSession(sessionElement)
	if err!=nil{
		return nil, err
	}else{
		return sessionContext, nil
	}
}


func parseSession(element *etree.Element) (*SessionContext,error){

	var context SessionContext
	var err error
	context.CommitNum,err =	strconv.Atoi(element.SelectAttr("commitNum").Value)
	if err!=nil{
		return nil, err
	}

	var sqls = make(map[int]*SqlContext)
	for _, sql := range element.SelectElements("Sql"){
		var orderNo int
		orderNo,err = strconv.Atoi(sql.SelectAttrValue("orderNo","1"))
		if err!=nil{
			return nil, fmt.Errorf("orderNo value is err ")
		}

		if _,ok:= sqls[orderNo];ok{
			return nil, fmt.Errorf("Sql orderNo : %v appears many times ",orderNo)
		}

		sqlContext,err:= parseSql(sql)
		if err!=nil{
			return nil, err
		}
		sqls[orderNo] = sqlContext
	}
	context.Sqls=sqls

	return &context,err
}

func parseSql(element *etree.Element) (*SqlContext,error){

	var sqlContext SqlContext
	//sqlType
	sqlContext.SqlType = element.SelectAttrValue("sqlType","select")
	//param
	var resultVars []string
	var varTypes []string
	str:=element.SelectAttr("resultVars")
	if str!=nil{
		for _, subStr := range strings.Split(str.Value, ",") {
			resultVars = append(resultVars, subStr)
		}
	}
	str=element.SelectAttr("varTypes")
	if str!=nil{
		for _, subStr := range strings.Split(str.Value, ",") {
			varTypes = append(varTypes, subStr)
		}
	}
	if len(resultVars)==0 && sqlContext.SqlType == "select" {
		return &SqlContext{}, fmt.Errorf("has no resultVars in select Sql")
	}
	if len(resultVars)!=len(varTypes) {
		return &SqlContext{}, fmt.Errorf("varTypes num is not equals resultVars num")
	}
	for i:=0;i<len(resultVars);i++{
		param:= Param{ParamName: resultVars[i],ParamType:varTypes[i]}
		sqlContext.ResultParams = append(sqlContext.ResultParams, param)
	}
	//sql
	sqlContext.SqlStr=strings.TrimSpace(element.Text())
	return &sqlContext, nil
}


func CheckInstance(instanceType string, instanceName string) error{
	switch instanceType {
	case InstanceTypeProvider:
		for _, s := range configContent.providers {
			if s.name == instanceName{
				return nil
			}
		}
	case InstanceTypeConsumer:
		for _, c := range configContent.consumers {
			if c.name == instanceName{
				return nil
			}
		}
	default:
	}
	return fmt.Errorf("not found instanceName in configFile")
}


func ParseInParam(procedureName,inParamStr string) (varMap map[string]interface{},err error){

	procedure,ok:= configContent.Procedures[procedureName]
	if !ok{
		err = fmt.Errorf("procedure : %v  is not exists in config.xml ",procedureName)
		logger.Error(err)
		return nil,err
	}
	var paramList []string
	if len(inParamStr)>0{
		paramList = strings.Split(inParamStr, SeparatorCharacter)
	}

	inParam:= procedure.ProcedureHead.InParam
	inParamNum:= len(inParam)
	argsNum:=len(paramList)
	if argsNum!= inParamNum{
		err = fmt.Errorf("procedure %v inparam num must be %v , not %v", procedureName,inParamNum,argsNum)
		logger.Error(err)
		return nil,err
	}

	varMap = make(map[string]interface{})
	//入参校验
	for i:=0;i<inParamNum;i++{
		param:=inParam[i]
		value := paramList[i]
		if strings.ToLower(param.ParamType) == ParamTypeInt {
			i64, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				err = fmt.Errorf("procedure[%v] inParam [%v] converting %v string to int: %v",
					procedureName,param.ParamName,  value,  err)
				logger.Error(err)
				return nil,err
			}
			varMap[param.ParamName] = int(i64)
		}else if strings.ToLower(param.ParamType) == ParamTypeString {
			varMap[param.ParamName] = value
		}
	}
	return varMap,nil
}



