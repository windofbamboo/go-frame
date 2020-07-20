package checkBill

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"github.com/wonderivan/logger"
	"reflect"
	"strconv"
	"strings"
)

// 配置文件 db配置
type DbInfo struct {
	ConnectName     string
	Driver          string
	ConnStr         string
	MaxIdleConnS    int
	MaxOpenConnS    int
	ConnMaxLifeTime int
}

// 配置文件 地州任务配置
type Task struct {
	RegionCode string
	BillDb     string
	CdrDb      string
	TaskNum    int
}

// 配置文件 内容
type ConfigContent struct {
	CommonAttributes map[string]interface{}
	CdrTables        map[int32][]string
	DbInfos          map[string]DbInfo
	Tasks            map[string]Task
}

var configContent ConfigContent

func ReadConfig(cfg string) error {

	if err := Init(cfg); err != nil {
		panic(err)
	}

	// common
	var commonAttributes = make(map[string]interface{})

	commonContent := viper.GetStringMap("common")
	for k, v := range commonContent {
		commonAttributes[k] = v
	}

	// cdrTable
	var cdrTables = make(map[int32][]string)

	cdrTableContent := viper.GetStringMap("cdrTable")
	for k, v := range cdrTableContent {
		var tableType = k

		if reflect.TypeOf(v).Kind() != reflect.String {
			logger.Warn(fmt.Sprintf("tableType : %v , value is not string", tableType))
			continue
		}
		var headStr = v.(string)

		headList := strings.Split(headStr, SeparatorCharacter)
		var heads []string
		for _, tableHead := range headList {
			heads = append(heads, tableHead)
		}

		if len(heads) > 0 {
			switch tableType {
			case strings.ToLower(BillTabPrefixName):
				cdrTables[BillTabPrefix] = heads
			case strings.ToLower(BillTabPrefixNoRegionName):
				cdrTables[BillTabPrefixNoRegion] = heads
			case strings.ToLower(BillTabPrefixModName):
				cdrTables[BillTabPrefixMod] = heads
			default:
				logger.Warn(fmt.Sprintf("inactive tableType : %s", tableType))
			}
		}
	}

	if len(cdrTables) == 0 {
		return errors.New("cdrTable is empty ")
	}

	// dbConnect
	var dbInfos = make(map[string]DbInfo)

	dbContent := viper.GetStringMap("dbInfo")
	for k, vMap := range dbContent {
		var dbInfo DbInfo
		dbInfo.ConnectName = k

		contentMap := vMap.(map[string]interface{})
		driver, ok := contentMap[strings.ToLower("driver")]
		if ok {
			if reflect.TypeOf(driver).Kind() != reflect.String {
				logger.Warn(fmt.Sprintf("dbInfo : %v , driver : %v , value is not string", dbInfo.ConnectName, driver))
				continue
			}
			dbInfo.Driver = driver.(string)
		} else {
			if reflect.TypeOf(driver).Kind() != reflect.String {
				logger.Warn(fmt.Sprintf("dbInfo : %v , not has driver", dbInfo.ConnectName))
				continue
			}
		}

		if connContent, ok := contentMap[strings.ToLower("connStr")]; ok {
			connMap := connContent.(map[string]interface{})
			if driver == "postgres" {
				user, sok1 := connMap[strings.ToLower("user")]
				password, sok2 := connMap[strings.ToLower("password")]
				host, sok3 := connMap[strings.ToLower("host")]
				port, sok4 := connMap[strings.ToLower("port")]
				dbname, sok5 := connMap[strings.ToLower("dbname")]
				sslmode, sok6 := connMap[strings.ToLower("sslmode")]

				if sok1 && sok2 && sok3 && sok4 && sok5 && sok6 {
					connStr := "user=" + user.(string) + " password=" + password.(string) + " host=" + host.(string) +
						" port=" + strconv.Itoa(port.(int)) + " dbname=" + dbname.(string) + " sslmode=" + sslmode.(string)
					dbInfo.ConnStr = connStr
				}
			} else if driver == "oci8" {
				user, sok1 := connMap[strings.ToLower("user")]
				password, sok2 := connMap[strings.ToLower("password")]
				tns, sok3 := connMap[strings.ToLower("tns")]

				if sok1 {
					if reflect.TypeOf(user).Kind() != reflect.Int {
						logger.Warn(fmt.Sprintf("dbInfo: %v , user in connstr is not string", dbInfo.ConnectName))
					}
				} else {
					logger.Warn(fmt.Sprintf("dbInfo: %v , driver is :%v, must has user in connstr",
						dbInfo.ConnectName, driver))
				}

				if sok2 {
					if reflect.TypeOf(password).Kind() != reflect.Int {
						logger.Warn(fmt.Sprintf("dbInfo: %v , password in connstr is not string", dbInfo.ConnectName))
					}
				} else {
					logger.Warn(fmt.Sprintf("dbInfo: %v , driver is :%v, must has password in connstr",
						dbInfo.ConnectName, driver))
				}

				if sok3 {
					if reflect.TypeOf(tns).Kind() != reflect.Int {
						logger.Warn(fmt.Sprintf("dbInfo: %v , tns in connstr is not string", dbInfo.ConnectName))
					}
				} else {
					logger.Warn(fmt.Sprintf("dbInfo: %v , driver is :%v, must has tns in connstr",
						dbInfo.ConnectName, driver))
				}

				if sok1 && sok2 && sok3 {
					connStr := user.(string) + "/" + password.(string) + "@" + tns.(string)
					dbInfo.ConnStr = connStr
				}
			} else {
				logger.Warn(fmt.Sprintf("dbInfo: %v , driver :%v, is unkrown",
					dbInfo.ConnectName, driver))
			}
		}
		if poolContent, ok := contentMap[strings.ToLower("poolSize")]; ok {
			poolMap := poolContent.(map[string]interface{})
			if maxIdleConnS, sok := poolMap[strings.ToLower("maxIdleConnS")]; sok {
				if reflect.TypeOf(maxIdleConnS).Kind() != reflect.Int {
					logger.Warn(fmt.Sprintf("maxIdleConnS : %v , kind is not int", maxIdleConnS))
				} else {
					dbInfo.MaxIdleConnS = maxIdleConnS.(int)
				}
			} else {
				logger.Warn(fmt.Sprintf("dbInfo: %v not has MaxIdleConnS in poolSize", dbInfo.ConnectName))
			}
			if maxOpenConnS, sok := poolMap[strings.ToLower("maxOpenConnS")]; sok {
				if reflect.TypeOf(maxOpenConnS).Kind() != reflect.Int {
					logger.Warn(fmt.Sprintf("maxOpenConnS : %v , kind is not int", maxOpenConnS))
				} else {
					dbInfo.MaxOpenConnS = maxOpenConnS.(int)
				}
			} else {
				logger.Warn(fmt.Sprintf("dbInfo: %v not has maxOpenConnS in poolSize", dbInfo.ConnectName))
			}
			if connMaxLifeTime, sok := poolMap[strings.ToLower("connMaxLifeTime")]; sok {
				if reflect.TypeOf(connMaxLifeTime).Kind() != reflect.Int {
					logger.Warn(fmt.Sprintf("connMaxLifeTime : %v , kind is not int", connMaxLifeTime))
				} else {
					dbInfo.ConnMaxLifeTime = connMaxLifeTime.(int)
				}
			} else {
				logger.Warn(fmt.Sprintf("dbInfo: %v not has connMaxLifeTime in poolSize", dbInfo.ConnectName))
			}
		}

		if dbInfo.Driver != "" && dbInfo.ConnStr != "" && dbInfo.MaxIdleConnS > 0 &&
			dbInfo.MaxOpenConnS > 0 && dbInfo.ConnMaxLifeTime > 0 {
			dbInfos[dbInfo.ConnectName] = dbInfo
		}
	}

	if len(dbInfos) == 0 {
		return errors.New("dbInfo is empty ")
	}

	// task
	var tasks = make(map[string]Task)

	taskContent := viper.GetStringMap("task")
	for _, vMap := range taskContent {
		var task Task
		contentMap := vMap.(map[string]interface{})
		if regionCode, ok := contentMap[strings.ToLower("regionCode")]; ok {
			aaa := regionCode.(int)
			task.RegionCode = fmt.Sprintf("%04d", aaa)
		}
		if billDb, ok := contentMap[strings.ToLower("billDb")]; ok {
			task.BillDb = billDb.(string)
		}
		if cdrDb, ok := contentMap[strings.ToLower("cdrDb")]; ok {
			task.CdrDb = cdrDb.(string)
		}
		if taskNum, ok := contentMap[strings.ToLower("taskNum")]; ok {
			task.TaskNum = taskNum.(int)
		}
		tasks[task.RegionCode] = task
	}

	if len(tasks) == 0 {
		return errors.New("task is empty ")
	}

	configContent.CdrTables = cdrTables
	configContent.DbInfos = dbInfos
	configContent.Tasks = tasks
	configContent.CommonAttributes = commonAttributes

	err := CheckConfig()
	if err != nil {
		return err
	}

	PrintConfig()
	return nil
}

func CheckConfig() error {

	//检查 CdrTable
	cdrTables := &(configContent.CdrTables)
	if _, ok := (*cdrTables)[BillTabPrefix]; !ok {
		return errors.New("not define prefix in cdrTable module ")
	}
	if _, ok := (*cdrTables)[BillTabPrefixNoRegion]; !ok {
		return errors.New("not define prefixNoRegion in cdrTable module ")
	}
	if _, ok := (*cdrTables)[BillTabPrefixMod]; !ok {
		return errors.New("not define prefix in prefixMod module ")
	}

	//检查 task 中的 db连接是否存在
	tasks := &(configContent.Tasks)
	dbInfos := &(configContent.DbInfos)
	for _, task := range *tasks {
		if _, ok := (*dbInfos)[task.CdrDb]; !ok {
			return errors.New("RegionCode : " + task.RegionCode + " CdrDb [ " + task.CdrDb + " ] not define in dbInfo module")
		}
		if _, ok := (*dbInfos)[task.BillDb]; !ok {
			return errors.New("RegionCode : " + task.RegionCode + " BillDb [ " + task.BillDb + " ]not define in dbInfo module")
		}
	}

	commonAttributes := &(configContent.CommonAttributes)
	if value, ok := (*commonAttributes)[strings.ToLower(AttributeRegionConcurrent)]; !ok {
		return errors.New("regionCodeConcurrent not define in common module")
	} else {
		if reflect.TypeOf(value).Kind() != reflect.Bool {
			return errors.New("the value of regionCodeConcurrent must be yes/no ")
		}
	}
	if value, ok := (*commonAttributes)[strings.ToLower(AttributeChannelConcurrent)]; !ok {
		return errors.New("channelConcurrent not define in common module")
	} else {
		if reflect.TypeOf(value).Kind() != reflect.Bool {
			return errors.New("the value of channelConcurrent must be yes/no ")
		}
	}
	if value, ok := (*commonAttributes)[strings.ToLower(AttributeRegionAuditUser)]; !ok {
		return errors.New("regionAuditUser not define in common module")
	} else {
		if reflect.TypeOf(value).Kind() != reflect.Bool {
			return errors.New("the value of regionAuditUser must be yes/no ")
		}
	}
	if value, ok := (*commonAttributes)[strings.ToLower(AttributeAuditInfoDb)]; !ok {
		return errors.New("auditInfoDb not define in common module")
	} else {
		if reflect.TypeOf(value).Kind() != reflect.String {
			return errors.New("the value of auditInfoDb is not string type ")
		} else {
			if _, sok := (*dbInfos)[value.(string)]; !sok {
				return errors.New("auditInfoDb : " + value.(string) + " , not define in dbInfo module")
			}
		}
	}
	if value, ok := (*commonAttributes)[strings.ToLower(AttributeInterfaceTableDb)]; !ok {
		return errors.New("interfaceTableDb not define in common module")
	} else {
		if reflect.TypeOf(value).Kind() != reflect.String {
			return errors.New("the value of interfaceTableDb is not string type ")
		} else {
			if _, sok := (*dbInfos)[value.(string)]; !sok {
				return errors.New("interfaceTableDb : " + value.(string) + " , not define in dbInfo module")
			}
		}
	}
	return nil
}

func PrintConfig() {

	logger.Info("==================== config file context ========================================= ")
	if len(configContent.CdrTables) == 0 {
		logger.Info("CdrTables is empty ... ")
	} else {
		logger.Info("CdrTables list : ")
		for tableType, headList := range configContent.CdrTables {

			var tableTypeName string
			switch tableType {
			case BillTabPrefix:
				tableTypeName = BillTabPrefixName
			case BillTabPrefixNoRegion:
				tableTypeName = BillTabPrefixNoRegionName
			case BillTabPrefixMod:
				tableTypeName = BillTabPrefixModName
			default:
				tableTypeName = BillTabUnknown
			}
			logMessage := fmt.Sprintf("  %s : ", tableTypeName)
			for i, head := range headList {
				if i == 0 {
					logMessage += fmt.Sprintf("%s", head)
				} else {
					logMessage += fmt.Sprintf(",%s", head)
				}
			}
			logger.Info(logMessage)
		}
	}

	if len(configContent.DbInfos) == 0 {
		logger.Info("DbInfos is empty ... ")
	} else {
		logger.Info("DbInfos list : ")
		for _, dbInfo := range configContent.DbInfos {
			logger.Info(fmt.Sprintf("  %s : { Driver: %s , ConnStr: %s , pool[ maxIdleConnS: %d maxOpenConnS: %d connMaxLifeTime: %d ] } ",
				dbInfo.ConnectName, dbInfo.Driver, dbInfo.ConnStr, dbInfo.MaxIdleConnS, dbInfo.MaxOpenConnS, dbInfo.ConnMaxLifeTime))
		}
	}

	if len(configContent.Tasks) == 0 {
		logger.Info("Tasks is empty ... ")
	} else {
		logger.Info("Tasks list : ")
		for _, task := range configContent.Tasks {
			logMessage := "  RegionCode: " + task.RegionCode + " ,BillDb: " + task.BillDb +
				" ,CdrDb: " + task.CdrDb + " ,TaskNum: " + strconv.Itoa(task.TaskNum)
			logger.Info(logMessage)
		}
	}

	if len(configContent.CommonAttributes) > 0 {
		logger.Info("common attribute list : ")
		for name, value := range configContent.CommonAttributes {
			logger.Info(fmt.Sprintf("  [ %s ] : %v ", name, value))
		}
	}

	logger.Trace("================================================================================== ")
}
