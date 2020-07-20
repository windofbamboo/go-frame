package checkBill

import (
	"fmt"
	"github.com/wonderivan/logger"
)

const (
	AttributeRegionConcurrent  = "regionCodeConcurrent"
	AttributeChannelConcurrent = "channelConcurrent"
	AttributeRegionAuditUser   = "regionAuditUser"

	AttributeInterfaceTableDb = "interfaceTableDb"
	AttributeAuditInfoDb      = "auditInfoDb"

	AuditScopeProvince string = "province"
	AuditScopeRegion   string = "region"
	AuditScopeTable    string = "table"
	AuditScopeAcct     string = "acct"

	DefaultRegionCode     string = "all"
	DefaultChannel        int    = -1
	DefaultConfigPath     string = "checkBill"
	DefaultConfigFileName string = "config.yaml"
	LogConfigFileName     string = "log.json"
	DefaultId             int64  = -1

	OraTimeLayout = "2006-01-02 15:04:05"

	DayLayout = "20060102"

	InitTag   = "0"
	FinishTag = "3"

	BillTabPrefix         int32 = 1
	BillTabPrefixNoRegion int32 = 2
	BillTabPrefixMod      int32 = 3

	BillTabPrefixName         string = "prefix"
	BillTabPrefixNoRegionName string = "prefixNoRegion"
	BillTabPrefixModName      string = "prefixMod"
	BillTabUnknown            string = "unKnown"

	DataSourceCdr    string = "cdr"
	DataSourceBill   string = "bill"
	DataSourceResult string = "result"
	DataSourceITable string = "info"

	SeparatorCharacter string = ","
)

func CheckErr(err error) {
	if err != nil {
		logger.Error(fmt.Sprintf(" err : %s ", err.Error()))
		panic(err)
	}
}
