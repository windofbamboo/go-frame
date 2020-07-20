package checkBill

import (
	"fmt"
	"github.com/wonderivan/logger"
)

func AuditTable(auditDay int) {

	logger.Info(fmt.Sprintf(" audit table begin ..."))
	acctList, err := QueryAcctList()
	CheckErr(err)

	c := make(chan AuditDetailResultSlice)
	for _, record := range acctList {
		go dealSingleAcct(record.RegionCode, record.AcctId, auditDay, c)
	}

	var res AuditDetailResultSlice
	for i := 0; i < len(acctList); i++ {
		acctRecords := <-c
		for _, record := range acctRecords {
			res = append(res, record)
		}
	}
	close(c)

	err = InsertCheckAcct(&res)
	CheckErr(err)
	logger.Info(fmt.Sprintf(" audit table end ..."))
}

func dealSingleAcct(regionCode string, acctId int64, auditDay int, c chan AuditDetailResultSlice) {

	acctRecords := AuditSingleAcct(regionCode, acctId, auditDay)
	c <- acctRecords
}
