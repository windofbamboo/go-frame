package checkBill

//接口表
type AuditDetail struct {
	RegionCode string
	AcctId     int64
}

type AuditDetailSlice []AuditDetail

func (s AuditDetailSlice) Len() int      { return len(s) }
func (s AuditDetailSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s AuditDetailSlice) Less(i, j int) bool {
	if s[i].RegionCode != s[j].RegionCode {
		return s[i].RegionCode < s[j].RegionCode
	}
	return s[i].AcctId < s[j].AcctId
}

//账户检查结果信息
type AuditDetailResult struct {
	AuditDay   int
	RegionCode string
	AcctId     int64
	UserId     int64
	ItemCode   int16
	BillFee    int32
	CdrFee     int32
}

type AuditDetailResultSlice []AuditDetailResult

func (s AuditDetailResultSlice) Len() int      { return len(s) }
func (s AuditDetailResultSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s AuditDetailResultSlice) Less(i, j int) bool {
	if s[i].AuditDay != s[j].AuditDay {
		return s[i].AuditDay < s[j].AuditDay
	}
	if s[i].RegionCode != s[j].RegionCode {
		return s[i].RegionCode < s[j].RegionCode
	}
	if s[i].AcctId != s[j].AcctId {
		return s[i].AcctId < s[j].AcctId
	}
	if s[i].UserId != s[j].UserId {
		return s[i].UserId < s[j].UserId
	}
	return s[i].ItemCode < s[j].ItemCode
}

//账单总表
type TotalBillRecord struct {
	UserId int64
	BillId int64
}

type TotalBillSlice []TotalBillRecord

func (s TotalBillSlice) Len() int      { return len(s) }
func (s TotalBillSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s TotalBillSlice) Less(i, j int) bool {
	if s[i].UserId != s[j].UserId {
		return s[i].UserId < s[j].UserId
	}
	return s[i].BillId < s[j].BillId
}

//账单子表
type SubBillRecord struct {
	BillId   int64
	ItemCode int16
	Fee      int32
}

type SubBillSlice []SubBillRecord

func (s SubBillSlice) Len() int      { return len(s) }
func (s SubBillSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SubBillSlice) Less(i, j int) bool {
	if s[i].BillId != s[j].BillId {
		return s[i].BillId < s[j].BillId
	}
	return s[i].ItemCode < s[j].ItemCode
}

//清单表 以及 规整后的记录
type CdrRecord struct {
	UserId   int64
	ItemCode int16
	Fee      int32
}

type CdrRecordSlice []CdrRecord

func (s CdrRecordSlice) Len() int      { return len(s) }
func (s CdrRecordSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s CdrRecordSlice) Less(i, j int) bool {
	if s[i].UserId != s[j].UserId {
		return s[i].UserId < s[j].UserId
	}
	return s[i].ItemCode < s[j].ItemCode
}

type DataRes struct {
	DataSource string
	DataMap    map[int64]CdrRecordSlice
}

// 比较结果 明细记录
type DiffRecord struct {
	UserId   int64
	ItemCode int16
	BillFee  int32
	CdrFee   int32
}

type DiffRecordSlice []DiffRecord

func (s DiffRecordSlice) Len() int      { return len(s) }
func (s DiffRecordSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s DiffRecordSlice) Less(i, j int) bool {
	if s[i].UserId != s[j].UserId {
		return s[i].UserId < s[j].UserId
	}
	return s[i].ItemCode < s[j].ItemCode
}

// 比较结果 汇总信息
type TotalRecord struct {
	BillFee int32
	CdrFee  int32
	DiffFee int32
}
