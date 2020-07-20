package checkBill

import (
	"math"
	"sort"
)

/**
合并处理  入参的key 是 userId; 合并两张清单的记录
*/
func CdrMapAppend(first *map[int64]CdrRecordSlice, second *map[int64]CdrRecordSlice) map[int64]CdrRecordSlice {

	var tMap = make(map[int64]CdrRecordSlice)

	cdrRecordAppend := func(tRecords1 *CdrRecordSlice, tRecords2 *CdrRecordSlice) CdrRecordSlice {
		var tRecords CdrRecordSlice
		for i := range *tRecords1 {
			value1 := &(*tRecords1)[i]
			var exists = false

			for j := range *tRecords2 {
				value2 := &(*tRecords2)[j]
				if value1.ItemCode == value2.ItemCode {
					var tRecord = CdrRecord{value1.UserId, value1.ItemCode, value1.Fee + value2.Fee}
					tRecords = append(tRecords, tRecord)
					exists = true
					break
				}
			}
			if !exists {
				tRecords = append(tRecords, *value1)
			}
		}

		for j := range *tRecords2 {
			value2 := &(*tRecords2)[j]
			var exists = false

			for i := range *tRecords1 {
				value1 := &(*tRecords1)[i]
				if value1.ItemCode == value2.ItemCode {
					exists = true
					break
				}
			}
			if !exists {
				tRecords = append(tRecords, *value2)
			}
		}
		return tRecords
	}

	//正向查找
	for userId, tRecords1 := range *first {
		if tRecords2, ok := (*second)[userId]; ok {
			tMap[userId] = cdrRecordAppend(&tRecords1, &tRecords2)
		} else {
			tMap[userId] = tRecords1
		}
	}
	//反向查找
	for userId := range *second {
		if _, ok := (*first)[userId]; !ok {
			tRecords := (*second)[userId]
			tMap[userId] = tRecords
		}
	}
	return tMap
}

/**
转换cdr 数组为 map，并做item级别的费用合并
整个清单表的记录
*/
func CdrParseMap(old *CdrRecordSlice) map[int64]CdrRecordSlice {

	var tMap = make(map[int64]CdrRecordSlice)
	if len(*old) <= 0 {
		return tMap
	}
	sort.Stable(*old)

	addRecord := func(record *CdrRecord) {
		if _, ok := tMap[record.UserId]; ok {
			tMap[record.UserId] = append(tMap[record.UserId], *record)
		} else {
			tMap[record.UserId] = CdrRecordSlice{*record}
		}
	}

	var userId int64
	var itemCode int16
	var fee int32

	var lastRecord = CdrRecord{UserId: 0, ItemCode: 0, Fee: 0}
	for i := 0; i < len(*old); i++ {
		userId = (*old)[i].UserId
		itemCode = (*old)[i].ItemCode
		fee = (*old)[i].Fee

		if lastRecord.UserId == 0 {
			lastRecord.UserId = userId
			lastRecord.ItemCode = itemCode
			lastRecord.Fee = fee
			continue
		}

		if lastRecord.UserId != userId || lastRecord.ItemCode != itemCode {
			addRecord(&lastRecord)

			lastRecord.UserId = userId
			lastRecord.ItemCode = itemCode
			lastRecord.Fee = fee
		} else {
			lastRecord.Fee += fee
		}
	}
	if lastRecord.UserId != 0 {
		addRecord(&lastRecord)
	}

	return tMap
}

/**
转换 数组为 map 整个账单表的记录
*/
func SubBillParseMap(old *SubBillSlice) map[int64]SubBillSlice {

	var tMap = make(map[int64]SubBillSlice)
	if len(*old) <= 0 {
		return tMap
	}
	sort.Stable(*old)

	addRecord := func(record *SubBillRecord) {
		if _, ok := tMap[record.BillId]; ok {
			tMap[record.BillId] = append(tMap[record.BillId], *record)
		} else {
			tMap[record.BillId] = SubBillSlice{*record}
		}
	}

	var billId int64
	var itemCode int16
	var fee int32

	var lastRecord = SubBillRecord{BillId: 0, ItemCode: 0, Fee: 0}
	for i := 0; i < len(*old); i++ {
		billId = (*old)[i].BillId
		itemCode = (*old)[i].ItemCode
		fee = (*old)[i].Fee

		if lastRecord.BillId == 0 {
			lastRecord.BillId = billId
			lastRecord.ItemCode = itemCode
			lastRecord.Fee = fee
			continue
		}

		if lastRecord.BillId != billId || lastRecord.ItemCode != itemCode {
			addRecord(&lastRecord)

			lastRecord.BillId = billId
			lastRecord.ItemCode = itemCode
			lastRecord.Fee = fee
		} else {
			lastRecord.Fee += fee
		}
	}
	if lastRecord.BillId != 0 {
		addRecord(&lastRecord)
	}

	return tMap
}

/**
转换日账数据
*/
func BillParseMap(totalBill *TotalBillSlice, usageBill *SubBillSlice, spBill *SubBillSlice) map[int64]CdrRecordSlice {

	usageBillMap := SubBillParseMap(usageBill)
	spBillMap := SubBillParseMap(spBill)

	getBillFee := func(totalRecord *TotalBillRecord) CdrRecordSlice {

		var billId = totalRecord.BillId
		var userId = totalRecord.UserId
		var itemFeeMap = make(map[int16]int32)

		if usageBills, ok := usageBillMap[billId]; ok {
			for i := range usageBills {
				itemFeeMap[usageBills[i].ItemCode] = usageBills[i].Fee
			}
		}
		if spBills, ok := spBillMap[billId]; ok {
			for i := range spBills {
				if fee, sok := itemFeeMap[spBills[i].ItemCode]; sok {
					itemFeeMap[spBills[i].ItemCode] = spBills[i].Fee + fee
				} else {
					itemFeeMap[spBills[i].ItemCode] = spBills[i].Fee
				}
			}
		}

		var tRecords CdrRecordSlice
		for k, v := range itemFeeMap {
			cdrRecord := CdrRecord{UserId: userId, ItemCode: k, Fee: v}
			tRecords = append(tRecords, cdrRecord)
		}
		return tRecords
	}

	// total 关联 子表，生成目标
	var tMap = make(map[int64]CdrRecordSlice)

	for j := range *totalBill {
		//转换
		var userId = (*totalBill)[j].UserId
		var tRecords = getBillFee(&(*totalBill)[j])

		if len(tRecords) > 0 {
			//合并到tMap中
			if _, ok := tMap[userId]; !ok {
				tMap[userId] = tRecords
			} else {
				for k := range tRecords {
					var exists = false
					for i := range tMap[userId] {
						if tMap[userId][i].ItemCode == tRecords[k].ItemCode {
							tMap[userId][i].Fee += tRecords[k].Fee
							exists = true
							break
						}
					}
					if !exists {
						tMap[userId] = append(tMap[userId], tRecords[k])
					}
				}
			}
		}
	}
	return tMap
}

/**
比较当天和前一天的账单,生成差值
*/
func GetBillMinus(thisDay *map[int64]CdrRecordSlice, lastDay *map[int64]CdrRecordSlice) map[int64]CdrRecordSlice {

	getMinusRecord := func(tRecords1 *CdrRecordSlice, tRecords2 *CdrRecordSlice) CdrRecordSlice {
		var minusRecord CdrRecordSlice
		for i := range *tRecords1 {
			value1 := &(*tRecords1)[i]
			var exists = false

			for j := range *tRecords2 {
				value2 := &(*tRecords2)[j]
				if value1.ItemCode == value2.ItemCode {
					if value1.Fee != value2.Fee {
						tRecord := CdrRecord{value1.UserId, value1.ItemCode, value1.Fee - value2.Fee}
						minusRecord = append(minusRecord, tRecord)
					}
					exists = true
					break
				}
			}
			if !exists {
				minusRecord = append(minusRecord, *value1)
			}
		}
		return minusRecord
	}

	var tMap = make(map[int64]CdrRecordSlice)
	//以当前天 减去 前一天; 不做反向比较
	for userId, tRecords1 := range *thisDay {
		tRecords2, ok := (*lastDay)[userId]
		if !ok {
			tMap[userId] = tRecords1
		} else {
			minusRecord := getMinusRecord(&tRecords1, &tRecords2)
			if len(minusRecord) > 0 {
				tMap[userId] = minusRecord
			}
		}
	}
	return tMap
}

/**
用于比较同一个用户的记录
*/
func diffCdr(billRecords *CdrRecordSlice, cdrRecords *CdrRecordSlice) DiffRecordSlice {

	var res DiffRecordSlice
	var userId = DefaultId

	getFeeMap := func(cdrRecords *CdrRecordSlice) map[int16]int32 {
		var feeMap = make(map[int16]int32)
		for i := range *cdrRecords {
			value := &(*cdrRecords)[i]
			if fee, ok := feeMap[value.ItemCode]; ok {
				feeMap[value.ItemCode] = fee + value.Fee
			} else {
				feeMap[value.ItemCode] = value.Fee
			}
		}
		if userId == DefaultId {
			userId = (*cdrRecords)[0].UserId
		}
		return feeMap
	}

	var billFeeMap = getFeeMap(billRecords)
	var cdrFeeMap = getFeeMap(cdrRecords)

	for itemCode, billFee := range billFeeMap {
		if fee, ok := cdrFeeMap[itemCode]; ok && billFee != fee {
			var record = DiffRecord{UserId: userId, ItemCode: itemCode, BillFee: billFee, CdrFee: fee}
			res = append(res, record)
		} else {
			var record = DiffRecord{UserId: userId, ItemCode: itemCode, BillFee: billFee, CdrFee: 0}
			res = append(res, record)
		}
	}

	for itemCode, cdrFee := range cdrFeeMap {
		if _, ok := billFeeMap[itemCode]; !ok {
			var record = DiffRecord{UserId: userId, ItemCode: itemCode, BillFee: 0, CdrFee: cdrFee}
			res = append(res, record)
		}
	}
	return res
}

/**
比较通道下的所有记录
*/
func DiffCdrMap(first *DataRes, second *DataRes) (DiffRecordSlice, map[int16]*TotalRecord) {

	var billMap *map[int64]CdrRecordSlice
	var cdrMap *map[int64]CdrRecordSlice

	if first.DataSource == DataSourceBill {
		billMap = &first.DataMap
		cdrMap = &second.DataMap
	} else {
		billMap = &second.DataMap
		cdrMap = &first.DataMap
	}

	var res DiffRecordSlice
	var totalMap = make(map[int16]*TotalRecord)

	addTotalRecord := func(itemCode *int16, record *TotalRecord) {
		if _, ok := totalMap[*itemCode]; ok {
			totalMap[*itemCode].CdrFee = totalMap[*itemCode].CdrFee + record.CdrFee
			totalMap[*itemCode].BillFee += record.BillFee
			totalMap[*itemCode].DiffFee += record.DiffFee
		} else {
			totalMap[*itemCode] = &TotalRecord{record.BillFee, record.CdrFee, record.DiffFee}
		}
	}

	// 轮询 billMap
	for userId, billRecords := range *billMap {
		if cdrRecords, ok := (*cdrMap)[userId]; ok {
			tDiffRecords := diffCdr(&billRecords, &cdrRecords)
			for i := range tDiffRecords {
				value := &tDiffRecords[i]
				res = append(res, *value)
				addTotalRecord(&value.ItemCode, &TotalRecord{value.BillFee, value.CdrFee, int32(math.Abs(float64(value.BillFee - value.CdrFee)))})
			}
		} else {
			for i := range billRecords {
				value := &billRecords[i]
				var record = DiffRecord{UserId: value.UserId, ItemCode: value.ItemCode, BillFee: value.Fee, CdrFee: 0}
				res = append(res, record)
				addTotalRecord(&value.ItemCode, &TotalRecord{value.Fee, 0, value.Fee})
			}
		}
	}
	// 轮询 cdrMap
	for userId := range *cdrMap {
		if _, ok := (*billMap)[userId]; !ok {
			cdrRecords := (*cdrMap)[userId]
			for i := range cdrRecords {
				value := &cdrRecords[i]
				var record = DiffRecord{UserId: value.UserId, ItemCode: value.ItemCode, BillFee: 0, CdrFee: value.Fee}
				res = append(res, record)
				addTotalRecord(&value.ItemCode, &TotalRecord{0, value.Fee, value.Fee})
			}
		}
	}
	return res, totalMap
}

/**
比较账户下的记录
*/
func DiffAcctBillMap(billMap *map[int64]CdrRecordSlice, cdrMap *map[int64]CdrRecordSlice) DiffRecordSlice {

	var res DiffRecordSlice
	// 轮询 billMap
	for userId, billRecords := range *billMap {
		if cdrRecords, ok := (*cdrMap)[userId]; ok {
			tDiffRecords := diffCdr(&billRecords, &cdrRecords)
			for i := range tDiffRecords {
				res = append(res, tDiffRecords[i])
			}
		} else {
			for i := range billRecords {
				value := &billRecords[i]
				var record = DiffRecord{UserId: value.UserId, ItemCode: value.ItemCode, BillFee: value.Fee, CdrFee: 0}
				res = append(res, record)
			}
		}
	}
	// 轮询 cdrMap
	for userId := range *cdrMap {
		if _, ok := (*billMap)[userId]; !ok {
			cdrRecords := (*cdrMap)[userId]
			for i := range cdrRecords {
				value := &cdrRecords[i]
				var record = DiffRecord{UserId: value.UserId, ItemCode: value.ItemCode, BillFee: 0, CdrFee: value.Fee}
				res = append(res, record)
			}
		}
	}
	return res
}
