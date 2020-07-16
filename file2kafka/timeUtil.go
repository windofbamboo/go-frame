package main

import (
	"strconv"
	"time"
)

var CstZone = time.FixedZone("CST", 8*3600) // 东八
var OraTimeLayout = "20060102150405"
var DayLayout = "20060102"


func GetCurrentTime() string {
	return time.Now().In(CstZone).Format(OraTimeLayout)
}

func GetUnixTime() int64 {
	return time.Now().In(CstZone).Unix()
}


func GetNDayBefore(days int) int {

	d, _ := time.ParseDuration("-24h")
	d1 := time.Now()
	for i := 0; i < days; i++ {
		d1 = d1.Add(d)
	}

	var currentTime = d1.In(CstZone).Format(DayLayout)
	day, err := strconv.ParseInt(currentTime, 10, 32)
	if err != nil {
		panic(err)
	}
	return int(day)
}

func GetLastDay(currentDay int) int {
	var year = currentDay / 10000
	var month = (currentDay / 100) % 100
	var day = currentDay % 100

	currentTime := time.Date(year, time.Month(month), day, 0, 0, 1, 100, time.Local)
	d, _ := time.ParseDuration("-24h")
	currentTime = currentTime.Add(d)

	var lastTime = currentTime.In(CstZone).Format(DayLayout)
	lastDay, err := strconv.ParseInt(lastTime, 10, 32)
	if err != nil {
		panic(err)
	}
	return int(lastDay)
}

func CheckDay(checkDay int) bool {

	checkTime, err := time.Parse(DayLayout, strconv.Itoa(checkDay))
	if err != nil {
		return false
	}
	var currentTime = checkTime.Format(DayLayout)
	day, err := strconv.ParseInt(currentTime, 10, 32)
	if err == nil && int(day) == checkDay {
		return true
	}
	return false
}
