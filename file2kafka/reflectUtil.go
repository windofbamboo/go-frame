package main

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Poni(o interface{}) map[string] reflect.Kind {
	t := reflect.TypeOf(o)

	tmap:= make(map[string] reflect.Kind)
	for i := 0; i < t.NumField(); i++ {
		// 取每个字段
		f := t.Field(i)
		tmap[f.Name]=f.Type.Kind()
		fmt.Printf("%s : %v \n", f.Name, f.Type.Kind())
	}
	return tmap
}

/**
	第一个参数是 &struct
 */
func SetValue(o interface{},columnName *[]string,msg *string) error{

	num:=len(*columnName)
	columns:= strings.Split(*msg,",")
	if len(columns) !=num{
		return errors.New("Num of Fields  err")
	}

	v := reflect.ValueOf(o)
	// 获取指针指向的元素
	v = v.Elem()

	for i := 0; i < num; i++ {
		// 取每个字段
		columnName:=(*columnName)[i]

		column := v.FieldByName(columnName)

		switch column.Type().Kind() {
		case reflect.Bool:
			if value,err:=strconv.ParseBool(columns[i]);err==nil{
				column.SetBool( value )
			}
		case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64:
			if value,err:=strconv.ParseInt(columns[i],10,64);err==nil{
				column.SetInt( value )
			}
		case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64:
			if value,err:=strconv.ParseUint(columns[i],10,64);err==nil{
				column.SetUint( value )
			}
		case reflect.Float32,reflect.Float64:
			if value,err:=strconv.ParseFloat(columns[i],64);err==nil{
				column.SetFloat( value )
			}
		case reflect.String:
			column.SetString(  columns[i] )
		default:

		}
	}

	return nil
}

/**

 */
func GetStr(o interface{},columnName *[]string) string{

	var columnStr string

	num:=len(*columnName)
	v := reflect.ValueOf(o)
	// 获取指针指向的元素
	v = v.Elem()

	for i := 0; i < num; i++ {
		// 取每个字段
		columnName:=(*columnName)[i]
		column := v.FieldByName(columnName)

		if i == 0{
			columnStr = fmt.Sprintf("%v", column.Interface())
		}else{
			columnStr += fmt.Sprintf(",%v", column.Interface())
		}
	}
	return columnStr
}


/**
	第一个参数是 实际的 struct 对象
 */
func GetStr2(o interface{},structType reflect.Type) string{

	v := reflect.ValueOf(o)
	v = v.Elem()

	var columnStr string

	for i := 0; i < structType.NumField(); i++ {
		if i == 0{
			columnStr = fmt.Sprintf("%v", v.Field(i).Interface())
		}else{
			columnStr += fmt.Sprintf(",%v", v.Field(i).Interface())
		}
	}
	return columnStr
}

/**
  把对象的字符串，变成对象，返回值是 Elem interface{}
  需要在外层 指定 类型
  不适合 protoBuffer编译出来的类型
*/
func Str2Object(msg *string,t reflect.Type) (interface{},error){

	//t := reflect.ValueOf(structMap[name]).Type()
	v := reflect.New(t).Elem()

	columns:= strings.Split(*msg,",")
	if len(columns) !=t.NumField(){
		return nil,errors.New("Num of Fields  err")
	}

	for i := 0; i < t.NumField(); i++ {
		// 取每个字段
		f := t.Field(i)
		columnName:=f.Name
		column := v.FieldByName(columnName)

		switch f.Type.Kind() {
		case reflect.Bool:
			if value,err:=strconv.ParseBool(columns[i]);err==nil{
				column.SetBool( value )
			}
		case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64:
			if value,err:=strconv.ParseInt(columns[i],10,64);err==nil{
				column.SetInt( value )
			}
		case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64:
			if value,err:=strconv.ParseUint(columns[i],10,64);err==nil{
				column.SetUint( value )
			}
		case reflect.Float32,reflect.Float64:
			if value,err:=strconv.ParseFloat(columns[i],64);err==nil{
				column.SetFloat( value )
			}
		case reflect.String:
			column.SetString(  columns[i] )
		default:

		}
	}
	return v.Interface(),nil
}

/**
	入参为 Str2Object 的出参
 */
func Object2Str(o interface{}) string{

	structType := reflect.TypeOf(o)
	v := reflect.ValueOf(o)

	var columnStr string

	for i := 0; i < structType.NumField(); i++ {
		if i == 0{
			columnStr = fmt.Sprintf("%v", v.Field(i).Interface())
		}else{
			columnStr += fmt.Sprintf(",%v", v.Field(i).Interface())
		}
	}
	return columnStr
}
