package main

import (
	"fmt"
	"reflect"
)

/**
 execute
      |
	  |--Init
      |--BeforeLoop
      |--LoopProcess
      |--AfterLoop
*/
type BaseProcess struct {
	Name string
	Typ  reflect.Type
}

func (*BaseProcess)Init() bool{
	return true
}

func (c *BaseProcess)BeforeLoop() {

	fmt.Printf("%s begin ... \n",c.Name)
}

func (c *BaseProcess)LoopProcess() {
}

func (c *BaseProcess)AfterLoop() {

	fmt.Printf("%s end ... \n",c.Name)
}

//统一的执行过程
func (c *BaseProcess)execute(){

	ptrNew := reflect.New(c.Typ)
	// 设置字段值
	trueNew := ptrNew.Elem()
	f := trueNew.FieldByName("Name")
	if f.Kind() == reflect.String {
		f.SetString(c.Name)
	}

	res:= ptrNew.MethodByName("Init").Call(nil)
	if ! res[0].Bool(){
		return
	}

	ptrNew.MethodByName("BeforeLoop").Call(nil)

	ptrNew.MethodByName("LoopProcess").Call(nil)

	ptrNew.MethodByName("AfterLoop").Call(nil)

}


