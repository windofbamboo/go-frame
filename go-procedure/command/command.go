package main

import (
	"flag"
	"fmt"
	"go-procedure/base"
)

func main() {

	var help = func() {
		fmt.Println(Help)
	}
	flag.Usage = func() {
		help()
	}

	environmentId,procedureName,paramStr:=flagInit()

	varMap := base.CommandInit(environmentId,procedureName,paramStr)

	base.InitPoolMap()
	defer base.DestroyPoolMap()

	err := base.ExecProcedure(environmentId,procedureName,&varMap)
	if err != nil {
		panic(err)
	}
}

func flagInit() (environmentId,procedureName,paramStr string){


	flag.StringVar(&environmentId, "d", base.NullStr, " database id ")
	flag.StringVar(&procedureName, "n", base.NullStr, " name of procedure ")
	flag.StringVar(&paramStr, "p", base.NullStr, " inParam list of procedure ")
	flag.Parse()

	return environmentId,procedureName,paramStr
}


