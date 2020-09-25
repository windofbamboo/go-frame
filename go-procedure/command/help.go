package main

const Help = `			 go-procedure

NAME
	procedure is a sql tool.It is developed by Go,compiled into a separate binary without any dependencies.

DESCRIPTION
		procedure can do the follow things:
		1. exec sql in database.
		2. commit in tx

param description
		-d [environmentId]      [ environment Id in config.xml ]
		-n [procedureName]		[ procedure name ]
		-p [paramStr]           [ input param of procedure , SeparatorCharacter is ',' ]

USAGE
	procedure -d [environmentId] -n [procedureName] -p [paramStr]
	
example	:
		procedure -d cdrdb -n p_asp_xxx -p asd,343,hh
`
