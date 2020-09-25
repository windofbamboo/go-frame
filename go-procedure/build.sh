#!/bin/bash

workhome=$(cd $(dirname $0) && pwd)
binpath=${workhome}/bin

target=(client server command)

for t in ${target[@]};do

	cd ${workhome}/${t}
	cmd="go build"
	echo "${cmd}"
	eval ${cmd}

	if [ ! -d ${binpath} ];then
		mkdir -p ${binpath}
	fi

	if [ -f ${workhome}/${t}/${t} ];then
		cmd="mv ${workhome}/${t}/${t} ${binpath}"
		echo "${cmd}"
		eval ${cmd}
	fi

done

