#!/bin/bash

workhome=$(cd $(dirname $0) && pwd)
binPath=${workhome}/bin

target=(client server)

for t in ${target[@]};do

	cd ${workhome}/${t}
	cmd="go build"
	echo "${cmd}"
	eval ${cmd}

	if [ ! -d ${binPath} ];then
		mkdir -p ${binPath}
	fi

	if [ -f ${workhome}/${t}/${t} ];then
		cmd="mv ${workhome}/${t}/${t} ${binPath}"
		echo "${cmd}"
		eval ${cmd}
	fi

done

