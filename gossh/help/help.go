package help

const Help = `			 gossh

NAME
	gossh is a smart ssh tool.It is developed by Go,compiled into a separate binary without any dependencies.

DESCRIPTION
		gossh can do the follow things:
		1.runs cmd on the remote host.
		2.push a local file or path to the remote host.
		3.pull remote host file to local.
		4.change password on the remote host

USAGE
	1.Single Mode
		remote-comand:
		gossh -t cmd -g group -node name -p password [-f] -exec command 
		gossh -t cmd -g group -node name -p password [-f] -cmdFile file.txt

		Files-transfer:   
		<push file>   
		gossh -t push -g group -node name -p password localFile  remotePath 

		<pull file> 
		gossh -t pull -g group -node name -p password remoteFile localPath 

		<change password> 
		gossh -t psw -g group -node name -p password newPsw

	2.Batch Mode
		Ssh-comand:
		gossh -t cmd -g group -p password [-f] -exec command 
		gossh -t cmd -g group -p password [-f] -cmdFile file.txt

		Files-transfer:
		gossh -t push -g group -p password localFile  remotePath 
		gossh -t pull -g group -p password remoteFile localPath

		<change password> 
		gossh -t psw -g group -p password newPsw 

`
