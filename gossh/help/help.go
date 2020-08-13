package help

const Help = `			 gossh

NAME
	gossh is a smart ssh tool.It is developed by Go,compiled into a separate binary without any dependencies.

DESCRIPTION
		gossh can do the follow things:
		1.runs cmd on the remote host.
		2.push a local file or path to the remote host.
		3.pull remote host file to local.

USAGE
	1.Single Mode
		remote-comand:
		gossh -t cmd -g group -node name -p passswrod [-f] -exec command 
		gossh -t cmd -g group -node name -p passswrod [-f] -cmdFile file.txt

		Files-transfer:   
		<push file>   
		gossh -t push -g group -node name -p passswrod localfile  remotepath 

		<pull file> 
		gossh -t pull -g group -node name -p passswrod remotefile localpath 

		<change password> 
		gossh -t pswChange -g group -node name -p passswrod newPsw 

	2.Batch Mode
		Ssh-comand:
		gossh -t cmd -g group -p passswrod [-f] -exec command 
		gossh -t cmd -g group -p passswrod [-f] -cmdFile file.txt

		Files-transfer:
		gossh -t push -g group -p passswrod localfile  remotepath 
		gossh -t pull -g group -p passswrod remotefile localpath

		<change password> 
		gossh -t pswChange -g group -p passswrod newPsw 
EMAIL
    	email.tata@qq.com 
`
