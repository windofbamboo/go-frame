package passwd

import (
	"fmt"
	"gossh/machine"
	"testing"
)

var(
	host = "192.168.190.50"
	port = 22
	rootUser = "root"
	rootPSW = "root123"
	user = "app"
	password = "app123"
	newPSW = "1sd$sda345!8"
)


func TestChangePSW(t *testing.T) {

	server := machine.NewCmdServer(host, port, user, password, "cmd", "", false, 10)
	client, err := server.GetSshClient()
	if err!=nil{
		panic(err)
	}

	p:= PassWd{client}

	bs,err:=p.ChangePSW(password,newPSW)
	if err!=nil{
		panic(err)
	}
	fmt.Printf(string(bs))
}

func TestChangePSWbyRoot(t *testing.T) {

	server := machine.NewCmdServer(host, port, rootUser, rootPSW, "cmd", "", false, 10)
	client, err := server.GetSshClient()
	if err!=nil{
		panic(err)
	}

	p:= PassWd{client}

	bs,err:=p.ChangePSWbyRoot(user,password)
	if err!=nil{
		panic(err)
	}
	fmt.Printf(string(bs))
}