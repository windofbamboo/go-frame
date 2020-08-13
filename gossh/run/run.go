package run

import (
	"gossh/config"
	"gossh/logs"
	"gossh/machine"
	"gossh/output"
	"path/filepath"
	"sync"
)

var (
	log = logs.NewLogger()
)

func SingleRun(node *config.LogicUser, cmd string , force bool, timeout int) {
	server := machine.NewCmdServer(node.Host, node.Port, node.User, node.Password,
		"cmd", cmd, force, timeout)
	r := server.SRunCmd()
	output.Print(r)
}

//func ServersRun(cmd string, cu *CommonUser, wt *sync.WaitGroup, crs chan machine.Result, ipFile string, ccons chan struct{}) {
func ServersRun( nodeList []config.LogicUser, cmd string,wt *sync.WaitGroup,
	crs chan machine.Result, ccons chan struct{}, safe bool, force bool, timeout int) {

	ls := len(nodeList)

	//ccons==1 串行执行,可以暂停
	if cap(ccons) == 1 {
		log.Debug("串行执行")
		for _, h := range nodeList {
			server := machine.NewCmdServer(h.Host, h.Port, h.User, h.Password, "cmd", cmd, force, timeout)
			r := server.SRunCmd()
			if r.Err != nil && safe {
				log.Debug("%s执行出错", h.Host)
				output.Print(r)
				break
			} else {
				output.Print(r)
			}
		}
	} else {
		log.Debug("并行执行")
		go output.PrintResults2(crs, ls, wt, ccons, timeout)

		for _, h := range nodeList {
			ccons <- struct{}{}
			server := machine.NewCmdServer(h.Host, h.Port, h.User, h.Password, "cmd", cmd, force, timeout)
			wt.Add(1)
			go server.PRunCmd(crs)
		}
	}
}

func SinglePush(src, dst string, node *config.LogicUser, f bool, timeout int) {
	server := machine.NewScpServer(node.Host, node.Port, node.User, node.Password,
		"scp", src, dst, f, timeout)
	cmd := "push " + server.FileName + " to " + server.Ip + ":" + server.RemotePath

	rs := machine.Result{
		Ip:  server.Ip,
		Cmd: cmd,
	}
	err := server.RunScpDir()
	if err != nil {
		rs.Err = err
	} else {
		rs.Result = cmd + " ok\n"
	}
	output.Print(rs)
}

//push file or dir to remote servers
func ServersPush(src, dst string, nodeList []config.LogicUser, wt *sync.WaitGroup,
	ccons chan struct{}, crs chan machine.Result, f bool,timeout int) {

	ls := len(nodeList)
	go output.PrintResults2(crs, ls, wt, ccons, timeout)

	for _, h := range nodeList {
		ccons <- struct{}{}
		server := machine.NewScpServer(h.Host, h.Port, h.User, h.Password, "scp", src, dst, f, timeout)
		wt.Add(1)
		go server.PRunScp(crs)
	}
}

func SinglePull(src, dst string,node *config.LogicUser,force bool) {
	server := machine.NewPullServer(node.Host, node.Port, node.User, node.Password, "scp", src, dst, force)
	err := server.PullScp()
	output.PrintPullResult(node.Host, src, dst, err)
}

// pull romote server file to local
func ServersPull(src, dst string, nodeList []config.LogicUser, force bool) {

	for _, h := range nodeList {
		localPath := filepath.Join(src, h.Host)
		server := machine.NewPullServer(h.Host, h.Port, h.User, h.Password, "scp", localPath, dst, force)
		err := server.PullScp()
		output.PrintPullResult(h.Host, localPath, dst, err)
	}
}

