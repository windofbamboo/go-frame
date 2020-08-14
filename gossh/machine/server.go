package machine

import (
	"errors"
	"fmt"
	"golang.org/x/crypto/ssh"
	"gossh/logs"
	psw "gossh/passwd"
	"gossh/scp"
	"gossh/tools"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	NO_PASSWORD = "GET PASSWORD ERROR\n"
	log = logs.NewLogger()
)

const (
	NO_EXIST = "0"
	IS_FILE  = "1"
	IS_DIR   = "2"
)

type Server struct {
	Ip         string
	Port       int
	User       string
	Psw        string
	Action     string
	Cmd        string
	FileName   string
	RemotePath string
	Force      bool
	Timeout    int
}

type ScpConfig struct {
	Src string
	Dst string
}

type Result struct {
	Ip     string
	Cmd    string
	Result string
	Err    error
}

func NewCmdServer(ip string, port int, user, psw, action, cmd string, force bool, timeout int) *Server {
	server := &Server{
		Ip:      ip,
		Port:    port,
		User:    user,
		Action:  action,
		Cmd:     cmd,
		Psw:     psw,
		Force:   force,
		Timeout: timeout,
	}

	return server
}

func NewScpServer(ip string, port int, user, psw, action, file, rpath string, force bool, timeout int) *Server {
	rfile := path.Join(rpath, path.Base(file))
	cmd := createShell(rfile)
	server := &Server{
		Ip:         ip,
		Port:       port,
		User:       user,
		Psw:        psw,
		Action:     action,
		FileName:   file,
		RemotePath: rpath,
		Cmd:        cmd,
		Force:      force,
		Timeout:    timeout,
	}

	return server
}
func NewPullServer(ip string, port int ,user, psw, action, file, rpath string, force bool) *Server {
	cmd := createShell(rpath)
	server := &Server{
		Ip:         ip,
		Port:       port,
		User:       user,
		Psw:        psw,
		Action:     action,
		FileName:   file,
		RemotePath: rpath,
		Cmd:        cmd,
		Force:      force,
	}

	return server
}

//run command for parallel
func (s *Server) PRunCmd(crs chan Result) {
	rs := s.SRunCmd()
	crs <- rs
}

func (s *Server) PPswCmd(oldPSW, newPSW string,crs chan Result) {
	rs := s.SPswCmd(oldPSW, newPSW)
	crs <- rs
}

// set Server.Cmd
func (s *Server) SetCmd(cmd string) {
	s.Cmd = cmd
}

//run command in sequence
func (s *Server) RunCmd() (result string, err error) {
	if s.Psw == NO_PASSWORD {
		return NO_PASSWORD, nil
	}
	client, err := s.GetSshClient()
	if err != nil {
		return "getSSHClient error", err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "newSession error", err
	}
	defer session.Close()

	cmd := s.Cmd
	bs, err := session.CombinedOutput(cmd)
	if err != nil {
		return string(bs), err
	}
	return string(bs), nil
}

//run command in sequence
func (s *Server) SRunCmd() Result {
	rs := Result{
		Ip:  s.Ip,
		Cmd: s.Cmd,
	}

	if s.Psw == NO_PASSWORD {
		rs.Err = errors.New(NO_PASSWORD)
		return rs
	}

	var client *ssh.Client
	var err error

	client, err = s.GetSshClient()
	if err != nil {
		rs.Err = err
		return rs
	}
	defer client.Close()

	var session *ssh.Session
	session, err = client.NewSession()
	if err != nil {
		rs.Err = err
		return rs
	}
	defer session.Close()

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,     // disable echoing
		ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}
	if err := session.RequestPty("xterm", 80, 40, modes); err != nil {
		rs.Err = err
		return rs
	}

	cmd := s.Cmd
	bs, err := session.CombinedOutput(cmd)
	if err != nil {
		rs.Err = err
		return rs
	}
	rs.Result = string(bs)
	return rs
}

func (s *Server) SPswCmd(oldPSW, newPSW string) Result {
	rs := Result{
		Ip:  s.Ip,
		Cmd: s.Cmd,
	}

	if s.Psw == NO_PASSWORD {
		rs.Err = errors.New(NO_PASSWORD)
		return rs
	}

	var client *ssh.Client
	var err error
	client, err = s.GetSshClient()
	if err != nil {
		rs.Err = err
		return rs
	}
	defer client.Close()

	p:= psw.NewPassWd(client)

	bs,err := p.ChangePSW(oldPSW, newPSW)
	if err!=nil{
		rs.Err = err
		return rs
	}
	rs.Result = string(bs)
	return rs
}


//execute a single command on remote server
func (s *Server) checkRemoteFile() (result string) {
	re, _ := s.RunCmd()
	return re
}

//PRunScp() can transport  file or path to remote host
func (s *Server) PRunScp(crs chan Result) {
	cmd := "push " + s.FileName + " to " + s.Ip + ":" + s.RemotePath
	rs := Result{
		Ip:  s.Ip,
		Cmd: cmd,
	}
	result := s.RunScpDir()
	if result != nil {
		rs.Err = result
	} else {
		rs.Result = cmd + " ok\n"
	}
	crs <- rs
}

func (s *Server) RunScpDir() (err error) {
	re := strings.TrimSpace(s.checkRemoteFile())
	log.Debug("server.checkRemoteFile()=%s\n", re)

	//远程机器存在同名文件
	if re == IS_FILE && s.Force == false {
		errString := "<ERROR>\nRemote Server's " + s.RemotePath + " has the same file " + s.FileName + "\nYou can use `-f` option force to cover the remote file.\n</ERROR>\n"
		return errors.New(errString)
	}

	rfile := s.RemotePath
	cmd := createShell(rfile)
	s.SetCmd(cmd)
	re = strings.TrimSpace(s.checkRemoteFile())
	log.Debug("server.checkRemoteFile()=%s\n", re)

	//远程目录不存在
	if re != IS_DIR {
		errString := "[" + s.Ip + ":" + s.RemotePath + "] does not exist or not a dir\n"
		return errors.New(errString)
	}

	client, err := s.GetSshClient()
	if err != nil {
		return err
	}
	defer client.Close()

	filename := s.FileName
	fi, err := os.Stat(filename)
	if err != nil {
		log.Debug("open source file %s error\n", filename)
		return err
	}
	myScp := scp.NewScp(client)
	if fi.IsDir() {
		err = myScp.PushDir(filename, s.RemotePath)
		return err
	}
	err = myScp.PushFile(filename, s.RemotePath)
	return err
}

//pull file from remote to local server
func (s *Server) PullScp() (err error) {

	//判断远程源文件情况
	re := strings.TrimSpace(s.checkRemoteFile())
	log.Debug("server.checkRemoteFile()=%s\n", re)

	//不存在报错
	if re == NO_EXIST {
		errString := "Remote Server's " + s.RemotePath + " doesn't exist.\n"
		return errors.New(errString)
	}

	//不支持拉取目录
	if re == IS_DIR {
		errString := "Remote Server's " + s.RemotePath + " is a directory ,not support.\n"
		return errors.New(errString)
	}

	//仅仅支持普通文件
	if re != IS_FILE {
		errString := "Get info from Remote Server's " + s.RemotePath + " error.\n"
		return errors.New(errString)
	}

	//本地目录
	dst := s.FileName
	//远程文件
	src := s.RemotePath

	log.Debug("src=%s", src)
	log.Debug("dst=%s", dst)

	//本地路径不存在，自动创建
	err = tools.MakePath(dst)
	if err != nil {
		return err
	}

	//检查本地是否有同名文件
	fileName := filepath.Base(src)
	localFile := filepath.Join(dst, fileName)

	flag := tools.FileExists(localFile)
	log.Debug("flag=%v", flag)
	log.Debug("localFile=%s", localFile)

	//-f 可以强制覆盖
	if flag && !s.Force {
		return errors.New(localFile + " is exist, use -f to cover the old file")
	}

	//执行pull
	client, err := s.GetSshClient()
	if err != nil {
		return err
	}
	defer client.Close()

	myScp := scp.NewScp(client)
	err = myScp.PullFile(dst, src)
	return err
}

//RunScp1() only can transport  file to remote host
func (s *Server) RunScpFile() (result string, err error) {
	client, err := s.GetSshClient()
	if err != nil {
		return "GetSSHClient Error\n", err
	}
	defer client.Close()

	filename := s.FileName
	session, err := client.NewSession()
	if err != nil {
		return "Create SSHSession Error", err
	}
	defer session.Close()

	go func() {
		Buf := make([]byte, 1024)
		w, _ := session.StdinPipe()
		defer w.Close()
		//File, err := os.Open(filepath.Abs(filename))
		File, err := os.Open(filename)
		if err != nil {
			log.Debug("open scp source file %s error\n", filename)
			return
		}
		defer File.Close()

		info, _ := File.Stat()
		newName := filepath.Base(filename)
		fmt.Fprintln(w, "C0644", info.Size(), newName)
		for {
			n, err := File.Read(Buf)
			fmt.Fprint(w, string(Buf[:n]))
			if err != nil {
				if err == io.EOF {
					fmt.Fprint(w, "\x00")
					return
				} else {
					fmt.Println("read scp source file error")
					return
				}
			}
		}
	}()

	cmd := "/usr/bin/scp -qt " + s.RemotePath
	bs, err := session.CombinedOutput(cmd)
	if err != nil {
		return string(bs), err
	}
	return string(bs), nil
}

// implement ssh auth method [password keyboard-interactive] and [password]
func (s *Server) GetSshClient() (client *ssh.Client, err error) {
	var authMethods []ssh.AuthMethod
	keyboardInteractiveChallenge := func(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
		if len(questions) == 0 {
			return []string{}, nil
		}

		answers = make([]string, len(questions))
		for i := range questions {
			yes, _ := regexp.MatchString("*yes*", questions[i])
			if yes {
				answers[i] = "yes"

			} else {
				answers[i] = s.Psw
			}
		}
		return answers, nil
	}
	authMethods = append(authMethods, ssh.KeyboardInteractive(keyboardInteractiveChallenge))
	authMethods = append(authMethods, ssh.Password(s.Psw))

	sshConfig := &ssh.ClientConfig{
		User: s.User,
		Auth: authMethods,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		Timeout: (time.Duration(s.Timeout)) * time.Second,
	}
	//psw := []ssh.AuthMethod{ssh.Password(server.Psw)}
	//Conf := ssh.ClientConfig{User: server.User, Auth: psw}
	ipPort := s.Ip + ":" + strconv.Itoa(s.Port)
	client, err = ssh.Dial("tcp", ipPort, sshConfig)
	return
}

//create shell script for running on remote server
func createShell(file string) string {
	s1 := "bash << EOF \n"
	s2 := "if [[ -f " + file + " ]];then \n"
	s3 := "echo '1'\n"
	s4 := "elif [[ -d " + file + " ]];then \n"
	s5 := `echo "2"
else 
echo "0"
fi
EOF`
	cmd := s1 + s2 + s3 + s4 + s5
	return cmd
}
