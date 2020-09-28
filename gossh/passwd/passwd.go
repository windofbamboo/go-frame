package passwd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"

	"golang.org/x/crypto/ssh"
)

var (
	cmd = "passwd"
	end = "exit"
)

type singleWriter struct {
	b  bytes.Buffer
	mu sync.Mutex
}

func (w *singleWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

type PassWd struct {
	client *ssh.Client
}

func NewPassWd(client *ssh.Client) *PassWd {
	psw := &PassWd{client}
	return psw
}

func pushCmd(stdinBuf *io.WriteCloser, cmd string, param ...string) error {

	var err error
	_, err = (*stdinBuf).Write(getByteCmd(cmd))
	if err != nil {
		return err
	}

	if len(param) > 0 {
		for i := range param {
			_, err = (*stdinBuf).Write(getByteCmd(param[i]))
			if err != nil {
				return err
			}
		}
	}
	_, err = (*stdinBuf).Write(getByteCmd(end))
	if err != nil {
		return err
	} else {
		return nil
	}
}

func getByteCmd(cmd string) []byte {
	cmd = cmd + "\n"
	return []byte(cmd)
}

func (psw *PassWd) ChangePSW(oldPSW, newPSW string) ([]byte, error) {

	session, err := psw.client.NewSession()
	if err != nil {
		log.Fatalln("Failed to create session: " + err.Error())
		return nil, err
	}
	defer session.Close()

	stdinBuf, err := session.StdinPipe()
	if err != nil {
		log.Fatalln("Failed to create input pipe: " + err.Error())
	}
	stdoutBuf, err := session.StdoutPipe()
	if err != nil {
		log.Fatalln("Failed to create input pipe: " + err.Error())
	}
	var b singleWriter
	session.Stdout = &b
	session.Stderr = &b

	sr := bufio.NewScanner(stdoutBuf)
	go func() {
		for sr.Scan() {
			fmt.Println("txt: ", sr.Text())
		}
	}()

	err = session.Shell()
	if err != nil {
		return nil, err
	}

	//压入命令
	err = pushCmd(&stdinBuf, cmd, oldPSW, newPSW, newPSW)
	if err != nil {
		return nil, err
	}

	err = session.Wait()
	return b.b.Bytes(), err
}

func (psw *PassWd) ChangePSWbyRoot(user, newPSW string) ([]byte, error) {

	session, err := psw.client.NewSession()
	if err != nil {
		log.Fatalln("Failed to create session: " + err.Error())
		return nil, err
	}
	defer session.Close()

	stdinBuf, err := session.StdinPipe()
	if err != nil {
		log.Fatalln("Failed to create input pipe: " + err.Error())
	}
	stdoutBuf, err := session.StdoutPipe()
	if err != nil {
		log.Fatalln("Failed to create input pipe: " + err.Error())
	}
	var b singleWriter
	session.Stdout = &b
	session.Stderr = &b

	sr := bufio.NewScanner(stdoutBuf)
	go func() {
		for sr.Scan() {
			fmt.Println("txt: ", sr.Text())
		}
	}()

	err = session.Shell()
	if err != nil {
		return nil, err
	}

	//压入命令
	cmd = fmt.Sprintf("echo \"%s\" | passwd --stdin %s", newPSW, user)
	err = pushCmd(&stdinBuf, cmd)
	if err != nil {
		return nil, err
	}

	err = session.Wait()

	return b.b.Bytes(), err
}
