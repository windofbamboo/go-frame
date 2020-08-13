package tools

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strings"
)

var (
	blackList = []string{"rm", "mkfs", "mkfs.ext3", "make.ext2", "make.ext4", "make2fs", "shutdown", "reboot", "init", "dd"}
)


//check the comand safe
//true:safe false:refused
func CheckSafe(cmd string) bool {

	lCmd := strings.ToLower(cmd)
	cmds := strings.Split(lCmd, " ")
	for _, ds := range cmds {
		for _, bk := range blackList {
			if ds == bk {
				return false
			}
		}
	}
	return true
}

//check path is exit
func FileExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	} else {
		return !fi.IsDir()
	}
}

func PathExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	} else {
		return fi.IsDir()
	}
}

func MakePath(path string) error {
	if FileExists(path) {
		return errors.New(path + " is a normal file ,not a dir")
	}

	if !PathExists(path) {
		return os.MkdirAll(path, os.ModePerm)
	} else {
		return nil
	}
}

func ReadCmdFile(fileName string,force bool) (string,error) {
	if !FileExists(fileName){
		return "",errors.New("file: "+fileName+" not found ")
	}

	// todo 解析文件，读取命令行
	f, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	lines := make([]string, 0)
	buf := bufio.NewReader(f)
	for {
		s, err := buf.ReadString('\n')
		if err != nil {
			//主要是兼容windows和linux文件格式
			if err == io.EOF && s != "" {
				goto Label1
			} else {
				newCmd := strings.Join(lines, "&&")
				return newCmd, nil
			}
		}
	Label1:
		line := strings.TrimSpace(s)
		if line == "" || line[0] == '#' {
			continue
		}
		if ok :=CheckSafe(line) ;!ok && force == false{
			return "", errors.New("Dangerous command in : [ "+line +" ]")
		}
		lines = append(lines, line)
	}

	//newCmd := strings.Join(lines, "&&")
	//return newCmd, err
}