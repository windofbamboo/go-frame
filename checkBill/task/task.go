package main

import (
	"checkBill"
	"errors"
	"fmt"
	"github.com/wonderivan/logger"
	"os"
	"sort"
	"time"
)

//func daemon(nochdir, noclose int) int {
//	var ret, ret2 uintptr
//	var err syscall.Errno
//	darwin := runtime.GOOS == "darwin"
//	// already a daemon
//	if syscall.Getppid() == 1 {
//		return 0
//	}
//	// fork off the parent process
//	ret, ret2, err = syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
//	if err != 0 {
//		return -1
//	}
//	// failure
//	if ret2 < 0 {
//		os.Exit(-1)
//	}
//	// handle exception for darwin
//	if darwin && ret2 == 1 {
//		ret = 0
//	}
//	// if we got a good PID, then we call exit the parent process.
//	if ret > 0 {
//		os.Exit(0)
//	}
//	/* Change the file mode mask */
//	_ = syscall.Umask(0)
//
//	// create a new SID for the child process
//	s_ret, s_errno := syscall.Setsid()
//	if s_errno != nil {
//		log.Printf("Error: syscall.Setsid errno: %d", s_errno)
//	}
//	if s_ret < 0 {
//		return -1
//	}
//	if nochdir == 0 {
//		os.Chdir("/")
//	}
//	if noclose == 0 {
//		f, e := os.OpenFile("/dev/null", os.O_RDWR, 0)
//		if e == nil {
//			fd := f.Fd()
//			syscall.Dup2(int(fd), int(os.Stdin.Fd()))
//			syscall.Dup2(int(fd), int(os.Stdout.Fd()))
//			syscall.Dup2(int(fd), int(os.Stderr.Fd()))
//		}
//	}
//	return 0
//}

func main() {

	//daemon(0, 1)

	validRegionCodes, channel := checkBill.TaskInit()

	lockFile := checkBill.GetFullLockFile(checkBill.AuditScopeProvince, []string{checkBill.DefaultRegionCode}, channel, checkBill.DefaultId)
	logger.Trace("lockFile :", lockFile)

	lock, err := os.Create(lockFile)
	if err != nil {
		panic(errors.New("create lockFile err"))
	}
	defer os.Remove(lockFile)
	defer lock.Close()

	err = checkBill.InitPoolMap()
	checkBill.CheckErr(err)

	dealTask(validRegionCodes, channel)

	checkBill.DestroyPoolMap()
}

func dealTask(validRegionCodes []string, channel int) {

	for {
		lastDay := checkBill.GetNDayBefore(1)

		taskMap := getValidTask(validRegionCodes, lastDay, channel)

		if len(taskMap) > 0 {
			checkBill.AuditRegion(&taskMap)
		} else {
			logger.Info(fmt.Sprintf("not has valid task, time is : %s ", checkBill.GetCurrentTime()))
		}

		time.Sleep(5 * time.Minute)
	}
}

func getValidTask(validRegionCodes []string, auditDay int, channel int) map[string]checkBill.CheckTaskSlice {

	finishTasks, err := checkBill.QueryCheckSchedule(validRegionCodes, auditDay, channel)
	checkBill.CheckErr(err)

	allTasks := checkBill.GetAllTask(validRegionCodes, auditDay, channel)

	validTasks := checkBill.GetValidTask(&allTasks, &finishTasks)

	sort.Sort(validTasks)

	taskMap := make(map[string]checkBill.CheckTaskSlice)
	for _, task := range validTasks {
		if _, ok := taskMap[task.RegionCode]; !ok {
			ctSlice := checkBill.CheckTaskSlice{task}
			taskMap[task.RegionCode] = ctSlice
		} else {
			taskMap[task.RegionCode] = append(taskMap[task.RegionCode], task)
		}
	}

	return taskMap
}
