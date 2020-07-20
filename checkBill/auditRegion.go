package checkBill

import (
	"fmt"
	"github.com/wonderivan/logger"
	"strings"
	"sync"
)

func AuditRegion(taskMap *map[string]CheckTaskSlice) {

	logger.Info(fmt.Sprintf(" audit begin ..."))
	isConcurrent := configContent.CommonAttributes[strings.ToLower(AttributeRegionConcurrent)].(bool)

	if isConcurrent {
		var wg sync.WaitGroup
		wg.Add(len(*taskMap))
		for _, ctSlice := range *taskMap {
			go func(ctSlice CheckTaskSlice) {
				defer wg.Done()
				dealRegionCode(&ctSlice)
			}(ctSlice)
		}
		wg.Wait()
	} else {
		for _, ctSlice := range *taskMap {
			dealRegionCode(&ctSlice)
		}
	}
	logger.Info(fmt.Sprintf(" audit end ..."))
}

func dealRegionCode(ctSlice *CheckTaskSlice) {

	regionCode := (*ctSlice)[0].RegionCode

	logger.Info(fmt.Sprintf("\tRegionCode [ %s ] begin ... ", regionCode))
	isConcurrent := configContent.CommonAttributes[strings.ToLower(AttributeChannelConcurrent)].(bool)

	if isConcurrent {
		var wg sync.WaitGroup
		wg.Add(len(*ctSlice))

		for _, task := range *ctSlice {
			go func(task *CheckTask) {
				defer wg.Done()
				if err := task.dealChannel(); err != nil {
					logger.Info(fmt.Sprintf("\t\tRegionCode : %s channel: %d deal err : %s ", task.RegionCode, task.Channel, err.Error()))
				} else {
					logger.Info(fmt.Sprintf("\t\tRegionCode : %s channel: %d deal sucess ...", task.RegionCode, task.Channel))
				}
			}(&task)
		}
		wg.Wait()
	} else {
		for _, task := range *ctSlice {
			if err := task.dealChannel(); err != nil {
				logger.Info(fmt.Sprintf("\t\tRegionCode : %s channel: %d deal err : %s ", task.RegionCode, task.Channel, err.Error()))
			} else {
				logger.Info(fmt.Sprintf("\t\tRegionCode : %s channel: %d deal sucess ...", task.RegionCode, task.Channel))
			}
		}
	}

	logger.Info(fmt.Sprintf("\tRegionCode [ %s ] end ... ", regionCode))
}
