package config

import (
	"errors"
	"github.com/spf13/viper"
	"strconv"
	"strings"
)

//ssh 定义内容
type ClusterNode struct {
	name, host string
	port       int
}

//group 定义内容
type GroupNode struct {
	name, clusterName, user, password string
}

type LogicUser struct {
	AliasName, Host, User, Password string
	Port                            int
}


func ReadConfig(cfg string) (map[string][]LogicUser, error) {

	if err := Init(cfg); err != nil {
		return nil, errors.New("init cfg err :" + err.Error())
	}

	// zk
	var groupName []string
	groupName = viper.GetStringSlice("groupName")

	//IpGroup
	var clusterMap = make(map[string][]ClusterNode)
	paramContent := viper.GetStringMap("IpGroup")
	for k, vMap := range paramContent {
		clusterName := k

		var nodeList []ClusterNode
		contentMap := vMap.(map[string]interface{})
		for s, iMap := range contentMap {
			var node ClusterNode
			node.name = s
			tMap := iMap.(map[string]interface{})

			if host, ok := tMap[strings.ToLower("host")]; ok {
				node.host = host.(string)
			}
			if port, ok := tMap[strings.ToLower("port")]; ok {
				a, err := strconv.Atoi(port.(string))
				if err != nil {
				}
				node.port = a
			}
			nodeList = append(nodeList, node)
		}
		clusterMap[clusterName] = nodeList
	}

	// clusterGroup
	var groupList []GroupNode
	paramContent = viper.GetStringMap("clusterGroup")
	for k, vMap := range paramContent {
		var groupNode GroupNode
		groupNode.name = k

		contentMap := vMap.(map[string]interface{})
		if cluster, ok := contentMap[strings.ToLower("cluster")]; ok {
			groupNode.clusterName = cluster.(string)
		}
		if user, ok := contentMap[strings.ToLower("user")]; ok {
			groupNode.user = user.(string)
		}
		if password, ok := contentMap[strings.ToLower("password")]; ok {
			groupNode.password = password.(string)
		}
		groupList = append(groupList, groupNode)
	}

	//detailGroup
	var detailMap = make(map[string][]LogicUser)
	paramContent = viper.GetStringMap("detailGroup")
	for k, vMap := range paramContent {
		groupName := k

		var nodeList []LogicUser
		contentMap := vMap.(map[string]interface{})
		for s, iMap := range contentMap {
			var node LogicUser
			node.AliasName = s
			tMap := iMap.(map[string]interface{})

			if host, ok := tMap[strings.ToLower("host")]; ok {
				node.Host = host.(string)
			}
			if port, ok := tMap[strings.ToLower("port")]; ok {
				a, err := strconv.Atoi(port.(string))
				if err != nil {
				}
				node.Port = a
			}
			if user, ok := tMap[strings.ToLower("user")]; ok {
				node.User = user.(string)
			}
			if password, ok := tMap[strings.ToLower("password")]; ok {
				node.Password = password.(string)
			}
			nodeList = append(nodeList, node)
		}
		detailMap[groupName] = nodeList
	}

	res:= parseData(groupName,clusterMap,groupList,detailMap)
	return res,nil
}

func parseData(groupName []string,
	clusterMap map[string][]ClusterNode,
	groupList []GroupNode,
	detailMap map[string][]LogicUser) map[string][]LogicUser {

	if len(groupName) == 0 {
		return nil
	}

	tmpRes :=make(map[string][]LogicUser)

	for _, groupNode := range groupList {
		if _,ok:=detailMap[groupNode.name];ok{
			continue
		}
		if list,ok:=clusterMap[groupNode.clusterName];!ok{
			continue
		}else{
			var nodeList []LogicUser
			for _, clusterNode := range list {
				var logicUser LogicUser
				logicUser.AliasName = clusterNode.name
				logicUser.Host = clusterNode.host
				logicUser.Port = clusterNode.port
				logicUser.User = groupNode.user
				logicUser.Password = groupNode.password

				nodeList = append(nodeList,logicUser)
			}
			tmpRes[groupNode.name] = nodeList
		}
	}

	for groupName, users := range detailMap {
		tmpRes[groupName] = users
	}

	return tmpRes
}
