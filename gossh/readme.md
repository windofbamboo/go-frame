# gossh 使用说明
用于在远程主机，执行命令
执行方式：包括单主机执行，多主机执行；
命令包含：简单命令，文件传送，密码修改

## 1.编译
在cmd目录的gossh子目录下,执行 go build

## 2.部署方式

+ ip.json说明

   | 键值  | 说明 |
   |:-----:|------|
   | groupName    | 所有的组名 |
   | IpGroup      | 主机编组   |
   | clusterGroup | 集群组，使用主机组和用户名构成 |
   | detailGroup  | 集群组, 使用主机，用户名等构成 |

+ ip.json配置举例

```
{
  "groupName": ["app", "etcd", "redis"],
  "IpGroup": {
    "cluster01": {
      "node01":{"host": "192.168.190.50", "port": "22"},
      "node02":{"host": "192.168.190.51", "port": "22"},
      "node03":{"host": "192.168.190.52", "port": "22"}
    },
    "cluster02": {
      "node01":{"host": "192.168.190.50", "port": "22"},
      "node02":{"host": "192.168.190.51", "port": "22"},
      "node03":{"host": "192.168.190.52", "port": "22"}
    },
    "cluster03": {
      "node01":{"host": "192.168.190.50", "port": "22"},
      "node02":{"host": "192.168.190.51", "port": "22"},
      "node03":{"host": "192.168.190.52", "port": "22"}
    }
  },
  "clusterGroup": {
      "app": {"cluster": "cluster01", "user": "app", "password": "app123"},
      "etcd": {"cluster": "cluster01", "user": "etcd", "password": "etcd123"},
      "redis": {"cluster": "cluster02", "user": "redis", "password": "redis123"}
  },
  "detailGroup": {
    "app": {
      "node01": {"host": "192.168.190.50", "port": "22", "user": "app", "password": "app123"},
      "node02": {"host": "192.168.190.51", "port": "22", "user": "app", "password": "app123"},
      "node03": {"host": "192.168.190.52", "port": "22", "user": "app", "password": "app123"}
    },
    "etcd": {
      "node01": {"host": "192.168.190.50", "port": "22", "user": "etcd", "password": "etcd123"},
      "node02": {"host": "192.168.190.51", "port": "22", "user": "etcd", "password": "etcd123"},
      "node03": {"host": "192.168.190.52", "port": "22", "user": "etcd", "password": "etcd123"}
    },
    "redis": {
      "node01": {"host": "192.168.190.50", "port": "22", "user": "redis", "password": "redis123"},
      "node02": {"host": "192.168.190.51", "port": "22", "user": "redis", "password": "redis123"},
      "node03": {"host": "192.168.190.52", "port": "22", "user": "redis", "password": "redis123"}
    }
  }
}
```
+ ip.json部署

  存放在gossh的同级目录下

## 3.命令执行

   gossh -h 可以查看帮助文档




