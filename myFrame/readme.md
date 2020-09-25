# myFrame 使用说明
myFrame读取kafka中的数据，并发送到远端，进行处理。接受远端的处理结果后，再对结果进行处理。

## 1.用途
可以当做是消费kafka的一个分布式实时流处理框架，底层是xRPC
+  服务端
    
   提供数据的处理方法，支持分布式，可以在多台机器上，启用多个实例

+  客户端

   实时消费kafka信息，发送给服务端进行处理；支持kafka事务。
   可以在多台机器上启动，形成主备，从而支持高可用

## 2.配置说明
配置文件有两个,myFrame.json 和 myLog.json

frame中配置zk和kafka连接信息；server超时设置；以及服务端和客户端的实例信息

log中配置日志相关信息
## 3.集群组成样例

服务端应用

| type  | name |    host    | port  |
|-------|:----:|------------|-------:|
| provider  | p1  | 192.168.190.50 | 8972 |
| provider  | p2  | 192.168.190.51 | 8972 |
| provider  | p3  | 192.168.190.52 | 8972 |

客户端应用

| type  | name |    host    | port  |
|-------|:----:|------------|-------:|
| consumer  | c1  | 192.168.190.50 | master |
| consumer  | c2  | 192.168.190.51 | backup |
| consumer  | c3  | 192.168.190.52 | backup |

## 4.部署方式

+ 环境变量
   
   CONFIG_PATH 为配置文件的基础目录
   实际配置文件存放在 ${CONFIG_PATH}/conf/myFrame/ 下面
+ 应用启停

服务端启动
```  
   节点1 nohup server -i p1 &
   节点2 nohup server -i p2 &
   节点3 nohup server -i p3 &
   节点1 nohup server -i p4 &
   节点2 nohup server -i p5 &
   节点3 nohup server -i p6 &
```
客户端启动 
```
   节点1 nohup client -i c1 &
   节点2 nohup client -i c2 &
   节点3 nohup client -i c3 &
```



