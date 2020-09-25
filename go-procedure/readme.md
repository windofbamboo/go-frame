# go-procedure 使用说明
    由于某些原因，不被允许在数据库创建存储过程。但实际上，还是有定期执行sql语句块的需求。
    为了绕过监管，达到定期在数据库中，执行sql语句块的目的；编写此小程序。
    通过在配置文件中，配置语句块，可以达到一些简单的存储过程的功能。
## 1.用途
+  打包sql语句块
    
    把简单的sql打包成语句块

+  定时执行任务

   使用cron的语法，设置定时任务

## 2.配置说明
配置文件分为三类：
+ config.xml
   
   主要用于配置 服务端信息，客户端信息；以及数据库的连接信息，服务注册信息
+ task配置
   
   主要用于配置定时的任务
+ procedures配置
   
   用户配置procedure信息；将语句块组合在一起，并命名；方便调用
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
   实际配置文件存放在 ${CONFIG_PATH}/conf/go-procedure/ 下面
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
   节点1 nohup client &
   节点2 nohup client &
   节点3 nohup client &
```