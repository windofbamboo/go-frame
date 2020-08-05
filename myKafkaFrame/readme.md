zk 节点说明
    
    注册目录-分配器
    /myKafkaFrame/workerName/registry/distributor
    注册目录-工作进程
    /myKafkaFrame/workerName/registry/worker/
    分配信息
    /myKafkaFrame/workerName/distributor/{work-instance}
    kafka-topic的占用情况
    /myKafkaFrame/workerName/kafka/lock
    kafka-偏移量
    /myKafkaFrame/workerName/kafka/Offset
    分发器，lock目录
    /myKafkaFrame/workerName/lock
    
分发器说明
    
    分配器，从/myKafkaFrame/workerName/registry/worker/下，获取所有的工作进程。并将topic分配给
    工作实例；写入/myKafkaFrame/workerName/distributor/{work-instance}中


工作进程说明

    注册：注册到/myKafkaFrame/workerName/registry/worker/下
    获取需要处理的topic：从/myKafkaFrame/workerName/distributor/{work-instance}中获取被分配的topic
    偏移量信息填写：偏移量，填写到/myKafkaFrame/workerName/kafka/Offset/下面

交互说明

    分配器 和 工作进程 ,互相之间的交互，通过zk完成

