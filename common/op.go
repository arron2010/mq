package common

const (
	OP_PUB = 1
)

const (
	//执行同步节点ID最小值，在捕获binlog日志消息，能获取务器ID
	//用来标识哪些记录是复制程序产生的binlog
	MIN_REPLICATION_SLAVE = 0x10000000
)
