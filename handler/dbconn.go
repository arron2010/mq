package handler

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type DBConn struct {
	pool *client.Pool
}

func NewDBConn(hostname string, user string, password string, serverID uint32, port uint16) *DBConn {
	conn := &DBConn{}
	cfg := config.GetConfig()
	addr := fmt.Sprintf("%s:%d", hostname, port)
	conn.pool = client.NewPoolEx(logs.Infof,
		cfg.MinOpenConns,
		cfg.MaxOpenConns,
		cfg.MaxIdleConns,
		addr,
		user,
		password,
		"",
		func(clientConn *client.Conn) (*client.Conn, error) {
			logs.Infof("%d |%s连接创建成功 Connection Id:%d ", serverID, hostname, clientConn.GetConnectionID())
			var err error
			if err = writeRegisterSlaveCommand(clientConn, hostname, user, password, serverID, port); err != nil {
				logs.Errorf("%s 无法注册为从属服务器，错误:%v", addr, err)
				return nil, err
			}

			if _, err = clientConn.ReadOKPacket(); err != nil {
				logs.Errorf("%s 无法读取数据包，错误:%v", addr, err)
				return nil, err
			}

			//if _, err = clientConn.Execute("set sql_log_bin=0;"); err != nil {
			//	logs.Errorf("%s 无法关闭binlog，错误:%v", addr,err)
			//	return nil, err
			//}

			return clientConn, nil

		})
	return conn
}

func writeRegisterSlaveCommand(conn *client.Conn, hostname string, user string, password string, serverID uint32, port uint16) error {
	conn.ResetSequence()

	// This should be the name of slave host not the host we are connecting to.
	data := make([]byte, 4+1+4+1+len(hostname)+1+len(user)+1+len(password)+2+4+4)
	pos := 4

	data[pos] = mysql.COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], serverID)
	pos += 4

	// This should be the name of slave hostname not the host we are connecting to.
	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(user))
	pos++
	n = copy(data[pos:], user)
	pos += n

	data[pos] = uint8(len(password))
	pos++
	n = copy(data[pos:], password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return conn.WritePacket(data)
}

func (conn *DBConn) Execute(sql string, args ...interface{}) (uint64, error) {
	innerConn, err := conn.pool.GetConn(context.Background())
	if err != nil {
		return 0, err
	}
	r, err := innerConn.Execute(sql, args...)
	conn.pool.PutConn(innerConn)
	if err != nil {
		return 0, err
	}

	return r.AffectedRows, nil
}

func (conn *DBConn) Query(sql string, args ...interface{}) ([]interface{}, error) {
	var (
		innerConn *client.Conn
		err       error
		result    *mysql.Result
		row       []interface{}
		val       interface{}
	)

	innerConn, err = conn.pool.GetConn(context.Background())
	if err != nil {
		return nil, err
	}
	result, err = innerConn.Execute(sql)
	if err != nil {
		return nil, err
	}
	colNum := result.ColumnNumber()
	row = make([]interface{}, 0, colNum)
	for i := 0; i < colNum; i++ {
		val, err = result.GetValue(0, i)
		if err != nil {
			val = nil
		}
		row = append(row, val)
	}
	return row, nil
}
