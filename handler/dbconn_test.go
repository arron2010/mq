package handler

import (
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"math/rand"
	"testing"
	"time"
)

func TestDBConn_Execute(t *testing.T) {
	logs.Initialize("/opt/mqlog.txt")
	configFile := `/opt/gopath/src/github.com/asim/mq/mq.yml`
	config.LoadConfig(configFile)
	conn := NewDBConn("127.0.0.1", "root", "12345", 1000, 3306)
	for i := 1; i <= 10; i++ {
		id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(10000))
		// Insert

		sql := fmt.Sprintf(`insert into eseap.t_user values (%d, "Hello DB","BBB",10,9.85,"2020-09-24 23:33:20")`, id)
		r, err := conn.Execute(sql)
		if err != nil {
			t.Fatal("测试不通过:", err)
		}
		fmt.Println("测试结果：", r)
		time.Sleep(1 * time.Second)
	}
}
