package handler

import (
	"context"
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/xp/shorttext-db/utils"
	"testing"
	"time"
)

func TestDBConn_Execute2(t *testing.T) {
	conn, _ := client.Connect("127.0.0.1:3306", "root", "12345", "eseap")
	time.Sleep(60 * time.Second)
	err := conn.Ping()
	if err != nil {
		fmt.Println(err)
	}
}
func TestDBConn_Execute(t *testing.T) {
	logs.Initialize("/opt/mqlog.txt")
	configFile := `/opt/gopath/src/github.com/asim/mq/mq.yml`
	config.LoadConfig(configFile)
	conn := NewDBConn("127.0.0.1", "root", "12345", 1000, 3306)
	//NewDBConn("127.0.0.1", "root", "12345", 1000, 3306)
	time.Sleep(10 * time.Second)
	fmt.Println("after 10 second......")
	for i := 1; i <= 10; i++ {
		go func() {

			c, err := conn.pool.GetConn(context.Background())
			if err != nil {
				fmt.Println("GetConn 错误------>", err)
				return
			}
			time.Sleep(1 * time.Second)
			fmt.Println("GetConnectionID-->", c.GetConnectionID(), "Sequence-->", c.Sequence)
			//err =c.Ping()
			//if err != nil{
			//	fmt.Println("Ping 错误------>",err," GetConnectionID-->",c.GetConnectionID())
			//	return
			//}
			//c.Close()
			conn.pool.PutConn(c)
			fmt.Println("成功------>", i)
		}()

		//id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(10000))
		//// Insert
		//sql := fmt.Sprintf(`insert into eseap.t_user values (%d, "Hello DB","BBB",10,9.85,"2020-09-24 23:33:20")`, id)
		//time.Sleep(1 * time.Second)
		//r, err := conn.Execute(sql)
		//if err != nil {
		//	t.Fatal("测试不通过:", err)
		//}
		//fmt.Println("测试结果：", r)

	}
	utils.WaitFor()
}
