package handler

import (
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/glogger"
	"github.com/asim/mq/proto"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func equal(a interface{}, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}
func TestEqual(t *testing.T) {
	a := 3
	b := 3
	if equal(a, b) {
		fmt.Println("1.整型比较成功")
	}

	a = 3
	b = 4
	if !equal(a, b) {
		fmt.Println("2.整型比较成功")
	}

	a1 := 3.23
	b1 := 3.23
	if equal(a1, b1) {
		fmt.Println("2.整型比较成功")
	}

	a2, _ := time.ParseInLocation(`2006-01-02 15:04:05`, `2021-06-11 15:04:0`, time.Local)
	b2, _ := time.ParseInLocation(`2006-01-02 15:04:05`, `2021-06-11 15:04:0`, time.Local)

	if equal(a2, b2) {
		fmt.Println("a2,b2 比较成功")
	}
}

func TestDBHandler_Insert(t *testing.T) {
	err := CreateDBHandler(getDAO())
	if err != nil {
		glogger.Errorf("CreateDBHandler错误:%v\n", err)
		t.Fatal("失败")
	}
	handler := GetDBHandler()
	t1 := time.Now()
	id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(9000))
	rows := []interface{}{id, "a1", "b1", 100, 12.36, t1}
	fmt.Println("自动生成ID-->", id)
	row := &proto.Row{}
	row.PKColumns = []uint32{0}
	row.Columns = []*proto.ColumnInfo{
		{Name: "id", IsAuto: 1},
		{Name: "name"},
		{Name: "desc"},
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}
	tableMapping := &config.TableMapping{Dest: "t_user3", Server: "test", DB: "eseap2"}
	err = handler.Insert(rows, row, tableMapping)
	if err != nil {
		glogger.Errorf("CreateDBHandler错误:%v\n", err)
	}
}

func getDAO() *config.ConfigDAO {
	var dao *config.ConfigDAO
	configFile := `/opt/gopath/src/github.com/asim/mq/mq.yml`
	err := config.LoadConfig(configFile)
	if err != nil {
		glogger.Errorf("配置信息加载发生错误！")
		return dao
	}
	dao, err = config.NewConfigDAO()
	if err != nil {
		glogger.Errorf("数据库连接发生错误！")
		return dao
	}
	return dao
}
