package handler

import (
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"github.com/pingcap/errors"
	"github.com/wj596/go-mysql-transfer/proto"
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
func TestCreateDBHandler(t *testing.T) {
	err := CreateDBHandler(getDAO())
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
		t.Fatal("失败")
	}
}
func TestDBHandler_Insert(t *testing.T) {
	logs.Initialize("/opt/mqlog.txt")
	err := CreateDBHandler(getDAO())
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
		t.Fatal("失败")
	}
	handler := GetDBHandler()
	now := time.Now()
	const format = "2006-01-02 15:04:05"
	myTime := now

	id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(10001))

	rows := []interface{}{2675, "a1", "b1", 300, 12.36, myTime}
	fmt.Println("自动生成ID-->", id)
	row := &proto.Row{}
	row.PKColumns = []uint32{0}

	//tableMapping := &config.Strategy{DestTable: "t_user3", DestServer: "test", DestDB: "eseap2"}
	topic := "/test1/eseap/t_user"
	tableMapping, _ := handler.getStrategy(topic)
	tbl, _ := handler.sourceTables[topic]
	//config.IGNORE_CONFLICT  USE_CONFLICT
	err = handler.insert(config.IGNORE_CONFLICT, rows, 6, tableMapping, tbl)
	if err != nil {
		fmt.Println(errors.ErrorStack(err))

		logs.Errorf("CreateDBHandler错误:%v\n", err)
	}
}

func getDAO() *config.ConfigDAO {
	var dao *config.ConfigDAO
	configFile := `/opt/gopath/src/github.com/asim/mq/mq.yml`
	err := config.LoadConfig(configFile)
	if err != nil {
		logs.Errorf("配置信息加载发生错误！")
		return dao
	}
	dao, err = config.NewConfigDAO()
	if err != nil {
		logs.Errorf("数据库连接发生错误！")
		return dao
	}
	return dao
}

func TestDBHandler_Update(t *testing.T) {
	logs.Initialize("/opt/mqlog.txt")
	err := CreateDBHandler(getDAO())
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
		t.Fatal("失败")
	}
	handler := GetDBHandler()
	t2 := time.Now()
	const format = "2006-01-02 15:04:05"
	t1 := t2.Format(format)
	id := 9858
	old := []interface{}{id, "a1", "b1", 102, 13.35, t1}
	newRow := []interface{}{id, "a2", "b2", 103, 13.35, t1}
	row := &proto.Row{}
	row.PKColumns = []uint32{0}
	topic := "/test1/eseap/t_user"
	tableMapping, _ := handler.getStrategy(topic)
	tbl, _ := handler.sourceTables[topic]
	err = handler.update(config.IGNORE_CONFLICT, old, newRow, 6, tbl, tableMapping)
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
	}
}

func TestDBHandler_Update2(t *testing.T) {
	logs.Initialize("/opt/mqlog.txt")
	err := CreateDBHandler(getDAO())
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
		t.Fatal("失败")
	}
	handler := GetDBHandler()
	t2 := time.Now()
	const format = "2006-01-02 15:04:05"
	t1 := t2.Format(format)
	id := 9858
	old := []interface{}{id, "a2", "b2", 103, 13.35, t1}
	newRow := []interface{}{id, "a2", "b1", nil, 13.35, t1}
	row := &proto.Row{}
	row.PKColumns = []uint32{0}
	topic := "/test1/eseap/t_user"
	tableMapping, _ := handler.getStrategy(topic)
	tbl, _ := handler.sourceTables[topic]
	err = handler.update(config.USE_CONFLICT, old, newRow, 6, tbl, tableMapping)
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
	}
}

func TestDBHandler_Delete(t *testing.T) {
	logs.Initialize("/opt/mqlog.txt")
	err := CreateDBHandler(getDAO())
	if err != nil {
		logs.Errorf("CreateDBHandler错误:%v\n", err)
		t.Fatal("失败")
	}
	handler := GetDBHandler()
	t1 := time.Now()
	id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(9000))
	rows := []interface{}{id, "a1", "b1", 100, 12.36, t1}
	topic := "/test1/eseap/t_user"
	tableMapping, _ := handler.getStrategy(topic)
	tbl, _ := handler.sourceTables[topic]
	handler.delete(rows, tbl, tableMapping)
}
func TestNewDefaultHandler(t *testing.T) {
	//loc := time.Local
	//tt := time.Now()
	//
	//fmt.Println(	tt.In(loc).String())
	//
	//id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(10000))
	//fmt.Println(id)
	err := errors.New("aaaa")
	errors.Trace(err)
	fmt.Printf("%+v", err)
	//logs.Initialize("/opt/mqlog.txt")
	//logs.Infof("hello %s", "kk")
	//logs.Infof("hello %s", "kk")
}
