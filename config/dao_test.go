package config

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestConfigDAO_GetTableInfo(t *testing.T) {
	dao, err := NewConfigDAO()
	if err != nil {
		t.Fatal(err)
	}
	tables := dao.GetTableInfo()
	tables = dao.GetTableInfo()
	tables = dao.GetTableInfo()
	tables = dao.GetTableInfo()
	fmt.Println(tables[0].Table)

	if len(tables) != 1 {
		t.Fatal("测试不通过")
	}
}

func TestConfigDAO_Insert(t *testing.T) {
	db, err := sql.Open("mysql", "root:12345@tcp(127.0.0.1:3306)/eseap2")
	if err != nil {
		fmt.Println(err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	sql := "INSERT INTO `eseap2` .`t_user3` (`id`,`name`,`desc`,`a`,`b`,`c`) VALUES(?,?,?,?,?,?)"
	t1 := time.Now()
	id := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(9000))
	rows := []interface{}{id, "a1", "b1", 100, 12.36, t1}
	r, err := db.Exec(sql, rows...)
	if err != nil {
		fmt.Println(err)
	}
	count, _ := r.RowsAffected()
	fmt.Println("影响记录数:", count, " id-->", id)
}
