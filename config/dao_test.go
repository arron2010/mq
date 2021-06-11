package config

import (
	"fmt"
	"testing"
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
