package client

import (
	"fmt"
	"testing"
	"time"
)

func TestStmt_Execute(t *testing.T) {
	tt := time.Now()
	str := formatTime(tt)
	fmt.Println(string(str))

}
