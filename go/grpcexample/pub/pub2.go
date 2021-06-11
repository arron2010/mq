package main

import (
	"fmt"
	mqgrpc "github.com/asim/mq/go/client/grpc"
	"github.com/asim/mq/proto"
	proto2 "github.com/golang/protobuf/proto"
)

func main() {

	c := mqgrpc.New()
	//resp, err := c.Ping("foo")
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(resp)
	text := "Hello"
	bufRow := &proto.Row{}
	bufRow.Table = text
	buf, _ := proto2.Marshal(bufRow)
	if err := c.Publish(11, "/test1/eseap/t_user", buf); err != nil {
		fmt.Println(err)
	}
}
