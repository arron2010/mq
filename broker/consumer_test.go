package broker

import (
	"fmt"
	"github.com/asim/mq/common"
	"github.com/asim/mq/logs"
	"github.com/pingcap/errors"
	"sync"
	"testing"
)

type testRowHandler struct {
}

func (h *testRowHandler) Process(topic string, conflictStrategy string, payload []byte) error {
	fmt.Println("handle row=>>", payload)
	return errors.New("process error")
}

func TestConsumer_Run(t *testing.T) {
	var waitGroup sync.WaitGroup
	path := "/opt/mq/"
	logs.Initialize("/opt/mqlog.txt")
	common.InitBolt(path + "topic.db")
	c := NewConsumer("test01", "10", path)
	c.rowHandler = &testRowHandler{}
	payload := []byte{1, 2, 3}
	c.resolveError(payload)
	waitGroup.Add(1)
	go func() {

		c.retry()
	}()
	waitGroup.Wait()

}

func TestConsumer_DB(t *testing.T) {
	path := "/opt/mq/"
	bucket := "test01"
	common.InitBolt(path + "topic.db")
	kv := common.GetAllKeyValues([]byte(bucket))
	mh := &common.MarshalHelper{}
	for _, item := range kv {
		timestamp, payload, errorMsg := common.DecodeFatalData(mh, item.Value)
		fmt.Println("timestamp", ":", timestamp, " payload:", payload, " errorMsg", " :", errorMsg)
	}
}

///uat_orderdb/uat_orderdb/d_order_sale.
func TestConsumerManager_Load(t *testing.T) {
	topic := "uat_orderdb/uat_orderdb/d_order_sale"
	path, err := common.CreateTopicDir("/opt/mq/", topic)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(path)
}
