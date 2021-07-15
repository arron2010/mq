package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/asim/mq/common"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"github.com/pingcap/errors"
	"strconv"
	"time"

	"github.com/asim/mq/handler"
)

const (
	checkTime   = time.Second * 2
	retryCount  = 3
	elapsedTime = 10
)

func NewConsumer(topic string, conflictStrategy string, path string) *Consumer {
	c := &Consumer{}
	c.topic = topic
	c.broker = Default
	c.stop = make(chan struct{}, 0)
	c.conflictStrategy = conflictStrategy
	common.CreateBucket(topic)
	queuePath, err := common.CreateTopicDir(path, topic)
	if err != nil {
		panic(err)
	}
	c.diskQueue = NewDiskQueue(common.CreateDiskQueueName(topic), queuePath, 1024*1024*10, 4,
		1024*1024*10, 1024, 10*time.Second, func(lvl LogLevel, f string, args ...interface{}) {
			logs.Info(fmt.Sprintf(lvl.String()+": "+f, args...))
		})
	return c
}

type Consumer struct {
	broker           Broker
	topic            string
	conflictStrategy string
	rowHandler       handler.Processor
	stop             chan struct{}
	diskQueue        common.DiskQueue
	mh               common.MarshalHelper
}

func (c *Consumer) Run() {
	go c.start()
	go c.retry()
}

func (c *Consumer) Stop() {
	c.stop <- struct{}{}
}

func (c *Consumer) start() {
	ch, err := c.broker.Subscribe(c.topic)
	if err != nil {
		logs.Errorf("%s 消费者启动失败:%v", c.topic, err)
	}
	logs.Infof("%s 消费者已启动", c.topic)

	for {
		select {
		case payload := <-ch:
			err := c.rowHandler.Process(c.topic, c.conflictStrategy, payload)
			if err != nil {
				logs.Errorf("主题[%s] 处理数据失败:%v", c.topic, err)
				c.resolveError(payload)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *Consumer) resolveError(payload []byte) {

	timestamp := time.Now().Unix()
	data, err := c.encode(0, timestamp, payload)
	if err != nil {
		logs.Errorf("消费者处理错误数据时，编码错误:%v", c.topic, err)
		c.putErrorData(timestamp, payload, err.Error())
	} else {
		logs.Infof("推入磁盘队列，消息大小:%d", len(data))
		c.diskQueue.Put(data)
	}
}

func (c *Consumer) retry() {
	ticker := time.NewTicker(checkTime)
	for range ticker.C {
		select {
		case msgOut := <-c.diskQueue.ReadChan():
			logs.Infof("从磁盘队列获取数据，重新处理，消息大小:%d", len(msgOut))
			n, timestamp, payload, err := c.decode(msgOut)
			if err != nil {
				logs.Errorf("主题[%s] 解码数据失败:%v", c.topic, err)
			} else {
				elapsed := time.Now().Unix() - timestamp
				if n < retryCount && elapsed < elapsedTime {
					err := c.rowHandler.Process(c.topic, c.conflictStrategy, payload)
					if err != nil {
						n = n + 1
						data, err := c.encode(n, timestamp, payload)
						if err != nil {
							c.putErrorData(timestamp, payload, err.Error())
						} else {
							c.diskQueue.Put(data)
						}
					}
				} else {
					c.putErrorData(timestamp, payload, "处理次数超过:"+strconv.Itoa(retryCount)+"次")
				}
			}
		}
	}
}

func (c *Consumer) encode(n uint8, timestamp int64, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	c.mh.WriteNumber(&buf, n)
	c.mh.WriteNumber(&buf, timestamp)
	c.mh.WriteSlice(&buf, data)
	return buf.Bytes(), errors.Trace(c.mh.GetErr())

}

func (c *Consumer) decode(data []byte) (n uint8, timestamp int64, payload []byte, err error) {
	buf := bytes.NewBuffer(data)
	c.mh.ReadNumber(buf, &n)
	c.mh.ReadNumber(buf, &timestamp)
	c.mh.ReadSlice(buf, &payload)
	return n, timestamp, payload, c.mh.GetErr()
}

func (c *Consumer) putErrorData(timestamp int64, payload []byte, errorMsg string) {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], timestamp)
	key := data[:n]

	val := common.EncodeFatalData(&c.mh, timestamp, payload, errorMsg)
	common.Put([]byte(c.topic), key, val)
	logs.Errorf("消息队列存在失信数据，timestamp:%d payload:%v", timestamp, payload)
}

type ConsumerManager struct {
	consumers map[string]*Consumer
	dao       *config.ConfigDAO
}

func NewConsumerManager(dao *config.ConfigDAO) *ConsumerManager {
	c := &ConsumerManager{}
	c.consumers = make(map[string]*Consumer)
	c.dao = dao
	return c
}

func (c *ConsumerManager) add(topic string, conflictStrategy string, path string) {
	consumer := NewConsumer(topic, conflictStrategy, path)

	consumer.rowHandler = handler.NewDefaultProcessor(c.dao)
	consumer.Run()
	c.consumers[topic] = consumer

}

func (c *ConsumerManager) GetConsumer(topic string) *Consumer {
	return c.consumers[topic]
}

func (c *ConsumerManager) Load() {
	path := config.GetConfig().DataPath
	tables := c.dao.GetTableInfo()
	for _, t := range tables {
		topic := t.GetFullName()
		c.add(topic, t.Handler, path)

	}
}
