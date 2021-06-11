package broker

import (
	"github.com/asim/mq/config"
	"github.com/asim/mq/glogger"
	"github.com/asim/mq/handler"
	proto2 "github.com/golang/protobuf/proto"

	"github.com/asim/mq/proto"
)

func NewConsumer(topic string) *Consumer {
	c := &Consumer{}
	c.topic = topic
	c.broker = Default
	c.stop = make(chan struct{}, 0)
	return c
}

type Consumer struct {
	broker     Broker
	topic      string
	rowHandler handler.Processor
	stop       chan struct{}
}

func (c *Consumer) Run() {
	go func() {
		ch, err := c.broker.Subscribe(c.topic)
		if err != nil {
			glogger.Errorf("%s 消费者启动失败:%v", c.topic, err)
		}
		glogger.Infof("%s 消费者已启动", c.topic)

		for {
			select {
			case payload := <-ch:
				var row *proto.Row
				row = &proto.Row{}
				err = proto2.Unmarshal(payload, row)
				if err != nil {
					glogger.Errorf("主题[%s] 消费者序列化失败:%v", c.topic, err)
				} else {
					err := c.rowHandler.Process(c.topic, row)
					glogger.Errorf("主题[%s] 处理数据失败:%v", c.topic, err)
				}
			case <-c.stop:
				return
			}
		}
	}()
}

func (c *Consumer) Stop() {
	c.stop <- struct{}{}
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

func (c *ConsumerManager) add(topic string, handlerName string) {
	consumer := NewConsumer(topic)
	switch handlerName {
	//TODO: 扩展其他行数据处理
	default:
		consumer.rowHandler = handler.NewDefaultHandler(c.dao)
	}
	consumer.Run()
	c.consumers[topic] = consumer
}

func (c *ConsumerManager) GetConsumer(topic string) *Consumer {
	return c.consumers[topic]
}

func (c *ConsumerManager) Load() {

	tables := c.dao.GetTableInfo()
	for _, t := range tables {
		topic := t.GetFullName()
		c.add(topic, t.Handler)
	}
}
