package handler

import (
	"github.com/asim/mq/config"
	"github.com/asim/mq/encoding"
	"github.com/asim/mq/logs"
	"github.com/golang/protobuf/proto"
	proto2 "github.com/wj596/go-mysql-transfer/proto"
)

const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

const (
	DEFAULT_HANDLER = "1000"
)

type Processor interface {
	Process(topic string, conflictStrategy string, payload []byte) error
}

type DefaultProcessor struct {
	dao       *config.ConfigDAO
	dbHandler *DBHandler
}

func NewDefaultProcessor(dao *config.ConfigDAO) Processor {
	h := &DefaultProcessor{}
	h.dao = dao

	h.dbHandler = GetDBHandler()
	return h
}
func (h *DefaultProcessor) Process(topic string, conflictStrategy string, payload []byte) error {

	var row *proto2.Row
	row = &proto2.Row{}

	var err error
	var dataRows [][]interface{}
	err = proto.Unmarshal(payload, row)
	if err != nil {
		logs.Errorf("主题[%s] 消费者序列化失败:%v", topic, err)
		return err
	}
	dataRows, err = encoding.DecodeProtoRow(row)
	if err != nil {
		return err
	}

	return h.dbHandler.handle(topic, conflictStrategy, dataRows, row)
}
