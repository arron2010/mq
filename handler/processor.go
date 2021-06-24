package handler

import (
	"github.com/asim/mq/config"
	"github.com/asim/mq/encoding"
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
	Process(topic string, conflictStrategy string, row *proto2.Row) error
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
func (h *DefaultProcessor) Process(topic string, conflictStrategy string, row *proto2.Row) error {
	var err error

	var dataRows [][]interface{}

	dataRows, err = encoding.DecodeProtoRow(row)
	if err != nil {
		return err
	}
	return h.dbHandler.handle(topic, dataRows, row)
}
