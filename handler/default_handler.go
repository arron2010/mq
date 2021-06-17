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
	Process(topic string, row *proto2.Row) error
}

type DefaultHandler struct {
	dao       *config.ConfigDAO
	dbHandler *DBHandler
}

func NewDefaultHandler(dao *config.ConfigDAO) Processor {
	h := &DefaultHandler{}
	h.dao = dao
	h.dbHandler = GetDBHandler()

	return h
}
func (h *DefaultHandler) Process(topic string, row *proto2.Row) error {
	var err error

	var dataRows [][]interface{}

	dataRows, err = encoding.DecodeProtoRow(row)
	if err != nil {
		return err
	}
	return h.dbHandler.handle(topic, dataRows, row)
}
