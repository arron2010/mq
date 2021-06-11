package handler

import (
	"github.com/asim/mq/config"
	"github.com/asim/mq/encoding"
	"github.com/asim/mq/glogger"
	"github.com/asim/mq/proto"
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
	Process(topic string, row *proto.Row) error
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
func (h *DefaultHandler) Process(topic string, row *proto.Row) error {
	var err error
	var tableMapping *config.TableMapping
	var rows [][]interface{}
	tableMapping, err = h.dao.GetTableMapping(topic)
	if err != nil {
		return err
	}
	rows, err = encoding.DecodeProtoRow(row)
	if err != nil {
		return err
	}
	l := len(rows)
	switch row.Action {
	case InsertAction:
		for i := 0; i < l; i++ {
			err = h.dbHandler.Insert(rows[i], row, tableMapping)
			if err != nil {
				glogger.Errorf("【%s】插入失败，数据:%v\n", topic, rows[i])
			}
		}
	case UpdateAction:
		for i := 0; i < l; i++ {
			if (i+1)%2 == 0 {
				old := rows[i-1]
				curr := rows[i]
				err = h.dbHandler.Update(old, curr, row, tableMapping)
				if err != nil {
					glogger.Errorf("【%s】更新失败，数据:%v\n", topic, rows[i])
				}
			}
		}
	case DeleteAction:
		for i := 0; i < l; i++ {
			err = h.dbHandler.Delete(rows[i], row, tableMapping)
			if err != nil {
				glogger.Errorf("【%s】删除失败，数据:%v\n", topic, rows[i])
			}
		}
	}
	glogger.Infof("Receive : %v\n", rows)

	return err
}
