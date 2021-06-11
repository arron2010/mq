package handler

import (
	"database/sql"
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/proto"
	"github.com/pingcap/errors"
	"reflect"
	"strings"
	"sync"
)

var once sync.Once
var _dbHandler *DBHandler

type DBHandler struct {
	conns map[string]*sql.DB
}

func GetDBHandler() *DBHandler {
	return _dbHandler
}

func CreateDBHandler(dao *config.ConfigDAO) error {
	_dbHandler = &DBHandler{}
	dbList, err := dao.GetDBInfo()
	_dbHandler.conns = make(map[string]*sql.DB, len(dbList))
	if err != nil {
		return err
	}
	config := config.GetConfig()
	for _, dbInfo := range dbList {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s",
			dbInfo.User,
			dbInfo.Password,
			dbInfo.Addr,
			dbInfo.DB))
		if err != nil {
			return errors.Trace(err)
		}
		db.SetMaxOpenConns(config.MaxIdleConns)
		db.SetMaxIdleConns(config.MaxIdleConns)
		_dbHandler.conns[dbInfo.Name] = db
	}
	return nil
}

func (h *DBHandler) Insert(rows []interface{}, row *proto.Row, tableMapping *config.TableMapping) error {
	var sql string
	switch tableMapping.MappingType {
	case config.PART_MAPPING:
		sql = h.buildInsertSql(row.Columns, tableMapping.Columns, tableMapping.Dest)
	default:
		sql = h.buildFullInsertSql(row.Columns, tableMapping.DB, tableMapping.Dest)
	}
	err := h.executeSql(tableMapping.Server, sql, rows)
	return err
}

func (h *DBHandler) Delete(rows []interface{}, row *proto.Row, tableMapping *config.TableMapping) error {
	return nil
}

func (h *DBHandler) Update(old []interface{}, curr []interface{}, row *proto.Row, tableMapping *config.TableMapping) error {
	var sql string
	oldLen := len(old)
	currLen := len(curr)
	if oldLen != currLen {
		return errors.New(fmt.Sprintf(" %s/%s/%s新旧记录长度不一致 旧记录:%v 新记录:%v",
			row.Server, row.DB, row.Table,
			old, curr))
	}
	colNames := make([]string, 0, oldLen)
	values := make([]interface{}, 0, oldLen)

	switch tableMapping.MappingType {
	case config.PART_MAPPING:
		panic("部分更新数据功能暂未实现")
	default:
		for i := 0; i < oldLen; i++ {
			if !reflect.DeepEqual(old[i], curr[i]) {
				colNames = append(colNames, row.Columns[i].Name+"=?")
				values = append(values, curr[i])
			}
		}
	}
	if len(colNames) == 0 {
		return nil
	}
	pkLen := len(row.PKColumns)
	pkCols := make([]string, pkLen, pkLen)
	for i := 0; i < pkLen; i++ {
		index := int(row.PKColumns[i])
		pkCols[i] = row.Columns[index].Name + "=?"
		values = append(values, curr[index])
	}

	sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s=?",
		tableMapping.Dest,
		strings.Join(colNames, ","),
		strings.Join(pkCols, "AND"))

	err := h.executeSql(tableMapping.Server, sql, values)
	return err
}

/*
生成全映射Insert SQL 字段名称、字段数据类型等各种属性与目标表保持一致
*/
func (h *DBHandler) buildFullInsertSql(cols []*proto.ColumnInfo, db string, table string) string {

	l := len(cols)
	colNames := make([]string, 0, l)
	values := make([]string, 0, l)
	for i := 0; i < l; i++ {
		colNames = append(colNames, "`"+cols[i].Name+"`")
		values = append(values, "?")

	}
	sql := fmt.Sprintf("INSERT INTO `%s` .`%s` (%s) VALUES(%s)", db, table,
		strings.Join(colNames, ","),
		strings.Join(values, ","))
	return sql
}
func (h *DBHandler) isPKColumn(index int, cols []uint32) bool {
	for _, v := range cols {
		if uint32(index) == v {
			return true
		}
	}
	return false
}
func (h *DBHandler) executeSql(db string, sql string, values []interface{}) error {
	conn, ok := h.conns[db]
	if !ok {
		return errors.New(fmt.Sprintf("%s数据库连接不存在", db))
	}
	stmtIns, err := conn.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(values...)
	if err != nil {
		return err
	}
	return nil
}

/*
生成部分映射SQL,暂时预留
*/
func (h *DBHandler) buildInsertSql(cols []*proto.ColumnInfo, mappingCols []*config.ColumnMapping, table string) string {
	panic("生成部分映射SQL，暂时未实现")
}
