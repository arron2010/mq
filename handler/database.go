package handler

import (
	"database/sql"
	"fmt"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/pingcap/errors"
	proto2 "github.com/wj596/go-mysql-transfer/proto"
	"reflect"
	"strings"
	"sync"
)

var once sync.Once
var _dbHandler *DBHandler

type DBHandler struct {
	conns        map[string]*sql.DB
	strategies   map[string]*config.Strategy
	sourceTables map[string]*schema.Table
	srcConns     map[string]*sql.DB
}

func GetDBHandler() *DBHandler {
	return _dbHandler
}

func CreateDBHandler(dao *config.ConfigDAO) error {
	_dbHandler = &DBHandler{}
	_dbHandler.conns = make(map[string]*sql.DB)
	_dbHandler.srcConns = make(map[string]*sql.DB)

	_dbHandler.strategies = make(map[string]*config.Strategy)
	_dbHandler.sourceTables = make(map[string]*schema.Table)

	dbList, err := dao.GetDBInfo()

	if err != nil {
		return err
	}
	cfg := config.GetConfig()
	for _, dbInfo := range dbList {
		if dbInfo.DBType != config.SOURCE_DB_TYPE && dbInfo.DBType != config.DEST_DB_TYPE {
			continue
		}

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s",
			dbInfo.User,
			dbInfo.Password,
			dbInfo.Addr,
			dbInfo.DB))
		if err != nil {
			return errors.Trace(err)
		}
		db.SetMaxOpenConns(cfg.MaxIdleConns)
		db.SetMaxIdleConns(cfg.MaxIdleConns)
		if dbInfo.DBType == config.DEST_DB_TYPE {
			_dbHandler.conns[dbInfo.Name] = db
		} else {
			_dbHandler.srcConns[dbInfo.Name] = db
		}
	}
	strategyList, err := dao.GetAllStrategies()
	if err != nil {
		return err
	}

	for _, s := range strategyList {
		server, db, table := config.TableInfo(s.Path)
		if len(table) == 0 {
			return errors.New(s.Path + "源表路径格式不正确")
		}
		currDB, ok := _dbHandler.srcConns[server]
		if !ok {
			return errors.New(server + "数据库不存在")
		}
		tbl, err := schema.NewTableFromSqlDB(currDB, db, table)
		if err != nil {
			return err
		}
		_dbHandler.sourceTables[s.Path] = tbl
		_dbHandler.strategies[s.Path] = s

	}
	return nil
}
func (h *DBHandler) handle(topic string, rows [][]interface{}, row *proto2.Row) error {

	tableMapping, ok := h.getStrategy(topic)
	if !ok {
		return errors.New(topic + "找不到映射策略")
	}

	tbl, ok := h.sourceTables[topic]
	if !ok {
		return errors.New(topic + "找不到源表信息")
	}

	var err error
	l := len(rows)
	switch row.Action {
	case InsertAction:
		for i := 0; i < l; i++ {
			err = h.insert(rows[i], int(row.ColumnCount), tableMapping, tbl)
			if err != nil {
				logs.Errorf("【%s】插入失败，数据:%v\n", topic, rows[i])
			}
		}
	case UpdateAction:
		for i := 0; i < l; i++ {
			if (i+1)%2 == 0 {
				old := rows[i-1]
				curr := rows[i]
				err = h.update(old, curr, tbl, tableMapping)
				if err != nil {
					logs.Errorf("【%s】更新失败，数据:%v\n", topic, rows[i])
				}
			}
		}
	case DeleteAction:
		for i := 0; i < l; i++ {
			err = h.delete(rows[i], tbl, tableMapping)
			if err != nil {
				logs.Errorf("【%s】删除失败，数据:%v\n", topic, rows[i])
			}
		}
	}
	return err
}

/*
插入数据
*/
func (h *DBHandler) insert(rows []interface{}, columnCount int, tableMapping *config.Strategy, tbl *schema.Table) error {
	var sql string

	switch tableMapping.MappingType {
	case config.PART_MAPPING:
		sql = h.buildInsertSql(tbl, tableMapping.Columns, tableMapping.DestTable)
	default:
		sql = h.buildFullInsertSql(columnCount, tbl, tableMapping.DestDB, tableMapping.DestTable)
	}
	err := h.executeSql(tableMapping.DestServer, sql, rows)
	if err != nil {
		logs.Errorf("Insert处理 | Topic:%s | SQL:%s | Values:%v | Error:%v", tableMapping.Path, sql, rows, err)
	}
	logs.Infof("Insert处理 | Topic:%s | SQL:%s | Values:%v\n", tableMapping.Path, sql, rows)

	return err
}

func (h *DBHandler) delete(rows []interface{}, tbl *schema.Table, tableMapping *config.Strategy) error {
	var sql string
	pkLen := len(tbl.PKColumns)
	values := make([]interface{}, 0, pkLen)
	pkCols := make([]string, pkLen, pkLen)
	for i := 0; i < pkLen; i++ {
		index := tbl.PKColumns[i]
		pkCols[i] = "`" + tbl.Columns[index].Name + "`" + "=?"
		values = append(values, rows[index])
	}

	sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s",
		tableMapping.DestDB,
		tableMapping.DestTable,
		strings.Join(pkCols, "AND"))
	err := h.executeSql(tableMapping.DestServer, sql, values)
	if err != nil {
		logs.Errorf("Delete处理 | Topic:%s | SQL:%s | Values:%v | Error:%v\n", tableMapping.Path, sql, values, err)
	}
	logs.Infof("Delete处理 | Topic:%s | SQL:%s | Values:%v\n", tableMapping.Path, sql, rows)
	return err
}

func (h *DBHandler) update(old []interface{}, curr []interface{}, tbl *schema.Table, tableMapping *config.Strategy) error {
	var sql string
	oldLen := len(old)
	currLen := len(curr)
	if oldLen != currLen {
		return errors.New(fmt.Sprintf(" %s 新旧记录长度不一致 旧记录:%v 新记录:%v",
			tableMapping.Path,
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
				colNames = append(colNames, "`"+tbl.Columns[i].Name+"`"+"=?")
				values = append(values, curr[i])
			}
		}
	}
	if len(colNames) == 0 {
		return nil
	}
	pkLen := len(tbl.PKColumns)
	pkCols := make([]string, pkLen, pkLen)
	for i := 0; i < pkLen; i++ {
		index := tbl.PKColumns[i]
		pkCols[i] = "`" + tbl.Columns[index].Name + "`" + "=?"
		values = append(values, curr[index])
	}

	sql = fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s",
		tableMapping.DestDB,
		tableMapping.DestTable,
		strings.Join(colNames, ","),
		strings.Join(pkCols, "AND"))
	err := h.executeSql(tableMapping.DestServer, sql, values)
	if err != nil {
		logs.Errorf("Update处理 | Topic:%s | SQL:%s | Values:%v | Error:%v\n", tableMapping.Path, sql, values, err)
	}
	logs.Infof("Update处理 | Topic:%s | SQL:%s | Values:%v\n", tableMapping.Path, sql, values)
	return err
}

/*
生成全映射Insert SQL 字段名称、字段数据类型等各种属性与目标表保持一致
*/
func (h *DBHandler) buildFullInsertSql(columnCount int, tbl *schema.Table, db string, table string) string {

	l := columnCount
	colNames := make([]string, 0, l)
	values := make([]string, 0, l)
	for i := 0; i < l; i++ {
		colNames = append(colNames, "`"+tbl.Columns[i].Name+"`")
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

func (h *DBHandler) getStrategy(topic string) (*config.Strategy, bool) {
	s, ok := h.strategies[topic]
	return s, ok
}

/*
生成部分映射SQL,暂时预留
*/
func (h *DBHandler) buildInsertSql(tbl *schema.Table, mappingCols []*config.ColumnStrategy, table string) string {
	panic("生成部分映射SQL，暂时未实现")
}
