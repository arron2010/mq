package handler

import (
	"database/sql"
	"fmt"
	"github.com/asim/mq/common"
	"github.com/asim/mq/config"
	"github.com/asim/mq/logs"
	"github.com/go-mysql-org/go-mysql/schema"
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	proto2 "github.com/wj596/go-mysql-transfer/proto"
	"reflect"
	"strings"
	"sync"
)

var once sync.Once
var _dbHandler *DBHandler

type DBHandler struct {
	conns        map[string]*DBConn
	strategies   map[string]*config.Strategy
	sourceTables map[string]*schema.Table
	srcConns     map[string]*sql.DB
}

func GetDBHandler() *DBHandler {
	return _dbHandler
}

func CreateDBHandler(dao *config.ConfigDAO) error {
	_dbHandler = &DBHandler{}
	_dbHandler.conns = make(map[string]*DBConn)
	srcConns := make(map[string]*config.DBInfo)

	_dbHandler.strategies = make(map[string]*config.Strategy)
	_dbHandler.sourceTables = make(map[string]*schema.Table)

	srcDBList, err := dao.GetSrcDBInfo()
	if err != nil {
		return err
	}
	for _, srcDBInfo := range srcDBList {

		srcConns[srcDBInfo.Name] = srcDBInfo
	}

	dbList, err := dao.GetDestDBInfo()
	if err != nil {
		return err
	}
	for _, dbInfo := range dbList {
		serverId := uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(1000)) + common.MIN_REPLICATION_SLAVE
		addrInfo := strings.Split(dbInfo.Addr, ":")
		port, _ := strconv.ParseInt(addrInfo[1], 10, 64)
		conn := NewDBConn(addrInfo[0], dbInfo.User, dbInfo.Password, uint32(serverId), uint16(port))
		_dbHandler.conns[dbInfo.Name] = conn
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
		currDB, ok := srcConns[server]
		if !ok {
			return errors.New(server + "数据库不存在")
		}
		conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s",
			currDB.User,
			currDB.Password,
			currDB.Addr,
			db))
		if err != nil {
			return errors.Trace(err)
		}
		tbl, err := schema.NewTableFromSqlDB(conn, db, table)
		if err != nil {
			return err
		}
		_dbHandler.sourceTables[s.Path] = tbl
		_dbHandler.strategies[s.Path] = s
		conn.Close()
	}

	return nil
}
func (h *DBHandler) handle(topic string, conflictStrategy string, rows [][]interface{}, row *proto2.Row) error {

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
			err = h.insert(conflictStrategy, rows[i], int(row.ColumnCount), tableMapping, tbl)
			if err != nil {
				logs.Errorf("【%s】插入失败，数据:%v\n", topic, rows[i])
			}
		}
	case UpdateAction:
		for i := 0; i < l; i++ {
			if (i+1)%2 == 0 {
				old := rows[i-1]
				curr := rows[i]
				err = h.update(conflictStrategy, old, curr, int(row.ColumnCount), tbl, tableMapping)
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
func (h *DBHandler) insert(conflictStrategy string, rows []interface{}, columnCount int, tableMapping *config.Strategy, tbl *schema.Table) error {
	var sql string
	if len(tableMapping.Columns) > 0 {
		sql = h.buildInsertSql(tbl, tableMapping.Columns, tableMapping.DestTable)
	} else {
		switch conflictStrategy {
		case config.IGNORE_CONFLICT:
			sql = h.buildInsertForAll("INSERT", columnCount, tbl, tableMapping.DestDB, tableMapping.DestTable)
		default:
			sql = h.buildInsertForAll("REPLACE", columnCount, tbl, tableMapping.DestDB, tableMapping.DestTable)
		}
	}

	rowCount, err := h.executeSql(tableMapping.DestServer, sql, rows)
	if err != nil {
		logs.Errorf("Insert处理 | Topic:%s | SQL:%s | Values:%v | Error:%+v", tableMapping.Path, sql, rows, err)
	}

	logs.Infof("Insert处理 | Topic:%s | SQL:%s |Effect Row:%d |Values:%v\n", tableMapping.Path, sql, rowCount, rows)
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
	_, err := h.executeSql(tableMapping.DestServer, sql, values)
	if err != nil {
		logs.Errorf("Delete处理 | Topic:%s | SQL:%s | Values:%v | Error:%v\n", tableMapping.Path, sql, values, err)
	}
	logs.Infof("Delete处理 | Topic:%s | SQL:%s | Values:%v\n", tableMapping.Path, sql, rows)
	return err
}

func (h *DBHandler) update(conflictStrategy string, old []interface{}, curr []interface{}, columnCount int, tbl *schema.Table, tableMapping *config.Strategy) error {
	var (
		sql    string
		values []interface{}
		err    error
		n      uint64
	)

	oldLen := len(old)
	currLen := len(curr)
	if oldLen != currLen {
		return errors.New(fmt.Sprintf(" %s 新旧记录长度不一致 旧记录:%v 新记录:%v",
			tableMapping.Path,
			old, curr))
	}

	values = make([]interface{}, 0, oldLen)
	if len(tableMapping.Columns) > 0 {
		sql = h.buildUpdateSql(tbl, tableMapping.Columns, tableMapping.DestTable)
	} else {
		switch conflictStrategy {
		case config.IGNORE_CONFLICT:
			sql, values = h.buildForceUpdateSql(old, curr, tbl, tableMapping)
		case config.USE_CONFLICT:
			sql, values = h.buildOptimisticUpdateSql(old, curr, tbl, tableMapping)
		default:
			sql = h.buildInsertForAll("REPLACE", columnCount, tbl, tableMapping.DestDB, tableMapping.DestTable)
			values = curr
		}
	}
	if len(sql) > 0 {
		n, err = h.executeSql(tableMapping.DestServer, sql, values)
		if err != nil {
			logs.Errorf("Update处理 | Topic:%s | SQL:%s | Values:%v | Error:%+v\n", tableMapping.Path, sql, values, err)
		}
		logs.Infof("Update处理 | Topic:%s | SQL:%s| Effect Rows:%d | Values:%v \n", tableMapping.Path, sql, n, values)
	}

	return err
}
func (h *DBHandler) buildForceUpdateSql(old []interface{}, curr []interface{}, tbl *schema.Table, tableMapping *config.Strategy) (string, []interface{}) {
	var (
		sql string
	)
	oldLen := len(old)

	colNames := make([]string, 0, oldLen)
	values := make([]interface{}, 0, oldLen)

	for i := 0; i < oldLen; i++ {
		if !reflect.DeepEqual(old[i], curr[i]) {
			colNames = append(colNames, "`"+tbl.Columns[i].Name+"`"+"=?")
			values = append(values, curr[i])
		}
	}
	//如果没有数据变化，就不进行更新
	if len(values) == 0 {
		return "", nil
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
		strings.Join(pkCols, " AND"),
	)
	return sql, values
}

/*
针对存在修改的数据进行更新，为解决并发的问题，更新条件带入修改前数据
*/
func (h *DBHandler) buildOptimisticUpdateSql(old []interface{}, curr []interface{}, tbl *schema.Table, tableMapping *config.Strategy) (string, []interface{}) {
	var (
		sql      string
		whereSql string
	)
	oldLen := len(old)

	colNames := make([]string, 0, oldLen)
	values := make([]interface{}, 0, oldLen)

	whereCol := make([]string, 0, oldLen)
	whereVal := make([]interface{}, 0, oldLen)

	for i := 0; i < oldLen; i++ {
		if !reflect.DeepEqual(old[i], curr[i]) {
			colNames = append(colNames, "`"+tbl.Columns[i].Name+"`"+"=?")
			values = append(values, curr[i])
			if old[i] != nil {
				whereCol = append(whereCol, "`"+tbl.Columns[i].Name+"`"+"=?")
				whereVal = append(whereVal, old[i])
			} else {
				whereCol = append(whereCol, "`"+tbl.Columns[i].Name+"`"+"IS NULL")
			}
		}
	}
	//如果没有数据变化，就不进行更新
	if len(values) == 0 {
		return "", nil
	}
	pkLen := len(tbl.PKColumns)
	pkCols := make([]string, pkLen, pkLen)
	for i := 0; i < pkLen; i++ {
		index := tbl.PKColumns[i]
		pkCols[i] = "`" + tbl.Columns[index].Name + "`" + "=?"
		values = append(values, curr[index])
	}
	if len(whereVal) == 0 {
		whereSql = ""
	} else {
		whereSql = " AND " + strings.Join(whereCol, " AND")
	}

	sql = fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s %s",
		tableMapping.DestDB,
		tableMapping.DestTable,
		strings.Join(colNames, ","),
		strings.Join(pkCols, " AND"),
		whereSql)

	values = append(values, whereVal...)
	return sql, values
}

/*
生成全映射Insert SQL 字段名称、字段数据类型等各种属性与目标表保持一致
*/
func (h *DBHandler) buildInsertForAll(op string, columnCount int, tbl *schema.Table, db string, table string) string {

	l := columnCount
	colNames := make([]string, 0, l)
	values := make([]string, 0, l)
	for i := 0; i < l; i++ {
		colNames = append(colNames, "`"+tbl.Columns[i].Name+"`")
		values = append(values, "?")
	}
	sql := fmt.Sprintf("%s INTO `%s` .`%s` (%s) VALUES(%s)", op, db, table,
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

func (h *DBHandler) executeSql(db string, sql string, values []interface{}) (uint64, error) {
	conn, ok := h.conns[db]
	if !ok {
		return 0, errors.New(fmt.Sprintf("%s数据库连接不存在", db))
	}
	return conn.Execute(sql, values...)

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

func (h *DBHandler) buildUpdateSql(tbl *schema.Table, mappingCols []*config.ColumnStrategy, table string) string {
	panic("生成部分映射SQL，暂时未实现")
}
