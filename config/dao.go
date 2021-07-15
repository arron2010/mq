package config

import (
	"database/sql"
	"fmt"
	"github.com/asim/mq/logs"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

const (
	_tableSyncInfoKey = "SourceTable"
)

var (
	DAO *ConfigDAO
)

type ConfigDAO struct {
	config *Config
	db     *sql.DB
	cache  *memCache
}

func NewConfigDAO() (*ConfigDAO, error) {
	dao := &ConfigDAO{}
	DAO = dao
	return dao, nil
}

func (c *ConfigDAO) GetConfig() *Config {
	return c.config
}

/*
获取源表信息
*/
func (c *ConfigDAO) GetTableInfo() []*SourceTable {

	db, err := sql.Open("mysql", GetConfig().ConfigDBAddr)
	if err != nil {
		return nil
	}
	defer db.Close()
	rows, err := db.Query(`select server_,db_, table_,handler_ ,path_ from t_config_strategy where deleted_="0"`)
	if err != nil {
		logs.Errorf("获取TableSyncInfo错误:%v\n", err)
		return nil
	}

	tables := make([]*SourceTable, 0, 8)
	for rows.Next() {
		t := &SourceTable{}
		err := rows.Scan(&t.Server, &t.DB, &t.Table, &t.Handler, &t.Path)
		if err != nil {
			logs.Errorf("扫描TableSyncInfo记录错误:%v\n", err)
			continue
		}
		tables = append(tables, t)
	}
	rows.Close()
	return tables
}
func (c *ConfigDAO) GetDestDBInfo() ([]*DBInfo, error) {
	return c.getDBInfo("t1.dest_server=t2.name_")
}

func (c *ConfigDAO) GetSrcDBInfo() ([]*DBInfo, error) {
	return c.getDBInfo("t1.server_=t2.name_")
}

/*
获取目标数据库信息
*/
func (c *ConfigDAO) getDBInfo(condition string) ([]*DBInfo, error) {
	db, err := sql.Open("mysql", GetConfig().ConfigDBAddr)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	sql := fmt.Sprintf(`select t2.name_,t2.user_,t2.password_,t2.addr_ FROM t_config_strategy t1,t_config_dbinfo t2 where %s and t2.deleted_=0`, condition)
	rows, err := db.Query(sql)
	if err != nil {
		logs.Errorf("获取DBInfo错误:%v\n", err)
		return nil, err
	}

	dbInfos := make([]*DBInfo, 0, 8)
	for rows.Next() {
		t := &DBInfo{}
		err := rows.Scan(&t.Name, &t.User, &t.Password, &t.Addr)
		if err != nil {
			logs.Errorf("扫描TableSyncInfo记录错误:%v\n", err)
			continue
		}
		dbInfos = append(dbInfos, t)
	}
	rows.Close()
	return dbInfos, nil
}

/*
获取映射策略
*/
func (c *ConfigDAO) GetAllStrategies() ([]*Strategy, error) {
	db, err := sql.Open("mysql", GetConfig().ConfigDBAddr)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(`select dest_path,path_ ,content_ ,type_ from t_config_strategy where deleted_="0"`)
	if err != nil {
		logs.Errorf("获取Strategy错误:%v\n", err)
		return nil, err
	}

	strategyList := make([]*Strategy, 0, 8)
	for rows.Next() {
		t := &Strategy{}

		err := rows.Scan(&t.DestPath, &t.Path, &t.Content, &t.MappingType)
		t.Content = strings.Trim(t.Content, " ")
		if err != nil {
			logs.Errorf("扫描Strategy记录错误:%v\n", err)
			continue
		}
		server, db, table := TableInfo(t.DestPath)
		t.DestServer = server
		t.DestDB = db
		t.DestTable = table

		strategyList = append(strategyList, t)
	}
	rows.Close()
	return strategyList, nil
}
