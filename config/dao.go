package config

import (
	"database/sql"
	"github.com/asim/mq/logs"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
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

	config := GetConfig()
	db, err := sql.Open("mysql", config.ConfigDBAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	dao.db = db
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

	rows, err := c.db.Query(`select server_,db_, table_,handler_ ,path_ from t_config_table where deleted_="0"`)
	if err != nil {
		logs.Errorf("获取TableSyncInfo错误:%v\n", err)
		return nil
	}

	defer rows.Close()
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
	return tables
}

/*
获取目标数据库信息
*/
func (c *ConfigDAO) GetDBInfo() ([]*DBInfo, error) {
	rows, err := c.db.Query(`select name_,user_, password_,addr_ ,db_ ,type_ from t_config_dbinfo where deleted_="0"`)
	if err != nil {
		logs.Errorf("获取DBInfo错误:%v\n", err)
		return nil, err
	}
	defer rows.Close()
	dbInfos := make([]*DBInfo, 0, 8)
	for rows.Next() {
		t := &DBInfo{}
		err := rows.Scan(&t.Name, &t.User, &t.Password, &t.Addr, &t.DB, &t.DBType)
		if err != nil {
			logs.Errorf("扫描TableSyncInfo记录错误:%v\n", err)
			continue
		}
		dbInfos = append(dbInfos, t)
	}
	return dbInfos, nil
}

/*
获取映射策略
*/
func (c *ConfigDAO) GetAllStrategies() ([]*Strategy, error) {
	rows, err := c.db.Query(`select dest_server,dest_db, dest_table,path_ ,content_ ,type_ from t_config_strategy where deleted_="0"`)
	if err != nil {
		logs.Errorf("获取Strategy错误:%v\n", err)
		return nil, err
	}
	defer rows.Close()
	strategyList := make([]*Strategy, 0, 8)
	for rows.Next() {
		t := &Strategy{}
		err := rows.Scan(&t.DestServer, &t.DestDB, &t.DestTable, &t.Path, &t.Content, &t.MappingType)
		if err != nil {
			logs.Errorf("扫描Strategy记录错误:%v\n", err)
			continue
		}
		strategyList = append(strategyList, t)
	}
	return strategyList, nil
}
