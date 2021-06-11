package config

import (
	"database/sql"
	"github.com/asim/mq/glogger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

const (
	_tableSyncInfoKey = "TableSyncInfo"
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

func (c *ConfigDAO) GetTableInfo() []*TableSyncInfo {

	rows, err := c.db.Query(`select server_,db_, table_,handler_ ,path_ from t_config_table where deleted_="0"`)
	if err != nil {
		glogger.Errorf("获取TableSyncInfo错误:%v\n", err)
		return nil
	}

	defer rows.Close()
	tables := make([]*TableSyncInfo, 0, 8)
	for rows.Next() {
		t := &TableSyncInfo{}
		err := rows.Scan(&t.Server, &t.DB, &t.Table, &t.Handler, &t.Path)
		if err != nil {
			glogger.Errorf("扫描TableSyncInfo记录错误:%v\n", err)
			continue
		}
		tables = append(tables, t)
	}
	return tables
}

func (c *ConfigDAO) GetDBInfo() ([]*DBInfo, error) {
	rows, err := c.db.Query(`select name_,user_, password_,addr_ ,db_ from t_dbinfo where deleted_="0"`)
	if err != nil {
		glogger.Errorf("获取DBInfo错误:%v\n", err)
		return nil, err
	}
	defer rows.Close()
	dbInfos := make([]*DBInfo, 0, 8)
	for rows.Next() {
		t := &DBInfo{}
		err := rows.Scan(&t.Name, &t.User, &t.Password, &t.Addr, &t.DB)
		if err != nil {
			glogger.Errorf("扫描TableSyncInfo记录错误:%v\n", err)
			continue
		}
		dbInfos = append(dbInfos, t)
	}
	return dbInfos, nil
}

func (c *ConfigDAO) GetTableMapping(path string) (*TableMapping, error) {
	return nil, nil
}
