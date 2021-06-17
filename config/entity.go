package config

import "strings"

const (
	/*全映射*/
	FULL_MAPPING = "10"
	/*部分映射*/
	PART_MAPPING = "20"

	SOURCE_DB_TYPE = "10"
	DEST_DB_TYPE   = "20"
)

type Config struct {
	ConfigDBAddr string `yaml:"config_db_addr"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
	LogFile      string `yaml:"log_file_path"`
}

/*
目标数据实例信息
*/
type DBInfo struct {
	Name     string
	User     string
	Password string
	Addr     string
	DB       string
	Drive    string
	DBType   string
}

/*
数据表信息
*/
type SourceTable struct {
	Server string
	DB     string
	Table  string
	/*源表处理者，用来路由源表数据处理者*/
	Handler string
	Path    string
}

/*
映射信息
*/
type Strategy struct {
	Path string

	/*目标表名*/
	DestTable string
	/*目标数据库*/
	DestDB string
	/*目标数据服务器*/
	DestServer  string
	Columns     []*ColumnStrategy
	MappingType string
	/*存储字段映射信息，字段映射结构为Column1:Column2,a;Column3:Column4,a;
	映射信息共节：第一节字段名映射；第二节映射处理
	*/
	Content string
}

func TableInfo(path string) (db string, table string) {
	eles := strings.Split(path, "/")
	l := len(eles)
	switch l {
	case 1:
		db = eles[0]
	case 2:
		db = eles[0]
		table = eles[1]
	case 3:
		db = "/" + eles[0] + "/" + eles[1]
		table = eles[2]
	}
	return db, table
}

type ColumnStrategy struct {
	Names     map[string]string
	DataTypes map[uint32]uint32
	Handler   string
}

func (t *SourceTable) GetFullName() string {
	return t.Path
}
