package config

import "strings"

const (
	/*全映射*/
	FULL_MAPPING = "10"
	/*部分映射*/
	PART_MAPPING = "20"

	SOURCE_DB_TYPE = "10"
	DEST_DB_TYPE   = "20"

	IGNORE_CONFLICT   = "10"
	USE_CONFLICT      = "20"
	OVERRIDE_CONFLICT = "30"
)

type Config struct {
	ConfigDBAddr string `yaml:"config_db_addr"`
	MinOpenConns int    `yaml:"min_open_conns"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
	LogFile      string `yaml:"log_file_path"`
	Peers        string `yaml:"peers"`
	DataPath     string `yaml:"topic_db_path"`
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
	/*当数据从源表到目标表，数据写入发生冲突时，如何解决
	10:忽略冲突, 直接用冲突记录
	20:遇到冲突, 覆盖冲突记录
	30
	*/
	Handler string
	Path    string
}

/*
映射信息
*/
type Strategy struct {
	/* 源表路径*/
	Path string
	/* 目标表路径*/
	DestPath string
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

func TableInfo(path string) (server string, db string, table string) {
	eles := strings.Split(path, "/")
	l := len(eles)
	switch l {
	case 2:
		server = eles[1]
	case 3:
		server = eles[1]
		db = eles[2]
	case 4:
		server = eles[1]
		db = eles[2]
		table = eles[3]
	}
	return server, db, table
}

type ColumnStrategy struct {
	Names     map[string]string
	DataTypes map[uint32]uint32
	Handler   string
}

func (t *SourceTable) GetFullName() string {
	return t.Path
}
