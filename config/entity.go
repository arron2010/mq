package config

import "strings"

const (
	/*全映射*/
	FULL_MAPPING = "10"
	/*部分映射*/
	PART_MAPPING = "20"
)

type Config struct {
	ConfigDBAddr string `yaml:"config_db_addr"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

/*
数据实例信息
*/
type DBInfo struct {
	Name     string
	User     string
	Password string
	Addr     string
	DB       string
	Drive    string
}

/*
数据表信息
*/
type TableSyncInfo struct {
	Server  string
	DB      string
	Table   string
	Handler string
	Path    string
}

/*
映射信息
*/
type TableMapping struct {
	Path   string
	Source string
	Dest   string
	/*目标数据库*/
	DB string
	/*目标数据服务器*/
	Server      string
	Columns     []*ColumnMapping
	MappingType string
	/*存储字段映射信息，字段映射结构为Column1:Column2,a;Column3:Column4,a;
	映射信息共节：第一节字段名映射；第二节类型映射；第三节映射处理过程。
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

type ColumnMapping struct {
	Names     map[string]string
	DataTypes map[uint32]uint32
	Handler   string
}

func (t *TableSyncInfo) GetFullName() string {
	return t.Path
}
