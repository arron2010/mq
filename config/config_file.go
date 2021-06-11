package config

import (
	"github.com/pingcap/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var (
	_mqConfig *Config
)

func LoadConfig(fileName string) error {
	_mqConfig = &Config{}
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return errors.Trace(err)
	}
	if err := yaml.Unmarshal(data, _mqConfig); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func GetConfig() *Config {
	return _mqConfig
}
