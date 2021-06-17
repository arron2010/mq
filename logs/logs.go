package logs

import "github.com/xp/glogger"

var _logger *glogger.GLogger

func Initialize(file string) {
	glogger.LogPath = file
	_logger = glogger.MustGetLogger("mq")
}

func Debugf(template string, args ...interface{}) {
	_logger.Debugf(template, args...)
}

func Infof(template string, args ...interface{}) {
	_logger.Infof(template, args...)
}

func Warnf(template string, args ...interface{}) {
	_logger.Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	_logger.Errorf(template, args...)
}
