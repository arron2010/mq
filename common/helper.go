package common

import (
	"os"
	"strings"
)

func CreateDiskQueueName(topic string) string {

	temp := strings.Split(topic, PATH_SEPERATOR)
	return temp[len(temp)-1]
}

func CreateTopicDir(path string, topic string) (string, error) {
	t := strings.TrimLeft(topic, PATH_SEPERATOR)
	index := strings.LastIndex(topic, PATH_SEPERATOR)
	topicDir := "diskqueue/" + t[:index]
	dir := path + topicDir
	_, err := os.Stat(dir)
	if err == nil {
		return dir, nil
	}
	if os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", err
		}
	}
	return dir, nil
}
