package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/buger/jsonparser"
	log "github.com/sirupsen/logrus"
)

// type config struct {
// 	configFile string
// 	config     []byte
// }

// GetLoggingConfig retrieves Logging configuration from config file
func GetLoggingConfig(configFile string) (logLevel log.Level, logFile *os.File) {
	logLevel = log.InfoLevel
	logFile = os.Stdout

	if config, err := ioutil.ReadFile(configFile); err == nil {
		if f, err := jsonparser.GetString(config, "logging", "logFile"); err == nil {
			switch f {
			case "stdout":
				logFile = os.Stdout
			case "stderr":
				logFile = os.Stderr
			default:
				logFile, err = os.Create(f)
				if err != nil {
					fmt.Printf("Cannot open file %s, logging to os.Stdout", configFile)
					logFile = os.Stdout
				}
			}
		}

		if l, err := jsonparser.GetString(config, "logging", "logLevel"); err == nil {
			if logLevel, err = log.ParseLevel(l); err != nil {
				fmt.Println(err)
				logLevel = log.InfoLevel
			}
		}
	}
	return
}

// GetKafkaConfig retrieves Kafka configuration from config file
func GetKafkaConfig(configFile string) (broker string, topic string) {
	broker = "localhost:9092"
	topic = "test_sessions"
	if config, err := ioutil.ReadFile(configFile); err == nil {
		if b, err := jsonparser.GetString(config, "kafka", "broker"); err == nil {
			broker = b
		}
		if t, err := jsonparser.GetString(config, "kafka", "topic"); err == nil {
			topic = t
		}
	}
	return
}
