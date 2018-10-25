package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/buger/jsonparser"
	log "github.com/sirupsen/logrus"
)

// GetLoggingConfig retrieves Logging configuration from config file
func GetLoggingConfig(configFile string) (logLevel log.Level, logFile *os.File) {
	// defaults
	logLevel = log.InfoLevel
	logFile = os.Stdout

	// configuration
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
	// defaults
	broker = "localhost:9092"
	topic = "test_sessions"

	// configuration
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

// GetGlobalConfig retrieves global configuration
func GetGlobalConfig(configFile string) (goroutines int, seed int64) {
	// defaults
	goroutines = 1
	seed = time.Now().UTC().UnixNano()

	// configuration
	if config, err := ioutil.ReadFile(configFile); err == nil {
		if g, err := jsonparser.GetInt(config, "goroutines"); err == nil {
			goroutines = int(g)
		}
		if s, err := jsonparser.GetInt(config, "seed"); err == nil {
			seed = s
		}

	}
	return
}

// GetModel retrieves the model parameters
func GetModel(configFile string) (mobileRatio float64,
	bufferdRatio float64,
	avgNwDelayMs int,
	avgNumEvents int,
	minNumEvents int,
	avgEventIntervalMs int,
	eventIntervalStddev int,
	avgBufferedDelayMs int) {

	// defaults
	mobileRatio = 0.75
	bufferdRatio = 0.1
	avgNwDelayMs = 10
	avgNumEvents = 50
	minNumEvents = 5
	avgEventIntervalMs = 5000
	eventIntervalStddev = 5000
	avgBufferedDelayMs = 60000

	// configuration
	if config, err := ioutil.ReadFile(configFile); err == nil {
		if m, err := jsonparser.GetFloat(config, "model", "mobileRatio"); err == nil {
			mobileRatio = m
		}
		if m, err := jsonparser.GetFloat(config, "model", "bufferdRatio"); err == nil {
			bufferdRatio = m
		}
		if m, err := jsonparser.GetInt(config, "model", "avgNwDelayMs"); err == nil {
			avgNwDelayMs = int(m)
		}
		if m, err := jsonparser.GetInt(config, "model", "avgNumEvents"); err == nil {
			avgNumEvents = int(m)
		}
		if m, err := jsonparser.GetInt(config, "model", "minNumEvents"); err == nil {
			minNumEvents = int(m)
		}
		if m, err := jsonparser.GetInt(config, "model", "avgEventIntervalMs"); err == nil {
			avgEventIntervalMs = int(m)
		}
		if m, err := jsonparser.GetInt(config, "model", "eventIntervalStddev"); err == nil {
			eventIntervalStddev = int(m)
		}
		if m, err := jsonparser.GetInt(config, "model", "avgBufferedDelayMs"); err == nil {
			avgBufferedDelayMs = int(m)
		}
	}
	return
}
