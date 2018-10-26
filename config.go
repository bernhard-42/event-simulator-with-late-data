package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/buger/jsonparser"
	log "github.com/sirupsen/logrus"
)

type logging struct {
	logLevel log.Level
	logFile  *os.File
}

type kafka struct {
	topic  string
	broker string
}

// Model holds the event simulator parameters
type Model struct {
	avgNumEvents        int
	minNumEvents        int
	avgEventIntervalMs  int
	eventIntervalStddev int
	avgNwDelayMs        int
	mobileRatio         float64
	bufferdRatio        float64
	avgBufferedDelayMs  int
}

// Config holds defaults and custom configurations
type Config struct {
	workers int
	seed    int64
	logging logging
	kafka   kafka
	model   Model
}

// ParseConfig converts json config file to a struct
func ParseConfig(configFile string) Config {
	// defaults
	config := Config{
		1,                           // workers
		time.Now().UTC().UnixNano(), // seed
		logging{
			log.InfoLevel, // logLevel
			os.Stdout,     // logFile
		},
		kafka{
			"test_sessions",  // topic
			"localhost:9092", // broker
		},
		Model{
			50,    // avgNumEvents
			5,     // minNumEvents
			5000,  // avgEventIntervalMs
			5000,  // eventIntervalStddev
			10,    // avgNwDelayMs
			0.75,  // mobileRatio
			0.1,   // bufferdRatio
			60000, // avgBufferedDelayMs
		},
	}

	if jsonconf, err := ioutil.ReadFile(configFile); err == nil {
		// global config
		if g, err := jsonparser.GetInt(jsonconf, "workers"); err == nil {
			config.workers = int(g)
		}
		if s, err := jsonparser.GetInt(jsonconf, "seed"); err == nil {
			config.seed = s
		}

		// logging config
		if f, err := jsonparser.GetString(jsonconf, "logging", "logFile"); err == nil {
			switch f {
			case "stdout":
				config.logging.logFile = os.Stdout
			case "stderr":
				config.logging.logFile = os.Stderr
			default:
				config.logging.logFile, err = os.Create(f)
				if err != nil {
					fmt.Printf("Cannot open file %s, logging to os.Stdout", configFile)
					config.logging.logFile = os.Stdout
				}
			}
		}
		if l, err := jsonparser.GetString(jsonconf, "logging", "logLevel"); err == nil {
			if config.logging.logLevel, err = log.ParseLevel(l); err != nil {
				fmt.Println(err)
				config.logging.logLevel = log.InfoLevel
			}
		}

		// kafka config
		if b, err := jsonparser.GetString(jsonconf, "kafka", "broker"); err == nil {
			config.kafka.broker = b
		}
		if t, err := jsonparser.GetString(jsonconf, "kafka", "topic"); err == nil {
			config.kafka.topic = t
		}

		// model config
		if m, err := jsonparser.GetFloat(jsonconf, "model", "mobileRatio"); err == nil {
			config.model.mobileRatio = m
		}
		if m, err := jsonparser.GetFloat(jsonconf, "model", "bufferdRatio"); err == nil {
			config.model.bufferdRatio = m
		}
		if m, err := jsonparser.GetInt(jsonconf, "model", "avgNwDelayMs"); err == nil {
			config.model.avgNwDelayMs = int(m)
		}
		if m, err := jsonparser.GetInt(jsonconf, "model", "avgNumEvents"); err == nil {
			config.model.avgNumEvents = int(m)
		}
		if m, err := jsonparser.GetInt(jsonconf, "model", "minNumEvents"); err == nil {
			config.model.minNumEvents = int(m)
		}
		if m, err := jsonparser.GetInt(jsonconf, "model", "avgEventIntervalMs"); err == nil {
			config.model.avgEventIntervalMs = int(m)
		}
		if m, err := jsonparser.GetInt(jsonconf, "model", "eventIntervalStddev"); err == nil {
			config.model.eventIntervalStddev = int(m)
		}
		if m, err := jsonparser.GetInt(jsonconf, "model", "avgBufferedDelayMs"); err == nil {
			config.model.avgBufferedDelayMs = int(m)
		}
	}

	return config
}

func (c Config) String() string {
	result := ""
	result += fmt.Sprintf("workers: %d\n", c.workers)
	result += fmt.Sprintf("seed: %d\n", c.seed)
	result += "logging: \n"
	result += fmt.Sprintf("    logLevel: %s\n", c.logging.logLevel)
	result += fmt.Sprintf("    logFile: %v\n", c.logging.logFile)
	result += "kafka: \n"
	result += fmt.Sprintf("    broker: %s\n", c.kafka.broker)
	result += fmt.Sprintf("    topic: %s\n", c.kafka.topic)
	result += "model: \n"
	result += fmt.Sprintf("    avgNumEvents: %d\n", c.model.avgNumEvents)
	result += fmt.Sprintf("    minNumEvents: %d\n", c.model.minNumEvents)
	result += fmt.Sprintf("    avgEventIntervalMs: %d\n", c.model.avgEventIntervalMs)
	result += fmt.Sprintf("    eventIntervalStddev: %d\n", c.model.eventIntervalStddev)
	result += fmt.Sprintf("    avgNwDelayMs: %d\n", c.model.avgNwDelayMs)
	result += fmt.Sprintf("    mobileRatio: %f\n", c.model.mobileRatio)
	result += fmt.Sprintf("    bufferdRatio: %f\n", c.model.bufferdRatio)
	result += fmt.Sprintf("    avgBufferedDelayMs: %d\n", c.model.avgBufferedDelayMs)
	return result
}
