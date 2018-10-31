package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// Logging holds the logging configuration
type Logging struct {
	LogLevelStr string `json:"logLevel"`
	LogFileStr  string `json:"logFile"`
	logLevel    log.Level
	logFile     *os.File
}

// Kafka holds the kafka configuration broker and topic
type Kafka struct {
	Topic  string `json:"topic"`
	Broker string `json:"broker"`
}

// Model holds the parameters for the simulation
type Model struct {
	MaxNumEvents        int     `json:"maxNumEvents"`
	MinNumEvents        int     `json:"minNumEvents"`
	AvgEventIntervalMs  int     `json:"avgEventIntervalMs"`
	EventIntervalStddev int     `json:"eventIntervalStddev"`
	AvgNwDelayMs        int     `json:"avgNwDelayMs"`
	MobileRatio         float64 `json:"mobileRatio"`
	BufferdRatio        float64 `json:"bufferdRatio"`
}

// Config holds defaults and custom configurations
type Config struct {
	Workers        int     `json:"workers"`
	Sessions       int     `json:"sessions"`
	SessionDelayMs int     `json:"sessionDelayMs"`
	Seed           int64   `json:"seed"`
	Logging        Logging `json:"logging"`
	Kafka          Kafka   `json:"kafka"`
	Model          Model   `json:"model"`
}

// ParseConfig converts json config file to a struct
func ParseConfig(configFile string) Config {
	// Set defaults
	config := Config{
		1,                           /* workers */
		10,                          /* sesions */
		10000,                       /* sessionDelayMs */
		time.Now().UTC().UnixNano(), /* seed */
		Logging{"info", "stdout", log.InfoLevel, os.Stdout},
		Kafka{
			"test_sessions",  /* topic */
			"localhost:9092", /* broker */
		},
		Model{
			50,   /* MaxNumEvents */
			5,    /* MinNumEvents */
			5000, /* AvgEventIntervalMs */
			5000, /* EventIntervalStddev */
			10,   /* AvgNwDelayMs */
			0.75, /* MobileRatio */
			0.1,  /* BufferdRatio */
		},
	}
	if jsonconf, err := ioutil.ReadFile(configFile); err == nil {
		err := json.Unmarshal(jsonconf, &config)
		if err == nil {
			switch config.Logging.LogFileStr {
			case "stdout":
				config.Logging.logFile = os.Stdout
			case "stderr":
				config.Logging.logFile = os.Stderr
			default:
				config.Logging.logFile, err = os.Create(config.Logging.LogFileStr)
				if err != nil {
					fmt.Printf("Cannot open file %s, logging to os.Stdout", configFile)
					config.Logging.logFile = os.Stdout
				}
			}
			if config.Logging.logLevel, err = log.ParseLevel(config.Logging.LogLevelStr); err != nil {
				fmt.Println(err)
				config.Logging.logLevel = log.InfoLevel
			}
		} else {
			panic(err)
		}
	}
	return config
}

func (c Config) String() string {
	result := ""
	result += fmt.Sprintf("global: {workers: %d, sessions: %d, SessionDelayMs: %d, seed: %d}", c.Workers, c.Sessions, c.SessionDelayMs, c.Seed)
	result += fmt.Sprintf("logging: {logLevel: %s, logFile: %s}", c.Logging.LogLevelStr, c.Logging.LogFileStr)
	result += fmt.Sprintf("kafka: {broker: %s, topic: %s}", c.Kafka.Broker, c.Kafka.Topic)
	result += fmt.Sprintf("model: {maxNumEvents: %d, minNumEvents: %d, avgEventIntervalMs: %d, "+
		"eventIntervalStddev: %d, avgNwDelayMs: %d, mobileRatio: %f, bufferdRatio: %f}",
		c.Model.MaxNumEvents, c.Model.MinNumEvents, c.Model.AvgEventIntervalMs,
		c.Model.EventIntervalStddev, c.Model.AvgNwDelayMs, c.Model.MobileRatio, c.Model.BufferdRatio)
	return result
}
