package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// Logging holds the logging configuration
type Logging struct {
	LogFileStr  string `yaml:"logFile"`
	LogLevelStr string `yaml:"logLevel"`
	logLevel    log.Level
	logFile     *os.File
}

// Kafka holds the kafka configuration broker and topic
type Kafka struct {
	Topic  string `yaml:"topic"`
	Broker string `yaml:"broker"`
}

// Model holds the parameters for the simulation
type Model struct {
	MaxNumEvents        int     `yaml:"maxNumEvents"`
	MinNumEvents        int     `yaml:"minNumEvents"`
	AvgEventIntervalMs  int     `yaml:"avgEventIntervalMs"`
	EventIntervalStddev int     `yaml:"eventIntervalStddev"`
	AvgNwDelayMs        int     `yaml:"avgNwDelayMs"`
	MobileRatio         float64 `yaml:"mobileRatio"`
	BufferdRatio        float64 `yaml:"bufferdRatio"`
}

// Config holds defaults and custom configurations
type Config struct {
	Workers        int     `yaml:"workers"`
	Sessions       int     `yaml:"sessions"`
	SessionDelayMs int     `yaml:"sessionDelayMs"`
	Seed           int64   `yaml:"seed"`
	Logging        Logging `yaml:"logging"`
	Kafka          Kafka   `yaml:"kafka"`
	Model          Model   `yaml:"model"`
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
	if yamlconf, err := ioutil.ReadFile(configFile); err == nil {
		err := yaml.Unmarshal(yamlconf, &config)
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
	result += fmt.Sprintf("Global: {Workers: %d, Sessions: %d, SessionDelayMs: %d, Seed: %d}, ", c.Workers, c.Sessions, c.SessionDelayMs, c.Seed)
	result += fmt.Sprintf("Logging: {LogLevel: %s, LogFile: %s}, ", c.Logging.LogLevelStr, c.Logging.LogFileStr)
	result += fmt.Sprintf("Kafka: {Broker: %s, Topic: %s}, ", c.Kafka.Broker, c.Kafka.Topic)
	result += fmt.Sprintf("Model: {MaxNumEvents: %d, MinNumEvents: %d, AvgEventIntervalMs: %d, "+
		"EventIntervalStddev: %d, AvgNwDelayMs: %d, MobileRatio: %f, BufferdRatio: %f}",
		c.Model.MaxNumEvents, c.Model.MinNumEvents, c.Model.AvgEventIntervalMs,
		c.Model.EventIntervalStddev, c.Model.AvgNwDelayMs, c.Model.MobileRatio, c.Model.BufferdRatio)
	return result
}
