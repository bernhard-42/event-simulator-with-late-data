package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"./kafkawriter"
	"./randpool"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

/*
	Initialization of global variables
*/

var (
	workers        int
	sessions       int
	sessionDelayMs int
	model          Model
	randPool       []randpool.RandPool
	kafkaWriter    kafkawriter.KafkaWriter
)

func init() {
	var configFile = flag.String("config", "./config.json", "config json file")
	flag.Parse()

	c := ParseConfig(*configFile)

	log.SetLevel(c.Logging.logLevel)
	log.SetOutput(c.Logging.logFile)

	workers, model, sessions, sessionDelayMs = c.Workers, c.Model, c.Sessions, c.SessionDelayMs

	rand.Seed(c.Seed)

	randPool = []randpool.RandPool{}
	for i := 0; i < sessions; i++ {
		rp := randpool.Create(sessions*model.MaxNumEvents*10, sessions*model.MaxNumEvents)
		randPool = append(randPool, *rp)
	}
	kafkaWriter = kafkawriter.Create(c.Kafka.Broker, c.Kafka.Topic)

	log.Info(c)
}

/*
	Type definitions
*/

type metadata struct {
	avgNwDelayMs        int
	numEvents           int
	avgEventIntervalMs  int
	eventIntervalStddev float64
	buffered            bool
	bufferedNumEvents   int
	bufferedStart       int
}

type data struct {
	Values int
	Errors int
}

type event struct {
	metadata  metadata
	Kind      string
	ClientID  int
	Timestamp int64
	SessionID string
	EventID   int
	Data      data
}

/*
	Type Stringers
*/

func (m metadata) String() string {
	return fmt.Sprintf("{avgNwDelayMs: %v, numEvents: %v, avgEventIntervalMs: %v, eventIntervalStddev: %v, buffered: %v, bufferedNumEvents: %v, bufferedStart: %v}",
		m.avgNwDelayMs, m.numEvents, m.avgEventIntervalMs, m.eventIntervalStddev, m.buffered, m.bufferedNumEvents, m.bufferedStart)
}

func (d data) String() string {
	return fmt.Sprintf("{Values: %d, Errors: %d}", d.Values, d.Errors)
}

func (e event) String() string {
	return fmt.Sprintf("{metadata: %v Kind: %v ClientID: %v Timestamp: %v SessionID: %v ID: %v Data: %v}",
		e.metadata, e.Kind, e.ClientID, e.Timestamp, e.SessionID, e.EventID, e.Data)
}

/*
	Helpers
*/

func sleep(millisec int) {
	delay := time.Duration(millisec) * time.Millisecond
	time.Sleep(delay)
}

func timestamp() int64 {
	now := time.Now()
	return now.UnixNano() / 1000000
}

/*
	Simulator pipeline: session = start -> emit -> delay -> publish
*/

func session(clientID int) {
	delay := randPool[clientID].Intn(sessionDelayMs)
	sleep(delay)

	var buffer = make(map[string][]event)

	ev := start(clientID)
	log.Info("Session ", clientID, " started (delay:", delay, "): ", clientID, ev.metadata)

	for i := 0; i < ev.metadata.numEvents; i++ {
		ev.emit(clientID)
		ev.delay(clientID, buffer)
	}
	log.Info("Session ", clientID, " finished")
}

// start the pipeline and create an event with defaults
func start(clientID int) event {
	kind := "d"
	if randPool[clientID].Float64() < model.MobileRatio {
		kind = "m"
	}
	nwDelayMs := 1 + randPool[clientID].Intn(model.AvgNwDelayMs)
	numEvents := randPool[clientID].Intn(model.MaxNumEvents) + model.MinNumEvents
	buffered := (kind == "m") && (randPool[clientID].Float64() < model.BufferdRatio)
	bufferedStart := 0
	bufferedNumEvents := 0
	if buffered {
		bufferedStart = int(float64(numEvents) * randPool[clientID].Float64())
		bufferedNumEvents = randPool[clientID].Intn(numEvents - bufferedStart)
	}
	metadata := metadata{nwDelayMs, numEvents, model.AvgEventIntervalMs, float64(model.EventIntervalStddev),
		buffered, bufferedNumEvents, bufferedStart}
	sessionID := fmt.Sprint(uuid.Must(uuid.NewV4()))
	ID := 0

	return event{metadata, kind, clientID, 0, sessionID, ID, data{-1, -1}}
}

// emit event with event specific values
func (e *event) emit(clientID int) {
	e.Timestamp = timestamp()
	e.EventID++
	value := 10 + randPool[clientID].Intn(90)
	errors := 0
	if randPool[clientID].Float64() < 0.1 {
		errors = 1 + randPool[clientID].Intn(value/10)
	}

	e.Data = data{value, errors}
}

// add network delays and notwork outage buffering
func (e event) delay(clientID int, buffer map[string][]event) event {
	// simulate intervals between clicks
	delay := int(randPool[clientID].NormFloat64()*e.metadata.eventIntervalStddev) + e.metadata.avgEventIntervalMs

	bufStart := e.metadata.bufferedStart
	bufEnd := bufStart + e.metadata.bufferedNumEvents
	if e.metadata.buffered && e.EventID >= bufStart && e.EventID <= bufEnd {
		if e.EventID < bufEnd {
			// fill buffer
			_, ok := buffer[e.SessionID]
			if !ok {
				var eventList = []event{}
				buffer[e.SessionID] = eventList
			}
			buffer[e.SessionID] = append(buffer[e.SessionID], e)
		} else if e.EventID == bufEnd {
			// drain buffer
			for _, e2 := range buffer[e.SessionID] {
				// no further network delay
				e2.publish()
			}
		}
	} else {
		// Simulate network delay
		sleep(e.metadata.avgNwDelayMs)

		e.publish()
	}

	sleep(delay)
	return e
}

// publish to kafka
func (e event) publish() {
	// send to Kafka
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Error(err)
	} else {
		if err = kafkaWriter.Send(bytes); err == nil {
			log.Debug("Sent message: ", string(bytes))
		} else {
			log.Error(err)
		}
	}
}

/*
	Worker pool
*/

func worker(jobs chan int, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		jobID, ok := <-jobs
		if ok {
			log.Debug("Worker ", workerID, " triggering session ", jobID)
			session(jobID)
			log.Debug("Worker ", workerID, " finished session ", jobID)
		} else {
			log.Debug("Worker ", workerID, " done")
			break
		}
	}
}

/*
	Main
*/

func main() {
	var wg sync.WaitGroup
	jobs := make(chan int)

	// Create a pool of workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(jobs, i, &wg)
	}

	// Fill the job list for number of sessions
	for i := 0; i < sessions; i++ {
		jobs <- i
	}

	close(jobs)
	wg.Wait()

	log.Info("Done")
}
