package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"./kafkawriter"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var (
	workers        int
	sessions       int
	sessionDelayMs int
	model          Model
	kafkaWriter    kafkawriter.KafkaWriter
)

func init() {
	var configFile = flag.String("config", "./config.json", "config json file")
	flag.Parse()

	c := ParseConfig(*configFile)
	rand.Seed(c.Seed)
	log.SetLevel(c.Logging.logLevel)
	log.SetOutput(c.Logging.logFile)

	workers, model, sessions, sessionDelayMs = c.Workers, c.Model, c.Sessions, c.SessionDelayMs
	kafkaWriter = kafkawriter.Create(c.Kafka.Broker, c.Kafka.Topic)

	log.Info(c)
}

type metadata struct {
	avgNwDelayMs        int
	numEvents           int
	avgEventIntervalMs  int
	eventIntervalStddev float64
	buffered            bool
	bufferedNumEvents   int
	bufferedStart       int
}

func (m metadata) String() string {
	return fmt.Sprintf("{avgNwDelayMs: %v, numEvents: %v, avgEventIntervalMs: %v, eventIntervalStddev: %v, buffered: %v, bufferedNumEvents: %v, bufferedStart: %v}",
		m.avgNwDelayMs, m.numEvents, m.avgEventIntervalMs, m.eventIntervalStddev, m.buffered, m.bufferedNumEvents, m.bufferedStart)
}

type data struct {
	Values int
	Errors int
}

func (d data) String() string {
	return fmt.Sprintf("{Values: %d, Errors: %d}", d.Values, d.Errors)
}

type event struct {
	metadata      metadata
	Kind          string
	ClientID      int
	Timestamp     int64
	Timestring    string
	SessionID     string
	ID            int
	Data          data
	SentTimestamp int64
}

func (e event) String() string {
	return fmt.Sprintf("{metadata: %v Kind: %v ClientID: %v Timestamp: %v Timestring: %v SessionID: %v ID: %v Data: %v SentTimestamp: %v}",
		e.metadata, e.Kind, e.ClientID, e.Timestamp, e.Timestring, e.SessionID, e.ID, e.Data, e.SentTimestamp)
}

func sleep(millisec int) {
	delay := time.Duration(millisec) * time.Millisecond
	time.Sleep(delay)
}

func timestamp() (time.Time, int64) {
	now := time.Now()
	return now, now.UnixNano() / 1000000
}

func start(clientID int) event {
	kind := "d"
	if rand.Float64() < model.MobileRatio {
		kind = "m"
	}
	nwDelayMs := 1 + rand.Intn(model.AvgNwDelayMs)
	numEvents := rand.Intn(model.AvgNumEvents) + model.MinNumEvents
	buffered := (kind == "m") && (rand.Float64() < model.BufferdRatio)
	bufferedStart := 0
	bufferedNumEvents := 0
	if buffered {
		bufferedStart = int(float64(numEvents) * rand.Float64())
		bufferedNumEvents = rand.Intn(numEvents - bufferedStart)
	}
	metadata := metadata{nwDelayMs, numEvents, model.AvgEventIntervalMs, float64(model.EventIntervalStddev),
		buffered, bufferedNumEvents, bufferedStart}
	sessionID := fmt.Sprint(uuid.Must(uuid.NewV4()))
	ID := 0

	return event{metadata, kind, clientID, 0, "", sessionID, ID, data{-1, -1}, 0}
}

func (e *event) emit() {
	var now time.Time
	now, e.Timestamp = timestamp()
	e.Timestring = now.Format(time.RFC3339)
	e.ID++
	value := 10 + rand.Intn(90)
	errors := 0
	if rand.Float64() < 0.1 {
		errors = 1 + rand.Intn(value/10)
	}

	e.Data = data{value, errors}
}

func (e event) delay(buffer map[string][]event) event {
	delay := int(rand.NormFloat64()*e.metadata.eventIntervalStddev) + e.metadata.avgEventIntervalMs // simulate intervals between clicks

	bufStart := e.metadata.bufferedStart
	bufEnd := bufStart + e.metadata.bufferedNumEvents
	if e.metadata.buffered && e.ID >= bufStart && e.ID <= bufEnd {
		if e.ID < bufEnd {
			// fill buffer
			_, ok := buffer[e.SessionID]
			if !ok {
				var eventList = []event{}
				buffer[e.SessionID] = eventList
			}
			buffer[e.SessionID] = append(buffer[e.SessionID], e)
		} else if e.ID == bufEnd {
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

func (e event) publish() {
	// set sent timestamp just for batch stream analysis
	_, e.SentTimestamp = timestamp()
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

type job struct {
	ID    int
	delay int
}

func session(clientID int, delay int) {
	var buffer = make(map[string][]event)
	sleep(delay)
	ev := start(clientID)
	log.Info("Job ", clientID, " started: ", clientID, ev.metadata)

	for i := 0; i < ev.metadata.numEvents; i++ {
		ev.emit()
		ev.delay(buffer)
	}
	log.Debug("Job ", clientID, " finished")
}

func worker(jobs chan job, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		job, ok := <-jobs
		if ok {
			log.Debug("Worker ", workerID, " starting job ", job.ID, " with delay ", job.delay)
			session(job.ID, job.delay)
			log.Debug("Worker ", workerID, " finished job ", job.ID)
		} else {
			log.Debug("Worker ", workerID, " done")
			break
		}
	}
}

func main() {
	var wg sync.WaitGroup
	jobs := make(chan job)

	// Create a pool of workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(jobs, i, &wg)
	}

	// Fill the job list for number of sessions
	for i := 0; i < sessions; i++ {
		jobs <- job{i, rand.Intn(sessionDelayMs)}
	}

	close(jobs)

	wg.Wait()
	// kafkaWriter.Close()
	log.Info("Done")
}
