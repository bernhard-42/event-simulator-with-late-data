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
	workers     int
	model       Model
	kafkaWriter kafkawriter.KafkaWriter
)

func init() {
	var configFile = flag.String("config", "./config.json", "config json file")
	flag.Parse()

	c := ParseConfig(*configFile)
	rand.Seed(c.Seed)
	log.SetLevel(c.Logging.logLevel)
	log.SetOutput(c.Logging.logFile)

	workers, model = c.Workers, c.Model
	kafkaWriter = kafkawriter.Create(c.Kafka.Broker, c.Kafka.Topic)

	log.Info(c)
}

type metadata struct {
	avgNwDelayMs        int
	numEvents           int
	avgEventIntervalMs  int
	eventIntervalStddev float64
	buffered            bool
	bufferedDelayMs     int
	bufferedNumEvents   int
	bufferedStart       int
}

func (m metadata) String() string {
	return fmt.Sprintf("{avgNwDelayMs: %v, numEvents: %v, avgEventIntervalMs: %v, eventIntervalStddev: %v, buffered: %v, bufferedDelayMs: %v, bufferedNumEvents: %v, bufferedStart: %v}",
		m.avgNwDelayMs, m.numEvents, m.avgEventIntervalMs, m.eventIntervalStddev, m.buffered, m.bufferedDelayMs, m.bufferedNumEvents, m.bufferedStart)
}

type data struct {
	Payload string
}

func (d data) String() string {
	return fmt.Sprintf("{Payload: %v}", d.Payload)
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
	nwDelayMs := rand.Intn(model.AvgNwDelayMs)
	numEvents := rand.Intn(model.AvgNumEvents) + model.MinNumEvents
	buffered := (kind == "m") && (rand.Float64() < model.BufferdRatio)
	bufferedDelayMs := 0
	bufferedStart := 0
	bufferedNumEvents := 0
	if buffered {
		bufferedDelayMs = rand.Intn(model.AvgBufferedDelayMs)
		bufferedStart = int(float64(numEvents) * rand.Float64())
		bufferedNumEvents = numEvents - bufferedStart
	}
	metadata := metadata{nwDelayMs, numEvents, model.AvgEventIntervalMs, float64(model.EventIntervalStddev),
		buffered, bufferedDelayMs, bufferedNumEvents, bufferedStart}
	sessionID := fmt.Sprint(uuid.Must(uuid.NewV4()))
	ID := 0

	return event{metadata, kind, clientID, 0, "", sessionID, ID, data{""}, 0}
}

func (e *event) emit() {
	var now time.Time
	now, e.Timestamp = timestamp()
	e.Timestring = now.Format(time.RFC3339)
	e.ID++
	e.Data = data{"hello world"}
}

func (e event) delay(buffer map[string][]event) event {
	delay := int(rand.NormFloat64()*e.metadata.eventIntervalStddev) + e.metadata.avgEventIntervalMs // simulate intervals between clicks

	if e.metadata.buffered && e.ID >= e.metadata.bufferedStart {
		_, ok := buffer[e.SessionID]
		if !ok {
			var eventList = []event{}
			buffer[e.SessionID] = eventList
		}
		buffer[e.SessionID] = append(buffer[e.SessionID], e)
		if e.ID == e.metadata.numEvents {
			sleep(e.metadata.bufferedDelayMs)
			for _, ev2 := range buffer[e.SessionID] {
				ev2.publish()
			}
		} else {
			sleep(delay)
		}
	} else {
		e.publish()
		sleep(delay)
	}

	return e
}

func (e event) publish() {
	sleep(e.metadata.avgNwDelayMs) // Simulate network delay
	_, e.SentTimestamp = timestamp()
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

func session(wg *sync.WaitGroup, clientID int, wait int) {
	var buffer = make(map[string][]event)
	defer wg.Done()
	sleep(wait)
	ev := start(clientID)
	log.Info("Worker (clientID ", clientID, ") started: ", clientID, ev.metadata)

	for i := 0; i < ev.metadata.numEvents; i++ {
		ev.emit()
		ev.delay(buffer)
	}
	log.Debug(fmt.Sprintf("Session (clientID %d) stopped", clientID))
}

func main() {
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		startDelay := 0
		if workers > 1 {
			startDelay = rand.Intn(30000)
		}
		go session(&wg, i, startDelay)
	}
	log.Info("All workers started")
	wg.Wait()
	log.Info("Done")
}
