package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"./kafkaprod"
	"github.com/davecgh/go-spew/spew"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var (
	workers  int
	model    Model
	producer kafkaprod.KafkaProducer
)

func init() {
	var configFile = flag.String("config", "./config.json", "config json file")
	flag.Parse()

	c := ParseConfig(*configFile)
	rand.Seed(c.seed)
	log.SetLevel(c.logging.logLevel)
	log.SetOutput(c.logging.logFile)

	workers, model = c.workers, c.model
	producer = kafkaprod.Create(c.kafka.broker, c.kafka.topic)

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

type data struct {
	Payload string
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
	if rand.Float64() < model.mobileRatio {
		kind = "m"
	}
	nwDelayMs := rand.Intn(model.avgNwDelayMs)
	numEvents := rand.Intn(model.avgNumEvents) + model.minNumEvents
	buffered := (kind == "m") && (rand.Float64() < model.bufferdRatio)
	bufferedDelayMs := 0
	bufferedStart := 0
	bufferedNumEvents := 0
	if buffered {
		bufferedDelayMs = rand.Intn(model.avgBufferedDelayMs)
		bufferedStart = int(float64(numEvents) * rand.Float64())
		bufferedNumEvents = numEvents - bufferedStart
	}
	metadata := metadata{nwDelayMs, numEvents, model.avgEventIntervalMs, float64(model.eventIntervalStddev),
		buffered, bufferedDelayMs, bufferedNumEvents, bufferedStart}
	sessionID := fmt.Sprint(uuid.Must(uuid.NewV4()))
	ID := 0

	return event{metadata, kind, clientID, 0, "", sessionID, ID, data{""}, 0}
}

func (ev *event) emit() {
	var now time.Time
	now, ev.Timestamp = timestamp()
	ev.Timestring = now.Format(time.RFC3339)
	ev.ID++
	ev.Data = data{"hello world"}
}

func (ev event) delay(buffer map[string][]event) event {
	delay := int(rand.NormFloat64()*ev.metadata.eventIntervalStddev) + ev.metadata.avgEventIntervalMs // simulate intervals between clicks

	if ev.metadata.buffered && ev.ID >= ev.metadata.bufferedStart {
		_, ok := buffer[ev.SessionID]
		if !ok {
			var eventList = []event{}
			buffer[ev.SessionID] = eventList
		}
		buffer[ev.SessionID] = append(buffer[ev.SessionID], ev)
		if ev.ID == ev.metadata.numEvents {
			sleep(ev.metadata.bufferedDelayMs)
			for _, ev2 := range buffer[ev.SessionID] {
				ev2.publish()
			}
		} else {
			sleep(delay)
		}
	} else {
		ev.publish()
		sleep(delay)
	}

	return ev
}

func (ev event) publish() {
	sleep(ev.metadata.avgNwDelayMs) // Simulate network delay
	_, ev.SentTimestamp = timestamp()
	bytes, err := json.Marshal(ev)
	if err != nil {
		log.Error(err)
	} else {
		producer.Send(bytes)
	}
}

func session(wg *sync.WaitGroup, clientID int, wait int) {
	var buffer = make(map[string][]event)
	defer wg.Done()
	sleep(wait)
	ev := start(clientID)
	log.Debug(spew.Sprintf("Session (clientID %d) started: %+v", clientID, ev.metadata))

	for i := 0; i < ev.metadata.numEvents; i++ {
		ev.emit()
		ev.delay(buffer)
	}
	log.Debug(spew.Sprintf("Session (clientID %d) stopped", clientID))
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
	log.Debug("started all")
	wg.Wait()
	log.Info("Done")
}
