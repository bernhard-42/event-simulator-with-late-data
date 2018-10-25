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

const mobileRatio = 0.75
const bufferdRatio = 0.3

var topic string
var producer kafkaprod.KafkaProducer

func init() {
	/* Initialize random generator */
	// rand.Seed(time.Now().UTC().UnixNano())
	rand.Seed(44)

	var configFile = flag.String("config", "./config.json", "config json file")

	/* Initialize logging */
	logLevel, logFile := GetLoggingConfig(*configFile)
	log.SetLevel(logLevel)
	log.SetOutput(logFile)

	/* Initialize Kafka Producer */
	broker, topic := GetKafkaConfig(*configFile)
	producer = kafkaprod.Create(broker, topic)
}

type metadata struct {
	kind                string
	avgNwDelay          time.Duration
	numEvents           int
	avgEventInterval    float64
	eventIntervalStddev float64
	buffered            bool
	bufferedDelay       time.Duration
	bufferedNumEvents   int
	bufferedStart       int
}

type data struct {
	Payload string
}

type event struct {
	metadata   metadata
	Timestamp  int64
	Timestring string
	SessionID  string
	ID         int
	Data       data
}

func start() event {
	kind := "d"
	if rand.Float32() < mobileRatio {
		kind = "m"
	}
	avgNwDelay := time.Duration(rand.Float64() / 100)
	numEvents := rand.Intn(50)
	avgEventInterval := 5.0
	eventIntervalStddev := 0.3
	buffered := rand.Float32() < bufferdRatio
	bufferedDelay := time.Duration(200.0 / 3.0 * rand.Float64())
	bufferedStart := 0
	bufferedNumEvents := 0
	if buffered {
		bufferedStart = int(float64(numEvents) * rand.Float64())
		bufferedNumEvents = numEvents - bufferedStart
	}
	metadata := metadata{kind, avgNwDelay, numEvents, avgEventInterval, eventIntervalStddev,
		buffered, bufferedDelay, bufferedNumEvents, bufferedStart}
	sessionID := fmt.Sprint(uuid.Must(uuid.NewV4()))
	ID := 0

	return event{metadata, 0, "", sessionID, ID, data{""}}
}

func (ev *event) emit() {
	now := time.Now()
	ev.Timestamp = now.UnixNano() / 1000000
	ev.Timestring = now.Format(time.RFC3339)
	ev.ID++
	ev.Data = data{"hello world"}
}

func (ev event) delay(buffer map[string][]event) event {
	delay := time.Duration(rand.NormFloat64()*ev.metadata.eventIntervalStddev*ev.metadata.avgEventInterval + ev.metadata.avgEventInterval)

	if ev.metadata.buffered && ev.ID >= ev.metadata.bufferedStart {
		_, ok := buffer[ev.SessionID]
		if !ok {
			var eventList = []event{}
			buffer[ev.SessionID] = eventList
		}
		buffer[ev.SessionID] = append(buffer[ev.SessionID], ev)
		if ev.ID == ev.metadata.numEvents {
			time.Sleep(ev.metadata.bufferedDelay * time.Second)
			for _, ev2 := range buffer[ev.SessionID] {
				ev2.publish()
			}
		} else {
			time.Sleep(delay * time.Second)
		}
	} else {
		ev.publish()
		time.Sleep(delay * time.Second)
	}
	return ev
}

func (ev event) publish() {
	bytes, err := json.Marshal(ev)
	if err != nil {
		log.Error(err)
	} else {
		time.Sleep(ev.metadata.avgNwDelay)
		producer.Send(bytes)
	}
}

func session(wg *sync.WaitGroup, wait float32) {
	var buffer = make(map[string][]event)
	defer wg.Done()
	time.Sleep(time.Duration(wait) * time.Second)

	ev := start()
	log.Debug(spew.Sprintf("Session start: %+v", ev.metadata))

	for i := 0; i < ev.metadata.numEvents; i++ {
		ev.emit()
		ev.delay(buffer)
	}
}

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go session(&wg, 30*rand.Float32())
	}
	wg.Wait()
	log.Info("Done")
}
