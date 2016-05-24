package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesoshttp/client"
	"github.com/vladimirvivien/mesoshttp/mesos"
	mesosjson "github.com/vladimirvivien/mesoshttp/mesos/json"
)

// Scheduler represents a Mesos scheduler
type Scheduler struct {
	framework  *mesos.FrameworkInfo
	client     *client.Client
	cpuPerTask float64
	memPerTask float64
	maxTasks   int
	events     chan *mesos.Event
	doneChan   chan struct{}
}

// New returns a pointer to new Scheduler
func New(user, master string) *Scheduler {
	return &Scheduler{
		client: client.New(master),
		framework: &mesos.FrameworkInfo{
			User:     proto.String(user),
			Name:     proto.String("Simple Scheduler"),
			Hostname: proto.String("localhost"),
		},
		cpuPerTask: 1.0,
		memPerTask: 128,
		maxTasks:   10,
		events:     make(chan *mesos.Event),
		doneChan:   make(chan struct{}),
	}
}

// Start starts the scheduler and subscribes to event stream
// returns a channel to wait for completion.
func (s *Scheduler) Start() <-chan struct{} {
	if err := s.Subscribe(); err != nil {
		log.Fatal(err)
	}
	go s.handleEvents()
	return s.doneChan
}

// Subscribe subscribes the scheduler to the Mesos cluster.
// It keeps the http connection opens with the Master to stream
// subsequent events.
func (s *Scheduler) Subscribe() error {
	// builds calls using Json-encoded structs.
	// Keep in mind, Mesos makes available well-defined
	// protobuf-encoded data structures.  This is used
	// as an illustrative tool to show support for json.
	call := &mesosjson.Call{
		Type: "SUBSCRIBE",
		Subscribe: &mesosjson.Subscribe{
			FrameworkInfo: &mesosjson.FrameworkInfo{
				User: "root",
				Name: "Test Framework",
			},
		},
	}

	resp, err := s.client.SendAsJson(call) // sending call as json-encoded.
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Subscribe with unexpected response status: %d", resp.StatusCode)
	}

	// hanlde streaming events
	go func() {
		defer func() {
			resp.Body.Close()
			close(s.events)
		}()
		dec := json.NewDecoder(resp.Body)
		for {
			event := new(mesos.Event)
			if err := dec.Decode(event); err != nil {
				if err == io.EOF {
					return
				}
				continue
			}
			s.events <- event
		}
	}()
	return nil
}

func (s *Scheduler) handleEvents() {
	defer close(s.doneChan)
	for ev := range s.events {
		switch ev.GetType() {
		case mesos.Event_SUBSCRIBED:
			s.Subscribed(ev)
		case mesos.Event_OFFERS:
			s.Offers(ev)
		case mesos.Event_ERROR:
			s.Error(ev)
		}

	}
}
