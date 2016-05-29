package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/user"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesos-http/client"
	"github.com/vladimirvivien/mesos-http/mesos"
)

// Scheduler represents a Mesos scheduler
type scheduler struct {
	framework    *mesos.FrameworkInfo
	executor     *mesos.ExecutorInfo
	taskLaunched int
	taskFinished int
	maxTasks     int

	eventClient *client.Client
	callClient  *client.Client
	cpuPerTask  float64
	memPerTask  float64
	events      chan *mesos.Event
	doneChan    chan struct{}
}

// New returns a pointer to new Scheduler
func New(master string, fw *mesos.FrameworkInfo, exec *mesos.ExecutorInfo) *scheduler {
	return &scheduler{
		eventClient: client.New(master),
		callClient:  client.New(master),
		framework:   fw,
		executor:    exec,
		cpuPerTask:  1,
		memPerTask:  128,
		maxTasks:    5,
		events:      make(chan *mesos.Event),
		doneChan:    make(chan struct{}),
	}
}

// start starts the scheduler and subscribes to event stream
// returns a channel to wait for completion.
func (s *scheduler) start() <-chan struct{} {
	if err := s.subscribe(); err != nil {
		log.Fatal(err)
	}
	go s.handleEvents()
	return s.doneChan
}

func (s *scheduler) stop() {
	close(s.events)
}

// Subscribe subscribes the scheduler to the Mesos cluster.
// It keeps the http connection opens with the Master to stream
// subsequent events.
func (s *scheduler) subscribe() error {
	// builds calls using Json-encoded structs.
	// Keep in mind, Mesos makes available well-defined
	// protobuf-encoded data structures.  This is used
	// as an illustrative tool to show support for json.
	call := &mesos.Call{
		Type: mesos.Call_SUBSCRIBE.Enum(),
		Subscribe: &mesos.Call_Subscribe{
			FrameworkInfo: s.framework,
		},
	}

	resp, err := s.eventClient.Send(call)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Subscribe with unexpected response status: %d", resp.StatusCode)
	}
	s.callClient.StreamID = s.eventClient.StreamID

	go s.qEvents(resp)

	return nil
}

func (s *scheduler) qEvents(resp *http.Response) {
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
}

func (s *scheduler) handleEvents() {
	defer close(s.doneChan)
	for ev := range s.events {
		switch ev.GetType() {

		case mesos.Event_SUBSCRIBED:
			sub := ev.GetSubscribed()
			s.framework.Id = sub.FrameworkId
			log.Println("Subscribed: FrameworkID: ", sub.FrameworkId.GetValue())

		case mesos.Event_OFFERS:
			offers := ev.GetOffers().GetOffers()
			log.Println("Received ", len(offers), " offers ")
			s.offers(offers)

		case mesos.Event_RESCIND:
			log.Println("Received rescind offers")

		case mesos.Event_UPDATE:
			status := ev.GetUpdate().GetStatus()
			s.status(status)

		case mesos.Event_MESSAGE:
			log.Println("Received message event")

		case mesos.Event_FAILURE:
			log.Println("Received failure event")
			fail := ev.GetFailure()
			if fail.ExecutorId != nil {
				log.Println(
					"Executor ", fail.ExecutorId.GetValue(), " terminated ",
					" with status ", fail.GetStatus(),
					" on agent ", fail.GetAgentId().GetValue(),
				)
			} else {
				if fail.GetAgentId() != nil {
					log.Println("Agent ", fail.GetAgentId().GetValue(), " failed ")
				}
			}

		case mesos.Event_ERROR:
			err := ev.GetError().GetMessage()
			log.Println(err)
		}

	}
}

var (
	master    = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	execPath  = flag.String("executor", "./exec", "Path to test executor")
	mesosUser = flag.String("user", "", "Framework user")
	principal = flag.String("principal", "", "Mesos authentication principal")
)

func init() {
	flag.Parse()
}

func main() {
	if *mesosUser == "" {
		u, err := user.Current()
		if err != nil {
			log.Fatal("Unable to determine user")
		}
		*mesosUser = u.Username
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "UNKNOWN"
	}

	fw := &mesos.FrameworkInfo{
		User:      mesosUser,
		Name:      proto.String("Go Http Scheduler"),
		Hostname:  proto.String(hostname),
		Principal: principal,
	}
	exec := &mesos.ExecutorInfo{
		Name:       proto.String("Mesos-http Executor"),
		ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
		Command:    &mesos.CommandInfo{Value: proto.String(*execPath)},
		Source:     proto.String("go-source"),
	}
	sched := New(*master, fw, exec)
	<-sched.start()
}
