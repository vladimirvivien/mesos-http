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
	"github.com/vladimirvivien/mesos-http/mesos/mesos"
	sched "github.com/vladimirvivien/mesos-http/mesos/sched"
)

// Scheduler represents a Mesos scheduler
type scheduler struct {
	framework    *mesos.FrameworkInfo
	executor     *mesos.ExecutorInfo
	command      *mesos.CommandInfo
	taskLaunched int
	taskFinished int
	maxTasks     int

	client     *client.Client
	callClient *client.Client
	cpuPerTask float64
	memPerTask float64
	events     chan *sched.Event
	doneChan   chan struct{}
}

// New returns a pointer to new Scheduler
func newSched(master string, fw *mesos.FrameworkInfo, cmd *mesos.CommandInfo) *scheduler {
	return &scheduler{
		client:     client.New(master, "/api/v1/scheduler"),
		framework:  fw,
		command:    cmd,
		cpuPerTask: 1,
		memPerTask: 128,
		maxTasks:   5,
		events:     make(chan *sched.Event),
		doneChan:   make(chan struct{}),
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

func (s *scheduler) send(call *sched.Call) (*http.Response, error) {
	payload, err := proto.Marshal(call)
	if err != nil {
		return nil, err
	}
	return s.client.Send(payload)
}

// Subscribe subscribes the scheduler to the Mesos cluster.
// It keeps the http connection opens with the Master to stream
// subsequent events.
func (s *scheduler) subscribe() error {
	call := &sched.Call{
		Type: sched.Call_SUBSCRIBE.Enum(),
		Subscribe: &sched.Call_Subscribe{
			FrameworkInfo: s.framework,
		},
	}

	resp, err := s.send(call)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Subscribe with unexpected response status: %d", resp.StatusCode)
	}
	log.Println("Mesos-Stream-Id:", s.client.StreamID)

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
		event := new(sched.Event)
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

		case sched.Event_SUBSCRIBED:
			sub := ev.GetSubscribed()
			s.framework.Id = sub.FrameworkId
			log.Println("Subscribed: FrameworkID: ", sub.FrameworkId.GetValue())

		case sched.Event_OFFERS:
			offers := ev.GetOffers().GetOffers()
			log.Println("Received ", len(offers), " offers ")
			go s.offers(offers)

		case sched.Event_RESCIND:
			log.Println("Received rescind offers")

		case sched.Event_UPDATE:
			status := ev.GetUpdate().GetStatus()
			go s.status(status)

		case sched.Event_MESSAGE:
			log.Println("Received message event")

		case sched.Event_FAILURE:
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

		case sched.Event_ERROR:
			err := ev.GetError().GetMessage()
			log.Println(err)

		case sched.Event_HEARTBEAT:
			log.Println("HEARTBEAT")
		}

	}
}

var (
	master    = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	execPath  = flag.String("executor", "./exec", "Path to test executor")
	mesosUser = flag.String("user", "", "Framework user")
	maxTasks  = flag.Int("maxtasks", 5, "Mesos authentication principal")
	cmd       = flag.String("cmd", "echo 'Hello World'", "Command to execute")
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
		User:     mesosUser,
		Name:     proto.String("Go-HTTP Scheduler"),
		Hostname: proto.String(hostname),
	}
	cmdInfo := &mesos.CommandInfo{
		Shell: proto.Bool(true),
		Value: proto.String(*cmd),
	}

	sched := newSched(*master, fw, cmdInfo)
	sched.maxTasks = *maxTasks
	<-sched.start()
}
