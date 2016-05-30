package executor

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesos-http/client"
	"github.com/vladimirvivien/mesos-http/mesos"
)

// Scheduler represents a Mesos scheduler
type executor struct {
	id          *mesos.ExecutorID
	frameworkID *mesos.FrameworkID

	client   *client.Client
	events   chan *mesos.Event
	doneChan chan struct{}
}

func newExec(agent string) *executor {
	return &executor{
		client:   client.New(agent),
		events:   make(chan *mesos.Event),
		doneChan: make(chan struct{}),
	}
}

func (e *executor) start() <-chan struct{} {
	if err := e.subscribe(); err != nil {
		log.Fatal(err)
	}
	go s.handleEvents()
	return s.doneChan
}

func (s *scheduler) stop() {
	close(s.events)
}

func (e *executor) subscribe() error {
	call := &mesos.Call{
		FrameworkId: e.frameworkID,
		ExecutorId:  e.id,
		Type:        mesos.Call_SUBSCRIBE.Enum(),
		Subscribe: &mesos.Call_Subscribe{
			UnacknowledgedTasks:   []*mesos.TaskInfo{},
			UnacknowledgedUpdates: []*mesos.Call_Update{},
		},
	}

	resp, err := e.client.Send(call)
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

func (e *executor) qEvents(resp *http.Response) {
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
		e.events <- event
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

		case mesos.Event_ERROR:
			err := ev.GetError().GetMessage()
			log.Println(err)
		}
	}
}

func main() {
	exec := newExec()
	exec.agent = os.Getenv("MESOS_AGENT_ENDPOINT")
	exec.frameworkID = &mesos.FrameworkID{
		Value: proto.String(os.Getenv("MESOS_FRAMEWORK_ID")),
	}
	exec.id = &mesos.ExecutorID{
		Value: proto.String(os.Getenv("MESOS_EXECUTOR_ID")),
	}
	<-exec.start()
}
