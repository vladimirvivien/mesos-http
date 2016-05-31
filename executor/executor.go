package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesos-http/client"
	exec "github.com/vladimirvivien/mesos-http/mesos/exec"
	"github.com/vladimirvivien/mesos-http/mesos/mesos"
)

// Scheduler represents a Mesos scheduler
type executor struct {
	id          *mesos.ExecutorID
	frameworkID *mesos.FrameworkID

	client   *client.Client
	events   chan *exec.Event
	doneChan chan struct{}
}

func newExec(agent string) *executor {
	return &executor{
		client:   client.New(agent),
		events:   make(chan *exec.Event),
		doneChan: make(chan struct{}),
	}
}

func (e *executor) start() <-chan struct{} {
	if err := e.subscribe(); err != nil {
		log.Fatal(err)
	}
	go e.handleEvents()
	return e.doneChan
}

func (e *executor) stop() {
	close(e.events)
}

func (e *executor) send(call *exec.Call) (*http.Response, error) {
	payload, err := proto.Marshal(call)
	if err != nil {
		return nil, err
	}
	return e.client.Send(payload)
}

func (e *executor) subscribe() error {
	call := &exec.Call{
		FrameworkId: e.frameworkID,
		ExecutorId:  e.id,
		Type:        exec.Call_SUBSCRIBE.Enum(),
		Subscribe: &exec.Call_Subscribe{
			UnacknowledgedTasks:   []*mesos.TaskInfo{},
			UnacknowledgedUpdates: []*exec.Call_Update{},
		},
	}

	resp, err := e.send(call)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Subscribe with unexpected response status: %d", resp.StatusCode)
	}
	log.Println("Mesos-Stream-Id:", e.client.StreamID)

	go e.qEvents(resp)

	return nil
}

func (e *executor) qEvents(resp *http.Response) {
	defer func() {
		resp.Body.Close()
		close(e.events)
	}()
	dec := json.NewDecoder(resp.Body)
	for {
		event := new(exec.Event)
		if err := dec.Decode(event); err != nil {
			if err == io.EOF {
				return
			}
			continue
		}
		e.events <- event
	}
}

func (e *executor) handleEvents() {
	defer close(e.doneChan)
	for ev := range e.events {
		switch ev.GetType() {

		case exec.Event_SUBSCRIBED:
			sub := ev.GetSubscribed()
			log.Println("Executor subscribed with id", sub.GetExecutorInfo().GetExecutorId())

		case exec.Event_ERROR:
			err := ev.GetError().GetMessage()
			log.Println(err)
		}
	}
}

func main() {
	agent := os.Getenv("MESOS_AGENT_ENDPOINT")
	if agent == "" {
		log.Fatal("MESOS_AGENT_ENDPOINT env not set")
	}
	fwid := os.Getenv("MESOS_FRAMEWORK_ID")
	if fwid == "" {
		log.Fatal("MESOS_FRAMEWORK_ID env not set")
	}
	execid := os.Getenv("MESOS_EXECUTOR_ID")
	if execid == "" {
		log.Fatal("MESOS_EXECUTOR_ID env not set")
	}

	exec := newExec(agent)
	exec.frameworkID = &mesos.FrameworkID{
		Value: proto.String(fwid),
	}
	exec.id = &mesos.ExecutorID{
		Value: proto.String(execid),
	}
	<-exec.start()
}
