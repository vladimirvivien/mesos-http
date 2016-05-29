package executor

import (
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesos-http/client"
	"github.com/vladimirvivien/mesos-http/mesos"
)

// Scheduler represents a Mesos scheduler
type executor struct {
	agent       string
	id          *mesos.ExecutorID
	frameworkID *mesos.FrameworkID
	eventClient *client.Client
	callClient  *client.Client
	events      chan *mesos.Event
	doneChan    chan struct{}
}

func newExec() *executor {
	return &executor{
		events:   make(chan *mesos.Event),
		doneChan: make(chan struct{}),
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
}
