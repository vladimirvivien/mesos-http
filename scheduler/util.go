package scheduler

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesoshttp/mesos"
)

func NewCpuResource(val float64) *mesos.Resource {
	return &mesos.Resource{
		Name:   proto.String("cpu"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: &val},
	}
}

func NewMemResource(val float64) *mesos.Resource {
	return &mesos.Resource{
		Name:   proto.String("mem"),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: &val},
	}
}

func NewTaskInfo(cmd string, offer *mesos.Offer, resources []*mesos.Resource) *mesos.TaskInfo {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	return &mesos.TaskInfo{
		Name: proto.String("hello-task"),
		TaskId: &mesos.TaskID{
			Value: proto.String(id),
		},
		AgentId:   offer.AgentId,
		Resources: resources,
		Command: &mesos.CommandInfo{
			Value: proto.String(cmd),
			Shell: proto.Bool(true),
		},
	}
}
