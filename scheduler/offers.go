package scheduler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesoshttp/mesos"
)

// Offers handle incoming offers
func (s *Scheduler) Offers(ev *mesos.Event) {
	cmd := "echo Hello"
	offers := ev.GetOffers().GetOffers()
	for _, offer := range offers {
		taskID := fmt.Sprintf("%d", time.Now().UnixNano())
		tasksPerOffer := s.tasksPerOffer(offer)
		tasks := make([]*mesos.TaskInfo, tasksPerOffer)
		for i := 0; i < tasksPerOffer; i++ {
			task := &mesos.TaskInfo{
				Name: proto.String(cmd),
				TaskId: &mesos.TaskID{
					Value: proto.String(taskID),
				},
				AgentId: offer.AgentId,
				Resources: []*mesos.Resource{
					&mesos.Resource{
						Name:   proto.String("cpu"),
						Type:   mesos.Value_SCALAR.Enum(),
						Scalar: &mesos.Value_Scalar{Value: proto.Float64(s.cpuPerTask)},
					},
					&mesos.Resource{
						Name:   proto.String("mem"),
						Type:   mesos.Value_SCALAR.Enum(),
						Scalar: &mesos.Value_Scalar{Value: proto.Float64(s.memPerTask)},
					},
				},
				Command: &mesos.CommandInfo{
					Value: proto.String(cmd),
					Shell: proto.Bool(true),
				},
			}
			tasks[i] = task
		}

		s.Accept(offer, tasks)
	}
}

//Accept accepts an offer and use it to launch task
func (s *Scheduler) Accept(offer *mesos.Offer, tasks []*mesos.TaskInfo) error {

	call := &mesos.Call{
		FrameworkId: s.framework.GetId(),
		Type:        mesos.Call_ACCEPT.Enum(),
		Accept: &mesos.Call_Accept{
			OfferIds: []*mesos.OfferID{
				offer.GetId(),
			},
			Operations: []*mesos.Offer_Operation{
				&mesos.Offer_Operation{
					Type: mesos.Offer_Operation_LAUNCH.Enum(),
					Launch: &mesos.Offer_Operation_Launch{
						TaskInfos: tasks,
					},
				},
			},
			Filters: &mesos.Filters{RefuseSeconds: proto.Float64(1)},
		},
	}

	resp, err := s.client.Send(call)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Accept call returned unexpected status: %d", resp.StatusCode)
	}

	return nil
}

func (s *Scheduler) tasksPerOffer(offer *mesos.Offer) int {
	var memCount float64
	for _, res := range offer.GetResources() {
		if res.GetName() == "mem" {
			memCount += *res.GetScalar().Value
		}
	}
	total := int(float64(memCount) / s.memPerTask)
	if s.maxTasks < total {
		return s.maxTasks
	}
	return total
}
