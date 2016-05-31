package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vladimirvivien/mesos-http/mesos/mesos"
	"github.com/vladimirvivien/mesos-http/mesos/sched"
)

// Offers handle incoming offers
func (s *scheduler) offers(offers []*mesos.Offer) {
	for _, offer := range offers {
		log.Println("Processing offer ", offer.Id.GetValue())

		cpus, mems := s.offeredResources(offer)
		var tasks []*mesos.TaskInfo
		//log.Println("cpus available for tasks: ", cpus, " mems available: ", mems)

		for s.taskLaunched < s.maxTasks &&
			cpus >= s.cpuPerTask &&
			mems >= s.memPerTask {

			taskID := fmt.Sprintf("%d", time.Now().UnixNano())
			//log.Println("Preparing task with id ", taskID, " for launch")
			task := &mesos.TaskInfo{
				Name: proto.String(fmt.Sprintf("task-%s", taskID)),
				TaskId: &mesos.TaskID{
					Value: proto.String(taskID),
				},
				AgentId: offer.AgentId,
				Resources: []*mesos.Resource{
					&mesos.Resource{
						Name:   proto.String("cpus"),
						Type:   mesos.Value_SCALAR.Enum(),
						Scalar: &mesos.Value_Scalar{Value: proto.Float64(s.cpuPerTask)},
					},
					&mesos.Resource{
						Name:   proto.String("mem"),
						Type:   mesos.Value_SCALAR.Enum(),
						Scalar: &mesos.Value_Scalar{Value: proto.Float64(s.memPerTask)},
					},
				},
				Executor: s.executor,
			}
			tasks = append(tasks, task)
			s.taskLaunched++
			cpus -= s.cpuPerTask
			mems -= s.memPerTask
			//log.Println("cpus remaining: ", cpus, " mems remaining: ", mems)
		}

		// setup launch call
		call := &sched.Call{
			FrameworkId: s.framework.GetId(),
			Type:        sched.Call_ACCEPT.Enum(),
			Accept: &sched.Call_Accept{
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
				//Filters: &mesos.Filters{RefuseSeconds: proto.Float64(1)},
			},
		}

		// send call
		resp, err := s.send(call)
		if err != nil {
			log.Println("Unable to send Accept Call: ", err)
			continue
		}
		if resp.StatusCode != http.StatusAccepted {
			log.Printf("Accept Call returned unexpected status: %d", resp.StatusCode)
		}
	}
}

// offeredResources
func (s *scheduler) offeredResources(offer *mesos.Offer) (cpus, mems float64) {
	for _, res := range offer.GetResources() {
		if res.GetName() == "cpus" {
			cpus += *res.GetScalar().Value
		}
		if res.GetName() == "mem" {
			mems += *res.GetScalar().Value
		}
	}
	return
}
