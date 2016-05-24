package scheduler

import (
	"log"

	"github.com/vladimirvivien/mesoshttp/mesos"
)

// Subscribed handle subscribed Event.
func (s *Scheduler) Subscribed(e *mesos.Event) {
	sub := e.GetSubscribed()
	s.framework.Id = sub.FrameworkId
	log.Println("Assigned FrameworkID: ", sub.FrameworkId.GetValue())
}
