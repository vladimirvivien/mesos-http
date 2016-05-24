package scheduler

import (
	"log"

	"github.com/vladimirvivien/mesoshttp/mesos"
)

func (s *Scheduler) Error(ev *mesos.Event) {
	err := ev.GetError().GetMessage()
	log.Println(err)
}
