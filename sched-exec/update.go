package main

import (
	"log"
	"net/http"

	"github.com/vladimirvivien/mesos-http/mesos/mesos"
	"github.com/vladimirvivien/mesos-http/mesos/sched"
)

func (s *scheduler) status(status *mesos.TaskStatus) {

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		log.Fatal(
			"Exiting because task ",
			status.GetTaskId().GetValue(),
			" is in unexpected statse ", status.GetState().String(),
			" with reason ", status.GetReason().String(),
			" from source ", status.GetSource().String(),
			" with message ", status.GetMessage(),
		)
	}

	// send ack
	if status.GetUuid() != nil {
		call := &sched.Call{
			FrameworkId: s.framework.GetId(),
			Type:        sched.Call_ACKNOWLEDGE.Enum(),
			Acknowledge: &sched.Call_Acknowledge{
				AgentId: status.GetAgentId(),
				TaskId:  status.GetTaskId(),
				Uuid:    status.GetUuid(),
			},
		}

		// send call
		resp, err := s.send(call)
		if err != nil {
			log.Println("Unable to send Acknowledge Call: ", err)
			return
		}
		if resp.StatusCode != http.StatusAccepted {
			log.Printf("Acknowledge call returned unexpected status: %d", resp.StatusCode)
		}
	}

	if status.GetState() == mesos.TaskState_TASK_ERROR {
		s.taskFinished++
		log.Println(
			"Task ID ", status.TaskId.GetValue(),
			" state = ", status.GetState().String(),
			" message = ", status.GetMessage(),
		)
	}

	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		log.Println("Finished task: ", status.GetTaskId().GetValue())
		s.taskFinished++
	}

	if s.taskFinished == s.maxTasks {
		log.Println("Scheduler executed all tasks")
		s.stop()
	}

}
