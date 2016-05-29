package main

import "github.com/vladimirvivien/mesos-http/scheduler"

func main() {
	sched := scheduler.New("root", "127.0.0.1:5050")
	<-sched.Start()
}
