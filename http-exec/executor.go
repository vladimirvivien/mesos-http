package main

import (
	"github.com/vladimirvivien/mesos-http/client"
	"github.com/vladimirvivien/mesos-http/mesos"
)

// Scheduler represents a Mesos scheduler
type executor struct {
	frameworkID *mesos.FrameworkID
	eventClient *client.Client
	callClient  *client.Client
	events      chan *mesos.Event
	doneChan    chan struct{}
}
