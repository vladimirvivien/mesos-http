package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/vladimirvivien/mesoshttp/mesos"
	mesosjson "github.com/vladimirvivien/mesoshttp/mesos/json"
)

type Client struct {
	streamID string
	url      string
	client   *http.Client
}

func New(addr string) *Client {
	return &Client{
		url: "http://" + addr + "/api/v1/scheduler",
		client: &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
			},
		},
	}
}

func (c *Client) Send(call *mesos.Call) (*http.Response, error) {
	payload := new(bytes.Buffer)
	if err := json.NewEncoder(payload).Encode(call); err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest("POST", c.url, payload)
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", "mesos-demo/0.1")
	httpReq.Header.Set("Mesos-Stream-Id", c.streamID)
	log.Printf("SENDING:%v", httpReq)

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("Unable to do request: %s", err)
	}
	c.streamID = httpResp.Header.Get("Mesos-Stream-Id")

	return httpResp, nil
}

func (c *Client) SendAsJson(call *mesosjson.Call) (*http.Response, error) {
	payload := new(bytes.Buffer)
	if err := json.NewEncoder(payload).Encode(call); err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest("POST", c.url, payload)
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", "mesos-demo/0.1")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("Unable to do request: %s", err)
	}
	c.streamID = httpResp.Header.Get("Mesos-Stream-Id")
	log.Println("Stream-ID: ", c.streamID)
	return httpResp, nil
}