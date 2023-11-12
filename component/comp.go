package component

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"

	"github.com/arleontr/goahc/pubsub"
	"github.com/arleontr/goahc/pubsub/mqs/goch"
	"github.com/arleontr/goahc/pubsub/mqs/nsq"
	"github.com/arleontr/goahc/pubsub/mqs/redis"
)

type PortType string

const (
	SOUTH PortType = "SOUTH"
	NORTH PortType = "NORTH"
)

type ComponentInterface interface {
	Init(name string, queuetype pubsub.QueueType) //Create two PUB topics (HOSTIP-Name-NORTH and HOSTIP-Name-SOUTH)
	Connect(toid string, port PortType)           //Self subscribes to toid's port {SOUTH or NORTH}
	SendSouth(msg []byte)                         //publish to HOSTIP-Name-SOUTH
	SendNorth(msg []byte)                         //publish to HOSTIP-Name-NORTH
	Run()                                         //Message and event handler is implemented here
	Stop()                                        //Stop the goroutine
	ToString() string                             //For debug purposes, print unique name in the system
}

// Component is the base class for implementing a primitive component type
type Component struct {
	name       string
	ID         string
	ctx        context.Context
	cancel     context.CancelFunc
	c          chan os.Signal
	queue      pubsub.PSMQ
	InChs      []<-chan []byte
	errChan    chan error
	NorthCh    chan<- []byte
	SouthCh    chan<- []byte
	northtopic string
	southtopic string
}

func New() *Component {
	c := &Component{}

	return c
}

func (c *Component) ToString() string {
	return "Component Name: " + c.name + " UUID: " + c.ID
}

// TODO: Convert queuetype and some future vars into variadic options...
func (c *Component) Init(name string, queuetype pubsub.QueueType) {
	c.name = name
	c.ID = c.name
	c.ctx = context.Background()
	c.ctx, c.cancel = context.WithCancel(c.ctx)
	c.c = make(chan os.Signal, 1)
	c.InChs = make([]<-chan []byte, 5) //At most 5 subscribers
	// TODO: Varios pub sub message queues are to be implemented
	switch queuetype {
	case pubsub.NSQ:
		c.queue = &nsq.PSMQ{}
	case pubsub.GOCH:
		c.queue = &goch.PSMQ{}
	case pubsub.REDIS:
		c.queue = &redis.PSMQ{}
	default:
		c.queue = &nsq.PSMQ{}
	}
	c.queue.Init()
	// Create pub topics north and south
	c.northtopic = c.ID + "-" + string(NORTH)
	c.southtopic = c.ID + "-" + string(SOUTH)
	c.NorthCh = c.queue.CreatePublishTopic(c.northtopic)
	c.SouthCh = c.queue.CreatePublishTopic(c.southtopic)
	fmt.Println("Component ", c.ID, " is initialized\n", c.northtopic, "\n", c.southtopic)
	c.errChan = c.queue.ErrChan()
}

func (c *Component) Stop() {
	<-c.ctx.Done()
	<-c.queue.Done()

}

func (c *Component) Connect(id string, port PortType) {
	topic := id + "-" + string(port)
	ch := c.queue.SubscribeToTopic(topic)
	c.InChs = append(c.InChs, ch)
	fmt.Println(c.ID, " subscribes to ", topic)
	fmt.Println(c.InChs)
}

func (c *Component) SendNorth(msg []byte) {
	c.NorthCh <- msg
}

func (c *Component) SendSouth(msg []byte) {
	c.SouthCh <- msg
}

func (c *Component) Select() (int,
	[]byte, error) {
	var zeroT []byte
	cases := make([]reflect.SelectCase, len(c.InChs)+1)
	for i, ch := range c.InChs {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	cases[len(c.InChs)] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.ctx.Done())}
	// ok will be true if the channel has not been closed.
	chosen, value, ok := reflect.Select(cases)
	if !ok {
		if c.ctx.Err() != nil {
			return -1, zeroT, c.ctx.Err()
		}
		return chosen, zeroT, errors.New("channel closed")
	}
	if ret, ok := value.Interface().([]byte); ok {
		return chosen, ret, nil
	}
	return chosen, zeroT, errors.New("failed to cast value")
}

func (c *Component) Run() {

	signal.Notify(c.c, os.Interrupt)
	defer func() {
		signal.Stop(c.c)
		c.cancel()
	}()

	for {
		select {
		// we will define new channel handlers here
		case <-c.c:
			c.cancel()
			c.Stop()
		case <-c.ctx.Done():
			c.Stop()
		case err := <-c.errChan:
			fmt.Println("Error channel happened ", err)
		default:
			fmt.Println("Running ", c.ID)
			chosen, val, err := c.Select()
			if err != nil {
				fmt.Println("Error occured ", err)
			} else {
				fmt.Println("I am ", c.ID, "received msg from ", chosen, string(val), err) //Process message val

			}

		}
	}
}
