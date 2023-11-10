package component

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/arleontr/goahc/conveyor"
	"github.com/arleontr/goahc/conveyor/queues/nsq"

	"github.com/google/uuid"
)


type ComponentInterface interface {
	Init(name string)
	Connect(to Component)
	Run()
	Stop()
	ToString() string
}


// Component is the base class for implementing a primitive component type
type Component struct {
	name string
	ID uuid.UUID
	ctx context.Context
	cancel context.CancelFunc
	c chan os.Signal
	Commch conveyor.Conveyor
}

func New () *Component{
	c := &Component{}
	
	return c
}

func (c * Component) ToString() string {
	return "Component Name: " + c.name + " UUID: " + c.ID.String()
}

func (c * Component) Init(name string) {
	c.name = name
	c.ID = uuid.New()
	c.ctx = context.Background()
	c.ctx, c.cancel = context.WithCancel(c.ctx)
	c.c = make(chan os.Signal, 1)
	
	c.Commch = &nsq.Conveyor{}
	c.Commch.Init()
	


}

func (c * Component) Stop() {
	<-c.ctx.Done()
	<-c.Commch.Done()

}

func (c *Component) Run (){
	signal.Notify(c.c, os.Interrupt)
	defer func() {
		signal.Stop(c.c)
		c.cancel()
	}()

	go func() {
		select {
			// we will define new channel handlers here 
		case <-c.c:
			c.cancel()
			c.Stop()
		case <-c.ctx.Done():
			c.Stop()
		default:
			for {
				fmt.Println(c.ToString())
				msgs := c.Commch.RegisterReceiver("IN-"+c.ID.String())
				for msg := range msgs {
					fmt.Println(string(msg))
				}
				
				time.Sleep(1 * time.Second)
			}
		}
	}()

}