package pubsub

import "fmt"

type PSMQ interface {
	Init()
	SubscribeToTopic(name string) <-chan []byte   //Subscribe to a topic
	CreatePublishTopic(name string) chan<- []byte //Create a publicaton topic
	//P2PErrorChan() <-chan error
	Close()
	Done() chan struct{}
}

type QueueType int

const (
	NSQ      QueueType = 0
	REDIS    QueueType = 1
	GOCH     QueueType = 2
	KAFKA    QueueType = 3
	NATS     QueueType = 4
	RABBITMQ QueueType = 5
	ZEROMQ   QueueType = 6
)

// Err represents a vice error.
type CommError struct {
	Message []byte
	Name    string
	CommErr error
}

func (e *CommError) Error() string {
	if len(e.Message) > 0 {
		return fmt.Sprintf("%s: |%s| <- `%s`", e.CommErr, e.Name, string(e.Message))
	}
	return fmt.Sprintf("%s: |%s|", e.CommErr, e.Name)
}
