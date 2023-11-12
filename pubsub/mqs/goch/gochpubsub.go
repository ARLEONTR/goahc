package goch

import (
	"fmt"
	"sync"
)

// This is a simple pubsub implementation using go channels

var lock = &sync.Mutex{}

// Agent is a simple pub/sub agent
type PubSubAgent struct {
	mu     sync.Mutex
	subs   map[string][]chan []byte
	quit   chan struct{}
	closed bool
}

var SingleGoPubSubAgent *PubSubAgent

// NewAgent creates a new Agent as a singleton, we do not create new agents over and over again
func NewPubSubAgent() *PubSubAgent {
	if SingleGoPubSubAgent == nil {
		lock.Lock()
		defer lock.Unlock()
		if SingleGoPubSubAgent == nil {
			fmt.Println("Creating single instance now.")
			SingleGoPubSubAgent = &PubSubAgent{
				subs: make(map[string][]chan []byte),
				quit: make(chan struct{}),
			}
		} else {
			fmt.Println("Single instance already created.")
		}
	} else {
		fmt.Println("Single instance already created.")
	}

	return SingleGoPubSubAgent
}

// Publish publishes a message to a topic
func (b *PubSubAgent) Publish(topic string, msg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	for _, ch := range b.subs[topic] {
		ch <- msg
	}
}

// Subscribe subscribes to a topic
func (b *PubSubAgent) Subscribe(topic string) <-chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	ch := make(chan []byte, 100)
	b.subs[topic] = append(b.subs[topic], ch)
	return ch
}

// Close closes the agent
func (b *PubSubAgent) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.closed = true
	close(b.quit)

	for _, ch := range b.subs {
		for _, sub := range ch {
			close(sub)
		}
	}
}

/*
func testme() {
	// Create a new agent
	agent := NewAgent()

	// Subscribe to a topic
	sub := agent.Subscribe("foo")

	// Publish a message to the topic
	go agent.Publish("foo", "hello world")

	// Print the message
	fmt.Println(<-sub)

	// Close the agent
	agent.Close()
}
*/
