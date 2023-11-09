package nsq

import (
	"sync"
	"time"

	"github.com/arleontr/goahc/channel"

	"github.com/nsqio/go-nsq"
)

const DefaultTCPAddr = "localhost:4150"


type Channel struct {
	sm        sync.Mutex
	sendChans map[string]chan []byte
	rm           sync.Mutex
	receiveChans map[string]chan []byte
	errChan chan error
	stopchan chan struct{}
	stopProdChan chan struct{}
	consumers []*nsq.Consumer
	producersWG sync.WaitGroup

	NewProducer func() (*nsq.Producer, error)

	NewConsumer func(name string) (*nsq.Consumer, error)

	ConnectConsumer func(consumer *nsq.Consumer) error
}

func New() *Channel {
	return &Channel{
		sendChans:    make(map[string]chan []byte),
		receiveChans: make(map[string]chan []byte),

		stopchan:     make(chan struct{}),
		stopProdChan: make(chan struct{}),
		errChan:      make(chan error, 10),

		consumers: []*nsq.Consumer{},

		NewProducer: func() (*nsq.Producer, error) {
			return nsq.NewProducer(DefaultTCPAddr, nsq.NewConfig())
		},
		NewConsumer: func(name string) (*nsq.Consumer, error) {
			return nsq.NewConsumer(name, "vice", nsq.NewConfig())
		},
		ConnectConsumer: func(consumer *nsq.Consumer) error {
			return consumer.ConnectToNSQD(DefaultTCPAddr)
		},
	}
}

func (t * Channel) Init (){
	t.sendChans=    make(map[string]chan []byte)
	t.receiveChans= make(map[string]chan []byte)
	t.stopchan=     make(chan struct{})
	t.stopProdChan= make(chan struct{})
	t.errChan=      make(chan error, 10)
	t.consumers= []*nsq.Consumer{}
	t.NewProducer= func() (*nsq.Producer, error) {
		return nsq.NewProducer(DefaultTCPAddr, nsq.NewConfig())
	}
	t.NewConsumer= func(name string) (*nsq.Consumer, error) {
		return nsq.NewConsumer(name, "vice", nsq.NewConfig())
	}
	t.ConnectConsumer = func(consumer *nsq.Consumer) error {
		return consumer.ConnectToNSQD(DefaultTCPAddr)
	}

}

func (t *Channel) ErrChan() <-chan error {
	return t.errChan
}

func (t *Channel) RegisterReceiver(name string) <-chan []byte {
	t.rm.Lock()
	defer t.rm.Unlock()

	ch, ok := t.receiveChans[name]
	if ok {
		return ch
	}
	var err error
	if ch, err = t.makeConsumer(name); err != nil {
		
		t.errChan <- &channel.CommError{Name: name, CommErr: err}
		return make(chan []byte)
	}
	t.receiveChans[name] = ch
	return ch
}

func (t *Channel) makeConsumer(name string) (chan []byte, error) {
	ch := make(chan []byte)
	consumer, err := t.NewConsumer(name)
	if err != nil {
		return nil, err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		body := message.Body
		message.Finish() 
		ch <- body
		return nil
	}))

	err = channel.Backoff(1*time.Second, 10*time.Minute, 0, func() error {
		return t.ConnectConsumer(consumer)
	})
	if err != nil {
		return nil, err
	}
	t.consumers = append(t.consumers, consumer)
	return ch, nil
}

func (t *Channel) RegisterSender(name string) chan<- []byte {
	t.sm.Lock()
	defer t.sm.Unlock()

	ch, ok := t.sendChans[name]
	if ok {
		return ch
	}
	var err error
	ch, err = t.makeProducer(name)
	if err != nil {
		t.errChan <- &channel.CommError{Name: name, CommErr: err}
		return make(chan []byte)
	}
	t.sendChans[name] = ch
	return ch
}

func (t *Channel) makeProducer(name string) (chan []byte, error) {
	ch := make(chan []byte)
	producer, err := t.NewProducer()
	if err != nil {
		return nil, err
	}
	t.producersWG.Add(1)
	go func() {
		defer func() {
			producer.Stop()
			t.producersWG.Done()
		}()
		for {
			select {
			case <-t.stopProdChan:
				return
			case msg := <-ch:
				err = channel.Backoff(1*time.Second, 10*time.Minute, 10, func() error {
					return producer.Publish(name, msg)
				})
				if err != nil {
					t.errChan <- &channel.CommError{Message: msg, Name: name, CommErr: err}
					continue
				}
			}
		}
	}()
	return ch, nil
}

func (t *Channel) Close() {
	
	close(t.stopProdChan)
	t.producersWG.Wait()

	for _, c := range t.consumers {
		c.Stop()
		<-c.StopChan
	}
	close(t.stopchan)
}

func (t *Channel) Done() chan struct{} {
	return t.stopchan
}