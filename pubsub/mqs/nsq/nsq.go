package nsq

import (
	"sync"
	"time"

	"github.com/arleontr/goahc/pubsub"
	"github.com/nsqio/go-nsq"
)

const DefaultTCPAddr = "localhost:4150"

type PSMQ struct {
	sm           sync.Mutex
	sendChans    map[string]chan []byte
	rm           sync.Mutex
	receiveChans map[string]chan []byte
	errChan      chan error
	stopchan     chan struct{}
	stopProdChan chan struct{}
	consumers    []*nsq.Consumer
	producersWG  sync.WaitGroup

	NewProducer func() (*nsq.Producer, error)

	NewConsumer func(name string) (*nsq.Consumer, error)

	ConnectConsumer func(consumer *nsq.Consumer) error
}

func New() *PSMQ {
	return &PSMQ{}
}

func (t *PSMQ) Init() {
	t.sendChans = make(map[string]chan []byte)
	t.receiveChans = make(map[string]chan []byte)
	t.stopchan = make(chan struct{})
	t.stopProdChan = make(chan struct{})
	t.errChan = make(chan error, 10)
	t.consumers = []*nsq.Consumer{}
	t.NewProducer = func() (*nsq.Producer, error) {
		return nsq.NewProducer(DefaultTCPAddr, nsq.NewConfig())
	}
	t.NewConsumer = func(name string) (*nsq.Consumer, error) {
		return nsq.NewConsumer(name, "GOAHC", nsq.NewConfig())
	}
	t.ConnectConsumer = func(consumer *nsq.Consumer) error {
		return consumer.ConnectToNSQD(DefaultTCPAddr)
	}

}

func (t *PSMQ) ErrChan() <-chan error {
	return t.errChan
}

func (t *PSMQ) SubscribeToTopic(name string) <-chan []byte {
	t.rm.Lock()
	defer t.rm.Unlock()

	ch, ok := t.receiveChans[name]
	if ok {
		return ch
	}
	var err error
	if ch, err = t.makeConsumer(name); err != nil {

		t.errChan <- &pubsub.CommError{Name: name, CommErr: err}
		return make(chan []byte)
	}
	t.receiveChans[name] = ch
	return ch
}

func (t *PSMQ) makeConsumer(name string) (chan []byte, error) {
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

	err = pubsub.Backoff(1*time.Second, 10*time.Minute, 0, func() error {
		return t.ConnectConsumer(consumer)
	})
	if err != nil {
		return nil, err
	}
	t.consumers = append(t.consumers, consumer)
	return ch, nil
}

func (t *PSMQ) CreatePublishTopic(name string) chan<- []byte {
	t.sm.Lock()
	defer t.sm.Unlock()

	ch, ok := t.sendChans[name]
	if ok {
		return ch
	}
	var err error
	ch, err = t.makeProducer(name)
	if err != nil {
		t.errChan <- &pubsub.CommError{Name: name, CommErr: err}
		return make(chan []byte)
	}
	t.sendChans[name] = ch
	return ch
}

func (t *PSMQ) makeProducer(name string) (chan []byte, error) {
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
				err = pubsub.Backoff(1*time.Second, 10*time.Minute, 10, func() error {
					return producer.Publish(name, msg)
				})
				if err != nil {
					t.errChan <- &pubsub.CommError{Message: msg, Name: name, CommErr: err}
					continue
				}
			}
		}
	}()
	return ch, nil
}

func (t *PSMQ) Close() {

	close(t.stopProdChan)
	t.producersWG.Wait()

	for _, c := range t.consumers {
		c.Stop()
		<-c.StopChan
	}
	close(t.stopchan)
}

func (t *PSMQ) Done() chan struct{} {
	return t.stopchan
}
