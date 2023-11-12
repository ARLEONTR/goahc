package redis

import (
	"sync"
	"time"

	"github.com/arleontr/goahc/pubsub"
	"github.com/go-redis/redis"
)

type Options struct {
	Client *redis.Client
}

type Option func(*Options)

func WithClient(c *redis.Client) Option {
	return func(o *Options) {
		o.Client = c
	}
}

type PSMQ struct {
	sendChans    map[string]chan []byte
	receiveChans map[string]chan []byte

	sync.Mutex
	wg sync.WaitGroup

	errChan     chan error
	stopchan    chan struct{}
	stopPubChan chan struct{}
	stopSubChan chan struct{}
	opts        []Option
	client      *redis.Client
}

// New returns a new Channel
func New(opts ...Option) *PSMQ {
	conv := &PSMQ{}
	conv.opts = opts
	return conv
}
func (t *PSMQ) Init() {
	var options Options
	for _, o := range t.opts {
		o(&options)
	}
	t.sendChans = make(map[string]chan []byte)
	t.receiveChans = make(map[string]chan []byte)
	t.errChan = make(chan error, 10)
	t.stopchan = make(chan struct{})
	t.stopPubChan = make(chan struct{})
	t.stopSubChan = make(chan struct{})
	t.client = options.Client
}

func (t *PSMQ) newConnection() (*redis.Client, error) {
	var err error
	if t.client != nil {
		return t.client, nil
	}

	t.client = redis.NewClient(&redis.Options{
		Network:    "tcp",
		Addr:       "127.0.0.1:6379",
		Password:   "",
		DB:         0,
		MaxRetries: 0,
	})

	_, err = t.client.Ping().Result()
	return t.client, err
}

func (t *PSMQ) SubscribeToTopic(name string) <-chan []byte {
	t.Lock()
	defer t.Unlock()

	ch, ok := t.receiveChans[name]
	if ok {
		return ch
	}

	ch, err := t.makeSubscriber(name)
	if err != nil {
		t.errChan <- &pubsub.CommError{Name: name, CommErr: err}
		return make(chan []byte)
	}

	t.receiveChans[name] = ch
	return ch
}

func (t *PSMQ) makeSubscriber(name string) (chan []byte, error) {
	c, err := t.newConnection()
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 1024)
	go func() {
		for {
			data, err := c.BRPop(0*time.Second, name).Result()
			if err != nil {
				select {
				case <-t.stopSubChan:
					return
				default:
					t.errChan <- &pubsub.CommError{Name: name, CommErr: err}
					continue
				}
			}

			ch <- []byte(data[len(data)-1])
		}
	}()
	return ch, nil
}

func (t *PSMQ) CreatePublishTopic(name string) chan<- []byte {
	t.Lock()
	defer t.Unlock()

	ch, ok := t.sendChans[name]
	if ok {
		return ch
	}

	ch, err := t.makePublisher(name)
	if err != nil {
		t.errChan <- &pubsub.CommError{Name: name, CommErr: err}
		return make(chan []byte)
	}
	t.sendChans[name] = ch
	return ch
}

func (t *PSMQ) makePublisher(name string) (chan []byte, error) {
	c, err := t.newConnection()
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 1024)
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			select {
			case <-t.stopPubChan:
				if len(ch) != 0 {
					_, err := t.client.Ping().Result()
					if err == nil {
						continue
					}
				}
				return
			case msg := <-ch:
				err := c.RPush(name, string(msg)).Err()
				if err != nil {
					t.errChan <- &pubsub.CommError{Message: msg, Name: name, CommErr: err}
				}
			}
		}
	}()
	return ch, nil
}

func (t *PSMQ) ErrChan() chan error {
	return t.errChan
}

func (t *PSMQ) Close() {
	close(t.stopSubChan)
	close(t.stopPubChan)
	t.wg.Wait()
	t.client.Close()
	close(t.stopchan)
}

func (t *PSMQ) Done() chan struct{} {
	return t.stopchan
}
