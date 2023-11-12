package goch

type PSMQ struct {
	errChan   chan error
	receiveCh <-chan []byte
	//sendCh       chan<- []byte
	sendChs     map[string]chan<- []byte
	stopchan    chan struct{}
	pubsubagent *PubSubAgent
}

func New() *PSMQ {
	return &PSMQ{}
}

func (c *PSMQ) Init() {
	c.errChan = make(chan error, 10)
	c.receiveCh = make(<-chan []byte)
	c.sendChs = make(map[string]chan<- []byte)
	c.stopchan = make(chan struct{})
	c.pubsubagent = NewPubSubAgent() //singleton
}

func (c *PSMQ) ErrChan() <-chan error {
	return c.errChan
}

func (c *PSMQ) SubscribeToTopic(name string) <-chan []byte {
	ch := c.pubsubagent.Subscribe(name)
	return ch
}

func (c *PSMQ) CreatePublishTopic(name string) chan<- []byte {
	ch := make(chan []byte)
	c.sendChs[name] = ch
	go func(c *PSMQ, name string, ch chan []byte) {
		for {
			msg := <-ch
			c.pubsubagent.Publish(name, msg)
		}
	}(c, name, ch)
	return ch //we do not have to do anything here this is a dummy channel
}

func (c *PSMQ) Close() {
	// TODO: Do we have to use mutual exclusion for in-flight msgs?
	close(c.stopchan)
}

func (c *PSMQ) Done() chan struct{} {
	return c.stopchan
}
