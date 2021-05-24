package broker

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asim/mq/go/client"
)

var (
	Default Broker = newBroker()
)

// 消息管理者内部实现，负责消息存储、分发、订阅。
type broker struct {
	exit    chan bool
	options *Options

	sync.RWMutex
	topics map[string][]chan []byte

	mtx       sync.RWMutex
	persisted map[string]bool
	counter   uint64
}

// internal message for persistence
type message struct {
	Timestamp int64  `json:"timestamp"`
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
}

// Broker is the message broker
type Broker interface {
	Close() error
	Publish(topic string, payload []byte) error
	Subscribe(topic string) (<-chan []byte, error)
	Unsubscribe(topic string, sub <-chan []byte) error
}

func newBroker(opts ...Option) *broker {
	options := new(Options)
	for _, o := range opts {
		o(options)
	}

	if options.Client == nil {
		options.Client = client.New()
	}

	return &broker{
		exit:      make(chan bool),
		options:   options,
		topics:    make(map[string][]chan []byte),
		persisted: make(map[string]bool),
	}
}

func (b *broker) publish(payload []byte, subscribers []chan []byte) {

	//当存在多个消息订阅者，只要被其中一个订阅者消费了消息，其他订阅者就不能再消费
	counter := atomic.AddUint64(&b.counter, 1)
	idx := counter % uint64(len(subscribers))

	select {
	// push the payload to subscriber
	case subscribers[idx] <- payload:
		// only wait 5 milliseconds for subscriber
	case <-time.After(time.Millisecond * 5):
	case <-b.exit:
		return
	}
}

func (b *broker) persist(topic string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.persisted[topic] {
		return nil
	}

	ch, err := b.Subscribe(topic)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(topic+".mq", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
	if err != nil {
		return err
	}

	go func() {
		var pending []byte
		newline := []byte{'\n'}
		t := time.NewTicker(time.Second)
		defer t.Stop()
		defer f.Close()

		for {
			select {
			case p := <-ch:
				b, err := json.Marshal(&message{
					Timestamp: time.Now().UnixNano(),
					Topic:     topic,
					Payload:   p,
				})
				if err != nil {
					continue
				}
				pending = append(pending, b...)
				pending = append(pending, newline...)
			case <-t.C:
				if len(pending) == 0 {
					continue
				}
				f.Write(pending)
				pending = nil
			case <-b.exit:
				return
			}
		}
	}()

	b.persisted[topic] = true

	return nil
}

func (b *broker) Close() error {
	select {
	case <-b.exit:
		return nil
	default:
		close(b.exit)
		b.Lock()
		b.topics = make(map[string][]chan []byte)
		b.Unlock()
		b.options.Client.Close()
	}
	return nil
}

func (b *broker) Publish(topic string, payload []byte) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	if b.options.Proxy {
		return b.options.Client.Publish(topic, payload)
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()
	if !ok {
		// persist?
		if !b.options.Persist {
			return nil
		}
		if err := b.persist(topic); err != nil {
			return err
		}
	}

	b.publish(payload, subscribers)
	return nil
}

func (b *broker) Subscribe(topic string) (<-chan []byte, error) {
	select {
	case <-b.exit:
		return nil, errors.New("broker closed")
	default:
	}

	if b.options.Proxy {
		return b.options.Client.Subscribe(topic)
	}

	ch := make(chan []byte, 100)
	b.Lock()
	b.topics[topic] = append(b.topics[topic], ch)
	b.Unlock()
	return ch, nil
}

func (b *broker) Unsubscribe(topic string, sub <-chan []byte) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	if b.options.Proxy {
		return b.options.Client.Unsubscribe(sub)
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()

	if !ok {
		return nil
	}

	var subs []chan []byte
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		subs = append(subs, subscriber)
	}

	b.Lock()
	b.topics[topic] = subs
	b.Unlock()
	return nil
}

func Publish(topic string, payload []byte) error {
	return Default.Publish(topic, payload)
}

func Subscribe(topic string) (<-chan []byte, error) {
	return Default.Subscribe(topic)
}

func Unsubscribe(topic string, sub <-chan []byte) error {
	return Default.Unsubscribe(topic, sub)
}

func New(opts ...Option) *broker {
	return newBroker(opts...)
}
