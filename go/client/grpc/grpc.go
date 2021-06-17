package client

import (
	"crypto/tls"
	"errors"
	"github.com/asim/mq/glogger"
	"sync"
	"time"

	"context"
	"github.com/asim/mq/go/client"
	"github.com/asim/mq/go/client/selector"
	"github.com/asim/mq/proto/grpc/mq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// internal grpcClient
type grpcClient struct {
	exit    chan bool
	options client.Options

	sync.RWMutex
	subscribers map[<-chan []byte]*subscriber
}

// internal subscriber
type subscriber struct {
	wg    sync.WaitGroup
	ch    chan<- []byte
	exit  chan bool
	topic string
}

func ping(addr string) (bool, error) {
	var dialOpts []grpc.DialOption
	var resp *mq.PingMessage
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))

	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	c := mq.NewMQClient(conn)
	resp, err = c.Ping(context.Background(), &mq.PingMessage{})
	if err != nil {
		return false, err
	}
	if resp.Timestamp == 0 {
		return false, errors.New("sever unavailable")
	}
	glogger.Infof("ping timestamp=%d", resp.Timestamp)
	return true, nil
}
func grpcPublish(addr, topic string, payload []byte) error {
	var dialOpts []grpc.DialOption

	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))

	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	c := mq.NewMQClient(conn)
	_, err = c.Pub(context.TODO(), &mq.PubRequest{
		Topic:   topic,
		Payload: payload,
	})

	return err
}

func grpcSubscribe(addr string, s *subscriber) error {
	var dialOpts []grpc.DialOption

	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))

	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return err
	}

	c := mq.NewMQClient(conn)
	sub, err := c.Sub(context.TODO(), &mq.SubRequest{
		Topic: s.topic,
	})
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-s.exit:
			conn.Close()
		}
	}()

	go func() {
		defer s.wg.Done()

		for {
			rsp, err := sub.Recv()
			if err != nil {
				conn.Close()
				return
			}

			select {
			case s.ch <- rsp.Payload:
			case <-s.exit:
				return
			}
		}
	}()

	return nil
}

func (c *grpcClient) run() {
	// is there a resolver?
	if c.options.Resolver == nil {
		return
	}

	t := time.NewTicker(time.Second * 30)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			var servers []string

			// iterate names
			for _, server := range c.options.Servers {
				ips, err := c.options.Resolver.Resolve(server)
				if err != nil {
					continue
				}
				servers = append(servers, ips...)
			}

			// only set if we have servers
			if len(servers) > 0 {
				c.options.Selector.Set(servers...)
			}
		case <-c.exit:
			return
		}
	}
}

func (c *grpcClient) Close() error {
	select {
	case <-c.exit:
		return nil
	default:
		close(c.exit)
		c.Lock()
		for _, sub := range c.subscribers {
			sub.Close()
		}
		c.Unlock()
	}
	return nil
}
func (c *grpcClient) Ping(topic string) (bool, error) {
	select {
	case <-c.exit:
		return false, errors.New("client closed")
	default:
	}
	servers, err := c.options.Selector.Get(topic)
	if err != nil {
		return false, err
	}
	var grr error
	var pong bool
	for _, addr := range servers {
		for i := 0; i < 1+c.options.Retries; i++ {
			pong, grr = ping(addr)
			if pong {
				break
			}
		}
	}
	return pong, grr
}
func (c *grpcClient) Publish(weight uint64, topic string, payload []byte) error {
	select {
	case <-c.exit:
		return errors.New("client closed")
	default:
	}

	servers, err := c.options.Selector.GetEx(weight)
	if err != nil {
		return err
	}

	var grr error
	for _, addr := range servers {
		for i := 0; i < 1+c.options.Retries; i++ {
			err := grpcPublish(addr, topic, payload)
			if err == nil {
				break
			}
			grr = err
		}
	}
	return grr
}

func (c *grpcClient) Subscribe(topic string) (<-chan []byte, error) {
	select {
	case <-c.exit:
		return nil, errors.New("client closed")
	default:
	}

	servers, err := c.options.Selector.Get(topic)
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, len(c.options.Servers)*256)

	s := &subscriber{
		ch:    ch,
		exit:  make(chan bool),
		topic: topic,
	}

	var grr error
	for _, addr := range servers {
		for i := 0; i < 1+c.options.Retries; i++ {
			err := grpcSubscribe(addr, s)
			if err == nil {
				s.wg.Add(1)
				break
			}
			grr = err
		}
	}

	return ch, grr
}

func (c *grpcClient) Unsubscribe(ch <-chan []byte) error {
	select {
	case <-c.exit:
		return errors.New("client closed")
	default:
	}

	c.Lock()
	defer c.Unlock()
	if sub, ok := c.subscribers[ch]; ok {
		return sub.Close()
	}
	return nil
}

func (s *subscriber) Close() error {
	select {
	case <-s.exit:
	default:
		close(s.exit)
		s.wg.Wait()
	}
	return nil
}

// New returns a grpc Client
func New(opts ...client.Option) *grpcClient {
	options := client.Options{
		Selector: new(selector.All),
		Servers:  client.Servers,
		Retries:  client.Retries,
	}

	for _, o := range opts {
		o(&options)
	}

	// set servers
	options.Selector.Set(options.Servers...)

	c := &grpcClient{
		exit:        make(chan bool),
		options:     options,
		subscribers: make(map[<-chan []byte]*subscriber),
	}
	go c.run()
	return c
}
