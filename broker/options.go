package broker

import (
	"github.com/asim/mq/go/client"
	"time"
)

type Options struct {
	Client          client.Client
	Proxy           bool
	Persist         bool
	ETCDAddresses   []string
	ETCDDialTimeout time.Duration
}

type Option func(o *Options)

func Client(c client.Client) Option {
	return func(o *Options) {
		o.Client = c
	}
}

func Proxy(b bool) Option {
	return func(o *Options) {
		o.Proxy = b
	}
}

func Persist(b bool) Option {
	return func(o *Options) {
		o.Persist = b
	}
}

func ETCDAddresses(addrs []string) Option {
	return func(o *Options) {
		o.ETCDAddresses = addrs
	}
}

func ETCDDialTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ETCDDialTimeout = timeout
	}
}
