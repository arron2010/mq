package stream

import (
	"context"
	"github.com/asim/mq/broker"
	"github.com/asim/mq/common"
	"github.com/asim/mq/config"
	"github.com/xp/shorttext-db/network"
	"strings"
)

func NewMQProcessor() *mqProcessor {
	p := &mqProcessor{}
	return p
}

type Stream struct {
	id int
}

func NewStream(id int) *Stream {
	s := &Stream{}
	s.id = id
	return s
}
func (s *Stream) newStreamServer() (*network.StreamServer, error) {
	cfg := config.GetConfig()
	peers := strings.Split(cfg.Peers, ",")
	return network.NewStreamServer(s.id, NewMQProcessor(), peers...)
}

func (s *Stream) Run() error {
	inner, err := s.newStreamServer()
	inner.SyncStart()
	return err
}

type mqProcessor struct {
}

func (p *mqProcessor) Process(ctx context.Context, m network.Message) error {
	switch m.Type {
	case common.OP_PUB:
		b := m.Data
		topic := m.Key
		if err := broker.Publish(topic, b); err != nil {
			return err
		}
	}

	return nil
}

func (p *mqProcessor) ReportUnreachable(id uint64) {

}
