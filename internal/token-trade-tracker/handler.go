package tokentradetracker

import (
	"context"
	"fmt"
	"time"

	bkafka "github.com/jinbangyi/solanaswap-go/pkg/kafka"
	"github.com/jinbangyi/solanaswap-go/pkg/log"
	"go.uber.org/zap"
)

// TODO when the process is killed, there may be some trade not written to kafka
type HandlerI interface {
	// start new tracker and bind to handler
	Start() error
	String() string
	WriteTrade(context context.Context, tradeChannel <-chan *Trade) error
}

type KafkaHandler struct {
	name string

	cacheEvent []bkafka.IEvent
	cacheLimit int
}

func NewKafkaHandler(name string) *KafkaHandler {
	return &KafkaHandler{
		name:       name,
		cacheEvent: make([]bkafka.IEvent, 500),
		cacheLimit: 100,
	}
}

func (kh *KafkaHandler) String() string {
	return fmt.Sprintf("kafkaHandler: %s", kh.name)
}

func (kh *KafkaHandler) Start() error {
	return nil
}

func (kh *KafkaHandler) flushEvent() error {
	if len(kh.cacheEvent) == 0 {
		return nil
	}

	err := bkafka.Send(kh.cacheEvent...)
	if err != nil {
		return err
	}

	kh.cacheEvent = make([]bkafka.IEvent, kh.cacheLimit)
	return nil
}

func (kh *KafkaHandler) WriteTrade(context context.Context, tradeChannel <-chan *Trade) error {
	// read batch size from trade, if reach batch size or timeout, write to kafka
	var sleepTime int = 30
	ticker := time.NewTicker(time.Duration(sleepTime) * time.Second)

out:
	for {
		select {
		case <-context.Done():
			log.Info("context done, flush to kafka and exist the handler", zap.String("Handler", kh.String()))
			err := kh.flushEvent()
			log.Error("failed to flush event", zap.Error(err))
			break out
		case <-ticker.C:
			log.Warn("no new trade found, flush to kafka", zap.String("Handler", kh.String()))
			err := kh.flushEvent()
			log.Error("failed to flush event", zap.Error(err))
		case t := <-tradeChannel:
			if len(kh.cacheEvent) >= kh.cacheLimit {
				err := kh.flushEvent()
				log.Error("failed to flush event", zap.Error(err))
			}
			// cache the event
			kh.cacheEvent = append(kh.cacheEvent, TradeEvent{
				Trade: t,
			})
		}
	}

	ticker.Stop()
	return nil
}
