package handler

import (
	"context"
	"fmt"

	"github.com/jinbangyi/solanaswap-go/config"
	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/btype"
	"github.com/jinbangyi/solanaswap-go/pkg/bkafka"
)

const (
	SolanaTradeTopic = "solana-trade"
)

type HandlerI interface {
	// return the handler instance id
	String() string
	// handle the trade
	WriteTrade(context context.Context, trade *btype.Trade) error
	Close(context context.Context) error
}

type KafkaHandler struct {
	producer *bkafka.Producer

	// handler instance name
	name string
}

func NewKafkaHandler(name string) *KafkaHandler {
	return &KafkaHandler{
		name:     name,
		producer: bkafka.NewProducer(SolanaTradeTopic, config.KAFKA_BROKERS),
	}
}

func (kh *KafkaHandler) String() string {
	return fmt.Sprintf("kafkaHandler: %s", kh.name)
}

func (kh *KafkaHandler) WriteTrade(context context.Context, trade *btype.Trade) error {
	return kh.producer.WriteMessage(context, trade)
}

func (kh *KafkaHandler) Close(context context.Context) error {
	return kh.producer.Close()
}
