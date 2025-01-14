package bkafka

import (
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/jinbangyi/solanaswap-go/pkg/goroutine"
	"github.com/jinbangyi/solanaswap-go/pkg/log"
)

var producers = map[string]*Producer{}

func AddProducer(producer *Producer) {
	producers[producer.topic] = producer
}

// Send 发送事件到消息队列
func Send(events ...IEvent) error {
	if len(events) == 0 {
		return nil
	}
	// 聚合发送到同一topic的消息
	var topicMsgs = make(map[string][]kafka.Message)
	for _, e := range events {
		data, err := jsoniter.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		for _, eventTopicPartition := range e.GetConsumers() {
			msg := kafka.Message{Key: []byte(eventTopicPartition.PartitionKey), Value: data}
			msgs, ok := topicMsgs[eventTopicPartition.Topic]
			if !ok {
				msgs = []kafka.Message{msg}
			} else {
				msgs = append(msgs, msg)
			}
			topicMsgs[eventTopicPartition.Topic] = msgs
		}
	}

	// 并发发送到不同 topic
	ctx, cancelFunc := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancelFunc()
	var wg errgroup.Group
	for topic, msgs := range topicMsgs {
		kafkaMsgs := msgs
		kafkaTopic := topic
		wg.Go(func() error {
			err := producers[kafkaTopic].WriteMessages(ctx, kafkaMsgs...)
			if err != nil {
				log.Error("failed to send kafka msg", zap.Error(err))
			}
			return err
		})
	}
	return wg.Wait()
}

// SendEventString 发送字符串事件到消息队列
func SendEventString(events ...*EventStringWithConsumers) error {
	if len(events) == 0 {
		return nil
	}
	// 聚合发送到同一topic的消息
	var topicMsgs = make(map[string][]kafka.Message)
	for _, e := range events {
		for _, eventTopicPartition := range e.Consumers {
			msg := kafka.Message{Key: []byte(eventTopicPartition.PartitionKey), Value: []byte(e.EventString)}
			msgs, ok := topicMsgs[eventTopicPartition.Topic]
			if !ok {
				msgs = []kafka.Message{msg}
			} else {
				msgs = append(msgs, msg)
			}
			topicMsgs[eventTopicPartition.Topic] = msgs
		}
	}

	// 并发发送到不同 topic
	ctx, cancelFunc := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancelFunc()
	var wg errgroup.Group
	for topic, msgs := range topicMsgs {
		kafkaMsgs := msgs
		kafkaTopic := topic
		wg.Go(func() error {
			err := producers[kafkaTopic].WriteMessages(ctx, kafkaMsgs...)
			if err != nil {
				log.Error("failed to send kafka msg", zap.Error(err))
			}
			return err
		})
	}
	return wg.Wait()
}

// 异步发送消息
func AsyncSend(events ...IEvent) {
	if len(events) == 0 {
		return
	}
	goroutine.Go(func() {
		if err := Send(events...); err != nil {
			log.Error("failed to send kafka msg", zap.Error(err), zap.Any("events", events))
		}
	})
}
