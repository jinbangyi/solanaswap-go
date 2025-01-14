package bkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jinbangyi/solanaswap-go/pkg/log"
	"github.com/jinbangyi/solanaswap-go/pkg/task"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type IKafkaWriter interface {
	WriteMessages(ctx context.Context, msgs []any) error
	CloseConn(ctx context.Context) error
}

type KafkaWriter struct {
	writer *kafka.Writer
}

func NewKafkaWriterWithBrokers(topic string, brokers []string) IKafkaWriter {
	return &KafkaWriter{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (k KafkaWriter) WriteMessages(ctx context.Context, msgs []any) error {
	messages := make([]kafka.Message, 0)

	for _, msg := range msgs {
		msgJson, err := json.Marshal(msg)
		if err != nil {
			log.Warn("---> Construct Message error:", zap.Error(err))
			return err
		}
		messages = append(messages, kafka.Message{
			Value: msgJson,
		})
	}

	if err := k.writer.WriteMessages(ctx, messages...); err != nil {
		log.Warn("---> Write Message error:", zap.Error(err))
		return err
	}
	return nil
}

func (k KafkaWriter) CloseConn(ctx context.Context) error {
	return k.writer.Close()
}

type IKafkaConsumer interface {
	// KafkaReaderConfig 配置 topic groupId brokers 等
	KafkaReaderConfig() kafka.ReaderConfig
	// ConsumeMsg 处理消息的业务逻辑
	ConsumeMsg(ctx context.Context, msgValue []byte) error
}

func NewDaemonTaskWithKafkaConsumer(ctx context.Context, wg *sync.WaitGroup, consumer IKafkaConsumer) *task.Daemon {
	var (
		daemonConfig = task.DaemonConfig{ErrorRetryAfter: time.Minute}
		TaskName     = fmt.Sprintf("KafkaConsumer:%s:%s", consumer.KafkaReaderConfig().Topic, consumer.KafkaReaderConfig().GroupID)
	)

	return task.NewDaemon(ctx, TaskName, func(ctx context.Context) error {
		var (
			r = kafka.NewReader(consumer.KafkaReaderConfig())
		)

		defer func() {
			if err := r.Close(); err != nil {
				log.Error(fmt.Sprintf("[%s] ---> CloseReaderErr", TaskName), zap.Error(err))
			} else {
				log.Info(fmt.Sprintf("[%s] ---> CloseReader", TaskName))
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				m, err := r.FetchMessage(ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Error(fmt.Sprintf("[%s] ---> FetchMessageErr", TaskName), zap.Error(err))
					}
					time.Sleep(1 * time.Second)
					continue
				}

				startTime := time.Now().UnixMilli()
				if err = consumer.ConsumeMsg(ctx, m.Value); err != nil {
					log.Error(fmt.Sprintf("[%s] ---> ConsumeMessageErr", TaskName), zap.Error(err), zap.ByteString("message", m.Value))
					continue
				} else {
					log.Info(fmt.Sprintf("[%s] ---> ConsumeMessageSuccess", TaskName), zap.Int64("cost", time.Now().UnixMilli()-startTime), zap.Int64("offset", m.Offset), zap.Int("partition", m.Partition))
				}

				if err := r.CommitMessages(ctx, m); err != nil {
					log.Error(fmt.Sprintf("[%s] ---> CommitMessageErr", TaskName), zap.Error(err), zap.ByteString("message", m.Value))
					continue
				}
			}
		}
	}, wg, daemonConfig)
}
