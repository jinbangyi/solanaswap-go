package bkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/jinbangyi/solanaswap-go/pkg/log"
	"github.com/segmentio/kafka-go"
)

type ProducerOption func(*Producer)

// WithAutoCreateTopic 设置是否自动创建 topic
func WithAutoCreateTopic(autoCreate bool) func(*Producer) {
	return func(s *Producer) {
		s.AllowAutoTopicCreation = autoCreate
	}
}

// WithPartitionCount 设置创建 topic 的分区数
func WithPartitionCount(count int) func(*Producer) {
	return func(s *Producer) {
		s.numPartitions = count
	}
}

// WithReplicationFactor 设置创建 topic 的副本数
func WithReplicationFactor(replicationFactor int) func(*Producer) {
	return func(s *Producer) {
		s.replicationFactor = replicationFactor
	}
}

func WithBatchSize(batchSize int) func(*Producer) {
	return func(s *Producer) {
		s.BatchSize = batchSize
	}
}

func WithBatchTimeout(batchTimeout time.Duration) func(*Producer) {
	return func(s *Producer) {
		s.BatchTimeout = batchTimeout
	}
}

func WithRequiredAcks(requiredAcks kafka.RequiredAcks) func(*Producer) {
	return func(s *Producer) {
		s.RequiredAcks = requiredAcks
	}
}

func WithWriteBackoffMin(writeBackoffMin time.Duration) func(*Producer) {
	return func(s *Producer) {
		s.WriteBackoffMin = writeBackoffMin
	}
}

func WithWriteBackoffMax(writeBackoffMax time.Duration) func(*Producer) {
	return func(s *Producer) {
		s.WriteBackoffMax = writeBackoffMax
	}
}

type ProducerI interface {
	WriteMessages(ctx context.Context, msgs [][]byte) error
	WriteMessage(ctx context.Context, msgs []byte) error
	// the producer will cache the message to speed up the write speed
	FlushEvent(ctx context.Context) error

	CloseConn(ctx context.Context) error
}

type Producer struct {
	*kafka.Writer

	numPartitions     int
	replicationFactor int

	// trade event cache
	cacheMessage [][]byte
	// cache limit
	cacheLimit int
	cacheTimeout time.Duration
	lastSendTime time.Time
}

func NewProducer(topic string, brokers []string, optFuncs ...ProducerOption) *Producer {
	producer := &Producer{
		Writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
			// WriteBackoffMin: 500 * time.Millisecond,
			// WriteBackoffMax: 5 * time.Second,
			// BatchSize:       1000,
			BatchTimeout:           10 * time.Second,
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
		},

		numPartitions:     1,
		replicationFactor: 1,
		cacheMessage:      make([][]byte, 500),
		cacheLimit:        500,
		cacheTimeout: 	2 * time.Second,
		lastSendTime: time.Now(),
	}

	// apply optional func
	for _, o := range optFuncs {
		o(producer)
	}

	// 自动创建 topic
	if producer.AllowAutoTopicCreation {
		if err := producer.createTopic(); err != nil {
			panic(err)
		}
	}
	return producer
}

func (p *Producer) FlushEvent(ctx context.Context) error {
	if len(p.cacheMessage) == 0 {
		return nil
	}

	messages := make([]kafka.Message, len(p.cacheMessage))

	for _, msg := range p.cacheMessage {
		// msgJson, err := json.Marshal(msg)
		// if err != nil {
		// 	return fmt.Errorf("---> Construct Message error: %w", err)
		// }
		messages = append(messages, kafka.Message{
			// TODO add key
			Value: msg,
		})
	}

	if err := p.Writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("---> Write Message error: %w", err)
	}

	p.cacheMessage = make([][]byte, p.cacheLimit)
	p.lastSendTime = time.Now()
	return nil
}

// msg can be any type, but must be able to marshal to json
func (p *Producer) WriteMessages(ctx context.Context, msgs [][]byte) error {
	for _, msg := range msgs {
		err := p.WriteMessage(ctx, msg)
		if err != nil {
			return fmt.Errorf("failed to flush event: %w", err)
		}
	}

	return nil
}

func (p *Producer) WriteMessage(ctx context.Context, msg []byte) error {
	log.Debug("cache message")
	p.cacheMessage = append(p.cacheMessage, msg)

	// TODO using internal buffer to speed up the write speed, should readd the task to tracker
	// TODO fix timeout
	if len(p.cacheMessage) >= p.cacheLimit || time.Since(p.lastSendTime) > p.cacheTimeout {
		if err := p.FlushEvent(ctx); err != nil {
			return fmt.Errorf("failed to flush event: %w", err)
		}
		log.Debug("flush message")
	}
	return nil
}

func (p *Producer) CloseConn(ctx context.Context) error {
	err := p.FlushEvent(ctx)
	if err != nil {
		return fmt.Errorf("close Producer failed, failed to flush event: %w", err)
	}
	return p.Writer.Close()
}

func (p *Producer) createTopic() error {
	conn, err := kafka.Dial(p.Addr.Network(), p.Addr.String())
	if err != nil {
		return fmt.Errorf("kafka dial: %w", err)
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             p.Topic,
		NumPartitions:     p.numPartitions,
		ReplicationFactor: p.replicationFactor,
	}
	err = conn.CreateTopics(topicConfig)
	if err != nil {
		return fmt.Errorf("kafka create topic: %w", err)
	}
	return nil
}
