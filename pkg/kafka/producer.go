package bkafka

import (
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer 生产者实例
type Producer struct {
	*kafka.Writer
	brokers           []string
	topic             string
	autoCreateTopic   bool
	numPartitions     int
	replicationFactor int
}

type ProducerOption func(*Producer)

// WithAutoCreateTopic 设置是否自动创建 topic
func WithAutoCreateTopic(autoCreate bool) func(*Producer) {
	return func(s *Producer) {
		s.autoCreateTopic = autoCreate
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

// WithBalancer 分区方法， 默认为 kafka.LeastBytes， 最好是用 hash key 进行分区
func WithBalancer(balancer kafka.Balancer) func(*Producer) {
	return func(s *Producer) {
		s.Writer.Balancer = balancer
	}
}

// NewProducer 创建一个指定 topic 的生产者，失败会 panic
func NewProducerMust(brokers []string, topic string, optFuncs ...ProducerOption) *Producer {
	var p = &Producer{
		brokers:           brokers,
		topic:             topic,
		autoCreateTopic:   false,
		numPartitions:     1,
		replicationFactor: 1,
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			BatchSize:    1000,
			BatchTimeout: 1 * time.Second,
			RequiredAcks: kafka.RequireOne,
		},
	}
	// apply optional func
	for _, o := range optFuncs {
		o(p)
	}

	// 自动创建 topic
	if p.autoCreateTopic {
		if err := p.createTopic(); err != nil {
			panic(err)
		}
	}
	return p
}

// 创建 producer topic
func (p *Producer) createTopic() error {
	conn, err := kafka.Dial("tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("kafka dial: %w", err)
	}
	defer conn.Close()
	topicConfig := kafka.TopicConfig{
		Topic:             p.topic,
		NumPartitions:     p.numPartitions,
		ReplicationFactor: p.replicationFactor,
	}
	err = conn.CreateTopics(topicConfig)
	if err != nil {
		return fmt.Errorf("kafka create topic: %w", err)
	}
	return nil
}
