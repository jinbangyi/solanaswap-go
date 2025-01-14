package bkafka

import (
	"time"
)

type IEvent interface {
	// eventID 应该是唯一且幂等的， 用于消息去重
	GetEventID() string
	// 事件类型
	GetEventType() string
	GetEventTime() time.Time
	/*
		订阅该事件的消费者信息
		注意：这里一个事件会被发给多个 topic，而不是一个 topic 被多个消费者订阅
		因为我们的事件主要是交易事件，具有时序性, 所以需要在不同的 topic 中顺序处理
	*/
	GetConsumers() []ConsumerTopicPartition
}

type EventBase struct {
	EventID   string    `json:"eventId"`
	EventType string    `json:"eventType"`
	EventTime time.Time `json:"eventTime"`
}

func (e EventBase) GetEventID() string {
	return e.EventID
}

func (e EventBase) GetEventType() string {
	return e.EventType
}

func (e EventBase) GetEventTime() time.Time {
	return e.EventTime
}

// event 对应的消费者信息
type ConsumerTopicPartition struct {
	Topic        string
	PartitionKey string
}

type EventStringWithConsumers struct {
	EventString string
	EventTime   time.Time
	Consumers   []ConsumerTopicPartition
}
