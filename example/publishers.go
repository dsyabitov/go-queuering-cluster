package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
)

type Publisher interface {
	Publish(ctx context.Context, qNum int, msg []byte) error
}

type NatsPublisher struct {
	js        jetstream.JetStream
	queueName string
}

func NewNatsPublisher(ctx context.Context, natsAddr string, queueName string) (*NatsPublisher, error) {
	// js, _, err := getStream(ctx, fmt.Sprintf("localhost:%d", conf.Nats))
	js, _, err := getStream(ctx, natsAddr)
	if err != nil {
		return nil, err
	}

	return &NatsPublisher{
		js:        js,
		queueName: queueName,
	}, err
}

func (n *NatsPublisher) Publish(ctx context.Context, qNum int, msg []byte) error {
	_, err := n.js.Publish(ctx, fmt.Sprintf("%s.%d", queueName, qNum), msg)
	return err
}

type RedisPublisher struct {
	rdb       *redis.Client
	queueName string
}

func NewRedisPublisher(redisAddr string, queueName string) *RedisPublisher {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &RedisPublisher{
		rdb:       rdb,
		queueName: queueName,
	}
}

func (r *RedisPublisher) Publish(ctx context.Context, qNum int, msg []byte) error {
	args := &redis.XAddArgs{
		Stream: fmt.Sprintf("%s.%d", r.queueName, qNum),
		MaxLen: 1000, //nolint:mnd //TODO
		Approx: true,
		Values: map[string]interface{}{
			"payload": string(msg),
		},
	}
	_, err := r.rdb.XAdd(ctx, args).Result()
	return err
}
