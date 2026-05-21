package main

import (
	"context"

	queueringcluster "github.com/dsyabitov/go-queuering-cluster"

	"github.com/redis/go-redis/v9"
)

type ExampleRedisProcessorFactory struct{}

func (r *ExampleRedisProcessorFactory) NewProcessor() queueringcluster.ClientRedisProcessor {
	return &ExampleRedisProcessor{}
}

type ExampleRedisProcessor struct{}

func (e *ExampleRedisProcessor) Process(ctx context.Context, qNum int, msg *redis.XMessage) error {
	return nil
}
