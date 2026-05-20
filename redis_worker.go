package hashringcluster

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type ClientRedisProcessor interface {
	Process(ctx context.Context, rdb *redis.Client, qNum int, msg *redis.XMessage)
}

type ClientRedisProcessorFactory interface {
	NewClientRedisProcessor() ClientRedisProcessor
}

type RedisWorkerFactory struct {
	RedisAddr string
	QueueName string
	NodeID    int
}

func (r *RedisWorkerFactory) NewWorker(qNum int) Worker {
	w := RedisWorker{
		name:       fmt.Sprintf("node-%d-worker-%d", r.NodeID, qNum),
		redisAddr:  r.RedisAddr,
		numRetries: numRetries,
		stream:     fmt.Sprintf("%s.%d", r.QueueName, qNum),
		qNum:       qNum,
	}
	return &w
}

type RedisWorker struct {
	redisAddr   string
	name        string
	stream      string
	qNum        int
	numRetries  int
	procFactory ClientRedisProcessorFactory
}

func (w *RedisWorker) Start(ctx context.Context, wg *sync.WaitGroup) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:               w.redisAddr,
		DialerRetries:      5,                      //nolint:mnd //temp
		DialerRetryTimeout: 100 * time.Millisecond, //nolint:mnd //temp // used when DialerRetryBackoff is nil
		// Optional: exponential backoff with jitter and a cap.
		//		DialerRetryBackoff: redis.DialRetryBackoffExponential(100*time.Millisecond, 2*time.Second),
	})

	err := rdb.XGroupCreateMkStream(ctx, w.stream, fmt.Sprintf("%s.group", w.stream), "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	go w.readMessages(ctx, rdb, wg)

	return nil
}

func (w *RedisWorker) readMessages(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer func() {
		_ = rdb.Close()
		wg.Done()
	}()

	clientProc := w.procFactory.NewClientRedisProcessor()

	for {
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    fmt.Sprintf("%s.group", w.stream),
			Consumer: fmt.Sprintf("%s.consumer", w.stream),
			Streams:  []string{w.stream, ">"},
			Count:    0,
			Block:    5 * time.Second, //nolint:mnd //tmp
			NoAck:    false,
			Claim:    0,
		}).Result()
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				break
			}
			if strings.Contains(err.Error(), "redis: nil") {
				slog.Info("Empty", "stream", w.stream)
				continue
			}
			panic(err)
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				clientProc.Process(ctx, rdb, w.qNum, &msg)
				rdb.XAck(ctx, "test", "test.group", msg.ID)
			}
		}
	}
}
