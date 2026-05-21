package queueringcluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

type ClientNatsProcessor interface {
	Process(ctx context.Context, qNum int, msg jetstream.Msg) error
}

type ClientNatsProcessorFactory interface {
	NewProcessor() ClientNatsProcessor
}

type NatsWorkerFactory struct {
	Stream           jetstream.Stream
	QueueName        string
	NodeID           int
	NumRetries       int
	ProcessorFactory ClientNatsProcessorFactory
}

func (n *NatsWorkerFactory) NewWorker(qNum int) Worker {
	w := NatsWorker{
		name:       fmt.Sprintf("node-%d-worker-%d", n.NodeID, qNum),
		stream:     n.Stream,
		numRetries: n.NumRetries,
		filter:     fmt.Sprintf("%s.%d", n.QueueName, qNum),
		proc:       n.ProcessorFactory.NewProcessor(),
		qNum:       qNum,
	}
	return &w
}

type NatsWorker struct {
	stream     jetstream.Stream
	name       string
	filter     string
	numRetries int
	proc       ClientNatsProcessor
	ctx        context.Context
	qNum       int
}

func (w *NatsWorker) Start(ctx context.Context, wg *sync.WaitGroup) error {
	var err error
	var cons jetstream.Consumer

	for i := 0; i < w.numRetries; i++ {
		cons, err = w.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:          w.name,
			FilterSubject: w.filter,
		})
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	consumeCtx, err := cons.Consume(w.process)
	if err != nil {
		return err
	}

	go func() {
	loop:
		for {
			select {
			case <-ctx.Done():
				consumeCtx.Stop()
				wg.Done()
				break loop
			case <-consumeCtx.Closed():
				// !TODO pull up message about stopped consume
				break loop
			}
		}
	}()

	return nil
}

func (w *NatsWorker) process(msg jetstream.Msg) {
	_ = w.proc.Process(w.ctx, w.qNum, msg) // TODO error handling
	err := msg.Ack()
	if err != nil {
		slog.Error("cant process ", "msg", msg)
		panic(err)
	}
}
