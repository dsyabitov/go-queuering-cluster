package hashringcluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NatsWorkerFactory struct {
	stream    jetstream.Stream
	QueueName string
	NodeID    int
}

func (n *NatsWorkerFactory) NewWorker(qNum int) Worker {
	w := NatsWorker{
		name:       fmt.Sprintf("node-%d-worker-%d", n.NodeID, qNum),
		stream:     n.stream,
		numRetries: numRetries,
		filter:     fmt.Sprintf("%s.%d", n.QueueName, qNum),
	}
	return &w
}

type NatsWorker struct {
	stream     jetstream.Stream
	name       string
	filter     string
	numRetries int
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
	err := msg.Ack()
	if err != nil {
		slog.Error("cant process ", "msg", msg)
		// slog.Error("error to acknowlage nats", "err", err, "worker", w.name, "qNum", w.qNum, "data", string(msg.Data()))
	}
}

func getStream(ctx context.Context, url string) (jetstream.JetStream, jetstream.Stream, error) {
	// !TODO disconnect if error
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, nil, err
	}

	stream, err := js.Stream(ctx, "SUBJECTS")
	if err != nil {
		return nil, nil, err
	}
	return js, stream, nil
}
