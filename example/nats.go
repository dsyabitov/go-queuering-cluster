package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsWaitTime = 2 * time.Second
)

func startNats(ctx context.Context, conf *NatsConf) {
	opts := &server.Options{
		Port:      int(conf.Port),
		JetStream: true,
		Debug:     true,
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		panic(err)
	}
	go ns.Start()

	if !ns.ReadyForConnections(natsWaitTime) {
		panic("not ready for connection")
	}

	slog.Info("started NATS server")

	stream := createNatsStream(ns.ClientURL())

loop:
	for {
		select {
		case <-time.After(15 * time.Second): //nolint:mnd // TODO
			printStreamStats(ctx, stream, int(conf.Queues))
		case <-ctx.Done():
			break loop
		}
	}
	<-ctx.Done()
	ns.LameDuckShutdown()
}

func createNatsStream(url string) jetstream.Stream {
	nc, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	cfg := jetstream.StreamConfig{
		Name:      "SUBJECTS",
		Retention: jetstream.WorkQueuePolicy,
		Subjects:  []string{"subjects.>"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		panic(err)
	}
	slog.Info("created the stream")
	return res
}

func printStreamStats(ctx context.Context, stream jetstream.Stream, queuesNum int) {
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter("subjects.>"))
	if err != nil {
		slog.Error("error getting stream info", "err", err)
		return
	}
	fmt.Printf("Consuments count: %d\n", info.State.Consumers)

	for i := 0; i < queuesNum; i++ {
		fmt.Printf(" |%4.d:%4.d", i, info.State.Subjects[fmt.Sprintf("subjects.%d", i)])
		if i%10 == 0 {
			fmt.Print("\n")
		}
	}
	fmt.Print("\n")
}
