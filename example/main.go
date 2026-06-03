package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/dsyabitov/go-queuering"
	queueringcluster "github.com/dsyabitov/go-queuering-cluster"
	"github.com/hashicorp/memberlist"
	"github.com/redis/go-redis/v9"
)

const (
	produceTimeout = 1 * time.Microsecond // Добавить в параметры запуска.
	queueName      = "subjects"
	numRetries     = 3 // TODO реализовать поддержку.
)

type NatsConf struct {
	Port   uint16 `short:"p" help:"Listen port for NATS server."`
	Queues uint16 `short:"q" help:"Number of queues monitored."`
}

type ProducerConf struct {
	RedisHost string `help:"Redis server host"`
	Nats      uint16 `short:"n" help:"Port on wich NATS server is listening."`
	RedisPort uint16 `help:"Port on wich Redis server is listening."`
	Queues    uint16 `short:"q" help:"Number of queues in message broker."`
}

type RedisConf struct {
	RedisHost string `short:"h" help:"Redis server host"`
	RedisPort uint16 `short:"p" help:"Port on wich Redis server is listening."`
	Queues    uint16 `short:"q" help:"Number of queues in message broker."`
}

type SubscriberConf struct {
	RedisHost string   `help:"Redis server host"`
	Cluster   []uint16 `short:"c" help:"Other nodes ports."`
	Nats      uint16   `short:"n" help:"Port on wich NATS server is listening."`
	RedisPort uint16   `help:"Port on wich Redis server is listening."`
	Port      uint16   `short:"p" help:"Port on wich subscriber node will listening."`
	Queues    uint16   `short:"q" help:"Number of queues in NATS."`
	Bootstrap bool     `short:"b" help:"If true - subscriber will start process jobs before join the cluster."`
}

type CmdArgs struct { //nolint:govet //fieldalignment - ignoring here to show help in properly order
	Nats       NatsConf       `cmd:"" help:"Start NATS server."`
	Producer   ProducerConf   `cmd:"" help:"Start tasks producer, wich will generate tasks for subscribers."`
	Subscriber SubscriberConf `cmd:"" help:"Start distributed subscriber node."`
	Redis      RedisConf      `cmd:"" help:"Start redis queues monitoring."`
}

func main() {
	configureLogger()
	args := CmdArgs{}
	cmd := kong.Parse(&args)

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)
	defer stop()

	wg := sync.WaitGroup{}

	switch cmd.Command() {
	case "nats":
		wg.Go(func() { startNats(ctx, &args.Nats) })
	case "producer":
		wg.Go(func() { startProducer(ctx, &args.Producer) })
	case "subscriber":
		wg.Go(func() { startSubscriber(ctx, &args.Subscriber) })
	case "redis":
		wg.Go(func() { startRedisMonitoring(ctx, &args.Redis) })
	default:
		_ = cmd.PrintUsage(true)
	}

	<-ctx.Done()
	wg.Wait()
}

func configureLogger() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))
}

func startProducer(ctx context.Context, conf *ProducerConf) {
	if conf.Nats > 0 && (conf.RedisHost != "" || conf.RedisPort > 0) {
		panic("only nats or redis parameters, not both")
	}

	gen := DataGen{
		100,
		20000,
		10.0,
		100.0,
	}

	mapper, err := queuering.NewQueueMapper(int(conf.Queues))
	if err != nil {
		panic(err)
	}

	var pub Publisher

	if conf.Nats > 0 {
		pub, err = NewNatsPublisher(ctx, fmt.Sprintf("localhost:%d", conf.Nats), queueName)
		if err != nil {
			panic(err)
		}
	} else {
		pub = NewRedisPublisher(fmt.Sprintf("%s:%d", conf.RedisHost, conf.RedisPort), queueName)
	}

	prod := Producer{
		gen:    &gen,
		mapper: mapper,
		pub:    pub,
	}

	prod.run(ctx)
}

func startSubscriber(ctx context.Context, conf *SubscriberConf) {
	if conf.Nats > 0 && (conf.RedisHost != "" || conf.RedisPort > 0) {
		panic("only nats or redis parameters, not both")
	}

	ring, err := queuering.NewHashRing(int(conf.Queues), int(conf.Queues))
	if err != nil {
		panic(err)
	}

	var wf queueringcluster.WorkerFactory
	if conf.Nats > 0 {
		stream := createNatsStream(fmt.Sprintf("localhost:%d", conf.Nats))
		wf = &queueringcluster.NatsWorkerFactory{
			Stream:           stream,
			NodeID:           int(conf.Port),
			QueueName:        queueName,
			NumRetries:       numRetries,
			ProcessorFactory: &ExampleNatsProcessorFactory{},
		}
	} else {
		wf = &queueringcluster.RedisWorkerFactory{
			RedisAddr:        []string{fmt.Sprintf("%s:%d", conf.RedisHost, conf.RedisPort)},
			QueueName:        queueName,
			NodeID:           int(conf.Port),
			NumRetries:       numRetries,
			ProcessorFactory: &ExampleRedisProcessorFactory{},
		}
	}

	n := queueringcluster.ServerNode{
		Name: fmt.Sprintf("node-%d", conf.Port),
		Subscriber: &queueringcluster.Subscriber{
			Workers:      make(map[int]queueringcluster.WorkerRec),
			WokerFactory: wf,
		},
		Hashring: ring,
		Port:     int(conf.Port),
		SyncEvt:  make(chan memberlist.NodeEvent),
	}

	go n.Start(ctx)
	time.Sleep(500 * time.Millisecond) //nolint:mnd // wait for started

	if len(conf.Cluster) > 0 {
		n.Join(fmt.Sprintf("localhost:%d", conf.Cluster[0]))
	}

	<-ctx.Done()
}

func startRedisMonitoring(ctx context.Context, conf *RedisConf) {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", conf.RedisHost, conf.RedisPort),
	})

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-time.After(5 * time.Second): //nolint:mnd //temp
		}
		for i := 0; i < int(conf.Queues); i++ {
			res := rdb.XLen(ctx, fmt.Sprintf("%s.%d", queueName, i))
			if res.Err() != nil {
				panic(res.Err())
			}

			fmt.Printf(" |%4.d:%4.d", i, res.Val())
			if i%10 == 0 {
				fmt.Print("\n")
			}
		}
		fmt.Print("\n")
	}
}
