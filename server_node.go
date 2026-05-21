package queueringcluster

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dsyabitov/go-cluster"
	"github.com/dsyabitov/go-queuering"
	"github.com/hashicorp/memberlist"
)

type ServerNode struct {
	Node       *cluster.Node
	Subscriber *Subscriber
	Hashring   *queuering.HashRing
	SyncEvt    chan memberlist.NodeEvent
	Name       string
	Queues     []int
	Port       int
}

func (n *ServerNode) Start(ctx context.Context) {
	var err error
	n.Node, err = cluster.NewNode(1, n.Port, n.Name)
	if err != nil {
		panic(err)
	}
	n.Hashring.AddNode(n.Name)

	go n.processEventSync(ctx)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case evt := <-n.Node.EventsOut:
			go n.processEvent(evt)

		case msg := <-n.Node.MessagesOut:
			go n.processMessage(msg)
		}
	}
}

func (n *ServerNode) processMessage(msg cluster.Message) {
	slog.Debug("got message", "msg", msg)
}

// EventsOut is buffered channel, so we make it locked via custom unbuffered channel.
func (n *ServerNode) processEvent(evt memberlist.NodeEvent) {
	n.SyncEvt <- evt
}

func (n *ServerNode) processEventSync(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case evt := <-n.SyncEvt:
			if evt.Event == memberlist.NodeUpdate {
				slog.Debug("cluster  event", "me", n.Name, "evt", "update", "node", evt.Node.Name, "addr", evt.Node.Addr, "meta", string(evt.Node.Meta))
				n.processNodeUpdate()
			}
			if evt.Event == memberlist.NodeJoin {
				slog.Debug("cluster  event", "me", n.Name, "evt", "join", "node", evt.Node.Name, "addr", evt.Node.Addr)
				n.processNodeJoin(evt.Node.Name)
			}
			if evt.Event == memberlist.NodeLeave {
				slog.Debug("cluster  event", "me", n.Name, "evt", "leave", "node", evt.Node.Name, "addr", evt.Node.Addr)
				n.processNodeLeave(evt.Node.Name)
			}
		}
	}
}

func (n *ServerNode) processNodeLeave(name string) {
	n.Hashring.RemoveNode(name)
	n.unsubscribe()
}

func (n *ServerNode) processNodeJoin(name string) {
	slog.Info("add node to queue", "node", name)
	dist := n.Hashring.AddNode(name)
	for k, v := range dist {
		slog.Info("distribution", "owner", n.Name, "key", k, "nodes", v)
	}
	n.unsubscribe()
}

func (n *ServerNode) processNodeUpdate() {
	tags := n.Node.Tags()
	state := tags["subscription"]
	if state == "unsubscribed" {
		slog.Debug("trying to subscribe", "me", n.Name)
		n.trySubscribe()
	} else {
		slog.Debug("current state", "me", n.Name, "state", state)
	}
	// TODO check all nodes subscribed
}

func (n *ServerNode) trySubscribe() {
	for _, m := range n.Node.Members() {
		tags, err := cluster.NodeTags{}.FromString(string(m.Meta))
		if err != nil {
			panic(err)
		}

		if val, ok := tags["subscription"]; !ok {
			panic(errors.New("no needed tag subscription"))
		} else if val != "unsubscribed" {
			slog.Warn("node ready for resubscribe", "node", m.Name)
			return
		}
	}
	if len(n.Node.Members()) > 1 {
		n.subscribe()
	}
}

func (n *ServerNode) subscribe() {
	slog.Info("subscribe", "me", n.Name)
	// myQueues := make(map[int]any)
	// var newQueues []int
	// var k string
	//
	// for k, newQueues = range n.hashring.GetDistribution() {
	// 	slog.Info("distribution", "node", n.name, "k", k)
	// 	if k == n.name {
	// 		slog.Info("distribution it's my", "node", n.name, "k", k, "newQueues len", len(newQueues))
	// 		for i := range newQueues {
	// 			myQueues[i] = struct{}{}
	// 		}
	// 		break
	// 	}
	// }
	//
	// for i := range n.queues {
	// 	if _, ok := myQueues[i]; !ok {
	// 		slog.Info("subscribe", "node", n.name, "qNum", i)
	// 		n.subscriber.Subscribe(i)
	// 	} else {
	// 		slog.Info("already subscribed ", "num", i)
	// 	}
	// }
	//
	// err := n.node.UpdateTag("subscription", "subscribed", 3*time.Second)
	// if err != nil {
	// 	panic(err)
	// }
	// n.queues = newQueues
	for _, num := range n.Queues {
		n.Subscriber.Subscribe(num)
	}
}

func (n *ServerNode) unsubscribe() {
	slog.Info("unsubscribe", "me", n.Name)
	myQueues := make(map[int]any)
	var newQueues []int
	var k string

	for k, newQueues = range n.Hashring.GetDistribution() {
		if k == n.Name {
			for i := range newQueues {
				myQueues[i] = struct{}{}
			}
			break
		}
	}

	for i := range n.Queues {
		if _, ok := myQueues[i]; !ok {
			n.Subscriber.Unsubscribe(i)
		}
	}
	err := n.Node.UpdateTag("subscription", "unsubscribed", 3*time.Second) //nolint:mnd //TODO
	if err != nil {
		panic(err)
	}
	n.Queues = newQueues
}

func (n *ServerNode) Join(anyNodeAddr string) {
	_, err := n.Node.Join([]string{anyNodeAddr})
	if err != nil {
		panic(err)
	}
}
