package hashringcluster

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
	node       *cluster.Node
	subscriber *Subscriber
	hashring   *queuering.HashRing
	syncEvt    chan memberlist.NodeEvent
	name       string
	queues     []int
	port       int
}

func (n *ServerNode) Start(ctx context.Context) {
	var err error
	n.node, err = cluster.NewNode(1, n.port, n.name)
	if err != nil {
		panic(err)
	}
	n.hashring.AddNode(n.name)

	go n.processEventSync(ctx)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case evt := <-n.node.EventsOut:
			go n.processEvent(evt)

		case msg := <-n.node.MessagesOut:
			go n.processMessage(msg)
		}
	}
}

func (n *ServerNode) processMessage(msg cluster.Message) {
	slog.Debug("got message", "msg", msg)
}

// EventsOut is buffered channel, so we make it locked via custom unbuffered channel.
func (n *ServerNode) processEvent(evt memberlist.NodeEvent) {
	n.syncEvt <- evt
}

func (n *ServerNode) processEventSync(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case evt := <-n.syncEvt:
			if evt.Event == memberlist.NodeUpdate {
				slog.Debug("cluster  event", "me", n.name, "evt", "update", "node", evt.Node.Name, "addr", evt.Node.Addr, "meta", string(evt.Node.Meta))
				n.processNodeUpdate()
			}
			if evt.Event == memberlist.NodeJoin {
				slog.Debug("cluster  event", "me", n.name, "evt", "join", "node", evt.Node.Name, "addr", evt.Node.Addr)
				n.processNodeJoin(evt.Node.Name)
			}
			if evt.Event == memberlist.NodeLeave {
				slog.Debug("cluster  event", "me", n.name, "evt", "leave", "node", evt.Node.Name, "addr", evt.Node.Addr)
				n.processNodeLeave(evt.Node.Name)
			}
		}
	}
}

func (n *ServerNode) processNodeLeave(name string) {
	n.hashring.RemoveNode(name)
	n.unsubscribe()
}

func (n *ServerNode) processNodeJoin(name string) {
	slog.Info("add node to queue", "node", name)
	dist := n.hashring.AddNode(name)
	for k, v := range dist {
		slog.Info("distribution", "owner", n.name, "key", k, "nodes", v)
	}
	n.unsubscribe()
}

func (n *ServerNode) processNodeUpdate() {
	tags := n.node.Tags()
	state := tags["subscription"]
	if state == "unsubscribed" {
		slog.Debug("trying to subscribe", "me", n.name)
		n.trySubscribe()
	} else {
		slog.Debug("current state", "me", n.name, "state", state)
	}
	// TODO check all nodes subscribed
}

func (n *ServerNode) trySubscribe() {
	for _, m := range n.node.Members() {
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
	if len(n.node.Members()) > 1 {
		n.subscribe()
	}
}

func (n *ServerNode) subscribe() {
	slog.Info("subscribe", "me", n.name)
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
	for _, num := range n.queues {
		n.subscriber.Subscribe(num)
	}
}

func (n *ServerNode) unsubscribe() {
	slog.Info("unsubscribe", "me", n.name)
	myQueues := make(map[int]any)
	var newQueues []int
	var k string

	for k, newQueues = range n.hashring.GetDistribution() {
		if k == n.name {
			for i := range newQueues {
				myQueues[i] = struct{}{}
			}
			break
		}
	}

	for i := range n.queues {
		if _, ok := myQueues[i]; !ok {
			n.subscriber.Unsubscribe(i)
		}
	}
	err := n.node.UpdateTag("subscription", "unsubscribed", 3*time.Second) //nolint:mnd //TODO
	if err != nil {
		panic(err)
	}
	n.queues = newQueues
}

func (n *ServerNode) Join(anyNodeAddr string) {
	_, err := n.node.Join([]string{anyNodeAddr})
	if err != nil {
		panic(err)
	}
}
