package queueringcluster

import (
	"context"
	"sync"
)

type WorkerRec struct {
	w      Worker
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

type Worker interface {
	Start(ctx context.Context, wg *sync.WaitGroup) error
}

type WorkerFactory interface {
	NewWorker(qNum int) Worker
}

type Subscriber struct {
	WokerFactory WorkerFactory
	Workers      map[int]WorkerRec
}

func (s *Subscriber) Subscribe(qNum int) {
	if _, ok := s.Workers[qNum]; ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	w := s.WokerFactory.NewWorker(qNum)
	err := w.Start(ctx, &wg)
	if err != nil {
		panic(err)
	}

	s.Workers[qNum] = WorkerRec{
		w:      w,
		cancel: cancel,
		wg:     &wg,
	}
}

func (s *Subscriber) Unsubscribe(qNum int) {
	r, ok := s.Workers[qNum]
	if !ok {
		return
	}
	r.cancel()
	r.wg.Wait()
	delete(s.Workers, qNum)
}
