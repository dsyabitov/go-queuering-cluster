package hashringcluster

import (
	"context"
	"sync"
)

type workerRec struct {
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
	wokerFactory WorkerFactory
	workers      map[int]workerRec
}

func (s *Subscriber) Subscribe(qNum int) {
	if _, ok := s.workers[qNum]; ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	w := s.wokerFactory.NewWorker(qNum)
	err := w.Start(ctx, &wg)
	if err != nil {
		panic(err)
	}

	s.workers[qNum] = workerRec{
		w:      w,
		cancel: cancel,
		wg:     &wg,
	}
}

func (s *Subscriber) Unsubscribe(qNum int) {
	r, ok := s.workers[qNum]
	if !ok {
		return
	}
	r.cancel()
	r.wg.Wait()
	delete(s.workers, qNum)
}
