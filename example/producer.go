package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/dsyabitov/go-queuering"
)

type Producer struct {
	gen    *DataGen
	mapper *queuering.QueueMapper
	pub    Publisher
}

func (p *Producer) run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-time.After(produceTimeout):
			param := p.gen.Next()
			qNum := p.mapper.MapInt64(param.ParamID)
			pushMsg := param.Serialize()

			err := p.pub.Publish(ctx, qNum, pushMsg)
			slog.Debug("produced", "qNum", qNum, "param", pushMsg)
			if err != nil {
				panic(err)
			}
		}
	}
}
