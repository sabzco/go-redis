package redis

import (
	"context"
	"github.com/redis/go-redis/v9/internal"
)

type SentinelWatcher struct {
	CurrentMaster string
	OnMaster      func(ctx context.Context, addr string)
	failover      *sentinelFailover
	pubsubList    []*PubSub
	sentinelList  []*SentinelClient
}

func (w *SentinelWatcher) Initialize(ctx context.Context, options *FailoverOptions) error {
	failover := &sentinelFailover{
		sentinelAddrs: options.SentinelAddrs,
		opt:           options,
	}
	w.failover = failover
	failover.onFailover = func(ctx context.Context, addr string) {
		w.CurrentMaster = addr
		w.OnMaster(ctx, addr)
	}

	masterAddr, err := failover.MasterAddr(ctx)
	if err == nil {
		failover.trySwitchMaster(ctx, masterAddr)
		w.pubsubList = make([]*PubSub, len(failover.sentinelAddrs))
		w.sentinelList = make([]*SentinelClient, len(failover.sentinelAddrs))
		for i, sentinelAddr := range failover.sentinelAddrs {
			internal.Logger.Printf(ctx, "Listening for +switch-master on %q", sentinelAddr)
			w.sentinelList[i] = NewSentinelClient(failover.opt.sentinelOptions(sentinelAddr))
			w.pubsubList[i] = w.sentinelList[i].Subscribe(ctx, "+switch-master")
			go failover.listen(w.pubsubList[i])
		}
	}
	return err
}

func (w *SentinelWatcher) MasterAddr(ctx context.Context) (string, error) {
	addr, err := w.failover.MasterAddr(ctx)
	if err == nil {
		w.failover.trySwitchMaster(ctx, addr)
	}
	return addr, err
}

func (w *SentinelWatcher) Close() error {
	var firstErr error
	if w.failover != nil {
		firstErr = w.failover.Close()
		w.failover = nil
	}
	for i, pubsub := range w.pubsubList {
		if pubsub != nil {
			err := pubsub.Close()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			w.pubsubList[i] = nil
		}
	}
	for i, sentinel := range w.sentinelList {
		if sentinel != nil {
			err := sentinel.Close()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			w.sentinelList[i] = nil
		}
	}
	return firstErr
}
