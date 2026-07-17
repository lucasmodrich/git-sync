package sync

import (
	"context"
	"sync"

	"github.com/lucasmodrich/git-sync/pkg/config"
)

func SyncWithConcurrency[T any](ctx context.Context, cfg config.Config, repos []T, syncFn func(T)) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.Concurrency)

	for _, repo := range repos {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(r T) {
			defer wg.Done()
			defer func() { <-sem }()
			syncFn(r)
		}(repo)
	}

	wg.Wait()
}
