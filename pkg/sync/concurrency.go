package sync

import (
	"sync"

	"github.com/AkashRajpurohit/git-sync/pkg/config"
)

func SyncWithConcurrency[T any](cfg config.Config, repos []T, syncFn func(T)) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.Concurrency)

	for _, repo := range repos {
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
