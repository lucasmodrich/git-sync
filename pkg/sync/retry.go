package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/logger"
)

// withAttemptTimeout scopes ctx to a single attempt's duration. cfg.Timeout <= 0
// disables the bound (unlimited attempt duration) — production config always
// sets a positive value via SetSensibleDefaults/ValidateConfig, but callers such
// as tests may construct a Config directly.
func withAttemptTimeout(ctx context.Context, cfg config.Config) (context.Context, context.CancelFunc) {
	if cfg.Timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
}

func retryOperation(ctx context.Context, cfg config.Config, operation func(context.Context) error, operationName string) error {
	var lastErr error

	runAttempt := func() error {
		attemptCtx, cancel := withAttemptTimeout(ctx, cfg)
		defer cancel()
		return operation(attemptCtx)
	}

	// If retry count is 0 or negative, just execute once without retries
	if cfg.Retry.Count <= 0 {
		return runAttempt()
	}

	for attempt := 1; attempt <= cfg.Retry.Count; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := runAttempt()
		if err == nil {
			if attempt > 1 {
				logger.Warnf("Operation %s succeeded after %d attempts", operationName, attempt)
			}
			return nil
		}

		lastErr = err
		if attempt < cfg.Retry.Count {
			logger.Warnf("Attempt %d/%d failed for %s: %v. Retrying in %d seconds...",
				attempt, cfg.Retry.Count, operationName, err, cfg.Retry.Delay)
			select {
			case <-time.After(time.Duration(cfg.Retry.Delay) * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %v", cfg.Retry.Count, lastErr)
}
