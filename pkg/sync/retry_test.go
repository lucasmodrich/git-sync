package sync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/logger"
)

func init() {
	logger.InitLogger("fatal")
}

func TestRetryOperation_SucceedsFirstTry(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 3, Delay: 0}}
	calls := 0

	err := retryOperation(context.Background(), cfg, func(context.Context) error {
		calls++
		return nil
	}, "test op")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryOperation_SucceedsAfterRetries(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 3, Delay: 0}}
	calls := 0

	err := retryOperation(context.Background(), cfg, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient failure")
		}
		return nil
	}, "test op")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryOperation_ExhaustsRetriesAndReturnsWrappedError(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 3, Delay: 0}}
	calls := 0
	wantErr := errors.New("permanent failure")

	err := retryOperation(context.Background(), cfg, func(context.Context) error {
		calls++
		return wantErr
	}, "test op")

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if !errors.Is(err, wantErr) && !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("expected wrapped error to mention %q, got %v", wantErr, err)
	}
}

func TestRetryOperation_ZeroRetryCountRunsOnce(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 0, Delay: 0}}
	calls := 0
	wantErr := errors.New("failure")

	err := retryOperation(context.Background(), cfg, func(context.Context) error {
		calls++
		return wantErr
	}, "test op")

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

// TestRetryOperation_PerAttemptTimeout confirms that an operation exceeding
// cfg.Timeout is aborted via ctx.Done() rather than being left to run for its
// full (much longer) intended duration — this is the core of the fix: a
// stalled git operation must not be able to block a worker slot forever.
func TestRetryOperation_PerAttemptTimeout(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 1, Delay: 0}, Timeout: 1}

	start := time.Now()
	err := retryOperation(context.Background(), cfg, func(attemptCtx context.Context) error {
		select {
		case <-time.After(30 * time.Second):
			return nil
		case <-attemptCtx.Done():
			return attemptCtx.Err()
		}
	}, "test op")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("expected attempt to be aborted around the 1s timeout, took %v", elapsed)
	}
}

// TestRetryOperation_TimeoutDisabledWhenZero confirms cfg.Timeout<=0 leaves
// the attempt context without a deadline, matching the documented escape
// hatch for callers that construct Config directly (production config always
// has a positive Timeout via SetSensibleDefaults/ValidateConfig).
func TestRetryOperation_TimeoutDisabledWhenZero(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 1, Delay: 0}, Timeout: 0}

	var sawDeadline bool
	_ = retryOperation(context.Background(), cfg, func(attemptCtx context.Context) error {
		_, sawDeadline = attemptCtx.Deadline()
		return nil
	}, "test op")

	if sawDeadline {
		t.Fatal("expected no deadline on the attempt context when Timeout is 0")
	}
}

// TestRetryOperation_ParentCancellationStopsRetryLoop simulates a shutdown
// signal arriving mid-retry: the loop must stop retrying and return promptly
// rather than sleeping through the cancelled context (the bug retryOperation
// had before it became context-aware).
func TestRetryOperation_ParentCancellationStopsRetryLoop(t *testing.T) {
	cfg := config.Config{Retry: config.RetryConfig{Count: 5, Delay: 30}}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := retryOperation(ctx, cfg, func(context.Context) error {
		calls++
		return errors.New("always fails")
	}, "test op")
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls >= cfg.Retry.Count {
		t.Fatalf("expected cancellation to cut the retry loop short, got %d of %d calls", calls, cfg.Retry.Count)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("expected cancellation to stop the 30s retry delay promptly, took %v", elapsed)
	}
}
