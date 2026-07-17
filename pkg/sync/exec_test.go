package sync

import (
	"context"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// longSleepCommand builds a genuinely long-running subprocess the same way
// newGitCommand builds git subprocesses (CommandContext + WaitDelay +
// configureProcessGroup), so the kill mechanism itself is exercised without
// depending on a real git remote or a real hang.
func longSleepCommand(ctx context.Context, seconds int) *exec.Cmd {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command", "Start-Sleep -Seconds "+strconv.Itoa(seconds))
	} else {
		cmd = exec.CommandContext(ctx, "sleep", strconv.Itoa(seconds))
	}
	cmd.WaitDelay = gitProcessWaitDelay
	configureProcessGroup(cmd)
	return cmd
}

// TestAttemptTimeoutKillsHungProcess is the end-to-end regression test for the
// resource-leak bug: before this fix, a stalled git invocation (exec.Command,
// no context) would block Wait() forever. Here a deliberately long-running
// process is bound to a short-lived context; Run() must return promptly
// rather than blocking for the process's full intended duration.
func TestAttemptTimeoutKillsHungProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	cmd := longSleepCommand(ctx, 30)

	start := time.Now()
	_ = cmd.Run()
	elapsed := time.Since(start)

	if elapsed > 10*time.Second {
		t.Fatalf("expected the process to be killed shortly after the context timeout, took %v", elapsed)
	}
}
