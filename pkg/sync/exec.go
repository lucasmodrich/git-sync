package sync

import (
	"context"
	"os/exec"
	"time"
)

// gitProcessWaitDelay bounds how long Wait() will wait for git's stdout/stderr
// pipes to close after the process itself has been signalled to exit. Without
// this, a lingering grandchild (ssh, git-remote-https, pack-objects) holding
// the pipe open can make CombinedOutput() block indefinitely even after the
// git process itself has died.
const gitProcessWaitDelay = 10 * time.Second

// newGitCommand builds a git subprocess bound to ctx: cancelling ctx (via
// timeout or shutdown) terminates the command instead of leaving it to run
// (and potentially hang) forever. configureProcessGroup is platform-specific —
// see exec_unix.go / exec_windows.go.
func newGitCommand(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.WaitDelay = gitProcessWaitDelay
	configureProcessGroup(cmd)
	return cmd
}
