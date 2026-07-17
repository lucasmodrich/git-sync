//go:build !windows

package sync

import (
	"os/exec"
	"syscall"
)

// configureProcessGroup puts the git subprocess in its own process group and
// arranges for context cancellation to kill the whole group (not just the
// direct child). Git commonly forks helpers — ssh, git-remote-https,
// pack-objects — that would otherwise survive a plain kill of the git process
// itself and be reparented as orphans.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
