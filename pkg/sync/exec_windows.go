//go:build windows

package sync

import "os/exec"

// configureProcessGroup is a no-op on Windows: exec.Cmd has no equivalent to
// POSIX process groups here, so a cancelled context falls back to the
// default Cmd.Cancel behaviour (killing the direct git process only). Any
// helper processes git spawned are not guaranteed to be killed. Deployment
// targets Linux containers (see Dockerfile), so this gap is accepted for the
// Windows development environment.
func configureProcessGroup(cmd *exec.Cmd) {}
