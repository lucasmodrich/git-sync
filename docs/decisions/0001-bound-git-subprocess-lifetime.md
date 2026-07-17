# Bound git subprocess lifetime with context, timeout, and a container reaper

## Status

Accepted

## Context and Problem Statement

Production logs showed periodic total-failure incidents: every repository in a
sync run would fail, with a mix of `exit status 255` and
`fork/exec /usr/bin/git: resource temporarily unavailable` (EAGAIN on
`fork()`). The pattern was consistent — it took days of continuous cron
operation to appear, and a container restart cleared it immediately, only for
it to recur after further days of uptime.

Investigation of `pkg/sync` found that every git invocation was built with
plain `exec.Command` and run via `.CombinedOutput()`, with no
`exec.CommandContext`, no deadline, and no way to kill a stalled operation.
`context.Context` was already threaded from `cmd/root.go` into each platform
client's `Sync(ctx, cfg)` method for API calls, but was dropped before
reaching `pkg/sync` — several platform clients (`raw`, `bitbucket`, `forgejo`,
`msdevops`) even discarded the parameter outright (`Sync(_ context.Context, ...)`).

`EAGAIN` on `fork()` is a process/PID-count signal, not a file-descriptor one
(that would be `EMFILE`). Live git concurrency is hard-capped by
`SyncWithConcurrency`'s semaphore (`cfg.Concurrency`, default 5), so a handful
of permanently-blocked subprocesses cannot by themselves exhaust a process
table — that plateaus. What fits an *unbounded* leak from a *bounded* live
concurrency is zombie accumulation: git commonly forks grandchildren (`ssh`,
`git-remote-https`, `pack-objects`). The container's `entrypoint.sh` execs
`su-exec` as its final step with no init process in the chain, which means the
`git-sync` Go binary itself runs as PID 1. Go's `exec.Cmd.Wait()` only reaps
the one child it directly forked — it provides no generic reap-any-orphan
loop, which is what a real init process supplies. Any grandchild that outlives
its immediate git parent (through a stall, or once a fix started killing
processes) reparents to PID 1 and becomes a permanent zombie: no FDs held, no
CPU or memory used, invisible until the process table itself fills — matching
`EAGAIN` specifically and explaining why a full container restart (which tears
down the whole PID namespace) clears the symptom outright.

This is the leading hypothesis, not a certainty confirmed against live
production PID/zombie counts — only a short log fragment was available. But
the missing subprocess timeout/cancellation is unambiguous from the source
itself regardless of the precise leak mechanism, and is the standard cause of
this failure signature in tools that shell out to git.

## Decision Drivers

- Any git clone/fetch that stalls (dead connection, unresponsive remote, DNS
  hang) must not be able to block a worker slot — or a whole cron run — forever.
- The fix must not convert hangs into a *worse* problem: killing a git
  process's direct child without also reaping its orphaned grandchildren would
  accelerate process-table exhaustion rather than prevent it.
- Development happens on Windows; deployment is Linux containers (per the
  existing Dockerfile). The fix must work correctly on the deployment target
  and degrade gracefully, not silently, on the development platform.
- Keep the change incremental and focused — this is a bug fix, not a
  rearchitecture of the sync pipeline.

## Considered Options

- **Timeout config placement**: a new top-level `Timeout` field vs. nesting it
  under the existing `RetryConfig`.
- **Process termination on timeout**: rely on `exec.CommandContext`'s default
  behaviour (kill the direct child only) vs. also killing the whole process
  group on Unix.
- **Container reaper**: bake `tini` into the image as PID 1 vs. rely on the
  operator passing `docker run --init` (Docker's bundled `docker-init`, a tini
  fork, since 1.13) vs. leave the Go binary as PID 1 and accept the zombie
  risk vs. hand-roll a reap loop in Go.
- **Cron-wedge handling**: add a separate whole-run timeout in `cmd/root.go`
  vs. rely on the per-attempt subprocess timeout to keep `SyncWithConcurrency`
  bounded (and therefore the cron mutex always released).

## Decision Outcome

Chosen: a **top-level `Timeout` field** (seconds, applied per attempt inside
`retryOperation`, independent of whether retries are configured), combined
with **`exec.CommandContext` + `Cmd.WaitDelay`** as a cross-platform backstop
and **`SysProcAttr{Setpgid: true}` + group-kill via `cmd.Cancel`** on Unix
(build-tagged, since process groups are POSIX-only), and **`tini` added to the
Dockerfile as PID 1**. No separate whole-run cron ceiling was added.

### Consequences

- Good: a stalled git operation is now bounded in wall-clock time regardless
  of cause, closing both the resource-leak path and the cron-mutex wedge
  (`SyncWithConcurrency`'s `wg.Wait()` always returns once every subprocess is
  bounded, so `runSync` always returns and the cron mutex is always released —
  no separate fix needed there).
- Good: killing the whole process group (not just git's direct child) plus a
  real init process reaping any orphan that still escapes closes the leak at
  both ends — a fix that only did one of these would still leak zombies (kill
  without reaper) or still block indefinitely on rare grandchild-holds-the-pipe
  cases (reaper without `WaitDelay`).
- Bad: existing users with genuinely large mirror/full clones could see
  legitimate long-running clones now time out if the default is set too
  conservatively. This is a **potential breaking change** for such configs —
  mitigated with a generous default (30 minutes) and a config field users can
  raise, but it must be called out explicitly when this ships.
- Bad: the process-group kill is Unix-only (build-tagged). On Windows,
  cancellation falls back to `exec.CommandContext`'s default behaviour (kill
  the direct process only); any grandchildren git spawned are not guaranteed
  to be killed. Accepted because deployment targets Linux containers; the
  Windows development environment does not need the same guarantee.

## Pros and Cons of the Options

### Top-level `Timeout` field

- Good: a bounded single attempt is meaningful independent of whether retries
  are configured — `retryOperation`'s `Retry.Count <= 0` path previously ran
  unbounded with no way to cap it at all.
- Good: keeps the semantics of `Retry` (count/delay between attempts) and
  `Timeout` (bound on one attempt) orthogonal and easy to reason about.
- Bad: adds a new top-level config key users must be told about.

### Nesting timeout under `RetryConfig`

- Good: keeps retry-adjacent settings in one place.
- Bad: implies the timeout only matters when retries are enabled, which is
  backwards — an unbounded single attempt is the actual bug.

### Process-group kill on Unix + `tini` reaper

- Good: addresses both the direct hang (via `WaitDelay`/`CommandContext`) and
  the grandchild-orphan leak (via group kill + reaper) together, matching the
  two-part failure mode identified above.
- Bad: two moving parts to get right — shipping the group-kill without the
  reaper (or vice versa) leaves a real gap; both were verified together
  (Dockerfile/entrypoint.sh read directly to confirm the PID 1 gap existed
  before writing this ADR).

### Rely on `docker run --init` instead of baking `tini` into the image

- Good: no image change, no `apk add tini`, marginally smaller image.
- Good: this is Docker's own bundled `docker-init` (a tini fork, available
  since Docker CE 1.13+), so functionally equivalent to what we bake in.
- Bad: opt-in at *run* time, not guaranteed by the image — an operator who
  runs `docker run <image>` without `--init` (or a Compose file without
  `init: true`) silently loses the fix, with nothing telling them so. Since
  git-sync is a distributed image consumed by others, not a container this
  project fully controls end-to-end, this reintroduces exactly the class of
  invisible-until-it-bites problem the fix exists to close.
- Bad: no Kubernetes pod-spec equivalent — a k8s/Helm deployment of this image
  would get zero reaper protection unless the operator rebuilt the image
  themselves, defeating the purpose of shipping an official one.
- Bad: uses whatever `docker-init` version ships with the operator's local
  Docker Engine — unpinned and outside this project's control, unlike the
  Alpine `tini` package version baked into a given image build.
- Note: this is exactly the trade-off tini's own README calls out — the
  Alpine-specific install section documents `apk add tini` +
  `ENTRYPOINT ["/sbin/tini", "--"]` as its own prescribed pattern for
  Alpine-based images, with `--init` presented as an alternative for ad-hoc
  runs where the operator controls and remembers the command, not as a
  replacement recommendation for image authors.

### Default `exec.CommandContext` behaviour only (no group kill, no reaper)

- Good: no Dockerfile change, no build-tagged platform code.
- Bad: does not address grandchild processes at all — git's `ssh`/
  `git-remote-https`/`pack-objects` helpers would survive a plain kill of the
  git parent and still leak as PID-1 orphans, leaving the core bug unresolved.

### Separate whole-run cron timeout in `cmd/root.go`

- Good: defence-in-depth against any future code path that reintroduces an
  unbounded blocking call.
- Bad: unnecessary scope for this fix — once every subprocess attempt is
  bounded, `SyncWithConcurrency` and the cron mutex are already bounded as a
  direct consequence. Adding a redundant ceiling here would be scope creep
  against the "keep changes focused" convention.
