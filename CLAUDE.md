# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

`git-sync` is a CLI tool that backs up Git repositories from hosted platforms (GitHub, GitLab, Bitbucket, Forgejo/Gitea) or arbitrary raw git URLs to a local directory. It performs bare clones by default and supports periodic sync via cron.

## Commands

```bash
# Build
go build -o git-sync .

# Run all tests
go test -v ./...

# Run a single test file or package
go test -v ./pkg/config/...

# Run a single test by name
go test -v ./pkg/config/... -run TestValidateConfig

# Run the binary
go run main.go --config path/to/config.yaml

# Tidy dependencies
go mod tidy
```

## Architecture

**Entry point:** `main.go` → `cmd/root.go` (cobra command). The root command loads config, selects a platform client, and calls `client.Sync(ctx, cfg)` — either once or on a cron schedule. `rootCtx` is cancelled on SIGINT/SIGTERM and is respected all the way down into git subprocess execution (see `pkg/sync` below).

**`pkg/client`** — defines the `Client` interface (`Sync(ctx context.Context, config Config) error`, `GetTokenManager() *token.Manager`). Every platform implements this. `ctx` must be threaded through to any git subprocess call, not just API calls — do not discard it with `Sync(_ context.Context, ...)`.

**Platform packages** (`pkg/github`, `pkg/gitlab`, `pkg/bitbucket`, `pkg/forgejo`) — each implements `Client`. The pattern is: fetch a filtered list of repos from the platform API, then call `sync.SyncWithConcurrency` to clone/update them in parallel. `pkg/raw` handles plain git URLs and also implements the same sync flow without needing a `Client`.

**`pkg/sync`** — the core git execution layer, shared by all platforms:
- `CloneOrUpdateRepo` / `CloneOrUpdateRawRepo` / `SyncWiki` / `SyncIssues` — all take `ctx` as the first parameter. `CloneOrUpdateRepo`/`CloneOrUpdateRawRepo` run `git clone` or `git fetch` depending on whether the repo already exists locally; `SyncWiki`/`SyncIssues` are the separate sync paths for wiki repos and issue backups.
- `SyncWithConcurrency` — generic semaphore-based worker pool bounded by `cfg.Concurrency`; stops launching new work once `ctx` is cancelled.
- `retryOperation` — wraps any operation with retry logic from `cfg.Retry`, bounds each individual attempt with `cfg.Timeout` (`withAttemptTimeout` in `retry.go`), and aborts the retry loop immediately on `ctx` cancellation instead of sleeping through a shutdown.
- `exec.go` / `exec_unix.go` / `exec_windows.go` — `newGitCommand` builds every git subprocess via `exec.CommandContext` with `Cmd.WaitDelay` as a cross-platform backstop. On Unix (build-tagged), it also puts the process in its own group and kills the whole group on cancellation, since git commonly forks helpers (`ssh`, `git-remote-https`, `pack-objects`) that a plain kill of the git process wouldn't reach. Windows falls back to the default single-process kill. See `docs/decisions/0001-bound-git-subprocess-lifetime.md` for why this exists.
- `stats.go` — package-level `SyncStats` (mutex-guarded), populated during sync and consumed by `LogSyncSummary`, which also fires notifications and telemetry.

**`pkg/token`** — `Manager` distributes multiple API tokens in round-robin using `atomic.Uint32`. Pass multiple tokens in `tokens: []` in config to distribute API rate limits.

**`pkg/config`** — YAML config loaded via Viper. `SetSensibleDefaults` migrates the deprecated `token` field to `tokens` and fills in server defaults per platform. `ValidateConfig` enforces required fields and valid enum values.

**`pkg/issues`** — Writes issue backups as both JSON (`issues/json/<number>.json`) and Markdown (`issues/md/<number>.md`), with an `index.json` for incremental sync. `ReadLastSyncTime` reads `index.json` to determine the `since` timestamp for subsequent fetches.

**`pkg/notification`** — `NotificationProvider` interface with ntfy and Gotify implementations. Called from `LogSyncSummary` after each sync run.

**`pkg/telemetry`** — PostHog-based opt-in telemetry. Disabled via `telemetry.enabled: false` in config or `GIT_SYNC_NO_TELEMETRY=1` env var.

## Config file

Default path: `~/.config/git-sync/config.yaml`. Override with `--config` flag or `GIT_SYNC_CONFIG_FILE` env var. Backup dir defaults to `~/git-backups`, overridable with `--backup-dir` or `GIT_SYNC_BACKUP_DIR`.

The `token` field is deprecated — use `tokens: []` (array) instead. Multiple tokens are distributed round-robin across concurrent operations.

`timeout` (seconds, default 1800) bounds each individual git clone/fetch attempt — see `pkg/sync/retry.go`. This exists so a stalled operation (dead connection, unresponsive remote) cannot block a worker slot indefinitely; it is applied per attempt, independently of `retry.count`/`retry.delay`.

### GitHub token requirements

Use a **classic Personal Access Token** (not a fine-grained token). Fine-grained tokens do not expose the necessary API endpoints for listing all repositories including organisation repos. The classic token requires the `repo` scope. If the organisation uses SSO, the token must also be SSO-authorised for that organisation in the GitHub token settings.

## Adding a new platform

1. Create `pkg/<platform>/<platform>.go` implementing `client.Client`.
2. Add a case to the `switch cfg.Platform` block in `cmd/root.go`.
3. Add the platform name to the allowlist in `config.ValidateConfig` and to `SetSensibleDefaults` in `config/defaults.go`.

## Docker image

The container runs `tini` as PID 1 (`ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh", ...]`), not the Go binary directly. This is required, not cosmetic: without a real init process, orphaned grandchild git processes (`ssh`, `git-remote-https`, `pack-objects`) never get reaped and slowly exhaust the container's process table. See `docs/decisions/0001-bound-git-subprocess-lifetime.md`.

`entrypoint.sh` (and any future `*.sh` file) must keep LF line endings — enforced via `.gitattributes` (`*.sh text eol=lf`). A CRLF shebang (`#!/bin/sh\r`) breaks `exec()` inside the Linux container with a confusing "No such file or directory" error. This bit a real checkout on Windows with `core.autocrlf=true` before `.gitattributes` was added — don't remove that rule.

## Backup directory layout

```
<backup_dir>/
  <owner>/
    <repo>/
      <repo>.git/       # bare/mirror/shallow/full clone
      <repo>.wiki.git/  # wiki clone (if include_wiki: true)
      issues/
        index.json
        json/<number>.json
        md/<number>.md
```
