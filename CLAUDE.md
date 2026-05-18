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

**Entry point:** `main.go` → `cmd/root.go` (cobra command). The root command loads config, selects a platform client, and calls `client.Sync(cfg)` — either once or on a cron schedule.

**`pkg/client`** — defines the `Client` interface (`Sync(Config) error`, `GetTokenManager() *token.Manager`). Every platform implements this.

**Platform packages** (`pkg/github`, `pkg/gitlab`, `pkg/bitbucket`, `pkg/forgejo`) — each implements `Client`. The pattern is: fetch a filtered list of repos from the platform API, then call `sync.SyncWithConcurrency` to clone/update them in parallel. `pkg/raw` handles plain git URLs and also implements the same sync flow without needing a `Client`.

**`pkg/sync`** — the core git execution layer, shared by all platforms:
- `CloneOrUpdateRepo` / `CloneOrUpdateRawRepo` — runs `git clone` or `git fetch` depending on whether the repo already exists locally.
- `SyncWiki` / `SyncIssues` — separate sync for wiki repos and issue backups.
- `SyncWithConcurrency` — generic semaphore-based worker pool bounded by `cfg.Concurrency`.
- `retryOperation` — wraps any operation with retry logic from `cfg.Retry`.
- `stats.go` — package-level `SyncStats` (mutex-guarded), populated during sync and consumed by `LogSyncSummary`, which also fires notifications and telemetry.

**`pkg/token`** — `Manager` distributes multiple API tokens in round-robin using `atomic.Uint32`. Pass multiple tokens in `tokens: []` in config to distribute API rate limits.

**`pkg/config`** — YAML config loaded via Viper. `SetSensibleDefaults` migrates the deprecated `token` field to `tokens` and fills in server defaults per platform. `ValidateConfig` enforces required fields and valid enum values.

**`pkg/issues`** — Writes issue backups as both JSON (`issues/json/<number>.json`) and Markdown (`issues/md/<number>.md`), with an `index.json` for incremental sync. `ReadLastSyncTime` reads `index.json` to determine the `since` timestamp for subsequent fetches.

**`pkg/notification`** — `NotificationProvider` interface with ntfy and Gotify implementations. Called from `LogSyncSummary` after each sync run.

**`pkg/telemetry`** — PostHog-based opt-in telemetry. Disabled via `telemetry.enabled: false` in config or `GIT_SYNC_NO_TELEMETRY=1` env var.

## Config file

Default path: `~/.config/git-sync/config.yaml`. Override with `--config` flag or `GIT_SYNC_CONFIG_FILE` env var. Backup dir defaults to `~/git-backups`, overridable with `--backup-dir` or `GIT_SYNC_BACKUP_DIR`.

The `token` field is deprecated — use `tokens: []` (array) instead. Multiple tokens are distributed round-robin across concurrent operations.

## Adding a new platform

1. Create `pkg/<platform>/<platform>.go` implementing `client.Client`.
2. Add a case to the `switch cfg.Platform` block in `cmd/root.go`.
3. Add the platform name to the allowlist in `config.ValidateConfig` and to `SetSensibleDefaults` in `config/defaults.go`.

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
