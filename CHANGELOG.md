# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers changes made in this fork, commencing from the point
of divergence from [AkashRajpurohit/git-sync](https://github.com/AkashRajpurohit/git-sync).
For the original project's history and acknowledgements, see [HERITAGE.md](./HERITAGE.md).

## [Unreleased]

## [0.2.0] - 2026-07-17

### Added

- `timeout` config option (default 1800 seconds) bounding each individual
  git clone/fetch attempt, so a stalled operation cannot block a worker
  slot indefinitely.

### Changed

- Threaded `context.Context` through the full sync pipeline — platform
  clients, `retryOperation`, and git subprocess execution — so shutdown
  signals and per-attempt timeouts are respected everywhere, not only in
  platform API calls.
- The Docker image now runs `tini` as PID 1, so orphaned git subprocesses
  (`ssh`, `git-remote-https`, `pack-objects`) are reaped instead of being
  left as zombies.

### Fixed

- Resolved a resource-exhaustion bug where a stalled git operation,
  combined with the container running without an init process, could
  slowly exhaust the process table over days of uptime — eventually
  causing every repository in a sync run to fail until the container was
  restarted.
- Fixed `entrypoint.sh`'s shebang breaking under Linux when the repository
  is checked out on Windows with `core.autocrlf=true` (CRLF line endings
  corrupted the interpreter path); added `.gitattributes` to enforce LF
  line endings for shell scripts.

### Removed

- Removed an unused project asset (`assets/catchintent-banner.png`) left
  over from before the fork.

### Security

- Upgraded `golang.org/x/crypto` (→ v0.54.0) and `golang.org/x/net`
  (→ v0.57.0), resolving 14 Dependabot alerts (7 critical, 2 high,
  5 moderate) covering SSH authentication-bypass, denial-of-service, and
  panic vulnerabilities in transitive dependencies.

## [0.1.0] - 2026-05-19

### Added

- Forked from [AkashRajpurohit/git-sync](https://github.com/AkashRajpurohit/git-sync).
  Original project history, feature log, and contributor acknowledgements
  are preserved in [HERITAGE.md](./HERITAGE.md).

### Changed

- Renamed the Go module and all import paths from
  `github.com/AkashRajpurohit/git-sync` to `github.com/lucasmodrich/git-sync`.
- Replaced `README.md` with a fork-owned version; preserved the original as
  `HERITAGE.md`.
- Updated `.all-contributorsrc`, the Docker image maintainer label, and
  GoReleaser ldflags/registry references for fork ownership.
- Bumped the Docker build image to `golang:1.25-alpine` to match `go.mod`.
- Replaced the upstream PostHog telemetry key with a fork-owned key.

### Fixed

- Removed `discussion_category_name` from the GoReleaser config —
  GitHub Discussions aren't enabled on this repository, which was causing
  release announcements to fail.

### Security

- Upgraded `golang.org/x/crypto` to v0.51.0, resolving CVE-2025-58181 and
  CVE-2025-47914.

[Unreleased]: https://github.com/lucasmodrich/git-sync/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lucasmodrich/git-sync/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lucasmodrich/git-sync/releases/tag/v0.1.0
