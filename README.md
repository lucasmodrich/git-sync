# git-sync

A CLI tool to back up and sync Git repositories from hosted platforms (GitHub, GitLab, Bitbucket, Forgejo/Gitea, Azure DevOps) or arbitrary raw git URLs to a local directory.

## Installation

```bash
go install github.com/lucasmodrich/git-sync@latest
```

Or build from source:

```bash
git clone https://github.com/lucasmodrich/git-sync.git
cd git-sync
go build -o git-sync .
```

## Quick start

```bash
# Interactive configuration wizard
git-sync init

# Run a sync
git-sync --config ~/.config/git-sync/config.yaml

# Preview without making changes
git-sync --dry-run --config ~/.config/git-sync/config.yaml
```

## Features

- Bare, mirror, shallow, or full clones
- Periodic sync via built-in cron scheduling
- Concurrent sync with configurable parallelism
- Multi-token round-robin to spread API rate limits
- Wiki and issue backup (GitHub)
- Notifications via ntfy or Gotify
- Platforms: GitHub, GitLab, Bitbucket, Forgejo/Gitea, Azure DevOps, raw git URLs

## Configuration

Default config path: `~/.config/git-sync/config.yaml`. Override with `--config` or `GIT_SYNC_CONFIG_FILE`.

```yaml
platform: github
tokens:
  - ghp_your_token_here
username: your-username
backup_dir: ~/git-backups
concurrency: 5
timeout: 1800 # per-attempt git operation timeout, in seconds
```

See [CLAUDE.md](./CLAUDE.md) for full architecture and configuration reference.

## Bugs and feature requests

Open an [issue](https://github.com/lucasmodrich/git-sync/issues/new) on GitHub.

## Heritage

This project is a fork of [AkashRajpurohit/git-sync](https://github.com/AkashRajpurohit/git-sync), originally created by [Akash Rajpurohit](https://akashrajpurohit.com/). The original README, feature history, and contributor acknowledgements are preserved in [HERITAGE.md](./HERITAGE.md).

This fork continues development independently under the same MIT licence.

## License

MIT — see [LICENSE](./LICENSE).

Copyright (c) 2024 Akash Rajpurohit  
Copyright (c) 2026 Lucas Modrich
