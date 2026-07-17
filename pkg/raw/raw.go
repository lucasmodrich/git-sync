package raw

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/logger"
	gitSync "github.com/lucasmodrich/git-sync/pkg/sync"
)

type RawClient struct{}

func NewRawClient() *RawClient {
	return &RawClient{}
}

// extractRepoInfo extracts the owner and repo name from a git URL
func (c RawClient) extractRepoInfo(url string) (string, string) {
	// Remove .git suffix if present
	url = strings.TrimSuffix(url, ".git")

	// Split the URL into parts
	parts := strings.Split(url, "/")
	if len(parts) < 2 {
		return "raw", filepath.Base(url)
	}

	return parts[len(parts)-2], parts[len(parts)-1]
}

func (c RawClient) Sync(ctx context.Context, cfg config.Config) error {
	if len(cfg.RawGitURLs) == 0 {
		return nil
	}

	gitSync.LogRepoCount(len(cfg.RawGitURLs), "raw")

	gitSync.SyncWithConcurrency(ctx, cfg, cfg.RawGitURLs, func(repoURL string) {
		owner, name := c.extractRepoInfo(repoURL)

		if cfg.DryRun {
			repoPath := gitSync.GetRepoPath(owner, name, cfg)
			action := "clone"
			if _, err := os.Stat(repoPath); err == nil {
				action = "update"
			}
			logger.Infof("[dry-run] Would %s: %s/%s", action, owner, name)
			gitSync.RecordRepoDryRun()
			return
		}

		gitSync.CloneOrUpdateRawRepo(ctx, owner, name, repoURL, cfg)
	})

	gitSync.LogSyncSummary(&cfg)
	return nil
}
