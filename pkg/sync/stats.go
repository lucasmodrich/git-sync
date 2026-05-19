package sync

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/logger"
	"github.com/lucasmodrich/git-sync/pkg/notification"
	"github.com/lucasmodrich/git-sync/pkg/telemetry"
	"github.com/lucasmodrich/git-sync/pkg/version"
)

type SyncStats struct {
	mu            sync.Mutex
	ReposSuccess  int
	ReposFailed   []string
	WikisSuccess  int
	WikisFailed   []string
	IssuesSuccess int
	IssuesFailed  []string
	DryRunRepos   int
	DryRunWikis   int
	DryRunIssues  int
}

var stats = &SyncStats{}

func recordRepoSuccess() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ReposSuccess++
}

func recordRepoFailure(repoName string, err error) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.ReposFailed = append(stats.ReposFailed, fmt.Sprintf("%s (Error: %v)", repoName, err))
}

func recordWikiSuccess() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.WikisSuccess++
}

func recordWikiFailure(wikiName string, err error) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.WikisFailed = append(stats.WikisFailed, fmt.Sprintf("%s (Error: %v)", wikiName, err))
}

func recordIssuesSuccess() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.IssuesSuccess++
}

func recordIssuesFailure(repoName string, err error) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.IssuesFailed = append(stats.IssuesFailed, fmt.Sprintf("%s (Error: %v)", repoName, err))
}

func RecordRepoDryRun() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.DryRunRepos++
}

func RecordWikiDryRun() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.DryRunWikis++
}

func RecordIssuesDryRun() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.DryRunIssues++
}

func LogRepoCount(count int, repoType string) {
	logger.Info("Total ", repoType, " repositories: ", count)
}

func LogSyncSummary(cfg *config.Config) {
	if cfg.DryRun {
		logger.Infof("[dry-run] Would sync %d repos, %d wikis, %d issue sets", stats.DryRunRepos, stats.DryRunWikis, stats.DryRunIssues)
		stats = &SyncStats{}
		return
	}

	logger.Infof("✅ Repositories: %d successfully synced", stats.ReposSuccess)
	if len(stats.ReposFailed) > 0 {
		logger.Errorf("❌ Failed repositories: %d", len(stats.ReposFailed))
		for _, r := range stats.ReposFailed {
			logger.Errorf("  - %s", r)
		}
	}

	logger.Infof("✅ Wikis: %d successfully synced", stats.WikisSuccess)
	if len(stats.WikisFailed) > 0 {
		logger.Errorf("❌ Failed wikis: %d", len(stats.WikisFailed))
		for _, w := range stats.WikisFailed {
			logger.Errorf("  - %s", w)
		}
	}

	logger.Infof("✅ Issues: %d repositories' issues synced", stats.IssuesSuccess)
	if len(stats.IssuesFailed) > 0 {
		logger.Errorf("❌ Failed issues: %d", len(stats.IssuesFailed))
		for _, i := range stats.IssuesFailed {
			logger.Errorf("  - %s", i)
		}
	}

	summary := &notification.SyncSummary{
		ReposSuccess:  stats.ReposSuccess,
		ReposFailed:   stats.ReposFailed,
		WikisSuccess:  stats.WikisSuccess,
		WikisFailed:   stats.WikisFailed,
		IssuesSuccess: stats.IssuesSuccess,
		IssuesFailed:  stats.IssuesFailed,
	}

	if err := notification.NotifyAll(&cfg.Notification, summary); err != nil {
		logger.Errorf("Failed to send notifications: %v", err)
	}

	telemetry.CaptureEvent("sync_completed", map[string]interface{}{
		"platform":       cfg.Platform,
		"clone_type":     cfg.CloneType,
		"concurrency":    cfg.Concurrency,
		"include_wiki":   cfg.IncludeWiki,
		"include_issues": cfg.IncludeIssues,
		"include_forks":  cfg.IncludeForks,
		"repos_success":  stats.ReposSuccess,
		"repos_failed":   len(stats.ReposFailed),
		"wikis_success":  stats.WikisSuccess,
		"wikis_failed":   len(stats.WikisFailed),
		"issues_success": stats.IssuesSuccess,
		"issues_failed":  len(stats.IssuesFailed),
		"app_version":    version.Version,
		"os":             runtime.GOOS,
		"arch":           runtime.GOARCH,
	})

	// Reset stats for next sync
	stats = &SyncStats{}
}
