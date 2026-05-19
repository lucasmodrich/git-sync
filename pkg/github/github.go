package github

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AkashRajpurohit/git-sync/pkg/config"
	"github.com/AkashRajpurohit/git-sync/pkg/helpers"
	"github.com/AkashRajpurohit/git-sync/pkg/issues"
	"github.com/AkashRajpurohit/git-sync/pkg/logger"
	gitSync "github.com/AkashRajpurohit/git-sync/pkg/sync"
	"github.com/AkashRajpurohit/git-sync/pkg/token"
	gh "github.com/google/go-github/v82/github"
	"golang.org/x/oauth2"
)

type GitHubClient struct {
	tokenManager *token.Manager
}

func NewGitHubClient(tokens []string) *GitHubClient {
	return &GitHubClient{
		tokenManager: token.NewManager(tokens),
	}
}

func (c *GitHubClient) GetTokenManager() *token.Manager {
	return c.tokenManager
}

func (c *GitHubClient) createClient() *gh.Client {
	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: c.tokenManager.GetNextToken()},
	)
	tc := oauth2.NewClient(ctx, ts)
	return gh.NewClient(tc)
}

func (c *GitHubClient) Sync(ctx context.Context, cfg config.Config) error {
	repos, err := c.getRepos(ctx, cfg)
	if err != nil {
		return err
	}

	gitSync.LogRepoCount(len(repos), cfg.Platform)

	gitSync.SyncWithConcurrency(cfg, repos, func(repo *gh.Repository) {
		owner := repo.GetOwner().GetLogin()
		repoName := repo.GetName()

		if cfg.DryRun {
			repoPath := gitSync.GetRepoPath(owner, repoName, cfg)
			action := "clone"
			if _, err := os.Stat(repoPath); err == nil {
				action = "update"
			}
			logger.Infof("[dry-run] Would %s: %s/%s", action, owner, repoName)
			gitSync.RecordRepoDryRun()
			if cfg.IncludeWiki && repo.GetHasWiki() {
				logger.Infof("[dry-run] Would sync wiki: %s/%s", owner, repoName)
				gitSync.RecordWikiDryRun()
			}
			if cfg.IncludeIssues && repo.GetHasIssues() {
				logger.Infof("[dry-run] Would sync issues: %s/%s", owner, repoName)
				gitSync.RecordIssuesDryRun()
			}
			return
		}

		repoAuthURL, _ := gitSync.BuildAuthURL(cfg.Server.Protocol, cfg.Server.Domain, "/"+owner+"/"+repoName+".git", cfg.Username, c.tokenManager.GetNextToken())
		gitSync.CloneOrUpdateRepo(owner, repoName, repoAuthURL, cfg)

		if cfg.IncludeWiki && repo.GetHasWiki() {
			wikiAuthURL, _ := gitSync.BuildAuthURL(cfg.Server.Protocol, cfg.Server.Domain, "/"+owner+"/"+repoName+".wiki.git", cfg.Username, c.tokenManager.GetNextToken())
			gitSync.SyncWiki(owner, repoName, wikiAuthURL, cfg)
		}
		if cfg.IncludeIssues && repo.GetHasIssues() {
			since, hasPrevSync := issues.ReadLastSyncTime(cfg.BackupDir, owner, repoName)
			allIssues, err := c.fetchIssues(ctx, owner, repoName, since, hasPrevSync)
			if err != nil {
				logger.Errorf("Failed to fetch issues for %s/%s: %v", owner, repoName, err)
			} else {
				gitSync.SyncIssues(owner, repoName, allIssues, cfg)
			}
		}
	})

	gitSync.LogSyncSummary(&cfg)
	return nil
}

func (c *GitHubClient) getRepos(ctx context.Context, cfg config.Config) ([]*gh.Repository, error) {
	logger.Debug("Fetching list of repositories ⏳")
	client := c.createClient()
	opt := &gh.RepositoryListByAuthenticatedUserOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}

	const maxTokenRetries = 3
	tokenRetries := 0
	var allRepos []*gh.Repository
	for {
		repos, resp, err := client.Repositories.ListByAuthenticatedUser(ctx, opt)
		if err != nil {
			tokenRetries++
			if tokenRetries >= maxTokenRetries {
				return nil, fmt.Errorf("failed to list repositories after %d token attempts: %w", maxTokenRetries, err)
			}
			logger.Debugf("Error with current token, trying next token (attempt %d/%d): %v", tokenRetries, maxTokenRetries, err)
			client = c.createClient()
			continue
		}
		tokenRetries = 0

		for _, repo := range repos {
			repoName := repo.GetName()
			isOrganizationRepo := repo.Owner.GetType() == "Organization"
			orgName := repo.Owner.GetLogin()

			if len(cfg.IncludeOrgs) > 0 {
				if isOrganizationRepo && helpers.IsIncludedInList(cfg.IncludeOrgs, orgName) {
					logger.Debug("[include_orgs] Repo included: ", repoName)
					allRepos = append(allRepos, repo)
				}
				continue
			}

			if len(cfg.ExcludeOrgs) > 0 {
				if isOrganizationRepo && helpers.IsIncludedInList(cfg.ExcludeOrgs, orgName) {
					logger.Debug("[exclude_orgs] Repo excluded: ", repoName)
					continue
				}
			}

			if len(cfg.IncludeRepos) > 0 {
				if helpers.IsIncludedInList(cfg.IncludeRepos, repoName) {
					logger.Debug("[include_repos] Repo included: ", repoName)
					allRepos = append(allRepos, repo)
				}
				continue
			}

			if len(cfg.ExcludeRepos) > 0 {
				if helpers.IsIncludedInList(cfg.ExcludeRepos, repoName) {
					logger.Debug("[exclude_repos] Repo excluded: ", repoName)
					continue
				}
			}

			if !cfg.IncludeForks && repo.GetFork() {
				logger.Debug("[include_forks] Repo excluded: ", repoName)
				continue
			}

			logger.Debug("Repo included: ", repoName)
			allRepos = append(allRepos, repo)
		}
		if resp.NextPage == 0 {
			break
		}

		logger.Debug("Fetching next page: ", resp.NextPage)
		opt.Page = resp.NextPage
	}

	return allRepos, nil
}

func (c *GitHubClient) fetchIssues(ctx context.Context, owner, repo string, since time.Time, incremental bool) ([]issues.Issue, error) {
	repoFullName := fmt.Sprintf("%s/%s", owner, repo)
	client := c.createClient()

	opt := &gh.IssueListByRepoOptions{
		State:       "all",
		ListOptions: gh.ListOptions{PerPage: 100},
	}

	if incremental {
		opt.Since = since
		logger.Debugf("Incremental fetch for %s (issues updated since %s)", repoFullName, since.Format(time.RFC3339))
	} else {
		logger.Debugf("Full fetch for %s ⏳", repoFullName)
	}

	var ghIssueList []*gh.Issue
	for {
		ghIssues, resp, err := client.Issues.ListByRepo(ctx, owner, repo, opt)
		if err != nil {
			return nil, err
		}

		for _, ghIssue := range ghIssues {
			if ghIssue.PullRequestLinks != nil {
				continue
			}
			ghIssueList = append(ghIssueList, ghIssue)
		}

		if resp.NextPage == 0 {
			break
		}
		opt.ListOptions.Page = resp.NextPage
	}

	logger.Debugf("Found %d issues for %s", len(ghIssueList), repoFullName)

	var commentsByIssue map[int][]issues.Comment
	if !incremental && len(ghIssueList) > 0 {
		var err error
		commentsByIssue, err = c.fetchAllComments(ctx, owner, repo)
		if err != nil {
			logger.Warnf("Bulk comment fetch failed for %s, falling back to per-issue: %v", repoFullName, err)
			commentsByIssue = nil
		}
	}

	allIssues := make([]issues.Issue, 0, len(ghIssueList))
	for _, ghIssue := range ghIssueList {
		issue := convertGitHubIssue(ghIssue)

		if commentsByIssue != nil {
			issue.Comments = commentsByIssue[issue.Number]
		} else {
			logger.Debugf("Fetching comments for issue #%d in %s", ghIssue.GetNumber(), repoFullName)
			comments, err := c.fetchIssueComments(ctx, client, owner, repo, ghIssue.GetNumber())
			if err != nil {
				logger.Warnf("Failed to fetch comments for issue #%d in %s: %v", ghIssue.GetNumber(), repoFullName, err)
			} else {
				issue.Comments = comments
			}
		}

		allIssues = append(allIssues, issue)
	}

	logger.Debugf("Fetched %d issues for %s", len(allIssues), repoFullName)
	return allIssues, nil
}

// fetchAllComments bulk-fetches all issue comments for the repo in a single
// paginated sweep rather than one API call per issue. This trades memory for
// fewer round-trips and is only used on full (non-incremental) syncs.
// For very large repositories (10k+ comments) consider switching to per-issue
// fetching via fetchIssueComments to cap peak memory usage.
func (c *GitHubClient) fetchAllComments(ctx context.Context, owner, repo string) (map[int][]issues.Comment, error) {
	client := c.createClient()
	opt := &gh.IssueListCommentsOptions{
		Sort:        gh.Ptr("updated"),
		Direction:   gh.Ptr("asc"),
		ListOptions: gh.ListOptions{PerPage: 100},
	}

	commentsByIssue := make(map[int][]issues.Comment)
	for {
		ghComments, resp, err := client.Issues.ListComments(ctx, owner, repo, 0, opt)
		if err != nil {
			return nil, err
		}

		for _, c := range ghComments {
			issueNum := extractIssueNumber(c.GetIssueURL())
			if issueNum == 0 {
				continue
			}
			commentsByIssue[issueNum] = append(commentsByIssue[issueNum], issues.Comment{
				ID:        c.GetID(),
				Body:      c.GetBody(),
				Author:    issues.User{Login: c.GetUser().GetLogin(), URL: c.GetUser().GetHTMLURL()},
				URL:       c.GetHTMLURL(),
				CreatedAt: c.GetCreatedAt().Time,
				UpdatedAt: c.GetUpdatedAt().Time,
			})
		}

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return commentsByIssue, nil
}

func extractIssueNumber(issueURL string) int {
	parts := strings.Split(issueURL, "/")
	if len(parts) == 0 {
		return 0
	}
	num, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0
	}
	return num
}

func (c *GitHubClient) fetchIssueComments(ctx context.Context, client *gh.Client, owner, repo string, issueNumber int) ([]issues.Comment, error) {
	opt := &gh.IssueListCommentsOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}

	var allComments []issues.Comment
	for {
		ghComments, resp, err := client.Issues.ListComments(ctx, owner, repo, issueNumber, opt)
		if err != nil {
			return nil, err
		}

		for _, c := range ghComments {
			allComments = append(allComments, issues.Comment{
				ID:        c.GetID(),
				Body:      c.GetBody(),
				Author:    issues.User{Login: c.GetUser().GetLogin(), URL: c.GetUser().GetHTMLURL()},
				URL:       c.GetHTMLURL(),
				CreatedAt: c.GetCreatedAt().Time,
				UpdatedAt: c.GetUpdatedAt().Time,
			})
		}

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return allComments, nil
}

func convertGitHubIssue(ghIssue *gh.Issue) issues.Issue {
	issue := issues.Issue{
		Number:    ghIssue.GetNumber(),
		Title:     ghIssue.GetTitle(),
		Body:      ghIssue.GetBody(),
		State:     ghIssue.GetState(),
		Author:    issues.User{Login: ghIssue.GetUser().GetLogin(), URL: ghIssue.GetUser().GetHTMLURL()},
		URL:       ghIssue.GetHTMLURL(),
		CreatedAt: ghIssue.GetCreatedAt().Time,
		UpdatedAt: ghIssue.GetUpdatedAt().Time,
	}

	if ghIssue.ClosedAt != nil {
		t := ghIssue.GetClosedAt().Time
		issue.ClosedAt = &t
	}

	if ghIssue.Milestone != nil {
		issue.Milestone = ghIssue.GetMilestone().GetTitle()
	}

	labels := make([]string, 0, len(ghIssue.Labels))
	for _, l := range ghIssue.Labels {
		labels = append(labels, l.GetName())
	}
	issue.Labels = labels

	assignees := make([]issues.User, 0, len(ghIssue.Assignees))
	for _, a := range ghIssue.Assignees {
		assignees = append(assignees, issues.User{Login: a.GetLogin(), URL: a.GetHTMLURL()})
	}
	issue.Assignees = assignees

	return issue
}
