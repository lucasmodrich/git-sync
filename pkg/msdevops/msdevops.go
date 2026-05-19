// Package msdevops provides functionality for synchronizing Azure DevOps repositories.
// It implements the platform-specific client interface for Azure DevOps integration.
package msdevops

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/AkashRajpurohit/git-sync/pkg/config"
	"github.com/AkashRajpurohit/git-sync/pkg/helpers"
	"github.com/AkashRajpurohit/git-sync/pkg/logger"
	gitSync "github.com/AkashRajpurohit/git-sync/pkg/sync"
	"github.com/AkashRajpurohit/git-sync/pkg/token"
	"github.com/microsoft/azure-devops-go-api/azuredevops/v7"
	"github.com/microsoft/azure-devops-go-api/azuredevops/v7/git"
)

// MSDevOpsClient implements the Azure DevOps platform client for repository synchronization.
type MSDevOpsClient struct {
	tokenManager *token.Manager
	serverConfig config.Server
}

// NewMSDevOpsClient creates a new Azure DevOps client instance.
func NewMSDevOpsClient(serverConfig config.Server, tokens []string) *MSDevOpsClient {
	return &MSDevOpsClient{
		tokenManager: token.NewManager(tokens),
		serverConfig: serverConfig,
	}
}

// GetTokenManager returns the token manager instance for this client.
func (c *MSDevOpsClient) GetTokenManager() *token.Manager {
	return c.tokenManager
}

// createConnection creates an Azure DevOps connection. The organisation is part of the
// base URL — the SDK resolves all service endpoints relative to it:
// https://<domain>/<organization>/_apis/...
func (c *MSDevOpsClient) createConnection() (*azuredevops.Connection, error) {
	if c.serverConfig.Protocol == "" || c.serverConfig.Domain == "" {
		return nil, fmt.Errorf("invalid server configuration: protocol and domain must be specified")
	}
	if c.serverConfig.Organization == "" {
		return nil, fmt.Errorf("server.organization must be set for msdevops")
	}

	organizationURL := fmt.Sprintf("%s://%s/%s",
		c.serverConfig.Protocol,
		c.serverConfig.Domain,
		c.serverConfig.Organization,
	)
	logger.Debugf("Creating Azure DevOps connection for: %s", organizationURL)

	pat := c.tokenManager.GetNextToken()
	if pat == "" {
		return nil, fmt.Errorf("a valid token was not available")
	}

	return azuredevops.NewPatConnection(organizationURL, pat), nil
}

// createClient initialises a Git API client bound to the connection's organisation URL.
func (c *MSDevOpsClient) createClient(ctx context.Context) (git.Client, error) {
	connection, err := c.createConnection()
	if err != nil {
		return nil, err
	}

	client, err := git.NewClient(ctx, connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure DevOps git client: %w", err)
	}

	return client, nil
}

// derefString returns the value of a string pointer or an empty string if nil.
func derefString(ref *string) string {
	if ref == nil {
		return ""
	}
	return *ref
}

// buildRepoAuthURL injects a PAT into an Azure DevOps clone URL using net/url so the
// token is correctly percent-encoded. Azure DevOps PAT auth uses an empty username.
func buildRepoAuthURL(rawURL, pat string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid repository URL %q: %w", rawURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("repository URL missing scheme or host: %q", rawURL)
	}
	u.User = url.UserPassword("", pat)
	return u.String(), nil
}

// Sync synchronizes all accessible Azure DevOps repositories for the configured project.
func (c *MSDevOpsClient) Sync(cfg config.Config) error {
	repos, err := c.getRepos(cfg)
	if err != nil {
		return fmt.Errorf("failed to get repositories: %w", err)
	}

	gitSync.LogRepoCount(len(repos), cfg.Platform)

	gitSync.SyncWithConcurrency(cfg, repos, func(repo git.GitRepository) {
		// Guard: Project is a pointer and may be absent for orphaned repositories.
		if repo.Project == nil {
			logger.Warnf("Skipping repository %q — missing project reference", derefString(repo.Name))
			return
		}

		projectName := derefString(repo.Project.Name)
		repoName := derefString(repo.Name)

		// Backup layout: <backup_dir>/<org>/<project>/<repo>/
		// filepath.Join handles OS-specific path separators correctly.
		repoOwner := filepath.Join(cfg.Server.Organization, projectName)

		// RemoteUrl is the HTTPS clone URL returned by the API. WebUrl is the browser
		// portal URL and is NOT a valid git remote — never use it as a fallback.
		remoteURL := derefString(repo.RemoteUrl)
		if remoteURL == "" {
			logger.Errorf("Skipping %s/%s — RemoteUrl is empty (repository may be disabled or migrating)", projectName, repoName)
			return
		}

		authURL, err := buildRepoAuthURL(remoteURL, c.tokenManager.GetNextToken())
		if err != nil {
			logger.Errorf("Failed to build auth URL for %s/%s: %v", projectName, repoName, err)
			return
		}

		gitSync.CloneOrUpdateRepo(repoOwner, repoName, authURL, cfg)
	})

	gitSync.LogSyncSummary(&cfg)
	return nil
}

// getRepos fetches repositories for the configured project and applies include/exclude filters.
func (c *MSDevOpsClient) getRepos(cfg config.Config) ([]git.GitRepository, error) {
	logger.Debug("Fetching list of repositories ⏳")
	ctx := context.Background()

	client, err := c.createClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure DevOps client: %w", err)
	}

	const maxTokenRetries = 3
	var allRepos []git.GitRepository
	for attempt := 1; attempt <= maxTokenRetries; attempt++ {
		repos, err := c.getUserRepos(ctx, client, cfg)
		if err == nil {
			allRepos = repos
			break
		}
		if attempt == maxTokenRetries {
			return nil, fmt.Errorf("failed to list repositories after %d token attempts: %w", maxTokenRetries, err)
		}
		logger.Debugf("Error with current token, trying next token (attempt %d/%d): %v", attempt, maxTokenRetries, err)
		client, err = c.createClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure DevOps client with new token: %w", err)
		}
	}

	var reposToInclude []git.GitRepository
	for _, repo := range allRepos {
		// Guard: Project pointer may be nil for orphaned or migrating repositories.
		if repo.Project == nil {
			logger.Warnf("Skipping repository %q — missing project reference", derefString(repo.Name))
			continue
		}

		repoName := derefString(repo.Name)
		projectName := derefString(repo.Project.Name)

		if projectName == "" || repoName == "" {
			logger.Warnf("Skipping repository with missing required fields: project=%q, name=%q", projectName, repoName)
			continue
		}

		if repo.IsDisabled != nil && *repo.IsDisabled {
			logger.Warnf("Skipping repo %s/%s — repository is disabled", projectName, repoName)
			continue
		}

		// include_orgs / exclude_orgs filter by Azure DevOps *project* name.
		// In ADO the organisation is the account-level entity (server.organization);
		// projects are the sub-containers, analogous to GitHub orgs or GitLab groups.
		if len(cfg.IncludeOrgs) > 0 {
			if helpers.IsIncludedInList(cfg.IncludeOrgs, projectName) {
				logger.Debug("[include_orgs] Repo included: ", repoName)
				reposToInclude = append(reposToInclude, repo)
			}
			continue
		}

		if len(cfg.ExcludeOrgs) > 0 {
			if helpers.IsIncludedInList(cfg.ExcludeOrgs, projectName) {
				logger.Debug("[exclude_orgs] Repo excluded: ", repoName)
				continue
			}
		}

		if len(cfg.IncludeRepos) > 0 {
			if helpers.IsIncludedInList(cfg.IncludeRepos, repoName) {
				logger.Debug("[include_repos] Repo included: ", repoName)
				reposToInclude = append(reposToInclude, repo)
			}
			continue
		}

		if len(cfg.ExcludeRepos) > 0 {
			if helpers.IsIncludedInList(cfg.ExcludeRepos, repoName) {
				logger.Debug("[exclude_repos] Repo excluded: ", repoName)
				continue
			}
		}

		// IsFork is the authoritative SDK field for fork detection.
		isFork := repo.IsFork != nil && *repo.IsFork
		if !cfg.IncludeForks && isFork {
			logger.Debug("[include_forks] Repo excluded: ", repoName)
			continue
		}

		logger.Debug("Repo included: ", repoName)
		reposToInclude = append(reposToInclude, repo)
	}

	return reposToInclude, nil
}

// getUserRepos calls the Azure DevOps REST API to list all repositories in the configured project.
func (c *MSDevOpsClient) getUserRepos(ctx context.Context, client git.Client, cfg config.Config) ([]git.GitRepository, error) {
	allRepos, err := client.GetRepositories(ctx, git.GetRepositoriesArgs{
		Project: &cfg.Workspace,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch repositories: %w", err)
	}

	if allRepos == nil {
		return []git.GitRepository{}, nil
	}

	return *allRepos, nil
}
