// Package msdevops provides functionality for synchronizing Azure DevOps repositories.
// It implements the platform-specific client interface for Azure DevOps integration.
package msdevops

import (
	"context"
	"fmt"
	"net/url"

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

// createConnection creates a new Azure DevOps connection using the provided token manager and server configuration.
func (c *MSDevOpsClient) createConnection() (*azuredevops.Connection, error) {
	if c.serverConfig.Protocol == "" || c.serverConfig.Domain == "" {
		return nil, fmt.Errorf("invalid server configuration: protocol and domain must be specified")
	}

	organizationURL := fmt.Sprintf("%s://%s", c.serverConfig.Protocol, c.serverConfig.Domain)
	logger.Debugf("Creating Azure DevOps connection for: %s", organizationURL)

	token := c.tokenManager.GetNextToken()
	if token == "" {
		return nil, fmt.Errorf("a valid token was not available")
	}

	return azuredevops.NewPatConnection(organizationURL, token), nil
}

// createClient initializes and returns a new Azure DevOps Git client using the provided token manager and server configuration.
func (c *MSDevOpsClient) createClient(ctx context.Context) (git.Client, error) {
	connection, err := c.createConnection()
	if err != nil {
		return nil, err
	}

	client, err := git.NewClient(ctx, connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure DevOps client: %w", err)
	}

	return client, nil
}


// derefString returns the value of a string pointer or an empty string if the pointer is nil.
func derefString(ref *string) string {
	if ref == nil {
		return ""
	}
	return *ref
}

// buildRepoAuthURL injects a PAT into an Azure DevOps remote URL using net/url so
// the token is correctly percent-encoded and the URL structure is never corrupted.
// Azure DevOps PAT auth uses an empty username with the token as the password.
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

// Sync synchronizes all accessible Azure DevOps repositories based on the provided configuration.
func (c *MSDevOpsClient) Sync(cfg config.Config) error {
	repos, err := c.getRepos(cfg)
	if err != nil {
		return fmt.Errorf("failed to get user repositories: %w", err)
	}

	gitSync.LogRepoCount(len(repos), cfg.Platform)

	gitSync.SyncWithConcurrency(cfg, repos, func(repo git.GitRepository) {
		repoOwner := derefString(repo.Project.Name)
		repoName := derefString(repo.Name)
		webURL := derefString(repo.RemoteUrl)
		if webURL == "" {
			webURL = derefString(repo.WebUrl)
		}

		authURL, err := buildRepoAuthURL(webURL, c.tokenManager.GetNextToken())
		if err != nil {
			logger.Errorf("Failed to build auth URL for %s/%s: %v", repoOwner, repoName, err)
			return
		}

		gitSync.CloneOrUpdateRawRepo(repoOwner, repoName, authURL, cfg)
	})

	gitSync.LogSyncSummary(&cfg)
	return nil
}

// getRepos fetches all accessible repositories for the authenticated user and applies filtering.
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

	// Apply filtering logic
	var reposToInclude []git.GitRepository
	for _, repo := range allRepos {
		repoName := derefString(repo.Name)
		projectName := derefString(repo.Project.Name)

		// Skip if essential fields are missing
		if projectName == "" || repoName == "" {
			logger.Warnf("Skipping repository with missing required fields: project=%q, name=%q", projectName, repoName)
			continue
		}

		// Check if repository is disabled (handle nil pointer safely)
		if repo.IsDisabled != nil && *repo.IsDisabled {
			logger.Warnf("Skipping repo %s/%s as it is disabled", projectName, repoName)
			continue
		}

		// Check include/exclude organizations (projects in Azure DevOps)
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

		// Check include/exclude repositories
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

		// Check fork inclusion
		// Note: Azure DevOps doesn't have a direct fork concept like GitHub,
		// but we can check if the repository is a fork by checking the parent repository reference
		isFork := repo.ParentRepository != nil
		if !cfg.IncludeForks && isFork {
			logger.Debug("[include_forks] Repo excluded: ", repoName)
			continue
		}

		logger.Debug("Repo included: ", repoName)
		reposToInclude = append(reposToInclude, repo)
	}

	return reposToInclude, nil
}

// getUserRepos fetches all accessible repositories for the authenticated user.
func (c *MSDevOpsClient) getUserRepos(ctx context.Context, client git.Client, cfg config.Config) ([]git.GitRepository, error) {
	allRepos, err := client.GetRepositories(ctx, git.GetRepositoriesArgs{
		Project: &cfg.Workspace, // Use the workspace from the config
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch repositories: %w", err)
	}

	if allRepos == nil {
		return []git.GitRepository{}, nil
	}

	return *allRepos, nil
}

