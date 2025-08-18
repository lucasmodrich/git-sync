// Package msdevops provides functionality for synchronizing Azure DevOps repositories.
// It implements the platform-specific client interface for Azure DevOps integration.
package msdevops

import (
	"context"
	"fmt"
	"strings"

	"github.com/AkashRajpurohit/git-sync/pkg/config"
	"github.com/AkashRajpurohit/git-sync/pkg/helpers"
	"github.com/AkashRajpurohit/git-sync/pkg/logger"
	gitSync "github.com/AkashRajpurohit/git-sync/pkg/sync"
	"github.com/AkashRajpurohit/git-sync/pkg/token"
	"github.com/microsoft/azure-devops-go-api/azuredevops/v7"
	"github.com/microsoft/azure-devops-go-api/azuredevops/v7/git"
	"github.com/microsoft/azure-devops-go-api/azuredevops/v7/wiki"
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

// maskToken masks the token in a URL for safe logging.
func maskToken(url string) string {
	// Find the token part (between :// and @)
	atIndex := strings.Index(url, "@")
	if atIndex == -1 {
		return url
	}

	protocolEnd := strings.Index(url, "://")
	if protocolEnd == -1 {
		return url
	}

	// If there's content between protocol and @, it's likely a token
	if atIndex > protocolEnd+3 {
		// Mask everything between protocol and @
		return url[:protocolEnd+3] + "****" + url[atIndex:]
	}

	return url
}

// createConnection creates a new Azure DevOps connection using the provided token manager and server configuration.
func (c *MSDevOpsClient) createConnection() (*azuredevops.Connection, error) {
	if c.serverConfig.Protocol == "" || c.serverConfig.Domain == "" {
		return nil, fmt.Errorf("invalid server configuration: protocol and domain must be specified")
	}

	organizationURL := fmt.Sprintf("%s://%s", c.serverConfig.Protocol, c.serverConfig.Domain)
	logger.Debugf("Creating Azure DevOps connection for: %s", maskToken(organizationURL))

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

// createWikiClient initializes and returns a new Azure DevOps Wiki client using the provided token manager and server configuration.
func (c *MSDevOpsClient) createWikiClient(ctx context.Context) (wiki.Client, error) {
	connection, err := c.createConnection()
	if err != nil {
		return nil, err
	}

	client, err := wiki.NewClient(ctx, connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure DevOps wiki client: %w", err)
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

		gitSync.CloneOrUpdateRepo(repoOwner, repoName, cfg)

		// Check if wiki synchronization is enabled
		if cfg.IncludeWiki {
			c.syncWiki(repo, cfg)
		}
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

	// Get all repositories with retry logic for token rotation
	var allRepos []git.GitRepository
	for {
		repos, err := c.getUserRepos(ctx, client, cfg)
		if err != nil {
			logger.Debugf("Error with current token, trying next token: %v", err)
			// Create a new client with the next token
			client, err = c.createClient(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to create Azure DevOps client with new token: %w", err)
			}
			continue
		}
		allRepos = repos
		break
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

// syncWiki synchronizes the wiki for a repository if it exists.
func (c *MSDevOpsClient) syncWiki(repo git.GitRepository, cfg config.Config) {
	ctx := context.Background()

	// Create a wiki client for wiki operations
	client, err := c.createWikiClient(ctx)
	if err != nil {
		logger.Errorf("Failed to create Azure DevOps wiki client for wiki sync: %v", err)
		return
	}

	repoOwner := derefString(repo.Project.Name)
	repoName := derefString(repo.Name)

	// Get wiki information
	wikis, err := client.GetAllWikis(ctx, wiki.GetAllWikisArgs{
		Project: repo.Project.Name,
	})
	if err != nil {
		logger.Debugf("Failed to get wikis for repository %s/%s: %v", repoOwner, repoName, err)
		return
	}

	if wikis == nil || len(*wikis) == 0 {
		logger.Debugf("No wikis found for repository %s/%s", repoOwner, repoName)
		return
	}

	// Use the first wiki (Azure DevOps typically has one wiki per repository)
	wikiObj := (*wikis)[0]

	// Get the remote URL directly from the wiki object
	wikiURL := derefString(wikiObj.RemoteUrl)
	if wikiURL == "" {
		logger.Warnf("Wiki URL is empty for repository %s/%s", repoOwner, repoName)
		return
	}

	// Construct authenticated URL
	protoLen := len(cfg.Server.Protocol + "://")
	if protoLen >= len(wikiURL) {
		logger.Errorf("Invalid wiki URL format: %s", wikiURL)
		return
	}

	token := c.tokenManager.GetNextToken()
	if token == "" {
		logger.Errorf("No valid token available for wiki %s/%s", repoOwner, repoName)
		return
	}

	// Construct authenticated URL safely
	wikiAuthURL := wikiURL[:protoLen] + token + "@" + wikiURL[protoLen:]

	logger.Info("Syncing wiki for repo: ", repoName)
	gitSync.CloneOrUpdateRawRepo(repoOwner, repoName+".wiki", wikiAuthURL, cfg)
}

/*
// createAuthenticatedURL constructs an authenticated URL for Git operations.
func (c *MSDevOpsClient) createAuthenticatedURL(baseURL string, cfg config.Config) string {
	protoLen := len(cfg.Server.Protocol + "://")
	if protoLen >= len(baseURL) {
		return baseURL
	}

	token := c.tokenManager.GetNextToken()
	if token == "" {
		return baseURL
	}

	// Handle different URL formats
	if strings.Contains(baseURL, "@") {
		// URL already contains authentication, replace it
		atIndex := strings.Index(baseURL, "@")
		protocolEnd := strings.Index(baseURL, "://") + 3
		return baseURL[:protocolEnd] + token + "@" + baseURL[atIndex+1:]
	}

	// Add authentication to URL
	return baseURL[:protoLen] + token + "@" + baseURL[protoLen:]
}
*/
