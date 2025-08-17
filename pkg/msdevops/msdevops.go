// Package msdevops provides functionality for synchronizing Azure DevOps repositories.
// It implements the platform-specific client interface for Azure DevOps integration.
package msdevops

import (
	"context"
	"fmt"

	"github.com/AkashRajpurohit/git-sync/pkg/config"
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

// createClient initializes and returns a new Azure DevOps Git client using the provided token manager and server configuration.
func (c *MSDevOpsClient) createClient(ctx context.Context) (git.Client, error) {
	if c.serverConfig.Protocol == "" || c.serverConfig.Domain == "" {
		return nil, fmt.Errorf("invalid server configuration: protocol and domain must be specified")
	}

	organizationURL := fmt.Sprintf("%s://%s", c.serverConfig.Protocol, c.serverConfig.Domain)
	logger.Debugf("Creating Azure DevOps client for: %s", organizationURL)

	token := c.tokenManager.GetNextToken()
	if token == "" {
		return nil, fmt.Errorf("a valid token was not available")
	}

	connection := azuredevops.NewPatConnection(organizationURL, token)
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

// Sync synchronizes all accessible Azure DevOps repositories based on the provided configuration.
func (c *MSDevOpsClient) Sync(cfg config.Config) error {
	ctx := context.Background()

	repos, err := c.getUserRepos(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to get user repositories: %w", err)
	}

	gitSync.LogRepoCount(len(repos), cfg.Platform)

	gitSync.SyncWithConcurrency(cfg, repos, func(repo git.GitRepository) {
		repoOwner := derefString(repo.Project.Name)
		repoName := derefString(repo.Name)
		repoURL := derefString(repo.WebUrl)

		// Skip if essential fields are missing
		if repoOwner == "" || repoName == "" || repoURL == "" {
			logger.Warnf("Skipping repository with missing required fields: owner=%q, name=%q, url=%q", repoOwner, repoName, repoURL)
			return
		}

		// Check if repository is disabled (handle nil pointer safely)
		if repo.IsDisabled != nil && *repo.IsDisabled {
			logger.Warnf("Skipping repo %s/%s as it is disabled", repoOwner, repoName)
			return
		}

		protoLen := len(cfg.Server.Protocol + "://")
		if protoLen >= len(repoURL) {
			logger.Errorf("Invalid repository URL format: %s", repoURL)
			return
		}

		// Get token once per repository operation
		token := c.tokenManager.GetNextToken()
		if token == "" {
			logger.Errorf("No valid token available for repository %s/%s", repoOwner, repoName)
			return
		}

		// Construct authenticated URL safely
		repoAuthURL := repoURL[:protoLen] + token + "@" + repoURL[protoLen:]
		gitSync.CloneOrUpdateRawRepo(repoOwner, repoName, repoAuthURL, cfg)
	})

	gitSync.LogSyncSummary(&cfg)
	return nil
}

// getUserRepos fetches all accessible repositories for the authenticated user.
func (c *MSDevOpsClient) getUserRepos(ctx context.Context, cfg config.Config) ([]git.GitRepository, error) {
	logger.Debug("Fetching list of repositories ⏳")

	client, err := c.createClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure DevOps client: %w", err)
	}

	allRepos, err := client.GetRepositories(ctx, git.GetRepositoriesArgs{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch repositories: %w", err)
	}

	if allRepos == nil {
		return []git.GitRepository{}, nil
	}

	return *allRepos, nil
}
