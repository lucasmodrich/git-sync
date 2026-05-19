package config

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/robfig/cron/v3"
)

// validateGitURL validates if the provided URL is a valid git repository URL
func validateGitURL(rawURL string) error {
	// Handle SSH URLs (git@github.com:user/repo.git)
	if strings.HasPrefix(rawURL, "git@") {
		parts := strings.Split(rawURL, ":")
		if len(parts) != 2 || parts[1] == "" {
			return fmt.Errorf("invalid SSH git URL format: %s", rawURL)
		}
		return nil
	}

	// Handle HTTPS/HTTP URLs
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid git URL: %s", rawURL)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("git URL must use http, https, or SSH (git@) protocol: %s", rawURL)
	}

	if u.Host == "" || u.Path == "" || u.Path == "/" {
		return fmt.Errorf("git URL must include a host and repository path: %s", rawURL)
	}

	return nil
}

func ValidateConfig(cfg Config) error {
	// Validate backup directory (required for all cases)
	if cfg.BackupDir == "" {
		return fmt.Errorf("backup directory cannot be empty")
	}

	// Validate clone type (required for all cases)
	if cfg.CloneType != "bare" && cfg.CloneType != "full" && cfg.CloneType != "mirror" && cfg.CloneType != "shallow" {
		return fmt.Errorf("clone_type can only be `bare`, `full`, `mirror` or `shallow`")
	}

	// Validate concurrency
	if cfg.Concurrency < 1 || cfg.Concurrency > 20 {
		return fmt.Errorf("concurrency must be between 1 and 20")
	}

	// Validate cron if provided
	if cfg.Cron != "" {
		_, err := cron.ParseStandard(cfg.Cron)
		if err != nil {
			return fmt.Errorf("invalid cron expression %s", cfg.Cron)
		}
	}

	if cfg.DryRun && cfg.Cron != "" {
		return fmt.Errorf("--dry-run cannot be combined with a cron schedule")
	}

	// Validate raw git URLs if provided
	for _, url := range cfg.RawGitURLs {
		if err := validateGitURL(url); err != nil {
			return err
		}
	}

	// If there are no raw git URLs, validate platform-specific configuration
	if len(cfg.RawGitURLs) == 0 {
		// Username is required for all platforms except msdevops, which authenticates
		// using a PAT embedded in the clone URL with no username component.
		if cfg.Username == "" && cfg.Platform != "msdevops" {
			return fmt.Errorf("username cannot be empty when no raw git URLs are provided")
		}

		// At least one token is required for platform-specific sync
		if len(cfg.Tokens) == 0 && cfg.Token == "" {
			return fmt.Errorf("at least one token must be provided when no raw git URLs are provided. See here: https://github.com/lucasmodrich/git-sync/wiki/Configuration")
		}

		if cfg.Platform != "github" && cfg.Platform != "gitlab" && cfg.Platform != "bitbucket" && cfg.Platform != "forgejo" && cfg.Platform != "gitea" && cfg.Platform != "msdevops" {
			return fmt.Errorf("platform can only be `github`, `gitlab`, `bitbucket`, `forgejo`, `gitea`, or `msdevops` when no raw git URLs are provided")
		}

		// Server configuration is required for platform-specific sync
		if cfg.Server.Domain == "" {
			return fmt.Errorf("server domain cannot be empty when no raw git URLs are provided")
		}

		if cfg.Server.Protocol != "https" && cfg.Server.Protocol != "http" {
			return fmt.Errorf("server protocol can only be http or https")
		}

		// Workspace is required for Bitbucket and Azure DevOps
		if (cfg.Platform == "bitbucket" || cfg.Platform == "msdevops") && cfg.Workspace == "" {
			return fmt.Errorf("workspace cannot be empty for %s", cfg.Platform)
		}

		// Organization (server.organization) is required for Azure DevOps — it forms part of the
		// API base URL: https://<domain>/<organization>
		if cfg.Platform == "msdevops" && cfg.Server.Organization == "" {
			return fmt.Errorf("server.organization cannot be empty for msdevops")
		}

		// Feature flags not yet implemented for Azure DevOps.
		// ADO wikis are a separate API resource; ADO Work Items require a different data model.
		// Enabling these flags would silently produce no output, so we fail fast instead.
		if cfg.Platform == "msdevops" && cfg.IncludeWiki {
			return fmt.Errorf("include_wiki is not yet supported for msdevops")
		}
		if cfg.Platform == "msdevops" && cfg.IncludeIssues {
			return fmt.Errorf("include_issues is not supported for msdevops (Azure DevOps Work Items require a separate integration)")
		}
	}

	return nil
}
