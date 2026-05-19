package sync

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/AkashRajpurohit/git-sync/pkg/config"
	"github.com/AkashRajpurohit/git-sync/pkg/issues"
	"github.com/AkashRajpurohit/git-sync/pkg/logger"
	"github.com/AkashRajpurohit/git-sync/pkg/token"
)

var tokenManager *token.Manager

func InitTokenManager(tokens []string) {
	tokenManager = token.NewManager(tokens)
}

func getBaseDirectoryPath(repoOwner, repoName string, config config.Config) string {
	return filepath.Join(config.BackupDir, repoOwner, repoName)
}

func getGitCloneCommand(cloneType, repoPath, repoURL string) *exec.Cmd {
	switch cloneType {
	case "bare":
		return exec.Command("git", "clone", "--bare", repoURL, repoPath)
	case "full":
		return exec.Command("git", "clone", repoURL, repoPath)
	case "mirror":
		return exec.Command("git", "clone", "--mirror", repoURL, repoPath)
	case "shallow":
		return exec.Command("git", "clone", "--depth", "1", repoURL, repoPath)
	default:
		return exec.Command("git", "clone", "--bare", repoURL, repoPath)
	}
}

func getGitFetchCommand(cloneType, repoPath, repoURL string) *exec.Cmd {
	switch cloneType {
	case "bare":
		return exec.Command("git", "--git-dir", repoPath, "fetch", "--prune", repoURL, "+*:*")
	case "full":
		return exec.Command("git", "-C", repoPath, "pull", "--prune", repoURL)
	case "mirror":
		return exec.Command("git", "-C", repoPath, "fetch", "--prune", repoURL, "+*:*")
	case "shallow":
		return exec.Command("git", "-C", repoPath, "pull", "--prune", repoURL)
	default:
		return exec.Command("git", "--git-dir", repoPath, "fetch", "--prune", repoURL, "+*:*")
	}
}

// buildAuthURL constructs a credential-embedded clone URL and a redacted
// variant safe for logging. username and password are URL-encoded by net/url.
func buildAuthURL(scheme, host, path, username, password string) (authURL, safeURL string) {
	u := &url.URL{
		Scheme: scheme,
		User:   url.UserPassword(username, password),
		Host:   host,
		Path:   path,
	}
	return u.String(), u.Redacted()
}

func CloneOrUpdateRepo(repoOwner, repoName string, config config.Config) {
	if tokenManager == nil {
		InitTokenManager(config.Tokens)
	}

	repoFullName := fmt.Sprintf("%s/%s", repoOwner, repoName)
	repoPath := filepath.Join(getBaseDirectoryPath(repoOwner, repoName, config), repoName+".git")
	authURL, safeURL := buildAuthURL(
		config.Server.Protocol,
		config.Server.Domain,
		"/"+repoFullName+".git",
		config.Username,
		tokenManager.GetNextToken(),
	)

	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		logger.Infof("Cloning repo: %s", safeURL)

		err := retryOperation(config, func() error {
			command := getGitCloneCommand(config.CloneType, repoPath, authURL)
			output, err := command.CombinedOutput()
			if err != nil {
				logger.Debugf("git clone output: %s", output)
			}
			return err
		}, fmt.Sprintf("clone %s", repoFullName))

		if err != nil {
			logger.Errorf("Failed to clone repo %s: %v", repoFullName, err)
			recordRepoFailure(repoFullName, err)
			return
		}

		logger.Info("Cloned repo: ", repoFullName)
		recordRepoSuccess()
	} else {
		logger.Info("Updating repo: ", repoFullName)

		err := retryOperation(config, func() error {
			command := getGitFetchCommand(config.CloneType, repoPath, authURL)
			output, err := command.CombinedOutput()
			if err != nil {
				logger.Debugf("git fetch output: %s", output)
			}
			return err
		}, fmt.Sprintf("update %s", repoFullName))

		if err != nil {
			logger.Errorf("Failed to update repo %s: %v", repoFullName, err)
			recordRepoFailure(repoFullName, err)
			return
		}

		logger.Info("Updated repo: ", repoFullName)
		recordRepoSuccess()
	}
}

func CloneOrUpdateRawRepo(repoOwner, repoName, repoURL string, config config.Config) {
	repoPath := filepath.Join(getBaseDirectoryPath(repoOwner, repoName, config), repoName+".git")
	repoLabel := fmt.Sprintf("%s/%s", repoOwner, repoName)

	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		logger.Infof("Cloning raw repo: %s", repoLabel)

		err := retryOperation(config, func() error {
			command := getGitCloneCommand(config.CloneType, repoPath, repoURL)
			output, err := command.CombinedOutput()
			if err != nil {
				logger.Debugf("git clone output: %s", output)
			}
			return err
		}, fmt.Sprintf("clone %s", repoLabel))

		if err != nil {
			logger.Errorf("Failed to clone raw repo %s: %v", repoLabel, err)
			recordRepoFailure(repoLabel, err)
			return
		}

		logger.Infof("Cloned raw repo: %s", repoLabel)
		recordRepoSuccess()
	} else {
		logger.Infof("Updating raw repo: %s", repoLabel)

		err := retryOperation(config, func() error {
			command := getGitFetchCommand(config.CloneType, repoPath, repoURL)
			output, err := command.CombinedOutput()
			if err != nil {
				logger.Debugf("git fetch output: %s", output)
			}
			return err
		}, fmt.Sprintf("update %s", repoLabel))

		if err != nil {
			logger.Errorf("Failed to update raw repo %s: %v", repoLabel, err)
			recordRepoFailure(repoLabel, err)
			return
		}

		logger.Infof("Updated raw repo: %s", repoLabel)
		recordRepoSuccess()
	}
}

func SyncWiki(repoOwner, repoName string, config config.Config) {
	if tokenManager == nil {
		InitTokenManager(config.Tokens)
	}

	repoFullName := fmt.Sprintf("%s/%s", repoOwner, repoName)
	repoWikiPath := filepath.Join(getBaseDirectoryPath(repoOwner, repoName, config), repoName+".wiki.git")

	wikiPath := "/" + repoFullName + ".wiki.git"
	if config.Platform == "bitbucket" {
		// Bitbucket wiki repos use a different URL pattern:
		// https://support.atlassian.com/bitbucket-cloud/docs/clone-a-wiki/
		wikiPath = "/" + repoFullName + ".git/wiki"
	}
	repoWikiURL, _ := buildAuthURL(config.Server.Protocol, config.Server.Domain, wikiPath, config.Username, tokenManager.GetNextToken())

	if _, err := os.Stat(repoWikiPath); os.IsNotExist(err) {
		logger.Info("Cloning wiki: ", repoFullName)
		wikiNotFound := false

		err := retryOperation(config, func() error {
			command := exec.Command("git", "clone", repoWikiURL, repoWikiPath)
			output, err := command.CombinedOutput()
			logger.Debugf("Output: %s\n", output)
			if err != nil && strings.Contains(string(output), "not found") {
				wikiNotFound = true
				// Don't retry for non-existent wikis
				return nil
			}
			return err
		}, fmt.Sprintf("clone wiki %s", repoFullName))

		if err != nil && !wikiNotFound {
			logger.Errorf("Failed to clone wiki %s: %v", repoFullName, err)
			recordWikiFailure(repoFullName, err)
			return
		}

		if wikiNotFound {
			logger.Warnf("The wiki for repository %s does not exist. Please check your repository settings and make sure that either wiki is disabled if it is not being used or create a wiki page to start with.", repoFullName)
		} else {
			logger.Info("Cloned wiki: ", repoFullName)
			recordWikiSuccess()
		}
	} else {
		logger.Info("Updating wiki: ", repoFullName)

		err := retryOperation(config, func() error {
			command := exec.Command("git", "-C", repoWikiPath, "pull", "--prune", "origin")
			output, err := command.CombinedOutput()
			logger.Debugf("Output: %s\n", output)
			return err
		}, fmt.Sprintf("update wiki %s", repoFullName))

		if err != nil {
			logger.Errorf("Failed to update wiki %s: %v", repoFullName, err)
			recordWikiFailure(repoFullName, err)
			return
		}

		logger.Info("Updated wiki: ", repoFullName)
		recordWikiSuccess()
	}
}

func SyncIssues(repoOwner, repoName string, allIssues []issues.Issue, cfg config.Config) {
	repoFullName := fmt.Sprintf("%s/%s", repoOwner, repoName)
	logger.Info("Syncing issues for: ", repoFullName)

	err := retryOperation(cfg, func() error {
		return issues.WriteIssues(cfg.BackupDir, repoOwner, repoName, allIssues)
	}, fmt.Sprintf("sync issues %s", repoFullName))

	if err != nil {
		logger.Errorf("Failed to sync issues for %s: %v", repoFullName, err)
		recordIssuesFailure(repoFullName, err)
		return
	}

	logger.Infof("Synced %d issues for %s", len(allIssues), repoFullName)
	recordIssuesSuccess()
}
