package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/lucasmodrich/git-sync/pkg/bitbucket"
	"github.com/lucasmodrich/git-sync/pkg/client"
	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/forgejo"
	"github.com/lucasmodrich/git-sync/pkg/github"
	"github.com/lucasmodrich/git-sync/pkg/gitlab"
	"github.com/lucasmodrich/git-sync/pkg/logger"
	"github.com/lucasmodrich/git-sync/pkg/msdevops"
	"github.com/lucasmodrich/git-sync/pkg/raw"
	"github.com/lucasmodrich/git-sync/pkg/telemetry"
	ch "github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
)

var (
	cfgFile   string
	backupDir string
	logLevel  string = "info"
	cron      string
	dryRun    bool
)

var rootCmd = &cobra.Command{
	Use:   "git-sync",
	Short: "A tool to backup and sync your git repositories",
	Run: func(cmd *cobra.Command, args []string) {
		logger.InitLogger(logLevel)

		configPath := config.GetConfigFile(cfgFile)
		var cfg config.Config

		// Check if config file exists
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			logger.Info("Config file not found, creating a new one...")
			cfg = config.GetInitialConfig()

			err = config.SaveConfig(cfg, cfgFile)
			if err != nil {
				logger.Fatal("Error in saving config file: ", err)
			}
			logger.Infof("Created new config file at: %s", configPath)
			logger.Info("Please update the configuration according to your needs. See: https://github.com/lucasmodrich/git-sync/wiki/Configuration")
			return
		}

		// Load existing config
		cfg, err := config.LoadConfig(cfgFile)
		if err != nil {
			if _, ok := err.(*config.InvalidConfigError); ok {
				logger.Errorf("Invalid configuration: %v", err)
				logger.Info("Please check for correct configuration format at: https://github.com/lucasmodrich/git-sync/wiki/Configuration")
				return
			}
			logger.Fatalf("Error loading config file: %v", err)
		}

		config.SetSensibleDefaults(&cfg)

		// If backupDir option is passed in the command line, use that instead of the one in the config file
		if backupDir != "" {
			cfg.BackupDir = config.GetBackupDir(backupDir)
		}

		// If cron option is passed in the command line, use that instead of the one in the config file
		if cron != "" {
			cfg.Cron = cron
		}

		cfg.DryRun = dryRun

		logger.Info("Config loaded from: ", configPath)
		logger.Debug("Validating config ⏳")

		err = config.ValidateConfig(cfg)
		if err != nil {
			logger.Fatalf("Error validating config: %s", err)
		}

		telemetry.Init(cfg.Telemetry)
		defer telemetry.Close()

		// Create backup directory if it doesn't exist
		os.MkdirAll(cfg.BackupDir, os.ModePerm)

		var platformClient client.Client
		var hasRawURLs bool = len(cfg.RawGitURLs) > 0

		// Only initialize platform client if raw URLs are not provided or if both are needed
		if !hasRawURLs || (cfg.Username != "" && len(cfg.Tokens) > 0) {
			switch cfg.Platform {
			case "github":
				platformClient = github.NewGitHubClient(cfg.Tokens)
			case "gitlab":
				platformClient = gitlab.NewGitlabClient(cfg.Server, cfg.Tokens)
			case "bitbucket":
				platformClient = bitbucket.NewBitbucketClient(cfg.Username, cfg.Tokens)
			case "forgejo", "gitea":
				// Forgejo and Gitea have same API, so we can use the same client
				platformClient = forgejo.NewForgejoClient(cfg.Server, cfg.Tokens)
			case "msdevops":
				platformClient = msdevops.NewMSDevOpsClient(cfg.Server, cfg.Tokens)
			default:
				if !hasRawURLs {
					logger.Fatalf("Platform %s not supported", cfg.Platform)
				}
			}
		}

		logger.Info("✅ Valid config found")
		if platformClient != nil {
			logger.Infof("Using Platform: %s", cfg.Platform)
		}
		if hasRawURLs {
			logger.Infof("Found %d raw git URLs to sync", len(cfg.RawGitURLs))
		}

		// rootCtx is cancelled when a shutdown signal is received; both API calls and
		// git subprocesses (via pkg/sync's per-attempt timeout and cancellation) respect it.
		rootCtx, cancelRoot := context.WithCancel(context.Background())
		defer cancelRoot()

		runSync := func(ctx context.Context) {
			if platformClient != nil {
				if err := platformClient.Sync(ctx, cfg); err != nil {
					logger.Errorf("Error syncing platform repositories: %s", err)
				}
			}
			if hasRawURLs {
				rawClient := raw.NewRawClient()
				if err := rawClient.Sync(ctx, cfg); err != nil {
					logger.Errorf("Error syncing raw repositories: %s", err)
				}
			}
		}

		if cfg.Cron != "" {
			var syncMu sync.Mutex
			c := ch.New()
			_, err := c.AddFunc(cfg.Cron, func() {
				if !syncMu.TryLock() {
					logger.Warn("Previous sync still running, skipping this cron tick")
					return
				}
				defer syncMu.Unlock()
				runSync(rootCtx)
			})
			if err != nil {
				logger.Fatalf("Error adding cron job: %s", err)
			}

			c.Start()
			logger.Infof("Cron job scheduled to run at: %s", cfg.Cron)

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			<-quit
			cancelRoot()
			logger.Info("Shutdown signal received, stopping cron scheduler...")
			cronCtx := c.Stop()
			<-cronCtx.Done()
			logger.Info("Cron scheduler stopped")
		} else {
			runSync(rootCtx)
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.config/git-sync/config.yaml)")
	rootCmd.PersistentFlags().StringVar(&backupDir, "backup-dir", "", "directory to backup repositories (default is $HOME/git-backups)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error, fatal)")
	rootCmd.PersistentFlags().StringVar(&cron, "cron", "", "cron expression to run the sync job periodically")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "show what would be synced without performing any git operations")
}
