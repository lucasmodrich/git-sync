package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/logger"
	"github.com/lucasmodrich/git-sync/pkg/wizard"
	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
)

var forceInit bool

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Interactively create a git-sync configuration file",
	Long:  "Run a step-by-step wizard to create a valid git-sync configuration file.",
	Run: func(cmd *cobra.Command, args []string) {
		logger.InitLogger(logLevel)

		configPath := config.GetConfigFile(cfgFile)

		if _, err := os.Stat(configPath); err == nil && !forceInit {
			logger.Fatalf("Config file already exists at %s. Run with --force to overwrite.", configPath)
		}

		fmt.Printf("Welcome to git-sync! Let's set up your configuration.\n")
		fmt.Printf("Config will be saved to: %s\n\n", configPath)

		cfg, err := wizard.Run()
		if err != nil {
			switch {
			case errors.Is(err, huh.ErrUserAborted):
				fmt.Println("\nSetup cancelled.")
			case errors.Is(err, wizard.ErrNotSaved):
				fmt.Println("\nConfiguration not saved. Run 'git-sync init' to start again.")
			default:
				logger.Fatalf("Configuration wizard failed: %v", err)
			}
			return
		}

		if err := config.SaveConfig(cfg, cfgFile); err != nil {
			logger.Fatalf("Failed to save configuration: %v", err)
		}

		fmt.Printf("✅ Configuration saved to %s\n", configPath)
		fmt.Printf("Run 'git-sync' to start syncing your repositories.\n")
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().BoolVar(&forceInit, "force", false, "overwrite an existing configuration file")
}
