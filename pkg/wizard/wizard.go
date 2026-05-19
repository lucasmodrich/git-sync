// Package wizard provides an interactive configuration wizard for git-sync.
package wizard

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/charmbracelet/huh"
)

// ErrNotSaved is returned when the user declines to save the configuration.
var ErrNotSaved = errors.New("configuration not saved")

// Run executes the interactive wizard and returns the resulting Config.
// Returns huh.ErrUserAborted if the user presses Ctrl+C, ErrNotSaved if they
// decline the final confirmation, or another error if the form fails.
func Run() (config.Config, error) {
	var (
		sourceType string
		platform   string

		// Per-platform auth — kept separate so defaults are pre-populated
		// without one platform's values bleeding into another's display.
		githubUsername string
		githubToken    string

		gitlabDomain   = "gitlab.com"
		gitlabUsername string
		gitlabToken    string

		bbWorkspace string
		bbUsername  string
		bbToken     string

		forgejoDomain   string
		forgejoUsername string
		forgejoToken    string

		giteaDomain   = "gitea.com"
		giteaUsername string
		giteaToken    string

		msdevopsDomain  = "dev.azure.com"
		msdevopsOrg     string
		msdevopsProject string
		msdevopsToken   string

		rawURLsInput string

		backupDir     = config.GetBackupDir("")
		cloneType     = "bare"
		includeForks  bool
		includeWiki   bool
		includeIssues bool

		wantAdvanced     bool
		concurrencyStr   = "5"
		retryCountStr    = "3"
		retryDelayStr    = "5"
		cronSchedule     string
		telemetryEnabled = true
	)

	isPlatform := func() bool { return sourceType != "raw" }
	isRaw := func() bool { return sourceType != "platform" }

	form := huh.NewForm(
		// ── Step 1: Source type ────────────────────────────────────────
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("What would you like to sync?").
				Options(
					huh.NewOption("A hosted platform (GitHub, GitLab, etc.)", "platform"),
					huh.NewOption("Raw Git URLs (no authentication needed)", "raw"),
					huh.NewOption("Both a platform and raw URLs", "both"),
				).
				Value(&sourceType),
		).Title("Source"),

		// ── Step 2: Platform selection ─────────────────────────────────
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select platform").
				Options(
					huh.NewOption("GitHub", "github"),
					huh.NewOption("GitLab", "gitlab"),
					huh.NewOption("Bitbucket", "bitbucket"),
					huh.NewOption("Forgejo", "forgejo"),
					huh.NewOption("Gitea", "gitea"),
					huh.NewOption("Azure DevOps (msdevops)", "msdevops"),
				).
				Value(&platform),
		).Title("Platform").
			WithHideFunc(func() bool { return !isPlatform() }),

		// ── Step 3a: GitHub ────────────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("GitHub username").
				Value(&githubUsername).
				Validate(notEmpty("username")),
			huh.NewInput().
				Title("Personal Access Token (classic)").
				Description("Requires the 'repo' scope. Fine-grained tokens are not supported for organisation repos.").
				EchoMode(huh.EchoModePassword).
				Value(&githubToken).
				Validate(notEmpty("token")),
		).Title("GitHub credentials").
			WithHideFunc(func() bool { return !isPlatform() || platform != "github" }),

		// ── Step 3b: GitLab ────────────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("GitLab server domain").
				Description("Use the default for GitLab.com, or enter your self-hosted instance domain.").
				Value(&gitlabDomain).
				Validate(notEmpty("domain")),
			huh.NewInput().
				Title("Username").
				Value(&gitlabUsername).
				Validate(notEmpty("username")),
			huh.NewInput().
				Title("Access Token").
				EchoMode(huh.EchoModePassword).
				Value(&gitlabToken).
				Validate(notEmpty("token")),
		).Title("GitLab credentials").
			WithHideFunc(func() bool { return !isPlatform() || platform != "gitlab" }),

		// ── Step 3c: Bitbucket ─────────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("Workspace slug").
				Description("Your Bitbucket workspace identifier (shown in the URL: bitbucket.org/<workspace>).").
				Value(&bbWorkspace).
				Validate(notEmpty("workspace")),
			huh.NewInput().
				Title("Username").
				Value(&bbUsername).
				Validate(notEmpty("username")),
			huh.NewInput().
				Title("App Password").
				Description("Create at bitbucket.org → Personal Settings → App Passwords. Requires Repositories: Read.").
				EchoMode(huh.EchoModePassword).
				Value(&bbToken).
				Validate(notEmpty("app password")),
		).Title("Bitbucket credentials").
			WithHideFunc(func() bool { return !isPlatform() || platform != "bitbucket" }),

		// ── Step 3d: Forgejo ───────────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("Forgejo server domain").
				Description("e.g. codeberg.org or your self-hosted instance domain.").
				Value(&forgejoDomain).
				Validate(notEmpty("domain")),
			huh.NewInput().
				Title("Username").
				Value(&forgejoUsername).
				Validate(notEmpty("username")),
			huh.NewInput().
				Title("Access Token").
				EchoMode(huh.EchoModePassword).
				Value(&forgejoToken).
				Validate(notEmpty("token")),
		).Title("Forgejo credentials").
			WithHideFunc(func() bool { return !isPlatform() || platform != "forgejo" }),

		// ── Step 3e: Gitea ─────────────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("Gitea server domain").
				Description("e.g. gitea.com or your self-hosted instance domain.").
				Value(&giteaDomain).
				Validate(notEmpty("domain")),
			huh.NewInput().
				Title("Username").
				Value(&giteaUsername).
				Validate(notEmpty("username")),
			huh.NewInput().
				Title("Access Token").
				EchoMode(huh.EchoModePassword).
				Value(&giteaToken).
				Validate(notEmpty("token")),
		).Title("Gitea credentials").
			WithHideFunc(func() bool { return !isPlatform() || platform != "gitea" }),

		// ── Step 3f: Azure DevOps ──────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("Azure DevOps domain").
				Description("Use dev.azure.com for Azure DevOps Services, or your on-premises domain.").
				Value(&msdevopsDomain).
				Validate(notEmpty("domain")),
			huh.NewInput().
				Title("Organization").
				Description("Your Azure DevOps organization name (appears in the URL after the domain).").
				Value(&msdevopsOrg).
				Validate(notEmpty("organization")),
			huh.NewInput().
				Title("Project").
				Description("The Azure DevOps project to sync repositories from.").
				Value(&msdevopsProject).
				Validate(notEmpty("project")),
			huh.NewInput().
				Title("Personal Access Token").
				Description("Requires Code: Read scope. Create at dev.azure.com → User Settings → Personal Access Tokens.").
				EchoMode(huh.EchoModePassword).
				Value(&msdevopsToken).
				Validate(notEmpty("token")),
		).Title("Azure DevOps credentials").
			WithHideFunc(func() bool { return !isPlatform() || platform != "msdevops" }),

		// ── Step 4: Raw Git URLs ───────────────────────────────────────
		huh.NewGroup(
			huh.NewText().
				Title("Raw Git URLs").
				Description("Enter one URL per line. Supports HTTPS and SSH. No authentication is applied.").
				Value(&rawURLsInput).
				Validate(func(s string) error {
					if strings.TrimSpace(s) == "" {
						return fmt.Errorf("at least one URL is required")
					}
					return nil
				}),
		).Title("Raw URLs").
			WithHideFunc(func() bool { return !isRaw() }),

		// ── Step 5: Common settings ────────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("Backup directory").
				Description("Local path where repositories will be stored.").
				Value(&backupDir).
				Validate(notEmpty("backup directory")),
			huh.NewSelect[string]().
				Title("Clone type").
				Description("Controls what git data is stored locally.").
				Options(
					huh.NewOption("bare   – git objects only, no working tree (recommended for backups)", "bare"),
					huh.NewOption("full   – full working tree", "full"),
					huh.NewOption("mirror – all refs and objects, stays in sync with remote", "mirror"),
					huh.NewOption("shallow – tip commit only, smallest possible", "shallow"),
				).
				Value(&cloneType),
			huh.NewConfirm().
				Title("Include forked repositories?").
				Value(&includeForks),
		).Title("Storage"),

		// ── Step 6: Wiki (not available for msdevops or raw-only) ─────
		huh.NewGroup(
			huh.NewConfirm().
				Title("Include wikis?").
				Description("Clone wiki repositories alongside each project.").
				Value(&includeWiki),
		).Title("Wikis").
			WithHideFunc(func() bool { return !isPlatform() || platform == "msdevops" }),

		// ── Step 7: Issues (GitHub and GitLab only) ────────────────────
		huh.NewGroup(
			huh.NewConfirm().
				Title("Include issues?").
				Description("Back up issues as JSON and Markdown files.").
				Value(&includeIssues),
		).Title("Issues").
			WithHideFunc(func() bool {
				return !isPlatform() || (platform != "github" && platform != "gitlab")
			}),

		// ── Step 8: Advanced settings gate ────────────────────────────
		huh.NewGroup(
			huh.NewConfirm().
				Title("Configure advanced settings?").
				Description("Concurrency, retry behaviour, cron schedule, and telemetry.").
				Value(&wantAdvanced),
		).Title("Advanced"),

		// ── Step 9: Advanced settings ──────────────────────────────────
		huh.NewGroup(
			huh.NewInput().
				Title("Concurrency").
				Description("Number of parallel clone/fetch operations (1–20).").
				Value(&concurrencyStr).
				Validate(validateInt(1, 20)),
			huh.NewInput().
				Title("Retry count").
				Description("Number of times to retry a failed operation before marking it as failed.").
				Value(&retryCountStr).
				Validate(validateInt(0, 100)),
			huh.NewInput().
				Title("Retry delay (seconds)").
				Description("Seconds to wait between retry attempts.").
				Value(&retryDelayStr).
				Validate(validateInt(0, 3600)),
			huh.NewInput().
				Title("Cron schedule (optional)").
				Description("Standard 5-field cron expression, e.g. '0 * * * *' for hourly. Leave blank for one-shot mode.").
				Value(&cronSchedule),
			huh.NewConfirm().
				Title("Enable telemetry?").
				Description("Sends anonymous usage data (platform, clone type, repo counts) to help improve git-sync. No credentials are ever transmitted.").
				Value(&telemetryEnabled),
		).Title("Advanced settings").
			WithHideFunc(func() bool { return !wantAdvanced }),
	)

	if err := form.Run(); err != nil {
		return config.Config{}, err
	}

	// ── Build config from wizard answers ──────────────────────────────

	var (
		username     string
		tokens       []string
		serverDomain string
		serverProto  = "https"
		organization string
		workspace    string
	)

	switch platform {
	case "github":
		username = githubUsername
		tokens = []string{githubToken}
		serverDomain = "github.com"
	case "gitlab":
		username = gitlabUsername
		tokens = []string{gitlabToken}
		serverDomain = gitlabDomain
	case "bitbucket":
		username = bbUsername
		tokens = []string{bbToken}
		workspace = bbWorkspace
		serverDomain = "bitbucket.org"
	case "forgejo":
		username = forgejoUsername
		tokens = []string{forgejoToken}
		serverDomain = forgejoDomain
	case "gitea":
		username = giteaUsername
		tokens = []string{giteaToken}
		serverDomain = giteaDomain
	case "msdevops":
		tokens = []string{msdevopsToken}
		serverDomain = msdevopsDomain
		organization = msdevopsOrg
		workspace = msdevopsProject
	}

	var rawURLs []string
	for _, u := range strings.Split(rawURLsInput, "\n") {
		if u = strings.TrimSpace(u); u != "" {
			rawURLs = append(rawURLs, u)
		}
	}

	concurrency, _ := strconv.Atoi(concurrencyStr)
	retryCount, _ := strconv.Atoi(retryCountStr)
	retryDelay, _ := strconv.Atoi(retryDelayStr)

	cfg := config.Config{
		Username: username,
		Tokens:   tokens,
		Platform: platform,
		Server: config.Server{
			Domain:       serverDomain,
			Protocol:     serverProto,
			Organization: organization,
		},
		Workspace:     workspace,
		BackupDir:     backupDir,
		CloneType:     cloneType,
		IncludeForks:  includeForks,
		IncludeWiki:   includeWiki,
		IncludeIssues: includeIssues,
		RawGitURLs:    rawURLs,
		Concurrency:   concurrency,
		Retry: config.RetryConfig{
			Count: retryCount,
			Delay: retryDelay,
		},
		Cron: cronSchedule,
		Telemetry: config.TelemetryConfig{
			Enabled: telemetryEnabled,
		},
	}

	// ── Summary + final confirm ────────────────────────────────────────
	printSummary(cfg)

	var confirm bool
	if err := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title("Save this configuration?").
				Value(&confirm),
		),
	).Run(); err != nil {
		return config.Config{}, err
	}

	if !confirm {
		return config.Config{}, ErrNotSaved
	}

	return cfg, nil
}

func printSummary(cfg config.Config) {
	fmt.Println()
	fmt.Println("── Configuration summary ────────────────────────────────────────")
	if cfg.Platform != "" {
		fmt.Printf("  Platform:       %s\n", cfg.Platform)
	}
	if cfg.Username != "" {
		fmt.Printf("  Username:       %s\n", cfg.Username)
	}
	if len(cfg.Tokens) > 0 && cfg.Tokens[0] != "" {
		fmt.Printf("  Token:          %s\n", redactToken(cfg.Tokens[0]))
	}
	if cfg.Server.Domain != "" {
		fmt.Printf("  Server:         %s://%s\n", cfg.Server.Protocol, cfg.Server.Domain)
	}
	if cfg.Server.Organization != "" {
		fmt.Printf("  Organization:   %s\n", cfg.Server.Organization)
	}
	if cfg.Workspace != "" {
		fmt.Printf("  Workspace:      %s\n", cfg.Workspace)
	}
	if len(cfg.RawGitURLs) > 0 {
		fmt.Printf("  Raw URLs:       %d URL(s)\n", len(cfg.RawGitURLs))
		for _, u := range cfg.RawGitURLs {
			fmt.Printf("    • %s\n", u)
		}
	}
	fmt.Printf("  Backup dir:     %s\n", cfg.BackupDir)
	fmt.Printf("  Clone type:     %s\n", cfg.CloneType)
	fmt.Printf("  Include forks:  %v\n", cfg.IncludeForks)
	if cfg.Platform != "msdevops" && cfg.Platform != "" {
		fmt.Printf("  Include wikis:  %v\n", cfg.IncludeWiki)
	}
	if cfg.Platform == "github" || cfg.Platform == "gitlab" {
		fmt.Printf("  Include issues: %v\n", cfg.IncludeIssues)
	}
	fmt.Printf("  Concurrency:    %d\n", cfg.Concurrency)
	fmt.Printf("  Retry:          %d attempts, %ds delay\n", cfg.Retry.Count, cfg.Retry.Delay)
	if cfg.Cron != "" {
		fmt.Printf("  Cron:           %s\n", cfg.Cron)
	}
	fmt.Printf("  Telemetry:      %v\n", cfg.Telemetry.Enabled)
	fmt.Println("─────────────────────────────────────────────────────────────────")
	fmt.Println()
}

// redactToken shows the first 4 characters of a token followed by asterisks.
func redactToken(t string) string {
	if len(t) <= 4 {
		return strings.Repeat("*", len(t))
	}
	return t[:4] + strings.Repeat("*", len(t)-4)
}

func notEmpty(field string) func(string) error {
	return func(s string) error {
		if strings.TrimSpace(s) == "" {
			return fmt.Errorf("%s cannot be empty", field)
		}
		return nil
	}
}

func validateInt(min, max int) func(string) error {
	return func(s string) error {
		n, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("must be a whole number")
		}
		if n < min || n > max {
			return fmt.Errorf("must be between %d and %d", min, max)
		}
		return nil
	}
}
