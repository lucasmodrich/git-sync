package config

import (
	"testing"
)

func TestValidateGitURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name:    "Valid HTTPS URL",
			url:     "https://github.com/user/repo.git",
			wantErr: false,
		},
		{
			name:    "Valid SSH URL",
			url:     "git@github.com:user/repo.git",
			wantErr: false,
		},
		{
			name:    "Valid HTTPS URL - No .git suffix",
			url:     "https://github.com/user/repo",
			wantErr: false,
		},
		{
			name:    "Invalid HTTPS URL - Missing path",
			url:     "https://github.com",
			wantErr: true,
		},
		{
			name:    "Invalid SSH URL - Wrong format",
			url:     "git@github.com/user/repo.git",
			wantErr: true,
		},
		{
			name:    "Invalid Protocol",
			url:     "ftp://github.com/user/repo.git",
			wantErr: true,
		},
		{
			name:    "Invalid URL format",
			url:     "not-a-url",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateGitURL(tt.url); (err != nil) != tt.wantErr {
				t.Errorf("validateGitURL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "Valid Config with Single Token",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Token:    "test",
			},
			wantErr: false,
		},
		{
			name: "Valid Config with Multiple Tokens",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Tokens:   []string{"token1", "token2", "token3"},
			},
			wantErr: false,
		},
		{
			name: "Valid Config with Both Token and Tokens",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Token:    "single_token",
				Tokens:   []string{"token1", "token2"},
			},
			wantErr: false,
		},
		{
			name: "Invalid Config - No Tokens",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
			},
			wantErr: true,
		},
		{
			name: "Invalid Concurrency - Zero",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 0,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Tokens:   []string{"token1"},
			},
			wantErr: true,
		},
		{
			name: "Invalid Concurrency - Negative",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: -1,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Tokens:   []string{"token1"},
			},
			wantErr: true,
		},
		{
			name: "Invalid Timeout - Zero",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     0,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Tokens:   []string{"token1"},
			},
			wantErr: true,
		},
		{
			name: "Invalid Timeout - Negative",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     -1,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Tokens:   []string{"token1"},
			},
			wantErr: true,
		},
		{
			name: "Valid Concurrency - Custom",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 10,
				Timeout:     30,
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Username: "test",
				Tokens:   []string{"token1"},
			},
			wantErr: false,
		},
		{
			name: "Empty BackupDir",
			cfg: Config{
				BackupDir: "",
			},
			wantErr: true,
		},
		{
			name: "Invalid Clone Type",
			cfg: Config{
				BackupDir: "test",
				CloneType: "invalid",
			},
			wantErr: true,
		},
		{
			name: "Invalid Cron Expression",
			cfg: Config{
				BackupDir: "test",
				CloneType: "bare",
				Cron:      "invalid",
			},
			wantErr: true,
		},
		{
			name: "Valid Raw Git URLs Only",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				RawGitURLs: []string{
					"https://github.com/user/repo1.git",
					"git@github.com:user/repo2.git",
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid Raw Git URL",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				RawGitURLs: []string{
					"ftp://github.com/user/repo1", // Invalid protocol
					"git@github.com:user/repo2.git",
				},
			},
			wantErr: true,
		},
		{
			name: "Empty Username with No Raw URLs",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Tokens:      []string{"token1"},
				Platform:    "github",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
			},
			wantErr: true,
		},
		{
			name: "Empty Server Domain with No Raw URLs",
			cfg: Config{
				Username:    "test",
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Platform:    "github",
				Concurrency: 5,
				Server: Server{
					Protocol: "https",
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid Server Protocol with No Raw URLs",
			cfg: Config{
				Username:    "test",
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Server: Server{
					Domain:   "test",
					Protocol: "ftp",
				},
			},
			wantErr: true,
		},
		{
			name: "Empty Platform",
			cfg: Config{
				Username:    "test",
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
			},
			wantErr: true,
		},
		{
			name: "Empty Workspace for Bitbucket with No Raw URLs",
			cfg: Config{
				Username:    "test",
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Platform:    "bitbucket",
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},
				Workspace: "",
			},
			wantErr: true,
		},
		{
			name: "Valid msdevops Config",
			cfg: Config{
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				Platform:    "msdevops",
				Server: Server{
					Domain:       "dev.azure.com",
					Protocol:     "https",
					Organization: "my-org",
				},
				Workspace: "my-project",
			},
			wantErr: false,
		},
		{
			name: "msdevops missing organization",
			cfg: Config{
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Platform:    "msdevops",
				Server: Server{
					Domain:   "dev.azure.com",
					Protocol: "https",
				},
				Workspace: "my-project",
			},
			wantErr: true,
		},
		{
			name: "msdevops missing workspace",
			cfg: Config{
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Platform:    "msdevops",
				Server: Server{
					Domain:       "dev.azure.com",
					Protocol:     "https",
					Organization: "my-org",
				},
			},
			wantErr: true,
		},
		{
			name: "msdevops include_wiki not supported",
			cfg: Config{
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Platform:    "msdevops",
				Server: Server{
					Domain:       "dev.azure.com",
					Protocol:     "https",
					Organization: "my-org",
				},
				Workspace:   "my-project",
				IncludeWiki: true,
			},
			wantErr: true,
		},
		{
			name: "msdevops include_issues not supported",
			cfg: Config{
				Tokens:      []string{"token1"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Platform:    "msdevops",
				Server: Server{
					Domain:       "dev.azure.com",
					Protocol:     "https",
					Organization: "my-org",
				},
				Workspace:     "my-project",
				IncludeIssues: true,
			},
			wantErr: true,
		},
		{
			name: "DryRun with Cron is rejected",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Cron:        "0 * * * *",
				DryRun:      true,
				RawGitURLs:  []string{"https://github.com/user/repo.git"},
			},
			wantErr: true,
		},
		{
			name: "DryRun without Cron is valid",
			cfg: Config{
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				DryRun:      true,
				RawGitURLs:  []string{"https://github.com/user/repo.git"},
			},
			wantErr: false,
		},
		{
			name: "Valid Mixed Config (Platform + Raw URLs)",
			cfg: Config{
				Username:    "test",
				Tokens:      []string{"token1", "token2"},
				BackupDir:   "test",
				CloneType:   "bare",
				Concurrency: 5,
				Timeout:     30,
				Server: Server{
					Domain:   "test",
					Protocol: "https",
				},

				RawGitURLs: []string{
					"https://github.com/user/repo1.git",
					"git@github.com:user/repo2.git",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateConfig(tt.cfg); (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
