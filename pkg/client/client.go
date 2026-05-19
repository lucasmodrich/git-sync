package client

import (
	"context"

	"github.com/lucasmodrich/git-sync/pkg/config"
	"github.com/lucasmodrich/git-sync/pkg/token"
)

type Client interface {
	Sync(ctx context.Context, config config.Config) error
	GetTokenManager() *token.Manager
}
