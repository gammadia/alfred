package local

import (
	"log/slog"
)

type Config struct {
	// Logger to use
	Logger *slog.Logger `json:"-"`
	// Path to workspace
	Workspace string `json:"workspace"`
}
