// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"log"
	"log/slog"
	"strings"
)

// LogModule is the module attribute on every MCP log record, so the relayer's
// process log can be filtered to MCP lines with module=mcp.
const LogModule = "mcp"

// Logger returns the relayer logger tagged with module=mcp. It reads the
// default logger on each call, so it follows the logger set at startup.
func Logger() *slog.Logger { return slog.Default().With("module", LogModule) }

// processLogWriter turns one standard-log write into relayer log records,
// one per line, so a recovered panic stack is kept.
type processLogWriter struct {
	logger *slog.Logger
}

// processErrorLog routes net/http's recovered panics and accept errors to logger
// instead of the standard log package.
func processErrorLog(logger *slog.Logger) *log.Logger {
	if logger == nil {
		logger = Logger()
	}
	return log.New(processLogWriter{logger: logger}, "", 0)
}

func (w processLogWriter) Write(p []byte) (int, error) {
	text := strings.TrimRight(string(p), "\r\n")
	if text == "" {
		if len(p) > 0 {
			w.logger.Error("")
		}
		return len(p), nil
	}
	for _, line := range strings.Split(text, "\n") {
		w.logger.Error(strings.TrimRight(line, "\r"))
	}
	return len(p), nil
}
