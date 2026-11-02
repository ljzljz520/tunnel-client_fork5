// Package runtimeapp wires the narrow customer execution surface without
// importing full-client command, UI, plugin, development, or cloudflared code.
package runtimeapp

import (
	"errors"

	controlplanefx "github.com/openai/tunnel-client/pkg/controlplane/fx"
	"github.com/openai/tunnel-client/pkg/dispatcher"
	"github.com/openai/tunnel-client/pkg/log"
	"github.com/openai/tunnel-client/pkg/mcpclient"
	"github.com/openai/tunnel-client/pkg/metrics"
	"github.com/openai/tunnel-client/pkg/oauth"
	"github.com/openai/tunnel-client/pkg/process"
	"github.com/openai/tunnel-client/pkg/runtimeconfig"
	"github.com/openai/tunnel-client/pkg/runtimeharpoon"
	"github.com/openai/tunnel-client/pkg/runtimehealth"
	"github.com/openai/tunnel-client/pkg/shutdown"
	"github.com/openai/tunnel-client/pkg/tlsconfig"
	"github.com/openai/tunnel-client/pkg/tracing"
	"go.uber.org/fx"
)

// Options returns the Fx options for the runtime-only client graph.
func Options(cfg *runtimeconfig.Config, opts ...fx.Option) []fx.Option {
	if cfg == nil {
		return append([]fx.Option{fx.Error(errors.New("tunnel-client runtime config is nil"))}, opts...)
	}
	resolved := *cfg
	cfg = &resolved
	cfg.ControlPlane.TraceContextEnabled = cfg.Tracing.Enabled
	cfg.MCP.TraceContextEnabled = cfg.Tracing.Enabled
	base := []fx.Option{
		fx.Supply(
			cfg,
			&cfg.ControlPlane,
			&cfg.Logging,
			&cfg.Tracing,
			&cfg.Health,
			&cfg.Process,
			&cfg.MCP,
			&cfg.Harpoon,
		),
		fx.Provide(func() *tlsconfig.Bundle { return cfg.TLS }),
		// Shared bounded-drain contract (CLI and SDK use the same graph).
		shutdown.Module(cfg.ControlPlane.ShutdownDrainTimeoutOrDefault()),
		log.Module,
		tracing.Module,
		dispatcher.Module,
		controlplanefx.Module,
		mcpclient.Module,
		metrics.MetricModule,
		oauth.Module,
		process.Module,
		runtimehealth.Module,
		runtimeharpoon.Module,
		fx.Invoke(tlsconfig.LogTrustReport),
		fx.Invoke(func(runtimehealth.Service) {}),
	}
	return append(base, opts...)
}

// New constructs a runtime-only tunnel-client Fx app.
func New(cfg *runtimeconfig.Config, opts ...fx.Option) *fx.App {
	return fx.New(Options(cfg, opts...)...)
}
