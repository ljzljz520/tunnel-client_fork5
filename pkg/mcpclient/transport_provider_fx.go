package mcpclient

import (
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/fx"

	"github.com/openai/tunnel-client/pkg/shutdown"
)

type injectableProviderParams struct {
	fx.In

	Transport mcp.Transport `name:"mcp_injected_transport" optional:"true"`
}

func newInjectableTransportProvider(p injectableProviderParams) TransportProvider {
	return injectableTransportProvider{transport: p.Transport}
}

type stdioProviderParams struct {
	fx.In

	CommandFactory *stdioCommandTransportFactory
}

func newStdioTransportProvider(p stdioProviderParams) TransportProvider {
	return stdioTransportProvider{
		commandFactory: p.CommandFactory,
	}
}

type stdioCommandTransportParams struct {
	fx.In

	Lifecycle   fx.Lifecycle
	Shutdowner  fx.Shutdowner
	Logger      *slog.Logger
	Observation *ProtocolObservation  `optional:"true"`
	Coordinator *shutdown.Coordinator `optional:"true"`
}

func newStdioCommandTransportFactoryProvider(p stdioCommandTransportParams) *stdioCommandTransportFactory {
	factory := newStdioCommandTransportFactory(p.Logger, p.Lifecycle, p.Shutdowner)
	factory.observation = p.Observation
	factory.coordinator = p.Coordinator
	return factory
}

func newChannelStdioRuntimeInfoProvider(factory *stdioCommandTransportFactory) ChannelStdioRuntimeInfoProvider {
	if factory == nil {
		return nil
	}
	return factory
}
