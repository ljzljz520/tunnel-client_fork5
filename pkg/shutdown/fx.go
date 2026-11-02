package shutdown

import (
	"time"

	"go.uber.org/fx"
)

// ReclamationMargin is added to the drain budget when setting the fx stop
// deadline, so terminal resources (worker pools, stdio children) still have
// room to be reclaimed after the drain ends — forcefully when it timed out.
const ReclamationMargin = 15 * time.Second

// FxStopTimeout returns the total fx shutdown deadline covering the bounded
// drain plus resource reclamation.
func FxStopTimeout(drainTimeout time.Duration) time.Duration {
	if drainTimeout <= 0 {
		drainTimeout = DefaultDrainTimeout
	}
	return drainTimeout + ReclamationMargin
}

// Module provides the process-wide shutdown Coordinator and sizes the fx stop
// deadline so the bounded drain and final resource reclamation fit within it.
// Both the CLI and the embedded SDK graphs install it, so every component
// shares the same shutdown contract.
func Module(drainTimeout time.Duration) fx.Option {
	return fx.Options(
		fx.Provide(NewCoordinator),
		fx.StopTimeout(FxStopTimeout(drainTimeout)),
	)
}
