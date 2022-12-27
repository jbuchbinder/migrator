package migrator

import "fmt"

const (
	// S_NEW is the status of a new migrator instance
	S_NEW = 0
	// S_TERMINATED is the migrator instance not running due to
	// intervention. This differs from S_STOPPED because it cannot be
	// restarted and should only be used for process termination.
	S_TERMINATED = 1
	// S_RUNNING is the status of a migrator which has been initialized
	S_RUNNING = 2
	// S_PAUSING is the status of a migrator when a stop has been
	// requested
	S_STOPPING = 3
	// S_STOPPED is the status of a migrator when it has been stopped
	S_STOPPED = 4
	// S_STARTING is the status of a migrator when a start has been
	// requested
	S_STARTING = 5
	// S_PAUSED is the status of a migrator when a pause has been
	// implemented
	S_PAUSED = 6
	// S_INVALID represents an invalid state
	S_INVALID = -1
)

type MigratorState int

func (m MigratorState) String() string {
	switch m {
	case S_NEW:
		return "S_NEW"
	case S_TERMINATED:
		return "S_TERMINATED"
	case S_RUNNING:
		return "S_RUNNING"
	case S_STARTING:
		return "S_STARTING"
	case S_STOPPING:
		return "S_STOPPING"
	case S_STOPPED:
		return "S_STOPPED"
	case S_PAUSED:
		return "S_PAUSED"
	default:
		return "S_INVALID"
	}
}

// MigratorStateFromString derives a migrator state from a string
func MigratorStateFromString(s string) (MigratorState, error) {
	switch s {
	case "S_NEW":
		return S_NEW, nil
	case "S_TERMINATED":
		return S_TERMINATED, nil
	case "S_RUNNING":
		return S_RUNNING, nil
	case "S_STARTING":
		return S_STARTING, nil
	case "S_STOPPING":
		return S_STOPPING, nil
	case "S_STOPPED":
		return S_STOPPED, nil
	case "S_PAUSED":
		return S_PAUSED, nil
	default:
		return S_INVALID, fmt.Errorf("invalid state: '%s'", s)
	}
}
