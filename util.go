package migrator

import (
	"time"
)

func intmax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func intmin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func int64max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func int64min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func timemax(a, b time.Time) time.Time {
	if a.Unix() > b.Unix() {
		return a
	}
	return b
}

func timemin(a, b time.Time) time.Time {
	if a.Unix() < b.Unix() {
		return a
	}
	return b
}

func paramInt(params map[string]interface{}, key string, defaultValue int) int {
	out := defaultValue
	if _, ok := params[key]; ok {
		out, ok = params[key].(int)
		if !ok {
			return defaultValue
		}
		return out
	}
	return defaultValue
}

func paramBool(params map[string]interface{}, key string, defaultValue bool) bool {
	out := defaultValue
	if _, ok := params[key]; ok {
		out, ok = params[key].(bool)
		if !ok {
			return defaultValue
		}
		return out
	}
	return defaultValue
}
