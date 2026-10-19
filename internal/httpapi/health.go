package httpapi

import (
	"net/http"
	"time"
)

// HealthDependencies reports only presence/state labels. It never includes
// URLs, keys, credentials, connection strings, or other sensitive material.
type HealthDependencies struct {
	Database string `json:"database"`
	NATS     string `json:"nats"`
	Arcjet   string `json:"arcjet"`
	AI       string `json:"ai"`
}

// HealthResponse is the stable legacy /healthz payload.
type HealthResponse struct {
	Status       string             `json:"status"`
	UptimeSecs   int64              `json:"uptime_seconds"`
	Dependencies HealthDependencies `json:"dependencies"`
}

// LiveResponse is the /health/live payload. Liveness never depends on
// downstream services.
type LiveResponse struct {
	Status     string `json:"status"`
	UptimeSecs int64  `json:"uptime_seconds"`
}

// ReadyResponse is the /health/ready payload. Readiness reflects only the
// dependencies required by the API process.
type ReadyResponse struct {
	Status       string             `json:"status"`
	UptimeSecs   int64              `json:"uptime_seconds"`
	Dependencies HealthDependencies `json:"dependencies"`
	Missing      []string           `json:"missing,omitempty"`
}

// HealthDeps configures the health handler. StartTime marks process start;
// Now may be overridden in tests for deterministic uptime.
type HealthDeps struct {
	StartTime          time.Time
	Now                func() time.Time
	DatabaseConfigured bool
	NATSConfigured     bool
	ArcjetEnabled      bool
	AIConfigured       bool
}

func (d HealthDeps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d HealthDeps) uptimeSeconds() int64 {
	now := d.now()
	if now.Before(d.StartTime) {
		return 0
	}
	return int64(now.Sub(d.StartTime) / time.Second)
}

// LiveDeps configures the liveness handler.
type LiveDeps struct {
	StartTime time.Time
	Now       func() time.Time
}

// ReadyDeps configures the readiness handler. Optional integrations never
// affect readiness.
type ReadyDeps struct {
	StartTime          time.Time
	Now                func() time.Time
	DatabaseConfigured bool
	NATSConfigured     bool
	ArcjetEnabled      bool
	AIConfigured       bool
}

func nowOrDefault(now func() time.Time) time.Time {
	if now != nil {
		return now()
	}
	return time.Now()
}

func configuredLabel(configured bool) string {
	if configured {
		return "configured"
	}
	return "not_configured"
}

func uptimeSeconds(start time.Time, now func() time.Time) int64 {
	current := nowOrDefault(now)
	if current.Before(start) {
		return 0
	}
	return int64(current.Sub(start) / time.Second)
}

func dependencyLabels(databaseConfigured, natsConfigured, arcjetEnabled, aiConfigured bool) HealthDependencies {
	arcjet := "disabled"
	if arcjetEnabled {
		arcjet = "enabled"
	}
	return HealthDependencies{
		Database: configuredLabel(databaseConfigured),
		NATS:     configuredLabel(natsConfigured),
		Arcjet:   arcjet,
		AI:       configuredLabel(aiConfigured),
	}
}

func requireGET(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		MethodNotAllowed(w, r, "")
		return false
	}
	return true
}

// LiveHandler reports whether the process is running. It never probes
// downstream services. Only GET is allowed.
func LiveHandler(deps LiveDeps) http.Handler {
	if deps.StartTime.IsZero() {
		deps.StartTime = nowOrDefault(deps.Now)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireGET(w, r) {
			return
		}
		WriteJSON(w, r, http.StatusOK, LiveResponse{
			Status:     "ok",
			UptimeSecs: uptimeSeconds(deps.StartTime, deps.Now),
		})
	})
}

// ReadyHandler reports whether the API process can serve traffic. Missing
// database or NATS configuration yields 503 with status not_ready. Optional
// Arcjet and AI settings are labels only. Only GET is allowed.
func ReadyHandler(deps ReadyDeps) http.Handler {
	if deps.StartTime.IsZero() {
		deps.StartTime = nowOrDefault(deps.Now)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireGET(w, r) {
			return
		}
		var missing []string
		if !deps.DatabaseConfigured {
			missing = append(missing, "database")
		}
		if !deps.NATSConfigured {
			missing = append(missing, "nats")
		}
		payload := ReadyResponse{
			Status:       "ready",
			UptimeSecs:   uptimeSeconds(deps.StartTime, deps.Now),
			Dependencies: dependencyLabels(deps.DatabaseConfigured, deps.NATSConfigured, deps.ArcjetEnabled, deps.AIConfigured),
		}
		if len(missing) > 0 {
			payload.Status = "not_ready"
			payload.Missing = missing
			WriteJSON(w, r, http.StatusServiceUnavailable, payload)
			return
		}
		WriteJSON(w, r, http.StatusOK, payload)
	})
}

// HealthHandler remains for compatibility and reports the same detailed
// payload as before. New deployments should use /health/live and
// /health/ready.
func HealthHandler(deps HealthDeps) http.Handler {
	if deps.StartTime.IsZero() {
		deps.StartTime = deps.now()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireGET(w, r) {
			return
		}
		WriteJSON(w, r, http.StatusOK, HealthResponse{
			Status:       "ok",
			UptimeSecs:   deps.uptimeSeconds(),
			Dependencies: dependencyLabels(deps.DatabaseConfigured, deps.NATSConfigured, deps.ArcjetEnabled, deps.AIConfigured),
		})
	})
}

// NewMux wires the public routes that exist so far. Health stays available
// without authentication; future authenticated routes are added here behind
// the same middleware chain by their callers.
func NewMux(live, ready http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	if live == nil {
		live = LiveHandler(LiveDeps{})
	}
	if ready == nil {
		ready = ReadyHandler(ReadyDeps{})
	}
	mux.Handle("/health/live", live)
	mux.Handle("/health/ready", ready)
	mux.Handle("/healthz", live)
	return mux
}
