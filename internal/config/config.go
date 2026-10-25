package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	defaultListenAddress        = "127.0.0.1:8080"
	defaultMetricsListenAddress = "127.0.0.1:9090"
	defaultDatabaseURL          = "postgres://localhost:5432/genesisgo?sslmode=disable"
	defaultNATSURL              = "nats://127.0.0.1:4222"
	defaultWorkerConcurrency    = 4
	defaultRunTimeout           = 2 * time.Minute
	defaultMaxEventsPerRun      = 1_000
	defaultMaxRequestBodySize   = 1 << 20
	defaultLogLevel             = "info"
	defaultCORSOrigins          = "http://localhost:3000"
	defaultRetentionPeriod      = 24 * time.Hour
	defaultMaxRuns              = 1_000
)

// Config contains validated runtime settings. Secret values must not be logged.
type Config struct {
	Environment             string
	ListenAddress           string
	MetricsListenAddress    string
	DatabaseURL             string
	NATSURL                 string
	WorkerConcurrency       int
	RunTimeout              time.Duration
	MaxEventsPerRun         int
	MaxRequestBodyBytes     int64
	RetentionPeriod         time.Duration
	MaxRuns                 int
	LogLevel                string
	CORSOrigins             []string
	APIKeysFile             string
	APIKeysEnabled          bool
	BootstrapKeyID          string
	BootstrapScopes         []string
	DemoAuthDisabled        bool
	ArcjetEnabled           bool
	ArcjetKey               string
	ArcjetTrustedProxies    []string
	ArcjetFallbackPerMinute int
	ArcjetFailClosed        bool
	OllamaURL               string
	OllamaModel             string
	TelemetryEndpoint       string
}

type lookup func(string) (string, bool)

// Load reads the local .env file when present and validates the resulting
// process environment. Values already present in the process take precedence.
func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, errors.New("load local environment file: invalid or unreadable .env")
	}
	return loadFrom(os.LookupEnv)
}

func loadFrom(get lookup) (Config, error) {
	var cfg Config
	var err error

	cfg.Environment = value(get, "APP_ENV", "development")
	switch cfg.Environment {
	case "development", "test", "production":
	default:
		return Config{}, errors.New("APP_ENV must be development, test, or production")
	}

	cfg.ListenAddress = value(get, "LISTEN_ADDR", defaultListenAddress)
	if err := validateListenAddress("LISTEN_ADDR", cfg.ListenAddress); err != nil {
		return Config{}, err
	}
	cfg.MetricsListenAddress = value(get, "METRICS_LISTEN_ADDR", defaultMetricsListenAddress)
	if err := validateListenAddress("METRICS_LISTEN_ADDR", cfg.MetricsListenAddress); err != nil {
		return Config{}, err
	}

	cfg.DatabaseURL = value(get, "DATABASE_URL", defaultDatabaseURL)
	if err := validateURL("DATABASE_URL", cfg.DatabaseURL, "postgres", "postgresql"); err != nil {
		return Config{}, err
	}

	cfg.NATSURL = value(get, "NATS_URL", defaultNATSURL)
	if err := validateURL("NATS_URL", cfg.NATSURL, "nats", "tls", "ws", "wss"); err != nil {
		return Config{}, err
	}

	cfg.WorkerConcurrency, err = positiveInt(get, "WORKER_CONCURRENCY", defaultWorkerConcurrency)
	if err != nil {
		return Config{}, err
	}
	cfg.RunTimeout, err = positiveDuration(get, "RUN_TIMEOUT", defaultRunTimeout)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxEventsPerRun, err = positiveInt(get, "MAX_EVENTS_PER_RUN", defaultMaxEventsPerRun)
	if err != nil {
		return Config{}, err
	}
	bodySize, err := positiveInt64(get, "MAX_BODY_BYTES", defaultMaxRequestBodySize)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxRequestBodyBytes = bodySize

	cfg.RetentionPeriod, err = positiveDuration(get, "RETENTION_PERIOD", defaultRetentionPeriod)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxRuns, err = positiveInt(get, "MAX_RUNS", defaultMaxRuns)
	if err != nil {
		return Config{}, err
	}

	cfg.LogLevel = strings.ToLower(value(get, "LOG_LEVEL", defaultLogLevel))
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, errors.New("LOG_LEVEL must be debug, info, warn, or error")
	}

	cfg.CORSOrigins, err = parseCORSOrigins(value(get, "CORS_ORIGINS", defaultCORSOrigins))
	if err != nil {
		return Config{}, err
	}

	cfg.APIKeysFile = strings.TrimSpace(value(get, "API_KEYS_FILE", ".local/api-keys.json"))
	cfg.APIKeysEnabled, err = parseBool(get, "API_KEYS_ENABLED", true)
	if err != nil {
		return Config{}, err
	}
	cfg.BootstrapKeyID = strings.TrimSpace(value(get, "BOOTSTRAP_KEY_ID", "local-admin"))
	if cfg.BootstrapKeyID == "" || len([]rune(cfg.BootstrapKeyID)) > 64 {
		return Config{}, errors.New("BOOTSTRAP_KEY_ID must be 1-64 characters")
	}
	rawScopes := strings.TrimSpace(value(get, "BOOTSTRAP_SCOPES", "admin"))
	if rawScopes == "" {
		return Config{}, errors.New("BOOTSTRAP_SCOPES must contain at least one scope")
	}
	cfg.BootstrapScopes, err = parseScopes(rawScopes)
	if err != nil {
		return Config{}, err
	}
	cfg.DemoAuthDisabled, err = parseBool(get, "DEMO_AUTH_DISABLED", false)
	if err != nil {
		return Config{}, err
	}
	if !cfg.APIKeysEnabled && cfg.Environment == "production" {
		return Config{}, errors.New("API_KEYS_ENABLED must be true in production")
	}
	if cfg.DemoAuthDisabled && cfg.Environment == "production" {
		return Config{}, errors.New("DEMO_AUTH_DISABLED must be false in production")
	}
	if cfg.APIKeysEnabled && strings.TrimSpace(cfg.APIKeysFile) == "" {
		return Config{}, errors.New("API_KEYS_FILE must be set when API_KEYS_ENABLED is true")
	}

	cfg.ArcjetEnabled, err = parseBool(get, "ARCJET_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	cfg.ArcjetKey = value(get, "ARCJET_KEY", "")
	if cfg.ArcjetEnabled {
		if len(cfg.ArcjetKey) < 16 || !strings.HasPrefix(cfg.ArcjetKey, "aj_") {
			return Config{}, errors.New("ARCJET_KEY must be a valid Arcjet key when ARCJET_ENABLED is true")
		}
	} else {
		cfg.ArcjetKey = ""
	}
	cfg.ArcjetTrustedProxies = parseTrustedProxies(value(get, "ARCJET_TRUSTED_PROXIES", ""))
	fallbackLimit, err := positiveInt(get, "ARCJET_FALLBACK_PER_MINUTE", 120)
	if err != nil {
		return Config{}, err
	}
	cfg.ArcjetFallbackPerMinute = fallbackLimit
	cfg.ArcjetFailClosed, err = parseBool(get, "ARCJET_FAIL_CLOSED", false)
	if err != nil {
		return Config{}, err
	}

	cfg.OllamaURL = value(get, "OLLAMA_URL", "")
	if cfg.OllamaURL != "" {
		if err := validateURL("OLLAMA_URL", cfg.OllamaURL, "http", "https"); err != nil {
			return Config{}, err
		}
	}
	cfg.OllamaModel = value(get, "OLLAMA_MODEL", "")
	cfg.TelemetryEndpoint = value(get, "TELEMETRY_ENDPOINT", "")
	if cfg.TelemetryEndpoint != "" {
		if err := validateURL("TELEMETRY_ENDPOINT", cfg.TelemetryEndpoint, "http", "https"); err != nil {
			return Config{}, err
		}
	}

	if cfg.Environment == "production" {
		if err := validateProduction(get, cfg); err != nil {
			return Config{}, err
		}
	}

	return cfg, nil
}

func value(get lookup, key, fallback string) string {
	if v, ok := get(key); ok {
		return strings.TrimSpace(v)
	}
	return fallback
}

func positiveInt(get lookup, key string, fallback int) (int, error) {
	raw := value(get, key, strconv.Itoa(fallback))
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func positiveInt64(get lookup, key string, fallback int64) (int64, error) {
	raw := value(get, key, strconv.FormatInt(fallback, 10))
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func positiveDuration(get lookup, key string, fallback time.Duration) (time.Duration, error) {
	raw := value(get, key, fallback.String())
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}

func parseBool(get lookup, key string, fallback bool) (bool, error) {
	raw := value(get, key, strconv.FormatBool(fallback))
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", key)
	}
	return v, nil
}

func parseScopes(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	scopes := make([]string, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, part := range parts {
		scope := strings.ToLower(strings.TrimSpace(part))
		switch scope {
		case "read", "run", "admin":
		default:
			return nil, errors.New("BOOTSTRAP_SCOPES must contain only read, run, or admin")
		}
		if !seen[scope] {
			seen[scope] = true
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 {
		return nil, errors.New("BOOTSTRAP_SCOPES must contain at least one scope")
	}
	return scopes, nil
}

func parseTrustedProxies(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func validateListenAddress(key, address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" && strings.Count(address, ":") == 0 {
		return fmt.Errorf("%s must be a host:port address", key)
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return fmt.Errorf("%s must not contain whitespace in the host", key)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s must contain a valid TCP port", key)
	}
	return nil
}

func validateURL(key, raw string, schemes ...string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Hostname() == "" || u.Opaque != "" || u.Fragment != "" {
		return fmt.Errorf("%s must be an absolute URL with a host", key)
	}
	for _, scheme := range schemes {
		if u.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("%s uses an unsupported URL scheme", key)
}

func parseCORSOrigins(raw string) ([]string, error) {
	var origins []string
	for _, part := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(part)
		if origin == "" {
			return nil, errors.New("CORS_ORIGINS must contain non-empty origins separated by commas")
		}
		u, err := url.Parse(origin)
		if err != nil || strings.Contains(origin, "*") || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
			u.User != nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("CORS_ORIGINS must contain only HTTP or HTTPS origins")
		}
		origins = append(origins, strings.TrimRight(origin, "/"))
	}
	return origins, nil
}

func validateProduction(get lookup, cfg Config) error {
	for _, key := range []string{"LISTEN_ADDR", "DATABASE_URL", "NATS_URL", "CORS_ORIGINS"} {
		if _, ok := get(key); !ok {
			return fmt.Errorf("%s must be explicitly set in production", key)
		}
	}
	for _, origin := range cfg.CORSOrigins {
		u, _ := url.Parse(origin)
		host := strings.ToLower(u.Hostname())
		if u.Scheme != "https" || host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "*" {
			return errors.New("CORS_ORIGINS must contain only public HTTPS origins in production")
		}
	}
	databaseURL, _ := url.Parse(cfg.DatabaseURL)
	if databaseURL.Query().Get("sslmode") != "require" &&
		databaseURL.Query().Get("sslmode") != "verify-ca" &&
		databaseURL.Query().Get("sslmode") != "verify-full" {
		return errors.New("DATABASE_URL must require TLS in production")
	}
	natsURL, _ := url.Parse(cfg.NATSURL)
	if natsURL.Scheme != "tls" && natsURL.Scheme != "wss" {
		return errors.New("NATS_URL must use TLS in production")
	}
	if cfg.TelemetryEndpoint != "" {
		telemetryURL, _ := url.Parse(cfg.TelemetryEndpoint)
		if telemetryURL.Scheme != "https" {
			return errors.New("TELEMETRY_ENDPOINT must use HTTPS in production")
		}
	}
	if cfg.LogLevel == "debug" {
		return errors.New("LOG_LEVEL=debug is not allowed in production")
	}
	return nil
}
