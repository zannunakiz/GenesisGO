package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func from(values map[string]string) (Config, error) {
	return loadFrom(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
}

func TestDefaults(t *testing.T) {
	cfg, err := from(nil)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Environment != "development" || cfg.ListenAddress != "127.0.0.1:8080" {
		t.Fatalf("unexpected safe defaults: environment=%q listen=%q", cfg.Environment, cfg.ListenAddress)
	}
	if cfg.WorkerConcurrency != 4 || cfg.RunTimeout != 2*time.Minute {
		t.Fatalf("unexpected worker defaults: concurrency=%d timeout=%s", cfg.WorkerConcurrency, cfg.RunTimeout)
	}
	if cfg.MaxEventsPerRun != 1_000 || cfg.MaxRequestBodyBytes != 1<<20 {
		t.Fatalf("unexpected request limits: events=%d body=%d", cfg.MaxEventsPerRun, cfg.MaxRequestBodyBytes)
	}
	if cfg.ArcjetEnabled || cfg.ArcjetKey != "" || cfg.OllamaURL != "" || cfg.OllamaModel != "" {
		t.Fatal("optional Arcjet and AI settings should not be required by default")
	}
	if len(cfg.ArcjetTrustedProxies) != 0 || cfg.ArcjetFailClosed {
		t.Fatal("optional Arcjet proxy and fail-closed settings should default to unset")
	}
	if cfg.ArcjetFallbackPerMinute != 120 {
		t.Fatalf("unexpected Arcjet fallback default: %d", cfg.ArcjetFallbackPerMinute)
	}
	if len(cfg.CORSOrigins) != 1 || cfg.CORSOrigins[0] != "http://localhost:3000" {
		t.Fatalf("unexpected CORS origins: %v", cfg.CORSOrigins)
	}
}

func TestLoadReadsLocalEnvFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("APP_ENV=test\nLOG_LEVEL=warn\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	previousDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(previousDirectory); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	}()

	var previousValues = make(map[string]string)
	var previouslySet = make(map[string]bool)
	for _, key := range []string{"APP_ENV", "LOG_LEVEL"} {
		if value, ok := os.LookupEnv(key); ok {
			previousValues[key] = value
			previouslySet[key] = true
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, key := range []string{"APP_ENV", "LOG_LEVEL"} {
			if previouslySet[key] {
				if err := os.Setenv(key, previousValues[key]); err != nil {
					t.Errorf("restore %s environment variable: %v", key, err)
				}
			} else {
				if err := os.Unsetenv(key); err != nil {
					t.Errorf("clear %s environment variable: %v", key, err)
				}
			}
		}
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != "test" || cfg.LogLevel != "warn" {
		t.Fatalf("local environment file was not loaded: environment=%q log_level=%q", cfg.Environment, cfg.LogLevel)
	}
}

func TestExistingLocalEnvironmentFileIsValid(t *testing.T) {
	values, err := godotenv.Read(filepath.Join("..", "..", ".env"))
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("no local environment file")
	}
	if err != nil {
		t.Fatal("local environment file could not be parsed")
	}

	cfg, err := loadFrom(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ArcjetEnabled && cfg.ArcjetKey == "" {
		t.Fatal("Arcjet is enabled without a configured key")
	}
}

func TestAuthDefaults(t *testing.T) {
	cfg, err := from(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.APIKeysEnabled || cfg.APIKeysFile != ".local/api-keys.json" {
		t.Fatalf("API keys must be enabled locally with a file store: %+v", cfg)
	}
	if cfg.BootstrapKeyID != "local-admin" || len(cfg.BootstrapScopes) != 1 || cfg.BootstrapScopes[0] != "admin" {
		t.Fatalf("unexpected bootstrap defaults: %+v", cfg)
	}
	if cfg.DemoAuthDisabled {
		t.Fatal("demo bypass must default to disabled")
	}
}

func TestAuthProductionGuards(t *testing.T) {
	base := map[string]string{
		"APP_ENV":      "production",
		"LISTEN_ADDR":  "0.0.0.0:8080",
		"DATABASE_URL": "postgres://db.internal:5432/genesisgo?sslmode=verify-full",
		"NATS_URL":     "tls://nats.internal:4222",
		"CORS_ORIGINS": "https://app.example.test",
	}
	for _, mutate := range []func(map[string]string){
		func(v map[string]string) { v["API_KEYS_ENABLED"] = "false" },
		func(v map[string]string) { v["DEMO_AUTH_DISABLED"] = "true" },
	} {
		values := make(map[string]string, len(base)+1)
		for k, v := range base {
			values[k] = v
		}
		mutate(values)
		if _, err := from(values); err == nil {
			t.Fatalf("expected production auth guard to fail for %v", values)
		}
	}
}

func TestValidOptionalServicesAndLimits(t *testing.T) {
	cfg, err := from(map[string]string{
		"WORKER_CONCURRENCY":         "8",
		"RUN_TIMEOUT":                "90s",
		"MAX_EVENTS_PER_RUN":         "2000",
		"MAX_BODY_BYTES":             "2048",
		"ARCJET_ENABLED":             "true",
		"ARCJET_KEY":                 "aj_prj_key_0123456789abcdef",
		"ARCJET_TRUSTED_PROXIES":     "10.0.0.1, 10.0.0.0/24",
		"ARCJET_FALLBACK_PER_MINUTE": "60",
		"ARCJET_FAIL_CLOSED":         "true",
		"OLLAMA_URL":                 "http://127.0.0.1:11434",
		"OLLAMA_MODEL":               "local-model",
		"TELEMETRY_ENDPOINT":         "http://127.0.0.1:4318",
		"CORS_ORIGINS":               "http://localhost:3000, https://demo.example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ArcjetEnabled || cfg.ArcjetKey == "" || cfg.OllamaModel != "local-model" {
		t.Fatal("configured optional service settings were not loaded")
	}
	if len(cfg.ArcjetTrustedProxies) != 2 || !cfg.ArcjetFailClosed {
		t.Fatal("configured Arcjet proxy and fail-closed settings were not loaded")
	}
	if cfg.ArcjetFallbackPerMinute != 60 {
		t.Fatal("configured Arcjet fallback limit was not loaded")
	}
	if cfg.WorkerConcurrency != 8 || cfg.RunTimeout != 90*time.Second || cfg.MaxEventsPerRun != 2000 || cfg.MaxRequestBodyBytes != 2048 {
		t.Fatal("configured positive limits were not loaded")
	}
	if len(cfg.CORSOrigins) != 2 {
		t.Fatalf("expected two CORS origins, got %v", cfg.CORSOrigins)
	}
}

func TestArcjetKeyIsDiscardedWhenDisabled(t *testing.T) {
	cfg, err := from(map[string]string{
		"ARCJET_ENABLED": "false",
		"ARCJET_KEY":     "aj_prj_key_secretmarker012345",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ArcjetKey != "" {
		t.Fatal("Arcjet key should not be retained when Arcjet is disabled")
	}
}

func TestInvalidValuesAreActionableAndRedacted(t *testing.T) {
	const secret = "secret-value-that-must-never-be-echoed"
	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{"environment", map[string]string{"APP_ENV": "staging"}, "APP_ENV"},
		{"listen address", map[string]string{"LISTEN_ADDR": "localhost:invalid"}, "LISTEN_ADDR"},
		{"database URL", map[string]string{"DATABASE_URL": "mysql://localhost/db"}, "DATABASE_URL"},
		{"NATS URL", map[string]string{"NATS_URL": "http://localhost:4222"}, "NATS_URL"},
		{"worker concurrency", map[string]string{"WORKER_CONCURRENCY": "0"}, "WORKER_CONCURRENCY"},
		{"run timeout", map[string]string{"RUN_TIMEOUT": "-1s"}, "RUN_TIMEOUT"},
		{"event limit", map[string]string{"MAX_EVENTS_PER_RUN": "-2"}, "MAX_EVENTS_PER_RUN"},
		{"body limit", map[string]string{"MAX_BODY_BYTES": "not-a-number"}, "MAX_BODY_BYTES"},
		{"log level", map[string]string{"LOG_LEVEL": "trace"}, "LOG_LEVEL"},
		{"CORS origin", map[string]string{"CORS_ORIGINS": "https://example.test/path"}, "CORS_ORIGINS"},
		{"CORS wildcard", map[string]string{"CORS_ORIGINS": "https://*.example.test"}, "CORS_ORIGINS"},
		{"Arcjet key required", map[string]string{"ARCJET_ENABLED": "true"}, "ARCJET_KEY"},
		{"Arcjet key format", map[string]string{"ARCJET_ENABLED": "true", "ARCJET_KEY": "invalid"}, "ARCJET_KEY"},
		{"Arcjet key redaction", map[string]string{"ARCJET_ENABLED": "true", "ARCJET_KEY": secret}, "ARCJET_KEY"},
		{"bootstrap key id", map[string]string{"BOOTSTRAP_KEY_ID": ""}, "BOOTSTRAP_KEY_ID"},
		{"bootstrap scopes", map[string]string{"BOOTSTRAP_SCOPES": "owner"}, "BOOTSTRAP_SCOPES"},
		{"bootstrap scopes empty", map[string]string{"BOOTSTRAP_SCOPES": "  "}, "BOOTSTRAP_SCOPES"},
		{"Ollama URL", map[string]string{"OLLAMA_URL": "file:///tmp/model"}, "OLLAMA_URL"},
		{"telemetry URL", map[string]string{"TELEMETRY_ENDPOINT": "localhost:4318"}, "TELEMETRY_ENDPOINT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := from(tt.values)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected an error mentioning %s, got %v", tt.want, err)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatal("configuration error leaked secret material")
			}
		})
	}
}

func TestProductionRequiresExplicitSafeConfiguration(t *testing.T) {
	_, err := from(map[string]string{"APP_ENV": "production"})
	if err == nil || !strings.Contains(err.Error(), "LISTEN_ADDR") {
		t.Fatalf("expected production defaults to be rejected, got %v", err)
	}

	values := map[string]string{
		"APP_ENV":        "production",
		"LISTEN_ADDR":    "0.0.0.0:8080",
		"DATABASE_URL":   "postgres://db.internal:5432/genesisgo?sslmode=verify-full",
		"NATS_URL":       "tls://nats.internal:4222",
		"CORS_ORIGINS":   "http://localhost:3000",
		"ARCJET_ENABLED": "false",
	}
	_, err = from(values)
	if err == nil || !strings.Contains(err.Error(), "CORS_ORIGINS") {
		t.Fatalf("expected production loopback CORS to be rejected, got %v", err)
	}

	values["CORS_ORIGINS"] = "https://app.example.test"
	values["NATS_URL"] = "nats://nats.internal:4222"
	_, err = from(values)
	if err == nil || !strings.Contains(err.Error(), "NATS_URL") {
		t.Fatalf("expected plaintext production NATS to be rejected, got %v", err)
	}

	values["NATS_URL"] = "tls://nats.internal:4222"
	values["DATABASE_URL"] = "postgres://db.internal:5432/genesisgo?sslmode=disable"
	_, err = from(values)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("expected plaintext production PostgreSQL to be rejected, got %v", err)
	}

	values["DATABASE_URL"] = "postgres://db.internal:5432/genesisgo?sslmode=verify-full"
	cfg, err := from(values)
	if err != nil {
		t.Fatalf("expected explicit TLS production configuration to be accepted, got %v", err)
	}
	if cfg.Environment != "production" {
		t.Fatalf("expected production environment, got %q", cfg.Environment)
	}
}
