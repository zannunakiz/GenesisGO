package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/config"
)

func main() {
	keyID := flag.String("key-id", "", "API key identifier (defaults to BOOTSTRAP_KEY_ID)")
	scopes := flag.String("scopes", "", "Comma-separated scopes: read, run, admin (defaults to BOOTSTRAP_SCOPES)")
	force := flag.Bool("force", false, "Rotate the key even when one already exists")
	flag.Parse()

	if err := run(*keyID, *scopes, *force); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(keyIDFlag, scopesFlag string, force bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.APIKeysEnabled {
		return fmt.Errorf("API_KEYS_ENABLED is false; enable API keys before bootstrapping")
	}
	id := strings.TrimSpace(keyIDFlag)
	if id == "" {
		id = cfg.BootstrapKeyID
	}
	var scopes []string
	if strings.TrimSpace(scopesFlag) == "" {
		scopes = cfg.BootstrapScopes
	} else {
		scopes = strings.Split(scopesFlag, ",")
	}

	store, err := auth.NewFileStore(cfg.APIKeysFile)
	if err != nil {
		return err
	}
	result, err := auth.Bootstrap(store, auth.BootstrapOptions{KeyID: id, Scopes: scopes, Force: force})
	if err != nil {
		return err
	}
	if result.Secret == "" {
		fmt.Printf("API key %q already exists and was left unchanged. Use --force to rotate it.\n", result.KeyID)
		return nil
	}
	if result.Rotated {
		fmt.Printf("Rotated API key %q with scopes [%s].\n", result.KeyID, strings.Join(result.Scopes, ", "))
	} else {
		fmt.Printf("Created API key %q with scopes [%s].\n", result.KeyID, strings.Join(result.Scopes, ", "))
	}
	fmt.Println("Copy this secret now; it is shown once and stored only as a digest.")
	fmt.Printf("GENESISGO_API_KEY=%s\n", result.Secret)
	return nil
}
