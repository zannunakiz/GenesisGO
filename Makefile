GO ?= go
NPM ?= npm
STATICCHECK_VERSION ?= v0.8.2

.PHONY: format lint test race-test integration-test build web-check web-build compose-up compose-down compose-ai compose-observability quality

format:
	$(GO) fmt ./...
	$(NPM) --prefix web run format

lint:
	$(GO) vet ./...
	$(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...
	$(NPM) --prefix web run lint

test:
	$(GO) test ./...

race-test:
	$(GO) test -race ./...

integration-test:
	$(GO) test -tags=integration ./...

build:
	$(GO) build ./...

web-check:
	$(NPM) --prefix web run lint
	$(NPM) --prefix web run typecheck
	$(NPM) --prefix web test

web-build:
	$(NPM) --prefix web run build

compose-up:
	docker compose up -d

compose-down:
	docker compose down

compose-ai:
	docker compose --profile ai up -d

compose-observability:
	docker compose --profile observability up -d

quality: lint test build web-check web-build
