# Repository baseline

## Scope inspected

The repository root contains a license and ignore configuration, with no application source tree. The only tracked files at inspection time were `.gitignore` and `LICENSE`. No Go module, frontend package manifest or lockfile, Dockerfile or Compose configuration, environment template, tests, migrations, or CI workflow was present.

The working tree already had a modification to `.gitignore` adding an ignore rule for a local text artifact. No tracked application files were changed. Two local-only text files were ignored by Git and were not treated as project source.

## Available tools

| Tool | Observed version or availability |
|---|---|
| Go | `go1.25.6 windows/amd64` |
| Node.js | `v25.9.0` |
| npm | `11.19.1` (`npm.cmd`; invoking the PowerShell shim is blocked by execution policy) |
| Docker Compose | `v2.39.4-desktop.1` |
| pnpm, Yarn, golangci-lint, govulncheck, gitleaks | Not installed |

## Baseline command outcomes

| Command | Outcome |
|---|---|
| `git status --short --untracked-files=all` | Before adding this report, it showed the pre-existing `.gitignore` modification and no other non-ignored changes. |
| `git diff --check` | Passed (exit 0). |
| `go test ./...` | Could not discover/run tests: no Go module exists (`directory prefix . does not contain main module or its selected dependencies`; exit 1). |
| `npm.cmd test -- --run` | Could not discover/run tests: no `package.json` exists (npm `ENOENT`; exit `-4058`). |
| `docker compose config --quiet` | Could not validate a Compose configuration: none exists (exit 1). |

No project-defined build, test, lint, integration, or security-scan commands were available to run.

## Security and ignore review

Manual review of the existing ignore rules found coverage for environment files (with example templates trackable), common local credentials, database and dump extensions, logs, test/coverage output, binaries, dependency/build directories, local model data, telemetry, and generated documentation. The local-only planning and text artifacts are ignored. Representative `git check-ignore` checks confirmed these rules; `.env.example`, manifests/lockfiles, migrations, tests, workflows, and this baseline document remain trackable.

The inspected repository content did not contain credentials, private keys, database contents, or customer/event data. This was a manual review, not an automated secret scan; `gitleaks` is unavailable. No application or dependency configuration exists yet to assess for runtime security.
