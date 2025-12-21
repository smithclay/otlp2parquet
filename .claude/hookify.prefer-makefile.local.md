---
name: prefer-makefile
enabled: true
event: bash
pattern: ^cargo\s+(build|test|check|clippy|run|doc|clean)
action: warn
---

**Prefer Makefile over direct cargo commands**

Your project guidelines specify: "prefer using the makefile over cargo commands"

**Available make targets:**
- `make build-cli` - Build CLI binary for desktop use
- `make build-lambda` - Build lambda functions
- `make dev` - Development build
- `make pre-commit` - Pre-commit checks
- `make clippy` - Run clippy lints
- `make check` - Run cargo check
- `make test` - Run tests
- `make test-e2e` - Run E2E tests
- `make wasm-full` - Build WASM
- `make wasm-size` - Check WASM size

Consider using the appropriate make target instead.
