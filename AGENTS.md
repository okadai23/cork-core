# Repository guidelines

## Tooling
- Install and enable pre-commit hooks:
  - `pip install pre-commit`
  - `pre-commit install`
  - Run on demand with `pre-commit run --all-files`
- Spelling checks are configured via `.cspell.json` and run through the `cspell` pre-commit hook.

## Common commands
- Format: `make fmt`
- Lint: `make lint`
- Test: `make test`
- Build: `make build`

## Documentation & schemas
- Core仕様は `docs/specification.md` を参照。
- 正規化/ハッシュ仕様は `docs/canonicalization.md` と `docs/hashing.md` に集約。
- ADRは `docs/adr/` 配下に追加する。
- JSON Schemaは `schemas/` 配下に置く（`cork.*.v0.1.schema.json`）。
- gRPC Protoは `proto/cork/v1/` 配下に置く。
