# CORK Core

CORK（COncurrency + ORder Kernel）のCore実装を管理するリポジトリです。
設計・仕様・開発ルールはドキュメントを参照してください。

## ドキュメント
- 仕様書: [`docs/specification.md`](docs/specification.md)
- 共通 Definition of Done: [`docs/dod.md`](docs/dod.md)
- MVPスコープ: [`docs/mvp.md`](docs/mvp.md)
- 正規化（Canonicalization）: [`docs/canonicalization.md`](docs/canonicalization.md)
- ハッシュ仕様: [`docs/hashing.md`](docs/hashing.md)
- ADR: [`docs/adr/`](docs/adr/)

## 開発コマンド
- Format: `make fmt`
- Lint: `make lint`
- Test: `make test`
- Build: `make build`

## ディレクトリ構成（主要）
- `src/`: Core実装
- `proto/`: gRPC Proto
- `schemas/`: JSON Schema
- `docs/`: 仕様・ドキュメント
- `scripts/`: 補助スクリプト
