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
- `crates/`: Rust crateのworkspace
  - `cork-core/`: gRPCサーバ + 実行エンジン（corkdバイナリ）
  - `cork-proto/`: tonic/prost生成コード
  - `cork-canon/`: JCS正規化
  - `cork-hash/`: SHA-256ハッシュ
  - `cork-schema/`: JSON Schema検証
  - `cork-store/`: in-memory store
- `proto/`: gRPC Proto
- `schemas/`: JSON Schema
- `docs/`: 仕様・ドキュメント
- `scripts/`: 補助スクリプト

## Protocol Buffers

protoファイルは `proto/cork/v1/` に配置されています。

### proto変更時の運用

1. `proto/cork/v1/*.proto` を編集
2. `cargo build -p cork-proto` で再生成
3. 生成コードは `target/` 以下に出力（コミット対象外）

### 生成コードの利用

```rust
use cork_proto::cork::v1::*;
use cork_proto::cork::v1::cork_core_server::CorkCoreServer;
```

## サーバ起動

```bash
cargo run -p cork-core --bin corkd
```

デフォルトで `[::1]:50051` でgRPCサーバが起動します。
