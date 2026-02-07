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

## CLI（corkctl）

`corkctl` から Core の gRPC API を呼び出して動作確認できます。JSON入力は
JCS canonicalize と SHA-256 付与を行ったうえで送信します。

### 起動

```bash
cargo run -p cork-core --bin corkctl -- serve --addr 127.0.0.1:50051
```

### SubmitRun

```bash
cargo run -p cork-core --bin corkctl -- submit-run \
  --contract ./contract.json \
  --policy ./policy.json \
  --input ./input.json \
  --input-content-type application/json
```

### ApplyGraphPatch

```bash
cargo run -p cork-core --bin corkctl -- apply-patch \
  --run-id <RUN_ID> \
  --patch ./patch.json
```

### StreamRunEvents

```bash
cargo run -p cork-core --bin corkctl -- stream-events --run-id <RUN_ID>
```

### GetRun

```bash
cargo run -p cork-core --bin corkctl -- get-run --run-id <RUN_ID>
```
