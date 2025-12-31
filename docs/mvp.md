# MVPスコープ

以下はMVPで実装する機能の一覧です。

- gRPC Core API
  - `SubmitRun` / `ApplyGraphPatch` / `StreamRunEvents` / `GetLogs` / `GetRun` / `GetCompositeGraph` / `CancelRun`
- Contract Manifest / Policy / GraphPatch の受理・検証
  - `patch_seq` の連番
  - `idempotency_key` など
- Canonicalization（Pre-normalization + JCS）＋ ハッシュ（SHA-256 + domain separation）
- RFC Editor、ほかに 1 件
- Event Log（`event_seq` 単調増加）＋ストリーム配信（UI一次情報源）
- In-memory の Composite Graph とステートストア（Run/Stage/Node output）
- JSON Pointer による参照解決（ValueRef）
- Crates、ほかに 1 件
- `LIST_HEURISTIC` スケジューラ（deps + 参照解決 + 資源（cpu/io/provider）で起動）
- Stage auto-commit（`quiescence_ms` / `max_open_ms` 等）
- ログ（`scope_id` + `scope_seq` による決定的マージ可能性の担保）
