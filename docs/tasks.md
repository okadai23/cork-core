# CORK v0.1 MVP タスクリスト（Rust Core）

本ドキュメントは、CORK v0.1 MVPをRust側（CORK Core）で実装するための詳細タスクリストです。
各タスクに **Definition of Done（DoD）**、**Acceptance Criteria（受け入れ条件）**、**サブタスク**、**触るファイル** を明記しています。

## 参照規格

- **RFC 8785**: [JSON Canonicalization Scheme (JCS)](https://www.rfc-editor.org/rfc/rfc8785)
- **RFC 6901**: [JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901)
- **gRPC deadline/cancel**: [Deadlines](https://grpc.io/docs/guides/deadlines/)
- **tonic::Status**: [Docs.rs](https://docs.rs/tonic/latest/tonic/struct.Status.html)

---

## マイルストーン概要

| マイルストーン | 概要 | タスク |
| --- | --- | --- |
| M0 | Skeleton（ビルド・proto・サーバ起動） | CORE-001, CORE-002, CORE-003 |
| M1 | Run作成とイベント配信（UI一次情報源の成立） | CORE-010, CORE-011, CORE-012 |
| M2 | GraphPatch適用と検証（連番・idempotency・参照式） | CORE-020〜CORE-026 |
| M3 | スケジューラとState/Ref解決（READY判定→Tool起動→状態遷移） | CORE-030〜CORE-035 |
| M4 | ログ取得とUI成立（GetLogs、scopeマージ、GetCompositeGraph） | CORE-040〜CORE-043 |
| 横断 | ユニットテスト・統合テスト | CORE-090, CORE-091 |

---

## 推奨実装順

1. CORE-001 → CORE-002（骨格）
2. CORE-020（canon/hash）＋ CORE-022/023（schema/論理検証）
3. CORE-010/011（Run+EventLog）
4. CORE-024/025/026（Patch受理と適用）
5. CORE-031/030/032（Stateと参照とREADY）
6. CORE-033/034（資源とスケジューラ）
7. CORE-040（Worker実行）
8. CORE-035（auto-commit）
9. CORE-041/042/043（UI成立と使い勝手）
10. CORE-090/091（固定化）

---

# M0: Skeleton

## CORE-001: Cargo workspace / crate構成の確定

### 内容

- `cork-core`（サーバ・スケジューラ本体）
- `cork-proto`（proto生成物）
- `cork-schema`（JSON schema読込・検証）
- `cork-hash`（canonicalize+hash）
- `cork-store`（in-memory store）

### 依存

なし

### 触るファイル

- `rust/Cargo.toml`（workspace）
- `rust/crates/*/Cargo.toml`
- `rust/crates/cork-core/src/main.rs`

### サブタスク

- [ ] workspace作成（edition=2024 or 2021で統一）
- [ ] `cork-core` に `corkd` バイナリ追加（Hello + gRPC起動）
- [ ] lint整備：`cargo fmt`, `clippy -D warnings` を通す
- [ ] `rust-toolchain.toml` を追加（任意）

### DoD

- workspaceが構築され、全crateがビルド可能
- `corkd`（server binary）で起動できる

### Acceptance Criteria

- [ ] `cargo run -p cork-core --bin corkd` で起動し、ポート待ち受けまで到達
- [ ] CI（ローカルでもOK）で fmt/clippy/test が通る

---

## CORE-002: protoコード生成（tonic-build）とAPIの型を確定

### 内容

- `proto/cork/v1/types.proto`, `core.proto`, `worker.proto` を配置
- `cork-proto` で tonic-build により生成
- `cork-core` から利用可能にする

### 依存

CORE-001

### 触るファイル

- `proto/cork/v1/*.proto`
- `rust/crates/cork-proto/build.rs`
- `rust/crates/cork-proto/src/lib.rs`

### サブタスク

- [ ] `build.rs` に `tonic_build::configure().compile(...)`
- [ ] 生成コードの `include!(concat!(env!("OUT_DIR"), ...))` を `cork-proto` に集約
- [ ] `cork-core` が `cork_proto::cork::v1::*` をuseできることを確認
- [ ] CI/ローカルでproto変更が検出できる運用にする（README）

### DoD

- 生成コードが workspace 内で参照可能
- `CorkCore` trait 実装の雛形がコンパイルできる

### Acceptance Criteria

- [ ] `cork-core` が `CorkCoreServer::new(...)` でサーバを起動できる
- [ ] 生成物がリポジトリにコミットされない運用（build時生成）or 生成物コミット運用、どちらかに統一

---

## CORE-003: CLI インターフェース（ローカル操作）

### 内容

- `corkctl` CLI で Core を操作できるようにする
- SubmitRun / ApplyGraphPatch / StreamRunEvents / GetRun を実行可能にする
- JSON 入力を canonicalize して送信する

### 依存

CORE-002

### 触るファイル

- `crates/cork-core/src/cli.rs`
- `crates/cork-core/src/bin/corkctl.rs`
- `crates/cork-core/Cargo.toml`

### サブタスク

- [x] CLI コマンド構成（serve/submit-run/apply-patch/stream-events/get-run）
- [x] 入力 JSON の canonicalize と sha256 付与
- [x] ユニットテストで CLI パースと canonicalize を検証

### DoD

- CLI から SubmitRun/ApplyGraphPatch/StreamRunEvents/GetRun が呼べる

### Acceptance Criteria

- [x] `corkctl submit-run` で run_id を取得できる
- [x] `corkctl stream-events` でイベントを購読できる

### 進捗

- [DONE] CLI モジュールと `corkctl` バイナリを追加し、canonicalize と sha256 付与を実装。
  - 変更ファイル: `crates/cork-core/src/cli.rs`, `crates/cork-core/src/bin/corkctl.rs`, `crates/cork-core/Cargo.toml`。
  - 検証: `make fmt`, `make lint`, `make test`。

# M1: Run作成とイベント配信（UI一次情報源の成立）

## CORE-010: In-memory RunRegistry（Run一覧・状態管理）

### 内容

- `run_id -> RunStateMachine` を保持
- `RunStatus`、`created_at/updated_at`、`hash_bundle` を格納

### 依存

CORE-002

### 触るファイル

- `rust/crates/cork-store/src/lib.rs`（RunRegistry trait + inmem）
- `rust/crates/cork-core/src/engine/run.rs`

### サブタスク

- [x] `RunId`（String）生成：uuid/ulidのどちらかに固定
- [x] `RunRegistry` 実装:
  - `create_run(...) -> Arc<RunCtx>`
  - `get_run(run_id) -> Option<Arc<RunCtx>>`
  - `list_runs(page_token, filters) -> page`
- [x] Run metadata:
  - created_at, updated_at
  - status（RUN_PENDING/RUN_RUNNING...）
  - hash_bundle（後で埋める）
- [x] page_token:
  - MVPは offset方式（base64("offset:123")）でOK

### DoD

- SubmitRunでRunが作られ、GetRun/ListRunsが応答できる

### Acceptance Criteria

- [ ] SubmitRun → GetRun で `RUN_PENDING` or `RUN_RUNNING` が返る
- [ ] ListRuns がページング形式（page_token）で最低限機能する（簡易でOK）

### 進捗

- [IN PROGRESS] In-memory RunRegistry と RunCtx/metadata を実装。
- 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/engine/run.rs`。
- 検証: `cargo test -p cork-store`, `cargo test -p cork-core`。

---

## CORE-011: Event Log（event_seq単調増加）と StreamRunEvents

### 内容

- `RunEvent.event_seq` を **Run単位で単調増加**採番
- `StreamRunEvents(since_event_seq)` で差分購読できる
- UIはこれを一次情報源として再構成する想定

### 依存

CORE-010

### 触るファイル

- `rust/crates/cork-store/src/lib.rs`（EventLog trait + inmem）
- `rust/crates/cork-core/src/api/core_service.rs`

### サブタスク

- [x] `EventLog`（Run単位）:
  - `next_seq: u64`
  - `events: Vec<RunEvent>`（MVPは全保持、retentionは後）
  - `broadcast::Sender<RunEvent>`（push通知）
- [x] `append(event)`:
  - `event_seq` を採番し、Vecにpushし、broadcast送信
- [x] `subscribe(since_seq)`:
  - backlog（events[since..]）→その後 broadcast stream
- [x] gRPC `StreamRunEvents` 実装:
  - `since_event_seq` から開始
  - クライアント切断時の扱い（dropでOK）

### 進捗

- [DONE] EventLog と gRPC StreamRunEvents を実装。
  - 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/api/core_service.rs`, `crates/cork-proto/build.rs`, `crates/cork-proto/Cargo.toml`。
  - 検証: `make fmt`, `make lint`, `make test`。

### DoD

- event_seqを保証するログがRun中に積まれ、再接続で追える

### Acceptance Criteria

- [ ] SubmitRun直後に `RunEvent` が最低1件出る（例：Run created / Stage ACTIVEなど）
- [ ] `since_event_seq` を指定すると、そのseq以降のみが流れる
- [ ] event_seqが欠番なく連続（Run内で0..）になっている

---

## CORE-012: CanonicalJsonDocument の sha256 検証

### 内容

- SubmitRun / ApplyGraphPatch で受け取った `CanonicalJsonDocument` の `sha256` が付与されている場合、サーバ側で再計算して一致検証
- 不一致なら `INVALID_ARGUMENT` 相当で拒否

### 依存

CORE-010

### 触るファイル

- `rust/crates/cork-hash/src/lib.rs`
- `rust/crates/cork-core/src/api/core_service.rs`

### サブタスク

- [x] `sha256(bytes) -> [u8;32]`
- [x] requestにsha256が含まれるなら再計算して一致確認
- [x] 不一致は `tonic::Status::invalid_argument(...)` または patch拒否理由へ

### DoD

- ドキュメント改竄・誤送信を確実に検知

### Acceptance Criteria

- [x] 正しいsha256 → 受理
- [x] 間違ったsha256 → 100%拒否（rejection_reasonに根拠）

### 進捗

- [DONE] CanonicalJsonDocument の sha256 再計算と照合を実装。
  - 変更ファイル: `crates/cork-hash/src/lib.rs`, `crates/cork-core/src/api/core_service.rs`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

# M2: GraphPatch適用と検証（連番・idempotency・参照式）

## CORE-020: JCS（RFC 8785）canonicalization + ハッシュ実装

### 内容

- Pre-normalization（ソート規則）→ JCS（RFC 8785）
- SHA-256 + domain separation（CORK-CONTRACT / POLICY / PATCH / COMPOSITE / RUNCFG）
- `contract_manifest_hash`, `policy_hash`, `run_config_hash` を算出

### 依存

CORE-010

### 触るファイル

- `rust/crates/cork-canon/src/lib.rs`
- `rust/crates/cork-hash/src/lib.rs`

### サブタスク

- [x] `prenorm_contract(Value) -> Value`（集合配列の安定ソート）
- [x] `prenorm_policy(Value) -> Value`
- [x] `prenorm_patch(Value) -> Value`（ops順序は保持・deps等のみソート）
- [x] `jcs_bytes(value) -> Vec<u8>`：`serde_json_canonicalizer::to_vec`採用
- [x] domain-separated hash:
  - prefix（"CORK-CONTRACT\0" 等）+ bytes を SHA-256
- [x] テスト:
  - フィールド順違いでも hash が一致
  - deps配列順違いでも一致（ソート対象のみ）

### DoD

- 同じ意味のJSONは常に同じハッシュになる

### Acceptance Criteria

- [x] フィールド順や空白差分があっても canonicalize 後の sha256 が一致する
- [x] 既知のテストベクタ（自作）で "ソート対象配列" が安定化している

### 進捗

- [DONE] CORE-020 canon/hash 実装とテストを追加。
  - 変更ファイル: `crates/cork-canon/src/lib.rs`, `crates/cork-hash/src/lib.rs`, `crates/cork-canon/Cargo.toml`, `crates/cork-hash/Cargo.toml`。
  - 検証: `make fmt`, `make lint`, `make test`。

---

## CORE-021: Composite Graph Hash（contract + patch列）

### 内容

- `composite_graph_hash = H(contract_hash || patch_hash0..N)` を実装
- patch受理ごとに更新

### 依存

CORE-020

### 触るファイル

- `rust/crates/cork-hash/src/lib.rs`
- `rust/crates/cork-core/src/engine/patch.rs`

### サブタスク

- [x] `patch_hash` を patch受理時に計算
- [x] `composite_hash = H(contract_hash || "\0" || patch_hash_0..N)`
- [x] RunStateに常に最新 hash_bundle を保持

### DoD

- UI/実験比較の軸として composite_hash を安定提供

### Acceptance Criteria

- [x] patchが0件の時と、1件以上の時で hash が変わる
- [x] 同じ contract + 同じ patch列で、再計算して常に一致

### 進捗

- [DONE] Composite hash 更新ロジックと patch 受理時の hash_bundle 更新を追加。
  - 変更ファイル: `crates/cork-hash/src/lib.rs`, `crates/cork-core/src/engine/patch.rs`。
  - 検証: `make fmt`, `make lint`, `make test`。

---

## CORE-022: Contract Manifest パース＆検証（Stage DAG）

### 内容

- stage_id一意
- dependency参照が存在する
- HARD/SOFTは受理（v0.1はHARDを実行強制）
- DAGである（cycle検出）

### 依存

CORE-020

### 触るファイル

- `rust/crates/cork-schema/src/lib.rs`（schema検証）
- `rust/crates/cork-core/src/engine/run.rs`（論理検証：DAG/参照）

### サブタスク

- [x] JSON Schema検証:
  - `schemas/` を埋め込み or 起動時ロード
  - `jsonschema::draft202012::new(...)` でvalidator生成
- [x] 論理検証:
  - stage_id一意
  - dependencyの存在チェック
  - DAG判定（Kahn or DFSでcycle検出）
- [x] "実行順序用"にトポロジカル順を内部保持

### DoD

- 不正manifestをSubmitRun時点で拒否

### Acceptance Criteria

- [x] cycleがあるmanifestは拒否
- [x] 存在しないstage参照は拒否
- [x] 正常manifestは stage order（トポロジカル順）を内部で保持できる

### 進捗

- [DONE] JSON Schema検証とDAG/参照チェックを実装し、トポロジカル順を保持。
  - 変更ファイル: `crates/cork-schema/src/lib.rs`, `crates/cork-core/src/engine/run.rs`。
  - 検証: `make fmt`, `make lint`, `make test`。

---

## CORE-023: Policy パース＆検証（必須フィールド + resource_pools）

### 内容

- 必須フィールド検証
- `resource_pools` を内部表現へ展開
- scheduler設定（LIST_HEURISTICのみ実装、他は予約）

### 依存

CORE-020

### 触るファイル

- `rust/crates/cork-schema/src/lib.rs`
- `rust/crates/cork-core/src/engine/run.rs`

### サブタスク

- [x] schema検証
- [x] resource_pools を ResourceManager 初期化用に正規化
- [x] scheduler.modeはMVPでは LIST_HEURISTIC のみ実装
- [x] stage_auto_commit 設定を RunCtx に格納

### DoD

- 不正policyをSubmitRunで拒否

### Acceptance Criteria

- [x] 欠落・型違いを検出して拒否
- [x] `resource_pools` が内部に登録され、後続のresource予約が可能

### 進捗

- [DONE] Policy の schema/論理検証と resource_pools 正規化、RunCtx への stage_auto_commit 格納を実装。
  - 変更ファイル: `crates/cork-schema/src/lib.rs`, `crates/cork-core/src/engine/run.rs`, `crates/cork-store/src/lib.rs`。
  - 検証: `make fmt`, `make lint`, `make test`。

---

## CORE-024: ApplyGraphPatch の受理・拒否（patch_seq厳密連番）

### 内容

- patch_seq は **厳密連番**（0,1,2…）。欠番が来たら拒否
- `patch.stage_id` は **現在ACTIVEなstage**のみ許可
- stageの expansion_policy.allow_dynamic / allow_kinds を強制

### 依存

CORE-022, CORE-010

### 触るファイル

- `rust/crates/cork-core/src/engine/patch.rs`
- `rust/crates/cork-core/src/api/core_service.rs`

### サブタスク

- [x] RunCtxに `next_patch_seq` を保持
- [x] patch_seq != next_patch_seq → 拒否（厳密連番）
- [x] patch.stage_id が ACTIVE stage かチェック
- [x] expansion_policy.allow_dynamic / allow_kinds をチェック
- [x] 拒否理由をenum化（後でCORE-043で整理）

### DoD

- patchの順序と適用対象stageを厳密統制

### Acceptance Criteria

- [x] patch_seq=0 が最初に来ない場合は拒否
- [x] 0→2 が来たら拒否
- [x] ACTIVEでないstage_idのpatchは拒否
- [x] allow_kindsにないkindのNODE_ADDEDは拒否

### 進捗

- [DONE] ApplyGraphPatchの検証とRunCtx拡張を実装。
  - 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/api/core_service.rs`, `crates/cork-core/src/engine/patch.rs`。
  - 検証: `make fmt`, `make lint`, `make test`。
  - 追記: patch_seq の原子比較/更新を追加。

---

## CORE-025: Tool side_effect と idempotency_key の強制（GraphPatch受理時）

### 内容

- ToolExec.side_effect != NONE の場合 `idempotency_key` が無い patch は拒否

### 依存

CORE-024

### 触るファイル

- `rust/crates/cork-core/src/engine/patch.rs`

### サブタスク

- [x] NODE_ADDEDの中の ToolExec を走査
- [x] side_effect != NONE なら idempotency_key必須
- [x] 無い場合 patch全体を拒否（部分適用は禁止）

### DoD

- 危険な副作用ツールの二重実行を防ぐための最低限ガードを実装

### Acceptance Criteria

- [x] side_effect=EXTERNAL_WRITE で idempotency_key無し → 100%拒否
- [x] side_effect=NONE で idempotency_key無し → 受理

### 進捗

- [DONE] NODE_ADDED の ToolExec を検査し、side_effect != NONE の idempotency_key 欠如を拒否。
  - 変更ファイル: `crates/cork-core/src/api/core_service.rs`。
  - 検証: `make fmt`, `make lint`, `make test`。

---

## CORE-026: GraphPatch ops 実装（NODE_ADDED / EDGE_ADDED / STATE_PUT / NODE_UPDATED）

### 内容

- Composite Graph に動的ノード・エッジを追加
- `STATE_PUT` で RunState/StageState を更新
- `NODE_UPDATED` で scheduling/htn/ttl を更新（最小でOK）

### 依存

CORE-024

### 触るファイル

- `rust/crates/cork-core/src/engine/patch.rs`
- `rust/crates/cork-store/src/lib.rs`（GraphStore/StateStore）

### サブタスク

- [x] GraphStore（inmem）:
  - nodes: HashMap<NodeId, NodeSpec>
  - edges: adjacency（deps）
- [x] NODE_ADDED:
  - NodeSpec格納（kind/exec/deps/scheduling/htn/ttl）
- [x] EDGE_ADDED:
  - deps追加（循環を "MVPは拒否" 推奨：stage内cycle検出）
- [x] STATE_PUT:
  - RunState/StageState JSON に pointer_mut で反映
- [x] NODE_UPDATED:
  - ttl/scheduling/htn を上書き

### DoD

- GraphPatchが実際に状態を変える（"受理するだけ"で終わらない）

### Acceptance Criteria

- [x] NODE_ADDED後、内部graphに node_id が出現する
- [x] EDGE_ADDED後、依存が追加される
- [x] STATE_PUTで RunState JSON が更新され、以後参照できる
- [x] すべての操作が RunEvent(graph_patch) として event_log に残る

### 進捗

- [DONE] GraphStore/StateStore と GraphPatch ops 適用を実装。
  - 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/engine/patch.rs`, `crates/cork-core/src/api/core_service.rs`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

# M3: ステート参照（JSON Pointer）とREADY判定、スケジューラ

## CORE-030: JSON Pointer 評価エンジン（RFC 6901）

### 内容

- `ValueRef`（RUN_STATE/STAGE_STATE/NODE_OUTPUT/NODE_ARTIFACT）を解決
- JSON Pointer（RFC 6901）で値を抽出
- NODE_OUTPUTは **JSONのとき** pointer適用、非JSONは `render_as=TEXT` かつ `json_pointer=""` のみ許可

### 依存

CORE-026

### 触るファイル

- `rust/crates/cork-core/src/engine/refs.rs`

### サブタスク

- [x] `resolve_value_ref(ref: ValueRef, store: &StateStore) -> ResolvedValue`
- [x] RUN_STATE / STAGE_STATE:
  - JSON Value に対して `.pointer(ptr)` で取得
- [x] NODE_OUTPUT:
  - `is_json == true` のときだけ `.pointer(ptr)` を許可
  - `is_json == false` の場合は `ptr=="" && render_as==TEXT` のみ許可
- [x] NODE_ARTIFACT:
  - artifacts[artifact_index] を返す
- [x] エラー型:
  - InvalidPointerSyntax（採用するなら json-pointer で parse）
  - NotFound / TypeMismatch / NotJsonOutput など

### DoD

- tool/llm input が参照式で組み立て可能になる

### Acceptance Criteria

- [x] RUN_STATEにSTATE_PUTした値を `/path/to/value` で参照できる
- [x] 不正ポインタ（構文不正）→ 参照解決エラーになる（ノードはREADYにならない）
- [x] 非JSON NODE_OUTPUTで pointer!="" → 解決失敗

### 進捗

- [DONE] JSON Pointer参照解決の実装とエラー型定義を追加。
- 変更ファイル: `crates/cork-core/src/engine/refs.rs`, `crates/cork-core/src/engine/mod.rs`, `crates/cork-store/src/lib.rs`。
- 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

## CORE-031: In-memory State Store（Run/Stage/Node outputs/artifacts）

### 内容

- RunState JSON（serde_json::Value）
- StageState JSON（stage_id -> Value）
- NodeOutput（node_id -> {payload, content_type, is_json, artifacts…}）
- 参照解決に必要なインデックスを提供

### 依存

CORE-026

### 触るファイル

- `rust/crates/cork-store/src/lib.rs`
- `rust/crates/cork-core/src/engine/run.rs`

### サブタスク

- [x] `run_state: serde_json::Value`（Objectで開始）
- [x] `stage_state: HashMap<StageId, Value>`
- [x] `node_outputs: HashMap<NodeId, NodeOutput>`
- [x] NodeOutput:
  - payload bytes + content_type
  - parsed_json: Option<Value>（content_typeがjsonならparseして保持）
  - artifacts: Vec<ArtifactRef>

### DoD

- 参照解決のための状態が一貫して保持される

### Acceptance Criteria

- [x] ノード完了時に NodeOutput が保存される
- [x] `NODE_ARTIFACT` が `artifact_index` で参照できる

### 進捗

- [DONE] NodeOutputのJSON判定と保存ヘルパを追加し、参照解決のテストを更新。
- 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/engine/run.rs`, `crates/cork-core/src/engine/refs.rs`。
- 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

## CORE-032: READY判定（deps + ref解決 + stage order）

### 内容

- Node deps が全て終端であること
- InputSpec 内の ref がすべて解決可能であること
- stage順序（Contract HARD）を破らないこと

### 依存

CORE-030, CORE-022

### 触るファイル

- `rust/crates/cork-core/src/engine/run.rs`

### サブタスク

- [x] NodeRuntimeState:
  - status（PENDING/READY/RUNNING/…）
  - last_error
- [x] deps終端条件:
  - SUCCEEDEDのみで良いか、FAILEDでも進めるか（MVPはSUCCEEDEDのみ推奨）
- [x] 参照解決の "事前チェック":
  - tool/llm input内の ref をすべて `resolve_value_ref` してみる
  - 1つでも不可→READYにしない
- [x] stage順序:
  - upstream stages が COMMITTED でなければ、このstageのノードをREADYにしない

### DoD

- "実行できるノードだけが READY になる" を保証

### Acceptance Criteria

- [x] deps未完了 → READYにならない
- [x] ref解決不可 → READYにならない
- [x] 上流stageが未完了なのに下流stage内nodeをREADYにしない

### 進捗

- [DONE] READY判定の評価ロジックとRuntimeState構造体を追加し、deps/ref/stage順序の判定テストを実装。
- 変更ファイル: `crates/cork-core/src/engine/run.rs`、`docs/tasks.md`。
- 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。
- [DONE] tool input内の入れ子refを再帰的に収集するよう修正し、READY判定のテストを追加。
  - 変更ファイル: `crates/cork-core/src/engine/run.rs`、`docs/tasks.md`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

## CORE-033: Resource Manager（cpu/io/provider + resource_pools）

### 内容

- policy.concurrency（cpu_max/io_max/per_provider_max）を資源容量に反映
- `resource_pools` をEXCLUSIVE/CUMULATIVEとして管理
- ノードの `scheduling.resources` / `alternatives` を予約
- 予約できない場合は READY→RUNNING に遷移できない

### 依存

CORE-023, CORE-026

### 触るファイル

- `rust/crates/cork-core/src/scheduler/resource.rs`

### サブタスク

- [x] ResourceId命名規約を実装:
  - `cpu`, `io`, `provider:<id>`, `tool:<name>`, `machine:<id>`
- [x] `Semaphore` ベースの予約:
  - capacityに応じてpermitを取る
- [x] alternatives（FJSP準備）:
  - MVPでは `alternatives` があれば "最初に取れるもの" を選ぶ（後で最適化）
- [x] 解放:
  - Node終了時にpermits返却
- [x] テスト:
  - capacity=1 の排他が効く

### DoD

- 「並列を堅く制御」する土台ができる

### Acceptance Criteria

- [x] cpu_max=1 のとき同時に2つRUNNINGにしない
- [x] provider:openai の max_concurrency に従い入場制御できる
- [x] EXCLUSIVE resource（machine:x）を同時に2つ取れない

### 進捗

- [DONE] ResourceManagerのプール初期化、予約/解放、alternatives選択の実装とユニットテストを追加。
  - 変更ファイル: `crates/cork-core/src/scheduler/resource.rs`, `crates/cork-core/src/scheduler/mod.rs`, `crates/cork-core/src/lib.rs`, `docs/tasks.md`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。
- [DONE] リクエスト量がプール容量を超える場合に恒久的エラーで返すよう修正し、容量超過テストを追加。
  - 変更ファイル: `crates/cork-core/src/scheduler/resource.rs`, `docs/tasks.md`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

## CORE-034: LIST_HEURISTIC スケジューラ（MVP）

### 内容

- READY集合から起動候補を選ぶ
- `policy.scheduler.tie_break`（FIFO/PRIORITY/SHORTEST…）を実装
- 起動時に scope を生成し、scope_seq を採番
- 状態遷移（NodeStateChanged）をRunEventとして発行

### 依存

CORE-032, CORE-033

### 触るファイル

- `rust/crates/cork-core/src/scheduler/list.rs`
- `rust/crates/cork-core/src/engine/run.rs`

### サブタスク

- [x] READY集合の収集
- [x] tie_break実装:
  - FIFO（node_idの安定順）
  - PRIORITY（scheduling.priority）
  - SHORTEST / LONGEST（estimated_duration_ms）
- [x] 起動手順:
  - ResourceManagerで予約
  - scope_id生成 + scope_seq採番（Log/RunEventに反映）
  - NodeStateChanged（READY→RUNNING）をemit
  - worker呼び出し（CORE-040）
- [ ] 終了手順（NodeFinished）:
  - output保存（CORE-031）
  - NodeStateChanged emit
  - 依存ノード再評価（READY判定を再実行）

### DoD

- "動く"スケジューラが成立（数理最適化は後）

### Acceptance Criteria

- [ ] READYノードが存在すれば実行が進む
- [x] tie_breakを変更すると起動順が変わる（テストで検証）
- [x] NodeStateChanged が event_seq順で流れる

### 進捗

- [DONE] LISTスケジューラのREADY集合収集とtie_breakソートを追加し、起動フローでResource予約・scope採番・READY→RUNNINGイベント発行・Worker呼び出しを連結。
  - 変更ファイル: `crates/cork-core/src/scheduler/list.rs`, `crates/cork-core/src/scheduler/mod.rs`, `crates/cork-core/src/engine/run.rs`, `crates/cork-core/Cargo.toml`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

## CORE-035: Stage auto-commit（Core責務）

### 内容

- `stage_auto_commit.enabled`
- `quiescence_ms` 以上 patchが来ない＋ PRE/WITHIN終端 → commit
- `max_open_ms` 超過時の扱い（commit か stage failure）を実装
- StageLifecycleEvent をRunEventとして発行

### 依存

CORE-034

### 触るファイル

- `rust/crates/cork-core/src/engine/autocommit.rs`
- `rust/crates/cork-core/src/engine/run.rs`

### サブタスク

- [x] stage metadata:
  - stage_started_at
  - last_patch_at
- [x] 判定:
  - PRE/WITHINが全終端
  - now - last_patch_at >= quiescence_ms
  - now - stage_started_at <= max_open_ms（超えた場合のポリシー：MVPは強制commit推奨）
- [x] commit実行:
  - StageLifecycleEvent emit（ACTIVE→COMMITTED）
  - 次stageをACTIVEへ
- [x] tick戦略:
  - Supervisorが `tokio::time::interval(200ms〜1s)` でTick

### DoD

- Planner無しでも stageが前進できる

### Acceptance Criteria

- [x] quiescence条件で COMMITTED に遷移し、次stageがACTIVEになる
- [x] UIが StageLifecycleEvent を見てステージ進行を復元できる

### 進捗

- [DONE] Stage auto-commit の判定ロジックとメタデータ更新を追加。
- [DONE] GraphPatchがアクティブステージに触れた時のみ last_patch_at を更新するよう修正。
- 変更ファイル: `crates/cork-core/src/engine/autocommit.rs`, `crates/cork-core/src/api/core_service.rs`, `crates/cork-store/src/lib.rs`, `crates/cork-core/src/engine/run.rs`。
- 検証: `make fmt`, `make lint`, `make test`。

---

# M4: Worker呼び出し、ログ、UI成立

## CORE-040: CorkWorker クライアント（InvokeTool / InvokeToolStream）

### 内容

- tonic client 実装
- deadlineを設定して呼び出し（gRPCのdeadline/cancel前提）
- Stream版では heartbeat/log chunk を受け取り RunEvent(log) として積む

### 依存

CORE-034

### 触るファイル

- `rust/crates/cork-core/src/worker/client.rs`

### サブタスク

- [x] tonic client生成（`CorkWorkerClient<Channel>`）
- [x] deadline算出:
  - run/stage/node budgetから残り時間を計算
  - `Request::set_timeout(Duration)` を設定
- [x] InvokeTool:
  - unaryで結果受領
- [x] InvokeToolStream:
  - heartbeat/log chunk を逐次RunEvent(log)に変換してappend
- [x] エラー処理:
  - tonic Status を Node FAILED reason に反映

### DoD

- "toolが走る"最小の実行ループが成立

### Acceptance Criteria

- [x] Workerが応答すれば Node が SUCCEEDED になる
- [x] Workerがエラーなら Node が FAILED になり reason が残る
- [x] deadline超過を模擬でき、deadline_exceeded相当の挙動になる

### 進捗

- [DONE] CorkWorker クライアントと InvokeTool / InvokeToolStream の結果処理を追加。
- [DONE] deadline算出と gRPC timeout 設定、ログ/失敗イベントの反映を実装。
- [DONE] caller指定deadlineとbudgetを比較して早い方を採用するよう修正。
- 変更ファイル: `crates/cork-core/src/worker/client.rs`, `crates/cork-core/src/worker/mod.rs`, `crates/cork-core/src/lib.rs`。
- 検証: `make fmt`, `make lint`, `make test`。

---

## CORE-041: Log Store（scope_id + scope_seq）と GetLogs

### 内容

- Run内ログを保存（in-memoryでOK）
- `scope_id` と `scope_seq` をCoreが必ず付与
- GetLogsでフィルタ（scope_id/span_id/stage/node）とページング

### 依存

CORE-034

### 触るファイル

- `rust/crates/cork-store/src/lib.rs`（LogStore）
- `rust/crates/cork-core/src/api/core_service.rs`

### サブタスク

- [x] LogRecordの付与:
  - scope_id, scope_seq はCoreが採番して必ず埋める
- [x] 保存:
  - run_idごとに Vec<LogRecord>
- [x] GetLogsフィルタ:
  - scope_id/span_id/stage_id/node_id で絞り込み
- [x] page_token（offset）実装

### DoD

- UIがログをマージ表示できる

### Acceptance Criteria

- [x] scope_idを指定してログ取得できる
- [x] scope_seq が単調増加で付いている
- [x] ページングが機能する（page_token）

### 進捗

- [DONE] LogStoreの追加とscope_id/scope_seq採番、Run単位のログ保存を実装。
  - 変更ファイル: `crates/cork-store/src/lib.rs`。
- [DONE] GetLogsのフィルタ・ページング実装とログ付与の連携を追加。
  - 変更ファイル: `crates/cork-core/src/api/core_service.rs`, `crates/cork-core/src/worker/client.rs`。
- [DONE] LogStore/GetLogsのテストを追加。
  - 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/api/core_service.rs`。
- [DONE] LogRecordのtsが保存されるようログ付与処理を補正。
  - 変更ファイル: `crates/cork-core/src/worker/client.rs`。
- 検証: `pre-commit run --all-files`, `make fmt`, `make lint`, `make test`。

---

## CORE-042: GetCompositeGraph（contract + patch列）

### 内容

- contract_manifest + patches_in_order を返す
- composite_graph_hash も返す

### 依存

CORE-021, CORE-026

### 触るファイル

- `rust/crates/cork-core/src/api/core_service.rs`
- `rust/crates/cork-store/src/lib.rs`（PatchStore/GraphStore）

### サブタスク

- [x] patch列を patch_seq順で保持（VecでOK）
- [x] GetCompositeGraphで contract + patches を返却
- [x] composite_hashも同時返却

### DoD

- 実験管理・比較の材料を取得できる

### Acceptance Criteria

- [x] patches_in_order が patch_seq で 0..N の順に返る
- [x] 返したデータでクライアント側が composite_hash 再計算して一致（統合テスト）

### 進捗

- [DONE] PatchStoreを追加し、patch_seq順での保存とcontract保持を実装。
  - 変更ファイル: `crates/cork-store/src/lib.rs`。
- [DONE] GetCompositeGraphでcontract/patches/composite_hashを返却する実装とテストを追加。
  - 変更ファイル: `crates/cork-core/src/api/core_service.rs`。
- [DONE] PatchStore/GetCompositeGraphのユニットテストを追加。
  - 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/api/core_service.rs`。
- 検証: `pre-commit run --all-files`, `make fmt`, `make lint`, `make test`。

---

## CORE-043: ApplyGraphPatch の拒否理由（rejection_reason）を体系化

### 内容

- 拒否理由を enum化（内部）し、外向き文字列にマップ
  - PATCH_SEQ_GAP
  - STAGE_NOT_ACTIVE
  - KIND_NOT_ALLOWED
  - IDEMPOTENCY_REQUIRED
  - INVALID_JSON_POINTER
  - UNKNOWN_NODE_ID など

### 依存

CORE-024〜026

### 触るファイル

- `rust/crates/cork-core/src/engine/patch.rs`
- `rust/crates/cork-core/src/api/core_service.rs`

### サブタスク

- [x] `enum PatchRejectReason { PatchSeqGap, StageNotActive, KindNotAllowed, IdempotencyRequired, InvalidJsonPointer, UnknownNodeId, CycleDetected, ... }`
- [x] `impl Display` でUI向けの短い文言を生成
- [x] rejection_reasonに格納

### DoD

- デバッグ可能な拒否理由が返る

### Acceptance Criteria

- [x] 代表的拒否ケースで rejection_reason が期待通り
- [x] UIが rejection_reason をそのまま表示して理解できる文面

### 進捗

- [DONE] ApplyGraphPatch の拒否理由 enum と表示文言を追加し、主要な拒否ケースのテストを拡充。
  - 変更ファイル: `crates/cork-core/src/engine/patch.rs`, `crates/cork-core/src/api/core_service.rs`, `docs/tasks.md`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

# 横断（品質）：テストとデモ

## CORE-090: ユニットテスト（ハッシュ・連番・参照式）

### 内容

- JCSハッシュ安定テスト（RFC 8785ベース）
- patch_seq連番（欠番拒否）
- side_effect idempotency必須
- JSON Pointer 抽出（RFC 6901）

### 依存

CORE-020, CORE-024, CORE-030

### 触るファイル

- `cork-canon/tests/*`
- `cork-hash/tests/*`
- `cork-core/tests/*`

### サブタスク

- [x] JCS同値性テスト（フィールド順、deps順）
  - RFC 8785が"hashable representation"を意図していることを根拠に採用
- [x] patch_seq厳密連番テスト
- [x] side_effect idempotency強制テスト
- [x] JSON Pointer参照テスト（RFC 6901）

### DoD

- MVPの壊れやすい部分を固定する

### Acceptance Criteria

- [x] `cargo test` で上記すべてが通る

### 進捗

- [DONE] patch_seq/side_effect/参照式の拒否条件テストを追加して仕様固定を完了。
  - 変更: `crates/cork-core/src/engine/patch.rs`, `crates/cork-core/src/engine/refs.rs` にテスト追加。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

## CORE-091: 統合テスト（最小Runが最後まで進む）

### 内容

- 小さなContract（collect→analyze）をSubmitRun
- patch_seq=0で NODE_ADDED(tool) + STATE_PUT
- Workerはテスト用stub（固定応答）
- StreamRunEventsで状態遷移を検証

### 依存

CORE-040 まで

### 触るファイル

- `cork-core/tests/e2e_minimal.rs`

### サブタスク

- [x] テストWorker stubを立てる（同一プロセスでtonic server起動）
- [x] SubmitRun（小さいContract：stage1→stage2）
- [x] stage1に patch_seq=0 をApply（NODE_ADDED tool）
- [x] tool完了→stage auto-commit→stage2へ
- [x] StreamRunEvents を購読し、期待イベント列を検証

### DoD

- "最小実装"が end-to-end で動く

### Acceptance Criteria

- [x] Runが RUN_SUCCEEDED まで到達
- [x] event_seqが連続である
- [x] composite_hashが安定
- [x] GetLogsで scope 単位ログが取れる

### 進捗

- [DONE] e2e最小フローの統合テストを追加し、SubmitRun→Patch→Worker実行→auto-commit→完了を検証。
  - 変更ファイル: `crates/cork-core/tests/e2e_minimal.rs`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。

---

# GitHub Issues 化のテンプレ

各Issue本文をこの形にすると、運用がブレません：

```markdown
## Goal（目的）
[このIssueで達成すること]

## Scope（このIssueでやること／やらないこと）
- やること:
- やらないこと:

## Implementation Notes（設計メモ・関係モジュール・型）
[関係するファイル、型、設計上の考慮点]

## Subtasks（チェックボックス）
- [ ] サブタスク1
- [ ] サブタスク2
- [ ] ...

## Definition of Done（共通DoD + このIssue固有）
- [ ] `cargo build` が通る
- [ ] `cargo fmt` が適用済み
- [ ] `cargo clippy -- -D warnings` が通る
- [ ] テストが追加され `cargo test` が通る
- [ ] [Issue固有のDoD]

## Acceptance Criteria（観測可能な条件）
- [ ] [条件1]
- [ ] [条件2]

## Tests（追加するテスト）
- [追加するテストファイル/内容]

## Docs（更新するdocs）
- [更新するドキュメント]
```

---

# 実務メモ（タスク分割のコツ）

- **GraphPatchの検証（CORE-024/025）** は最初に固めると後工程が安定します
- **event_seq/patch_seq** を強制すると、UIも実験管理も一気に楽になります
- **参照式（CORE-030）** は "READY判定" と一体で作るとバグが減ります

---

# Post-MVP: Hardening / Ops / Correctness

> 目的:
> - MVPが「動く」状態から「落ちにくい」「再現できる」「運用できる」状態へ引き上げる
> - Spec との齟齬/曖昧さを潰し、並列実行・順序保証・結果管理を“壊れない”形にする
>
> 参照:
> - docs/specification.md
> - docs/dod.md（共通DoD）
> - docs/mvp.md

---

## 実装順（推奨）
P0（バグ/性能劣化の芽）を先に全部潰す:
1. CORE-100 (expansion_policy defaults)
2. CORE-101 (std::sync ロック排除)
3. CORE-103 (Patchの完全原子性)
4. CORE-102 (policyの保存・取得)
5. CORE-112 (backpressure/retention)
6. CORE-150 (入力サイズ制限/クォータ)

その後、運用耐性:
7. CORE-111 (watchdog: TTL/idle/patch嵐)
8. CORE-113 (worker呼び出しのresilience)
9. CORE-141 (graceful shutdown / drain)
10. CORE-110 (永続ストア / 再起動復元)

観測性/将来拡張:
11. CORE-120 (structured tracing/metrics)
12. CORE-130 (Job-shop/SHOP3向けメタデータ拡張)
13. CORE-143 (Spec conformance test suite)
14. CORE-152 (fuzz / property tests)

---

# P0: Spec Correctness / 破綻防止（最優先）

## [x] CORE-100: expansion_policy の defaults フォールバック/マージ対応（Spec整合）
**Priority:** P0
**Type:** Bugfix / Spec compliance
**Depends on:** 既存 SubmitRun 実装

**Goal**
- `stage.expansion_policy` が省略された正当な contract を SubmitRun が reject しない
- `defaults.expansion_policy` を仕様通りに適用する

**Scope**
- `stage.expansion_policy` 優先
- 無ければ `defaults.expansion_policy` を使用
- 「defaults + stage override」のマージルールを明文化（allow_kindsの扱い等）

**Subtasks**
- [x] `parse_expansion_policy` を修正し、stage->defaults の順で解決
- [x] マージルールを実装（最低限: stageが存在する場合はstageを採用、将来に備えて merge 関数を分離）
- [x] contract schema の該当箇所（defaults の定義）と整合を再確認
- [x] ユニットテスト追加:
  - [x] stage省略 + defaultsあり => OK
  - [x] stage省略 + defaultsなし => Reject（reason明確化）
- [x] E2E統合テストに defaults 省略ケースを追加（既存 minimal を置換 or 追加）

**DoD**
- テストで上記ケースが固定化され、今後の回帰を防ぐ
- 拒否時のエラーが「何が足りないか」特定できる

**Acceptance Criteria**
- [x] stage側 expansion_policy 省略の contract を SubmitRun が受理し、Run が開始できる

**Tests**
- unit: `parse_expansion_policy_*`
- integ: `e2e_minimal_defaults_expansion_policy`

**Docs**
- docs/specification.md の該当章に「defaults適用順序」を追記

### 進捗
- [DONE] defaults優先度のパース/マージを追加し、欠落時のエラーメッセージを明確化。
  - 変更ファイル: `crates/cork-core/src/api/core_service.rs`。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。
- [DONE] defaults適用順序を仕様書に追記し、defaults-onlyのE2E/ユニットテストを追加。
  - 変更ファイル: `docs/specification.md`, `crates/cork-core/tests/e2e_minimal.rs`, `crates/cork-core/src/api/core_service.rs`。
  - 検証: `make test`。


---

## [x] CORE-101: async コンテキストの `std::sync::{Mutex,RwLock}` を排除（Tokioでブロックしない）
**Priority:** P0
**Type:** Perf / Stability
**Depends on:** なし（独立、ただし広範囲変更）

**Goal**
- 高並列で gRPC が詰まる/レイテンシが跳ねる要因（ブロッキングロック）を排除する

**Scope**
- `std::sync::Mutex/RwLock` を棚卸しし、用途別に置換:
  - マップ系: `dashmap::DashMap`
  - 共有状態: `tokio::sync::{Mutex,RwLock}`
  - Supervisor内の単一スレッド所有に寄せられるものはロック自体を消す

**Subtasks**
- [x] 置換対象の一覧を作る（ファイル/型/保持時間）
- [x] `RunRegistry` / `PatchStore` / `EventLog` / `LogStore` のロックを非ブロッキング化
- [x] ロック保持時間を短縮（cloneしてから処理、など）
- [x] 並列テスト追加（apply_patch + stream_events + get_logs を同時に叩く）

**DoD**
- `cargo clippy -D warnings` を維持
- 並列テストが安定して通る

**Acceptance Criteria**
- 100並列程度の混在アクセスでタイムアウト/ハングしない（CIで再現可能）

**Tests**
- `tokio::test` で負荷テスト（軽量）
- 可能なら `loom`（P2でも可）

**Docs**
- docs/adr/ に「asyncロック方針」を1本追加（なぜDashMap/ tokio lock か）

### 進捗
- [DONE] 置換対象: `crates/cork-store/src/lib.rs`（RunCtx metadata / RunRegistry / PatchStore / GraphStore / StateStore / EventLog / LogStore）、`crates/cork-core/src/api/core_service.rs`（event_logs）。
  - マップ系は DashMap、共有状態は Tokio RwLock に置換。RunRegistry の run_order は clone してロック保持を短縮。
  - 検証: `make fmt`, `make lint`, `make test`, `pre-commit run --all-files`。
- [DONE] 並列テストを追加（apply_graph_patch + stream_run_events + get_logs を同時に実行）。
  - 変更ファイル: `crates/cork-core/src/api/core_service.rs`。
- [DONE] ADR 0002（非同期ロック方針）を追加。
  - 変更ファイル: `docs/adr/0002-async-lock-policy.md`。


---

## [x] CORE-103: ApplyGraphPatch の完全原子性（partial applyを絶対に起こさない）
**Priority:** P0
**Type:** Correctness / Data integrity
**Depends on:** CORE-101（推奨：ロック整理後の方が安全）

**Goal**
- Patch適用が失敗した場合に、Graph/State/PatchStore が“一部だけ更新された”状態を作らない

**Scope**
- Preflight（検証）と Commit（適用）を分離
- 失敗パスで state/graph が変化しないことをテストで保証

**Subtasks**
- [x] Patch適用を `validate_patch(...) -> ValidatedPatch` と `commit_patch(validated)` に分割
- [x] `commit_patch` は「全部適用できる」前提でのみ mutate
- [x] in-memory での実装案:
  - [ ] Copy-on-write（cloneして適用→成功したらswap）
  - [x] もしくは transaction log を積んで最後に反映
- [x] 失敗ケースのテスト追加:
  - [x] ops途中で不正 node_id / edge / pointer が出る
  - [x] allow_kinds違反
  - [x] side_effect の key 欠落
  - [x] => いずれも graph/state が不変であることを検証

**DoD**
- 「rejectされたpatchで状態が変わらない」を統合テストで保証

**Acceptance Criteria**
- patchが reject された場合に `GetCompositeGraph` / `GetRun` の観測結果が変化しない

**Tests**
- unit: apply_patch_atomicity（before/after equality）
- integ: “invalid patch then valid patch” の順で流しても壊れない

**Docs**
- docs/specification.md に「Patch原子性（all-or-nothing）」を明記

### 進捗
- [DONE] GraphPatch適用をvalidate/commitに分割し、検証でのみ失敗させてからcommitする構成に変更。
  - 変更ファイル: `crates/cork-core/src/engine/patch.rs`, `crates/cork-core/src/api/core_service.rs`, `crates/cork-store/src/lib.rs`。
- [DONE] 失敗時にgraph/state/patchesが変化しないことをユニット・統合テストで検証。
  - 変更ファイル: `crates/cork-core/src/engine/patch.rs`, `crates/cork-core/src/api/core_service.rs`。
- [DONE] 仕様書にGraphPatchのall-or-nothingを追記。
  - 変更ファイル: `docs/specification.md`。


---

## [x] CORE-102: SubmitRun で policy をストアへ保存し、取得可能にする（実験管理の土台）
**Priority:** P0
**Type:** Correctness / Experiment reproducibility
**Depends on:** CORE-103（推奨：整合性ルールが固まってから）

**Goal**
- contract と policy が run_id から再取得でき、再現性の材料になる

**Scope**
- `set_policy(run_id, CanonicalJsonDocument)` / `get_policy(run_id)` をストアに追加
- `GetRun` で policy hash / schema_id / sha256 を返す（返せない場合は GetPolicy API を追加）

**Subtasks**
- [x] PatchStore（or RunStore）に policy 保存領域を追加
- [x] SubmitRun で policy を保存
- [x] GetRun で policyのhash bundle を返却（または `GetPolicy` RPCを追加）
- [x] テスト:
  - [x] SubmitRun -> GetRun で policy_hash が一致
  - [x] SubmitRun -> GetCompositeGraph の hash_bundle と整合

**DoD**
- runをキーに “policyを含む再現材料” が取れる

**Acceptance Criteria**
- 運用上「そのRunがどのpolicyで動いたか」を後から確実に追える

**Docs**
- docs/specification.md に policy保管の要件を追記

### 進捗
- [DONE] PatchStoreにpolicy保存領域を追加し、SubmitRun/GetRunでpolicy取得を可能にした。
  - 変更ファイル: `crates/cork-store/src/lib.rs`, `crates/cork-core/src/api/core_service.rs`, `proto/cork/v1/core.proto`, `crates/cork-core/src/engine/run.rs`。
- [DONE] GetRunでpolicyのschema_id/sha256とhash bundleの整合性を検証する統合テストを追加。
  - 変更ファイル: `crates/cork-core/tests/e2e_minimal.rs`。
- [DONE] 仕様書にpolicy保存とGetRun返却要件を追記。
  - 変更ファイル: `docs/specification.md`。


---

## [ ] CORE-112: StreamRunEvents / GetLogs の backpressure & retention（OOM防止）
**Priority:** P0
**Type:** Stability / Availability
**Depends on:** CORE-101（推奨）

**Goal**
- ログ・イベントが大量に出てもメモリが無制限に増えない
- 遅い購読者がいてもサーバが固まらない

**Scope**
- bounded channel（キュー上限）導入
- retention（件数/バイト/期間）導入
- slow subscriber の扱い方針を決める（drop / disconnect / latest-only）

**Subtasks**
- [ ] `EventLog` の broadcast/backlog を bounded 化（方針決定）
- [ ] `LogStore` の保持上限（max_records/max_bytes）を config 可能に
- [ ] `since_event_seq` が retention の外に落ちた場合の扱い:
  - [ ] エラー（OUT_OF_RANGE） or “最古seqから再開” を仕様化
- [ ] 負荷テスト（大量ログ + 遅い購読者）

**DoD**
- メモリ上限が設定で制御できる
- 遅い購読者で他が巻き込まれない

**Acceptance Criteria**
- 大量ログ発生時でも corkd が OOM しない（少なくともCIで再現可能な規模で確認）

**Docs**
- docs/specification.md に retention/backpressure の仕様を追記


---

## [ ] CORE-150: 入力サイズ制限・クォータ（巨大JSON/巨大patchで落ちない）
**Priority:** P0
**Type:** Security / Stability
**Depends on:** CORE-103

**Goal**
- 悪意/事故（巨大な contract/policy/patch/log）で corkd が落ちない

**Scope**
- gRPC max message size（受信/送信）の設定
- contract/policy/patch の max bytes / max nodes / max edges / max ops
- 1 stage あたりの max patch count（別タスク CORE-111 と連携）

**Subtasks**
- [ ] サーバ設定で gRPC message size 上限を設定（デフォルトを安全側に）
- [ ] SubmitRun: contract/policy の最大サイズ・最大ステージ数制限
- [ ] ApplyGraphPatch: ops数/ノード数/エッジ数制限
- [ ] reject reason を体系化（CORE-043 に寄せる）
- [ ] テスト（境界値: 上限-1/上限/上限+1）

**DoD**
- “巨大入力でプロセスが落ちない” がテストで担保される

**Acceptance Criteria**
- 上限超過時は必ず reject され、Run状態やストアが壊れない


---

# P1: 運用耐性（長時間実行 / 失敗が日常の環境）

## [ ] CORE-111: watchdog（TTL / idle / patch嵐 / 無限ループ検知）
**Priority:** P1
**Type:** Availability
**Depends on:** CORE-112（推奨：ログ過多対策と相性）

**Goal**
- Run が永遠にぶら下がる/patch が無限に投げられる状況を Core が自律的に収束させる

**Scope**
- run_ttl / idle_ttl
- max_total_patch_count / max_patch_count_per_stage
- 進捗（progress）の定義: node state change / stage commit / patch applied 等

**Subtasks**
- [ ] policy に watchdog 設定を追加（後方互換: optional）
- [ ] Supervisor tick で watchdog 判定
- [ ] 閾値超えで CancelRun 相当を発火（reason付き）
- [ ] テスト:
  - [ ] idle_ttl 超過で cancel
  - [ ] patch嵐で cancel
  - [ ] stage open too long（max_open_ms）と整合

**DoD**
- “止まらないRun” が時間/回数で必ず止まる

**Acceptance Criteria**
- 指定TTLを越える Run が残り続けない


---

## [ ] CORE-113: Worker呼び出しの resilience（timeout / retry / backoff / circuit breaker）
**Priority:** P1
**Type:** Reliability
**Depends on:** CORE-101

**Goal**
- Workerが不安定でも CorkCore が固まらず、再試行 or 明確な失敗で収束する

**Scope**
- per-node timeout（grpc-timeout）
- retry（指数バックオフ + jitter）
- circuit breaker（一定時間 fail-fast）

**Subtasks**
- [ ] エラー分類（retryable / non-retryable）を導入
- [ ] retry policy を policy 側から設定可能に（デフォルトも用意）
- [ ] circuit breaker 状態を run event として出す（可観測に）
- [ ] 統合テスト:
  - [ ] Workerが一時的に落ちる → retryで回復
  - [ ] 永続失敗 → 最終的にFAILED

**DoD**
- 失敗時の state 遷移が一貫し、理由がログに残る

**Acceptance Criteria**
- Worker障害で corkd がハングしない


---

## [ ] CORE-141: graceful shutdown（drain / flush / consistency）
**Priority:** P1
**Type:** Ops readiness
**Depends on:** CORE-103, CORE-112

**Goal**
- SIGTERM 等で落とすときに、状態/ログが中途半端にならない

**Scope**
- サーバ停止時:
  - 新規リクエスト停止
  - 進行中 run の扱い（cancel or drain）を設定可能に
  - event/log flush（永続化がある場合は必須）

**Subtasks**
- [ ] shutdown signal を受けて gRPC server を drain
- [ ] Supervisor に cancel/drain コマンド送付
- [ ] “停止時ポリシー” を設定で切替
- [ ] テスト: 停止→再起動後に run が整合した状態で見える

**DoD**
- “落とし方” が決まり、運用で事故らない

**Acceptance Criteria**
- shutdown でデータ不整合が起きない（少なくともE2Eで確認）


---

## [ ] CORE-110: 永続ストア（SQLite等）/ 再起動復元（実験管理の実体化）
**Priority:** P1
**Type:** Durability
**Depends on:** CORE-103, CORE-141

**Goal**
- corkd 再起動で Run/Graph/Patch/Event/Logs が復元される

**Scope（段階導入）**
- Phase 1: RunRegistry + PatchStore + EventLog（最低限の復元）
- Phase 2: GraphStore + StateStore + LogStore

**Subtasks**
- [ ] Store trait の境界を再点検（永続化しやすい責務に）
- [ ] SQLite 実装（feature flag）を追加
- [ ] migration（schema_version）導入
- [ ] 統合テスト:
  - [ ] 起動→SubmitRun→Patch→停止→再起動→GetCompositeGraph一致
  - [ ] event_seq / patch_seq の連続性の扱いを仕様化（再起動で連続維持 or 再計算）

**DoD**
- “再起動しても再現できる” が動く

**Acceptance Criteria**
- 再起動後に、Runの履歴と composite graph が一致する


---

# P1: 観測性 / 利用可能性

## [ ] CORE-120: structured tracing + metrics（run_id/scope_id/patch_seq を相関IDに）
**Priority:** P1
**Type:** Observability
**Depends on:** CORE-101

**Goal**
- 並列実行のデバッグを “ログ/メトリクスで” 可能にする（運用で詰まらない）

**Scope**
- tracing span に run_id/stage_id/node_id/scope_id/patch_seq を必ず付与
- Prometheus などのメトリクス（req latency / queue depth / running nodes / worker errors）

**Subtasks**
- [ ] gRPC interceptor で request_id を付与
- [ ] 主要APIハンドラに span を張る
- [ ] metrics exporter を追加（feature flagでも可）
- [ ] “観測項目一覧” を docs に固定

**DoD**
- “どのRunの何が遅い/失敗した” が観測可能

**Acceptance Criteria**
- run_id だけで関連ログ/イベントが追える


---

# P2: 将来の最適化（Job-shop / SHOP3）に向けた下地

## [ ] CORE-130: Graphの制約メタデータ拡張（Job-shop / HTN 将来対応のための保存層）
**Priority:** P2
**Type:** Future-proofing
**Depends on:** CORE-103

**Goal**
- 後で CP-SAT / Job-shop / SHOP3 に繋げられるデータ項目を“壊さず”保持する

**Scope**
- Node/Edge に optional メタデータ追加（schedulerが未利用でもOK）
  - resource_profile（cpu/io/mem/gpu の相対コスト）
  - exclusive_key（mutex group）
  - release_time / deadline / due_date
  - estimated_duration_ms
  - preemptible（将来）
- canonicalization/hashing への影響を仕様化（prenorm対象の決定）

**Subtasks**
- [ ] schema を更新（後方互換）
- [ ] cork-canon の prenorm に追加ルール（必要なら）
- [ ] GetCompositeGraph で欠損なく返す
- [ ] unit test: hash stability（順序の違いが同値扱いになる箇所の固定）

**DoD**
- “将来の最適化” のために、今のGraphが捨て情報にならない

**Acceptance Criteria**
- メタデータ付きpatchが受理・保持・取得できる


---

# P2: テスト強化（壊れないことの担保）

## [ ] CORE-143: Spec conformance test suite（仕様準拠を自動で証明）
**Priority:** P2
**Type:** Quality
**Depends on:** CORE-100, CORE-103

**Goal**
- 実装が進んでも spec から逸脱しない（回帰しない）

**Scope**
- 仕様の“重要不変条件”をテストにする:
  - patch_seq 厳密連番
  - event_seq 単調増加（Run内）
  - reject時に状態不変（原子性）
  - side_effect の idempotency_key 必須
  - JSON pointer / value ref の境界

**Subtasks**
- [ ] spec の不変条件を列挙してテスト一覧化
- [ ] E2E + unit に配分して実装
- [ ] 破壊的変更が必要な場合は ADR 化

**DoD**
- 仕様逸脱が CI で止まる

**Acceptance Criteria**
- PRで spec を壊すと必ずテストが落ちる


---

## [ ] CORE-152: fuzz / property tests（Patch/JSON入力の堅牢性）
**Priority:** P2
**Type:** Security / Robustness
**Depends on:** CORE-150

**Goal**
- 変なJSON/境界値入力で panic / OOM / deadlock しない

**Scope**
- `apply_patch(validate)` の fuzz
- JSON pointer / ref 解決の fuzz
- 可能なら proptest で “原子性” を性質として固定

**Subtasks**
- [ ] fuzz target（cargo-fuzz）追加
- [ ] proptest で patch ops をランダム生成し、reject時不変性をチェック
- [ ] CIで短時間 fuzz を回す（nightly job でも可）

**DoD**
- “入力が悪くても落ちない” の確率を上げる

**Acceptance Criteria**
- 最低限の fuzz ランが CI で回り、panic を検知できる
