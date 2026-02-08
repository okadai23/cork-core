# CORK v0.1 仕様書（Kernel/Core）

## 1. 目的と背景

### 1.1 CORKが解決する問題
CORK（COncurrency + ORder Kernel）は、LLM/Tool実行を「実行OS（Kernel）」として統制することを目的とする。既存のAI Agentフレームワークの課題を、以下の一貫したデータモデルと実行規則で解決する。

- **実行時間制約・締切・TTLの弱さ**
  - Run/Stage/Nodeに **Budget/Deadline/TTL** を持たせ、期限超過や上限超過を確実に執行する。
- **履歴・キャッシュが活用できない**
  - **Event Log** と **GraphPatch** を採用し、実行の全履歴を再生・比較できる。
- **並列実行管理とログ回収の難しさ**
  - **Structured Concurrency（Scope）** と **決定的ログマージ** をネイティブに提供する。
- **最適化/計画の外部委譲が難しい**
  - Planner/Optimizerは外部（例: Python）で自由に実装しつつ、Kernelが **順序・資源・制約** を統制する。
- **実験管理・比較が難しい**
  - Contract + Patch の合成グラフをハッシュ化し、Run/Variant/Experimentを比較可能にする。

### 1.2 本仕様における「非機能」
本仕様では非機能を以下に限定する。

- LLMアクセス/Tool呼び出しの **並行（Concurrency）**
- **順序保持（Order）** と依存管理
- Deadline/Timeout/Cancel/Retry/Rate-limit の **確実な執行**
- ログ・結果の **回収可能性（mergeable）**
- **再実行・再現性・比較可能性**

---

## 1.3 参照ドキュメント
- 共通 Definition of Done: [`docs/dod.md`](dod.md)
- MVPスコープ: [`docs/mvp.md`](mvp.md)
- 正規化仕様: [`docs/canonicalization.md`](canonicalization.md)
- ハッシュ仕様: [`docs/hashing.md`](hashing.md)

---

## 2. 参照規格・外部前提
CORKは相互運用性を確保するため、以下の規格・概念を採用する。

- **JSON Pointer (RFC 6901)**: JSON内部の参照式
- **JSON Canonicalization (RFC 8785 / JCS)**: 決定的JSON表現とハッシュ化
- **gRPC deadline/cancel**: 期限超過時の自動キャンセル
- **OpenTelemetry Logs/Trace**: TraceId/SpanId相関、Span Links
- **ジョブショップ/フレキシブルジョブショップ**: 資源制約と順序制約の同時扱い
- **HTN / SHOP3**: strict partial orderと ordered task decomposition

---

## 3. システム全体像

### 3.1 コンポーネント

- **CORK Core（Rust）**
  - Graph Scheduling（順序・依存・資源）
  - Structured Concurrency（Scope）
  - Budget/Deadline/TTL/RateLimit/Retry/Idle/Guard
  - Event Log（GraphPatch、状態遷移、policy介入）
  - キャッシュ、チェックポイント
  - gRPCサーバ（Core API）

- **CORK Worker（Python）**
  - Tool Runner
  - Agent/Planner（動的GraphPatch生成）
  - Optimizer/Planner（DSPy、バンディット、数理最適化、HTN）

- **CORK Control Plane/UI（TypeScript）**
  - StreamRunEvents / GetLogs を一次情報源に可視化
  - Run/Experiment比較、差分表示
  - Scope単位のログマージ表示

### 3.2 データフロー（簡略）
1. UI/SDK → Core：Contract Manifest + Policy + inputでRun作成
2. Core：Contract順にStageを進め、WorkerへTool/LLM実行依頼
3. Planner：必要に応じてGraphPatchを送信
4. Core：GraphPatchを検証しComposite Graphに適用
5. Core：イベント（状態遷移、patch、ログ、policy介入）をストリーム
6. UI：イベントから実行全体を再構成・比較

---

## 4. コア概念

### 4.1 二層グラフモデル

- **Contract Graph（固定・順序強制）**
  - ユーザー定義の「必ず守る順序」
  - Stage DAGで表現し、HARD edgeは破れない

- **Expansion Graph（動的拡張）**
  - Run中にPlanner/Agentが追加するノードや依存
  - **GraphPatch**としてCoreに適用しEvent Logに記録

- **Composite Graph**
  - Contract + Patch列（patch_seq順）の適用結果
  - 実験比較・再現・キャッシュのためハッシュ化

### 4.2 Structured Concurrency（Scope）

- すべてのNode実行はScopeに所属
- Scopeは親子構造を持ち、キャンセル/Deadline/ログが伝播
- ログは scope_id + scope_seq により決定的にマージ可能

### 4.3 順序付けルール（JSP/HTN互換）

1. **Contract order（HARD）**: Stage DAGの必須順序
2. **Precedence（部分順序）**: Node依存エッジ（HTNの≺）
3. **Resource feasibility（資源制約）**: 機械/CPU/IO/Providerなど容量・排他

### 4.4 Contract defaults の適用順序
- Contract Manifestのdefaultsは、Stageが省略した設定を補うために使う。
- **expansion_policyの優先順位**:
  1. `stage.expansion_policy` が存在する場合はそれを採用
  2. 無い場合は `defaults.expansion_policy` を採用
- 両方に `expansion_policy` が無い場合は契約不備として拒否する。
- 将来の拡張に備え、`defaults` と `stage` のマージは「stageが上書きする」前提で定義する。

---

## 5. ID・順序・再現性

### 5.1 ID規約

| ID | 意味 | 規約/備考 |
| --- | --- | --- |
| run_id | Run一意ID | UUID/ULID等 |
| stage_id | Contract Stage ID | 英数/`_`/`-`、1〜64文字 |
| node_id | Composite GraphのノードID | 動的ノードは決定的生成 |

**動的ノードID推奨**: `{stage_id}/dyn/{seq}`（Stage内単調増加）

### 5.2 GraphPatchのpatch_seq
- patch_seqは **0,1,2,...** の厳密連番
- 欠番のPatchはCoreが拒否

### 5.3 GraphPatchの原子性（all-or-nothing）
- GraphPatchは **検証と適用を分離** し、rejectされた場合は Graph/State/PatchStore に **一切変更を加えない**。

---

## 6. 参照式（state参照）

### 6.1 参照式形式
- **JSON Pointer（RFC 6901）**を採用
- 空文字列 `""` は「対象全体」

### 6.2 参照対象（ref_type）
- RUN_STATE
- STAGE_STATE
- NODE_OUTPUT
- NODE_ARTIFACT

### 6.3 非JSON出力の扱い
- NODE_OUTPUT参照は **出力がJSONのときのみ**
- 例外: `render_as=TEXT` かつ `json_pointer=""` の場合のみ許可
- それ以外は解決失敗（依存未解決として扱う）

---

## 7. Stage Auto-Commit

### 7.1 基本方針
PlannerはStageCommittedを送らなくてよい。Coreが自動推定し、必ずRunEventとして記録する。

### 7.2 Auto-Commit条件（v0.1）
Stage SがCOMMITTEDになる条件（すべて満たす）:

1. SがACTIVE
2. S内のPRE/WITHINノードがすべて終端状態
3. quiescence_ms以上、SへのGraphPatchが到着していない
4. exclude_when_waiting=trueのとき、Sがdurable_waitを含むならcommitしない
5. max_open_ms超過時は強制commit（またはpolicyによりstage failure）

---

## 8. side_effect ToolとIdempotency

- ToolExecで `side_effect != NONE` の場合、`idempotency_key` は必須
- 欠けているGraphPatchはCoreが拒否

---

## 9. 最適化/HTN対応メタモデル

### 9.1 SchedulingProfile（ジョブショップ/FJSP）
ノードに以下を持たせ、将来の最適化に備える。

- estimated_duration_ms
- resources[]（固定資源要求）
- alternatives[]（代替資源＋処理時間）
- earliest_start_unix_ms, deadline_unix_ms
- priority

### 9.2 HTN/SHOP3メタデータ

- task_network_id, task_id, parent_task_id
- task_name, task_args_json
- task_type（PRIMITIVE/COMPOUND）
- method_name, operator_name
- decomposition_depth

---

## 10. 資源プール命名規約

- `cpu`: CPUバウンド資源
- `io`: IOバウンド資源
- `provider:<provider_id>`: LLM/外部プロバイダ単位
- `tool:<tool_name>`: tool単位の排他/制限
- `machine:<id>`: ジョブショップ機械（排他資源）

---

## 11. 観測とログマージ

### 11.1 ログ相関（OpenTelemetry）
全ログに trace_id/span_id を付与し、ログとスパンを相関可能にする。

### 11.2 並列ログマージ
全ログに `scope_id` と `scope_seq` を付与し、以下順で決定的マージする。

1. timestamp
2. scope_id
3. scope_seq
4. span_id（タイブレーク）

### 11.3 Fan-in/Fan-out（MERGE）
MERGEノードは入力ノードspanへのリンクを張り、採用/棄却を属性で保持する。

---

## 12. 正規化とハッシュ

- RFC 8785 (JCS) による決定的JSON表現
- Domain separation を付与してハッシュ化
- contract_manifest_hash / policy_hash / patch_hash / composite_graph_hash / run_config_hash を定義

詳細は以下のドキュメントを参照:
- `docs/canonicalization.md`
- `docs/hashing.md`

---

## 13. API（gRPC）とdeadline/cancel
CoreとWorker間はgRPCで接続し、deadline/cancelを必ず伝播する。

### 13.1 Runの再現材料
- Runごとに `contract_manifest` と `policy` を保存し、`run_id` から再取得できること。
- `GetRun` は `hashes.policy_hash` に加えて、`policy.schema_id` と `policy.sha256` を返す。

---

## 14. 参照実装資料

- JSON Schema:
  - `schemas/cork.contract_manifest.v0.1.schema.json`
  - `schemas/cork.policy.v0.1.schema.json`
- ADR:
  - `docs/adr/0001-cork-v0.1-core.md`
- GraphPatch Schema:
  - `schemas/cork.graph_patch.v0.1.schema.json`
- gRPC Proto:
  - `proto/cork/v1/types.proto`
  - `proto/cork/v1/core.proto`
  - `proto/cork/v1/worker.proto`
- タスクリスト:
  - `docs/tasks.md`

---

## 15. リポジトリ構成（Workspace / Crate / Module）

### 15.1 ディレクトリ構成

```
cork/
  proto/                       # *.proto (source of truth)
    cork/v1/types.proto
    cork/v1/core.proto
    cork/v1/worker.proto

  schemas/                     # JSON Schema (source of truth)
    cork.contract_manifest.v0.1.schema.json
    cork.policy.v0.1.schema.json
    cork.graph_patch.v0.1.schema.json

  docs/
    canonicalization.md
    hashing.md
    graph_semantics.md
    scheduler_state_machine.md

  rust/
    Cargo.toml                 # workspace
    crates/
      cork-proto/              # tonic/prost generated code
        build.rs
        src/lib.rs
      cork-canon/              # Pre-normalization + JCS
        src/lib.rs
      cork-hash/               # domain separated SHA-256
        src/lib.rs
      cork-schema/             # jsonschema validation + compiled schema cache
        src/lib.rs
      cork-store/              # in-memory stores + traits (run/event/log/state/graph)
        src/lib.rs
      cork-core/               # gRPC server + engine + scheduler + worker client
        src/main.rs
        src/lib.rs
        src/api/mod.rs
        src/api/core_service.rs
        src/engine/mod.rs
        src/engine/run.rs
        src/engine/patch.rs
        src/engine/refs.rs
        src/engine/autocommit.rs
        src/scheduler/mod.rs
        src/scheduler/list.rs
        src/scheduler/resource.rs
        src/worker/mod.rs
        src/worker/client.rs
        src/util/mod.rs
        src/util/pagination.rs
```

### 15.2 Crate責務

| Crate | 責務 |
| --- | --- |
| cork-proto | APIの型を他crateから再利用（UI/テスト/worker側でも使える） |
| cork-canon | 正規化（Pre-normalization + JCS）。後々 "実験IDの根幹" になるので分離 |
| cork-hash | Domain-separated SHA-256 ハッシュ |
| cork-schema | JSON Schema検証を単離（Draft 2020-12対応） |
| cork-store | in-memory実装（MVP）→ RocksDB/Postgres等に差し替えやすくする |
| cork-core | gRPCサーバと実行エンジン（Kernel） |

### 15.3 cork-core の内部モジュール責務

#### api/*
- gRPC `CorkCore` 実装（tonic）
- リクエスト検証（sha256一致、run存在など）
- engineへのコマンド発行（同期/非同期）

#### engine/*
- Run/Stage/Node 状態機械
- GraphPatch 検証・適用
- EventLog 追記（event_seq採番）
- Stage auto-commit
- 参照解決（JSON Pointer）と READY 判定

#### scheduler/*
- LIST_HEURISTIC 実装（優先度・FIFOなど）
- ResourceManager（cpu/io/provider/machine…）
- "実行可能集合"からの起動決定

#### worker/*
- `CorkWorker` クライアント（tonic client）
- `Request::set_timeout()` による grpc-timeout 設定
- InvokeTool / InvokeToolStream の結果→NodeOutput / Log 反映

#### util/*
- page_token（offset/base64）や time 変換など

---

### 15.4 EventLog / LogStore の retention & backpressure

- EventLog / LogStore は **保持上限を超えたデータを自動で削除** し、メモリが無制限に増えないようにする。
  - EventLog: `max_events`, `max_bytes`, `max_age`, `broadcast_capacity`
  - LogStore: `max_records`, `max_bytes`, `max_age`
  - 設定は in-memory 実装の `*Config` で指定し、`CorkCoreServiceConfig` 経由でサービスに適用できる。
- StreamRunEvents は bounded な broadcast を利用し、購読者が遅延して **読み取りに追いつけない場合は UNAVAILABLE で切断** する（disconnect 方針）。
- `since_event_seq` が retention で保持されている最古 seq よりも古い場合は **OUT_OF_RANGE** を返す。
  - クライアントは再接続時に最新 or 最古 seq からのリプレイに切り替える。
- GetLogs は retention 範囲内のログのみ返却し、保持外の古いログは取得できない。

## 16. 採用ライブラリ（Rust MVP）

| 用途 | ライブラリ | 備考 |
| --- | --- | --- |
| gRPC | `tonic`, `prost`, `tokio` | 必須 |
| JSON | `serde`, `serde_json` | - |
| JSON Pointer | `serde_json::Value::pointer` | RFC 6901準拠。追加検証は `json-pointer` crate |
| JSON Schema | `jsonschema` | Draft 2020-12対応 |
| JCS (RFC 8785) | `serde_json_canonicalizer` | `to_vec/to_string` が RFC 8785 compatible |
| Hash | `sha2` | SHA-256 |
| 同期 | `dashmap`, `parking_lot` or `tokio::sync::RwLock` | RunRegistry等 |
| キャンセル | `tokio-util` | CancellationToken |
| ログ | `tracing`, `tracing-subscriber` | - |

---

## 17. ランタイム骨格（RunSupervisor）

MVPを最短で成立させるため、Runごとに **単一のSupervisorタスク** を立てる。

### 17.1 設計

- 1 Run = 1 Supervisor（tokio task）
- Supervisorは `mpsc::Receiver<RunCommand>` を持つ
- 外部（gRPCハンドラ）からは **状態を直接いじらず**、基本 `RunCommand` を送る
- Supervisor内で状態更新→EventLog append→必要なら scheduler tick

### 17.2 RunCommand（例）

- `ApplyPatch(CanonicalJsonDocument patch)`
- `NodeFinished { node_id, result }`
- `Tick`（タイマー）
- `Cancel { reason }`

### 17.3 利点

- patch_seq厳密連番も、Supervisorが逐次適用すれば簡単
- event_seqも、Supervisorで一意に採番できる
- stage auto-commitも、tickで判定できる
- READY判定→資源予約→worker call→結果反映が一本道になる
