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
