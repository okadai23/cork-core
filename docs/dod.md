# 共通 Definition of Done（DoD）

各タスクが「Done」になるために、最低限満たすこと:

- `cargo build` が通る（stable）
- `cargo fmt` が適用済み
- `cargo clippy -- -D warnings` が通る
- 該当ユニットテスト／統合テストが追加され、`cargo test` が通る
- public API（gRPC/JSON schema）の変更がある場合、`docs/` に仕様の差分が追記されている
- 失敗時のエラーは `tonic::Status` もしくは `ApplyGraphPatchResponse.rejection_reason` で説明可能
- deadlineは `deadline_exceeded` などで返せること
