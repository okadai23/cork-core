# ADR 0002: 非同期ロック方針（DashMap + Tokio sync）

- ステータス: 承認
- 日付: 2025-01-01

## コンテキスト
Core は gRPC を非同期実行し、RunRegistry / PatchStore / EventLog / LogStore などの
共有状態に並行アクセスする。`std::sync::Mutex/RwLock` を async コンテキストで
使うと、スレッドのブロッキングによりレイテンシが跳ねる/詰まるリスクがある。

## 決定
以下のルールで同期プリミティブを統一する。

1. **マップ系の共有状態**は `dashmap::DashMap` を使う。
   - 例: run_id などのキーでアクセスするストア。
2. **非マップの共有状態**は `tokio::sync::{Mutex,RwLock}` を使う。
   - 例: Run のメタデータや順序リストなど。
3. **単一スレッド所有に寄せられる場合**はロック自体を削除する。

## 影響
- `std::sync::{Mutex,RwLock}` を async コンテキストで使う実装は撤廃する。
- ロック保持時間は短く保ち、必要なデータは clone してから処理する。

## 代替案
- `std::sync` を継続する案
  - async 実行系でのブロッキングリスクが残るため却下。
- すべて `tokio::sync` で統一する案
  - マップ系は DashMap の方が実装/性能面で有利なため却下。

## 参照
- `docs/specification.md`
- `docs/tasks.md`
