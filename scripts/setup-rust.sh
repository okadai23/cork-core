#!/usr/bin/env bash
set -euo pipefail

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup が見つからないためインストールします。"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  source "$HOME/.cargo/env"
fi

sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev

rustup toolchain install stable
rustup default stable
rustup component add rustfmt clippy

echo "Rust 開発環境のセットアップが完了しました。"
