#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pushd "${repo_root}" >/dev/null
cargo test
node --check sidecars/webex-ws-sidecar/index.cjs
popd >/dev/null

echo "wxcd smoke checks passed"

