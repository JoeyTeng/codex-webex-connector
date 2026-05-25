#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pushd "${repo_root}" >/dev/null
cargo test
python3 -B -m unittest scripts.tests.test_w7_live_upgrade_e2e
python3 -B scripts/w7_live_upgrade_e2e.py >/dev/null
node --check sidecars/webex-ws-sidecar/index.cjs
popd >/dev/null

echo "wxcd smoke checks passed"
