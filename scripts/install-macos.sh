#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
state_dir="${HOME}/Library/Application Support/codex-webex-connector"
release_id="$(date '+%Y-%m-%dT%H-%M-%S')"
release_dir="${state_dir}/releases/${release_id}"
config_dir="${state_dir}/config"
env_path="${config_dir}/wxcd.env"
manifest_path="${state_dir}/current/plugin/manifest.json"
plist_path="${HOME}/Library/LaunchAgents/com.example.wxcd.supervisor.plist"
node_path="$(command -v node)"
codex_path="$(command -v codex)"

if [[ ! -f "${repo_root}/.env" ]]; then
  echo "Missing ${repo_root}/.env" >&2
  exit 1
fi

mkdir -p "${release_dir}/bin" "${release_dir}/plugin" "${release_dir}/sidecars/webex-ws-sidecar" "${config_dir}" "${state_dir}/logs" "${HOME}/Library/LaunchAgents"

pushd "${repo_root}" >/dev/null
cargo build --release --package wxcd-worker --package wxcd-supervisor
pnpm install --dir sidecars/webex-ws-sidecar
target_dir="$(cargo metadata --format-version 1 --no-deps | python -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
popd >/dev/null

cp "${target_dir}/release/wxcd-worker" "${release_dir}/bin/"
cp "${target_dir}/release/wxcd-supervisor" "${release_dir}/bin/"
rsync -a "${repo_root}/plugin/" "${release_dir}/plugin/"
rsync -a "${repo_root}/sidecars/webex-ws-sidecar/" "${release_dir}/sidecars/webex-ws-sidecar/"

if [[ ! -f "${config_dir}/wxcd.toml" ]]; then
  sed \
    -e "s|/Users/hoteng/Program/GitHub/codex-webex-connector|${repo_root}|g" \
    -e "s|manifest_path = \"plugin/manifest.json\"|manifest_path = \"${manifest_path}\"|g" \
    "${repo_root}/wxcd.example.toml" \
    > "${config_dir}/wxcd.toml"
else
  config_tmp="$(mktemp "${config_dir}/wxcd.toml.XXXXXX")"
  sed \
    -e "s|^manifest_path = .*|manifest_path = \"${manifest_path}\"|g" \
    "${config_dir}/wxcd.toml" \
    > "${config_tmp}"
  mv "${config_tmp}" "${config_dir}/wxcd.toml"
fi

cp "${repo_root}/.env" "${env_path}"
chmod 600 "${env_path}"

rm -f "${state_dir}/current"
ln -s "${release_dir}" "${state_dir}/current"

sed \
  -e "s|__WXCD_HOME__|${state_dir}|g" \
  -e "s|__WXCD_CONFIG__|${config_dir}/wxcd.toml|g" \
  -e "s|__WXCD_ENV__|${env_path}|g" \
  -e "s|__WXCD_NODE__|${node_path}|g" \
  -e "s|__WXCD_CODEX__|${codex_path}|g" \
  "${repo_root}/launchd/com.example.wxcd.supervisor.plist" > "${plist_path}"

launchctl bootout "gui/$(id -u)" "${plist_path}" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "${plist_path}"
launchctl kickstart -k "gui/$(id -u)/com.example.wxcd.supervisor"

echo "Installed wxcd release ${release_id}"
