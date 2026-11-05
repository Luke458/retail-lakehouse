#!/usr/bin/env bash
# Build the deployable bundle: wheel + DAGs + dbt project + verity config + a manifest.
#   scripts/package.sh <out dir>
set -euo pipefail
cd "$(dirname "$0")/.."
out=${1:-dist}
rm -rf "$out" && mkdir -p "$out"
uv build --quiet --wheel --out-dir "$out"
sha=$(git rev-parse HEAD)
tar --exclude='__pycache__' --exclude='.user.yml' -czf "$out/bundle-${sha:0:12}.tar.gz" dags dbt config scripts
cat > "$out/MANIFEST.json" <<JSON
{"commit": "$sha", "built_at": "$(date -u +%FT%TZ)", "files": [$(cd "$out" && ls | grep -v MANIFEST | sed 's/.*/"&"/' | paste -sd,)]}
JSON
ls "$out"
