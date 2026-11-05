#!/usr/bin/env bash
# Download the Favorita source data (public, from Kaggle's Corporación
# Favorita competition, as parquet on Hugging Face) and verify each file.
#
#   scripts/get-data.sh            # into $FAVORITA_SOURCE (default data/source/favorita)
#
# About 900 MB, almost all of it train.parquet. Files already present with the
# right checksum are kept; a file with the wrong checksum is an error.
set -euo pipefail
. "$(dirname "$0")/env.sh"
BASE=https://huggingface.co/datasets/hsnalmasri/favorita/resolve/main
mkdir -p "$FAVORITA_SOURCE"
cd "$FAVORITA_SOURCE"

while read -r sha file; do
  if [[ -f $file ]] && echo "$sha  $file" | sha256sum -c --quiet - 2>/dev/null; then
    echo "$file: present, checksum ok"
    continue
  fi
  echo "$file: downloading"
  curl -fL --retry 3 -o "$file.part" "$BASE/$file"
  echo "$sha  $file.part" | sha256sum -c --quiet - || { echo "$file: checksum mismatch" >&2; exit 1; }
  mv "$file.part" "$file"
done <<'SUMS'
bed6c567e7e35bb0a994c4386e2c2c4dcc44ba6e5baaa0225652abe51b16b97c holidays_events.parquet
7af19d030af504e0f9be71af5196ea44c791d901e04873f04c5bfc9c885a2bc9 items.parquet
ac9c8210d88ba555561707552242315004799dae65f4dbf79d3dfd082b756dd8 stores.parquet
444def79b1d4557286226758009bfb84b9d9a77d2352a30d0cd0a573f9915808 transactions.parquet
c35a8d957ecc3d32f28ac55dda10b737a1272390fc4efff55ad5b44f8b2c6e20 train.parquet
SUMS
