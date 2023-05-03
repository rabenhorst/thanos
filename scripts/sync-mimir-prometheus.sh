#!/bin/bash

set -e

# Variables
REPO_URL="https://github.com/grafana/mimir-prometheus.git"
TMP_FOLDER=$(mktemp -d -t mimir-prometheus-XXXXXX)
REPO_FOLDER="$TMP_FOLDER/mimir-prometheus"
COPY_TARGET="internal/mimir-prometheus"
PACKAGES=("config" "tsdb" "prompb" "storage" "model/exemplar" "model/labels" "model/relabel")

# Clone the repository
git clone $REPO_URL $REPO_FOLDER

# Remove existing files in the target folder
rm -rf "${COPY_TARGET:?}"/*

# Create target folders if they don't exist
mkdir -p $COPY_TARGET
mkdir -p "$COPY_TARGET/model"

# Copy packages
for pkg in "${PACKAGES[@]}"; do
  cp -r "${REPO_FOLDER}/${pkg}" "${COPY_TARGET}/${pkg}"
done

# Delete unnecessary tests
rm "${COPY_TARGET}/storage/fanout_test.go" "${COPY_TARGET}/storage/remote/read_handler_test.go"

# Replace imports within internal/mimir-prometheus folder
for pkg in "${PACKAGES[@]}"; do
  # Works only on macOS (BSD sed)
  find "${COPY_TARGET}" -type f -name "*.go" -exec \
    sed -i "" "s+github.com/prometheus/prometheus/${pkg}+github.com/thanos-io/thanos/internal/mimir-prometheus/${pkg}+g" {} \;
done

# gofmt copied files
gofmt -s -w "${COPY_TARGET}"

# Clean up temporary folder
rm -rf "$TMP_FOLDER"

# Print warning about series.go
echo "Warning: 'internal/mimir-prometheus/storage/series.go' was overwritten but is patched."
echo "         Make sure to restore (or merge) it after sync until seriesToChunkEncoder Iterator"
echo "         is fixed (https://github.com/prometheus/prometheus/pull/12185)."
