#!/usr/bin/env bash
# Small helper to sweep SurrealDB batch_capacity and batch_timeout_ms
# Writes metrics to .data/db_metrics_<cap>_<timeout>_<mode>.json
# Usage: ./scripts/batch_sweep.sh
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
TMP_DIR="$ROOT_DIR/.tmp_experiments"
METRICS_DIR="$ROOT_DIR/.data"
mkdir -p "$TMP_DIR" "$METRICS_DIR"

BATCHS=(100 500 1000 1500)
TIMEOUTS=(100 500)
# modes: streaming, streaming_chunked (chunked inline-CREATEs), and initial_batch
MODES=(streaming streaming_chunked initial_batch)

for mode in "${MODES[@]}"; do
  for cap in "${BATCHS[@]}"; do
    for to in "${TIMEOUTS[@]}"; do
      cfg="$TMP_DIR/exp_${cap}_${to}.toml"
      metrics_file="$METRICS_DIR/db_metrics_${cap}_${to}_${mode}.json"
      cat > "$cfg" <<EOF
batch_capacity = ${cap}
batch_timeout_ms = ${to}
channel_capacity = 100
max_retries = 3
EOF
      echo "=== Running mode=${mode} batch_capacity=${cap} batch_timeout_ms=${to} ==="
      export SURREAL_METRICS_FILE="$metrics_file"
      if [ "$mode" = "initial_batch" ]; then
        export SURREAL_INITIAL_BATCH=1
      else
        unset SURREAL_INITIAL_BATCH || true
      fi
  # Run the long-running indexer with the provided config. This will build if needed.
  # The indexer is event-driven; for controlled experiments you can run a wrapper
  # that triggers a single pass. Here we start the indexer-indexer which will
  # process according to the config (embedding/surreal settings control DB modes).
  cargo run -p hyperzoekt --bin hyperzoekt-indexer -- --config "$cfg"

      if [ -f "$metrics_file" ]; then
        echo "Metrics for cap=${cap} to=${to} mode=${mode}:"
        jq . "$metrics_file" || cat "$metrics_file"
      else
        echo "No metrics file produced at $metrics_file"
      fi
      echo
      # small pause between runs
      sleep 1
    done
  done
done

echo "Sweep complete. Metrics in $METRICS_DIR"
