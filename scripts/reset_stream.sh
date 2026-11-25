#!/usr/bin/env bash
# Xóa toàn bộ trạng thái luồng streaming để chạy lại demo từ đầu:
#   checkpoint Spark + data lake streaming + bảng PostgreSQL + topic Kafka.
# Dữ liệu raw/bronze/silver (batch) được GIỮ NGUYÊN.
#
#   ./scripts/reset_stream.sh                       # chế độ mặc định (ổ đĩa local)
#   ./scripts/reset_stream.sh .env.bigdata          # chế độ Big Data (checkpoint + lake trên HDFS)
set -euo pipefail

ENV_FILE="${1:-}"
if [ -n "$ENV_FILE" ]; then
  ENV_FILE="$(cd "$(dirname "$ENV_FILE")" && pwd)/$(basename "$ENV_FILE")"   # đường dẫn tuyệt đối trước khi cd
fi
cd "$(dirname "$0")/.."

COMPOSE=(docker compose)
if [ -n "$ENV_FILE" ]; then
  COMPOSE=(docker compose --env-file "$ENV_FILE")
  set -a; . "$ENV_FILE"; set +a
fi
CKPT="${WQ_CHECKPOINT_PATH:-/app/data/checkpoints}"
LAKE="${WQ_LAKE_PATH:-/app/data/lake}"

"${COMPOSE[@]}" stop producer stream-processor >/dev/null 2>&1 || true

if [[ "$CKPT" == hdfs://* ]]; then
  # Đường dẫn trên HDFS: xóa bằng CLI của NameNode (bỏ phần hdfs://namenode:8020)
  CKPT_P="/${CKPT#hdfs://*/}"
  LAKE_P="/${LAKE#hdfs://*/}"
  "${COMPOSE[@]}" exec -T namenode hdfs dfs -rm -r -f -skipTrash "$CKPT_P" "$LAKE_P/stream"
else
  # Checkpoint do container (root) tạo => xóa bằng container để tránh lỗi quyền trên Linux
  "${COMPOSE[@]}" run --rm --no-deps --entrypoint rm producer -rf /app/data/checkpoints /app/data/lake/stream
fi

"${COMPOSE[@]}" exec -T postgres psql -U wq -d wq -c \
  "TRUNCATE measurements, measurements_imputed, measurements_hourly, predictions, alerts, stream_progress, batch_latency;"
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --delete --topic wq.sensor.raw >/dev/null 2>&1 || true
sleep 3
"${COMPOSE[@]}" up -d kafka-init >/dev/null
echo "Đã reset. Chạy lại: ${COMPOSE[*]} up -d stream-processor producer"
