#!/usr/bin/env bash
# Xóa toàn bộ trạng thái luồng streaming để chạy lại demo từ đầu:
#   checkpoint Spark + data lake streaming + bảng PostgreSQL + topic Kafka.
# Dữ liệu raw/bronze/silver (batch) được GIỮ NGUYÊN.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose stop producer stream-processor >/dev/null 2>&1 || true
# Checkpoint do container (root) tạo => xóa bằng container để tránh lỗi quyền trên Linux
docker compose run --rm --no-deps --entrypoint rm producer -rf /app/data/checkpoints /app/data/lake/stream
docker compose exec -T postgres psql -U wq -d wq -c \
  "TRUNCATE measurements, measurements_imputed, measurements_hourly, predictions, alerts, stream_progress, batch_latency;"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --delete --topic wq.sensor.raw >/dev/null 2>&1 || true
sleep 3
docker compose up -d kafka-init >/dev/null
echo "Đã reset. Chạy lại: docker compose up -d stream-processor producer"
