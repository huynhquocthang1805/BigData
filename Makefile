# Lối tắt cho các lệnh hay dùng. Windows không có `make`: chạy trực tiếp lệnh ở cột phải (trong Git Bash/WSL).
.PHONY: help build up down ps logs data etl test reset-stream bench-batch bench-stream plots tools

help:            ## Liệt kê lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

build:           ## Build image dự án
	docker compose build

up:              ## Chạy toàn bộ hệ thống
	docker compose up -d

down:            ## Dừng hệ thống (giữ dữ liệu)
	docker compose down

ps:              ## Trạng thái các container
	docker compose ps

logs:            ## Xem log streaming + producer
	docker compose logs -f --tail 50 stream-processor producer

data:            ## Tải dữ liệu USGS (bỏ qua file đã có)
	docker compose run --rm --no-deps data-init python -m ingestion.download_usgs

etl:             ## Chạy lại batch ETL (raw -> bronze -> silver -> gold)
	docker compose run --rm --no-deps data-init python -m batch.etl_usgs --master 'local[*]'

test:            ## Chạy unit test
	docker compose run --rm --no-deps data-init python -m pytest -q -p no:cacheprovider

reset-stream:    ## Xóa trạng thái streaming để demo lại từ đầu
	./scripts/reset_stream.sh

bench-batch:     ## Benchmark batch ETL (scale x cores)
	docker compose run --rm --no-deps -e WQ_DRIVER_MEMORY=6g data-init python -m benchmarks.bench_batch

bench-stream:    ## Benchmark streaming (throughput/latency theo tốc độ phát)
	docker compose stop producer stream-processor
	docker compose run --rm --no-deps data-init python -m benchmarks.bench_streaming

plots:           ## Vẽ biểu đồ kết quả benchmark
	docker compose run --rm --no-deps data-init python -m benchmarks.plot_results

tools:           ## Bật Kafka UI (http://localhost:8090)
	docker compose --profile tools up -d kafka-ui
