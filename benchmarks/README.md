# Benchmark hiệu năng (Big Data Performance) — TV1

Mục tiêu: trả lời bằng **số liệu đo được** (không phải mô tả định tính) ba câu hỏi:

1. **Batch:** thời gian/thông lượng ETL thay đổi thế nào khi tăng **kích thước dữ liệu** và **số core**? Spark so với pandas đơn tiến trình thì sao? Chạy cluster (2 worker) khác local thế nào?
2. **Streaming:** hệ thống chịu được **tốc độ dữ liệu** bao nhiêu message/giây mà độ trễ vẫn ổn định? Độ trễ end-to-end p50/p95 là bao nhiêu?
3. **Chi phí ML online:** điền khuyết + dự báo trong mỗi micro-batch tốn thêm bao nhiêu thời gian?

## Cách chạy

Chạy trong container (cùng môi trường với hệ thống). Tắt producer/stream demo trước để không tranh CPU.

```bash
docker compose stop producer stream-processor
```

```bash
docker compose run --rm --no-deps -e WQ_DRIVER_MEMORY=6g data-init python -m benchmarks.bench_batch --scales 1 2 4 --cores 1 2 4 --cluster spark://spark-master:7077 --cluster-cores 2 4
```

```bash
docker compose run --rm --no-deps data-init python -m benchmarks.bench_streaming --master spark://spark-master:7077 --cores 4 --rates 500 1000 2000 4000 8000 --duration 45
```

```bash
docker compose run --rm --no-deps data-init python -m benchmarks.plot_results
```

Kết quả được **nối thêm** (append) vào `benchmarks/results/batch_scaling.csv` và `stream_scaling.csv`
(chạy lại nhiều lần ⇒ biểu đồ lấy trung vị). Ghi rõ cấu hình máy khi đưa vào báo cáo.

## Phương pháp

**Batch** (`bench_batch.py`): chạy đúng hàm `batch.etl_usgs.run_etl` (đọc 180 file CSV → bronze → silver lưới
15 phút → gold) và ghi Parquet thật. `--scale k` nhân bản số trạm k lần trong Spark (×1 = 5,96 triệu giá trị đầu
vào, ×4 = 23,8 triệu). pandas baseline làm cùng logic trên một tiến trình. Thông lượng = số giá trị đầu vào / tổng
thời gian (gồm cả khởi động job, không gồm khởi động JVM).

**Streaming** (`bench_streaming.py`): mỗi mức tốc độ R dùng topic Kafka mới, checkpoint mới và database riêng
`wq_bench`. Stream processor thật (query `serving_sink`, trigger 2 s) chạy trên cluster; producer phát đúng R
message/giây trong D giây rồi dừng; script chờ đến khi Spark xử lý hết backlog. Đo:
- *throughput* = số bản ghi đã ghi vào PostgreSQL / (thời gian phát + thời gian xả backlog);
- *độ trễ end-to-end* = thời điểm micro-batch ghi xong vào PostgreSQL − thời điểm producer gửi (`producer_ts`),
  lấy trung vị p50 và phân vị p95 qua các micro-batch (bỏ batch khởi động);
- *drain_s* = thời gian còn phải xử lý sau khi producer dừng (≈ 0 nghĩa là theo kịp).
Một mức R được coi là **bền vững** nếu throughput ≈ R, drain ≈ 0 và p95 không tăng dần theo thời gian.

## Kết quả mẫu

Đo trên máy chạy thử **4 vCPU, 15 GB RAM** (mọi container dùng chung 4 vCPU này). Nhóm cần **chạy lại trên máy của
nhóm** và thay số liệu; xu hướng mới là điều cần phân tích.

<!-- RESULTS_START -->
(đang cập nhật)
<!-- RESULTS_END -->
