# Hướng dẫn cho TV1 — System Architecture, Streaming, Integration & Deployment

Tài liệu này dành cho bạn (TV1, trưởng nhóm). Nó trả lời 4 câu: **phần của mình gồm những gì**, **chạy thế nào**,
**hiểu và giải thích thế nào khi bảo vệ**, và **còn phải làm gì tiếp**. Phần khung đã được dựng và chạy thử
end-to-end; việc của bạn là nắm vững, chạy lại trên máy mình, hoàn thiện các mục 🔧 và đo số liệu thật.

---

## 0. Phần việc của TV1 và trạng thái hiện tại

| # | Hạng mục | File chính | Trạng thái |
|---|---|---|---|
| 1 | Kiến trúc tổng thể + sơ đồ | `docs/architecture.md` | ✅ có sơ đồ tổng thể, tuần tự, triển khai, ERD |
| 2 | Data contract + interface mô hình | `wq/contracts.py`, `wq/models/base.py`, `docs/data_contract.md` | ✅ v1.0.0 — 🔧 họp nhóm chốt (tuần 1) |
| 3 | Kafka replay producer | `ingestion/replay_producer.py` | ✅ speedup/rate, bơm dữ liệu trễ/trùng, idempotent |
| 4 | Spark Structured Streaming | `streaming/stream_processor.py` | ✅ 3 query: lake / serving + ML / hourly; listener |
| 5 | Serving: PostgreSQL + FastAPI | `deploy/postgres/init.sql`, `api/main.py` | ✅ 12 endpoint |
| 6 | Dashboard khung | `dashboard/app.py` | ✅ 4 tab — 🔧 TV2 thêm trang giám sát/WQI |
| 7 | Triển khai Docker Compose | `docker-compose.yml`, `docker/Dockerfile` | ✅ 1 lệnh chạy cả hệ thống |
| 8 | Benchmark hiệu năng | `benchmarks/` | ✅ script + kết quả mẫu (máy 4 vCPU) — 🔧 chạy lại trên máy nhóm |
| 9 | Tích hợp model TV3/TV4 | `wq/models/registry.py`, biến `WQ_IMPUTER`/`WQ_FORECASTER` | ✅ cơ chế sẵn — 🔧 tuần 3 thay baseline bằng model thật |
| 10 | Báo cáo Ch.1, 4, 5 (hiệu năng), 6 | `docs/report/` | ✅ bản nháp — 🔧 điền số liệu cuối |
| 11 | Unit test | `tests/` (20 test) | ✅ pass |

---

## 1. Chuẩn bị máy (làm 1 lần)

1. Cài **Docker Desktop**. Windows: bật **WSL2**, và **clone repo bên trong WSL** (ví dụ `~/BigData`), không
   để trong `C:\` — Spark đọc/ghi qua bind mount từ `C:\` chậm hơn nhiều lần.
2. Docker Desktop → Settings → Resources: **≥ 4 CPU, ≥ 8 GB RAM** (khuyến nghị 6 CPU / 12 GB nếu có).
3. Cài Git, VS Code (extension Python, Docker). Không bắt buộc cài Python/Java/Spark trên máy — mọi thứ chạy trong container.

```bash
git clone https://github.com/huynhquocthang1805/BigData.git
cd BigData
git checkout claude/charming-darwin-oid090   # hoặc nhánh chính sau khi merge
```

## 2. Chạy toàn bộ hệ thống lần đầu (~15 phút)

```bash
docker compose build
```

```bash
docker compose up -d
```

Thứ tự khởi động tự động: `kafka`, `postgres` → `kafka-init` (tạo topic 6 partition) → `spark-master`,
2 `spark-worker` → `stream-processor` (Spark driver) → `data-init` (tải ~6 triệu giá trị từ USGS ~3 phút +
ETL ~1 phút) → `producer` (phát lại 6 tháng dữ liệu test, 1 giờ dữ liệu/giây) → `api`, `dashboard`.

Kiểm tra:

```bash
docker compose ps
```

```bash
docker compose logs -f data-init
```

```bash
docker compose logs -f --tail 20 stream-processor
```

Log streaming bình thường trông như sau (mỗi 5 giây một micro-batch):

```
[serving] batch=8 rows=198 stations=10 latency_p50=3869ms p95=6120ms db=1053ms ml=1925ms total=2979ms
```

Mở trình duyệt: Dashboard http://localhost:8501 · API http://localhost:8000/docs · Spark Master http://localhost:8080 ·
Spark app (tab **Structured Streaming**) http://localhost:4040 · Kafka UI: `docker compose --profile tools up -d kafka-ui` → http://localhost:8090

Các lệnh khác: `make help` (Linux/WSL/Git Bash). Chạy lại demo từ đầu: `./scripts/reset_stream.sh`
rồi `docker compose up -d stream-processor producer`.

## 3. Hiểu hệ thống (để viết báo cáo và trả lời giảng viên)

Đọc theo thứ tự: `docs/architecture.md` → `wq/contracts.py` → `ingestion/replay_producer.py` →
`streaming/stream_processor.py` (đọc từ hàm `start_queries`) → `api/main.py`.

### 3.1 Hành trình của một bản ghi
1. **Producer** đọc dữ liệu silver (đã chuẩn hóa lưới 15 phút), bỏ các mốc mà mọi cảm biến đều thiếu
   (giả lập trạm mất kết nối), gửi JSON vào Kafka với **key = station_id** → cùng trạm vào cùng partition.
2. **Spark** đọc Kafka theo micro-batch 5 giây, `from_json` theo schema cố định, `try_to_timestamp`, QC phạm vi
   vật lý (giá trị vô lý → NULL + bật bit `qc_flags`).
3. **Watermark 2 giờ** + `dropDuplicatesWithinWatermark(station_id, event_time)`: chấp nhận dữ liệu trễ ≤ 2h
   (theo thời gian đo), loại bản ghi trùng.
4. Ba query độc lập cùng đọc luồng đã làm sạch:
   - `lake_sink`: ghi Parquet vào `data/lake/stream/` (lưu trữ lâu dài, exactly-once);
   - `serving_sink` (`foreachBatch`): upsert `measurements` + sinh `alerts` (1 job, chạy song song trên executor),
     rồi **ML online**: đọc 48h lịch sử (JDBC, song song theo nhóm trạm) → `groupBy(station).applyInPandas(impute)`
     → `applyInPandas(forecast)` → ghi `measurements_imputed`, `predictions`; cuối cùng ghi độ trễ vào `batch_latency`;
   - `hourly_agg`: cửa sổ 1 giờ theo event-time, ghi trung bình/min/max/tỉ lệ thiếu vào `measurements_hourly`.
5. `StreamingQueryListener` ghi tiến độ mỗi micro-batch vào `stream_progress`. FastAPI đọc PostgreSQL,
   dashboard gọi FastAPI.

### 3.2 Những điểm "ăn điểm" phải nói được khi bảo vệ
- **Phân vùng & song song:** 6 partition ⇒ tối đa 6 task đọc song song; key theo trạm ⇒ giữ thứ tự từng trạm.
- **Event-time vs processing-time, watermark, late data, dedup** — có số liệu chứng minh (producer bơm 1% trễ, 0,5% trùng).
- **Fault tolerance:** checkpoint (offset + state) + sink idempotent ⇒ *effectively exactly-once*. Demo được (mục 6).
- **Backpressure:** `maxOffsetsPerTrigger` giới hạn kích thước micro-batch khi dữ liệu dồn về.
- **ML phân tán theo khóa:** mỗi trạm một nhóm `applyInPandas`; model nạp một lần mỗi Python worker.
- **Causal imputation:** streaming không thấy tương lai ⇒ chỉ dùng imputer `causal=True` (filtering).
- **Training/serving consistency:** batch và streaming dùng chung contract + code QC.
- **Bài học thực tế:** Spark 4 bật ANSI mode ⇒ một chuỗi thời gian hỏng có thể làm sập cả query nếu dùng
  `to_timestamp`; nhóm phát hiện nhờ unit test và chuyển sang `try_to_timestamp`/`try_cast`.
- **Đo đạc định lượng** (khác bài mẫu BKByte chỉ đánh giá định tính).

## 4. Việc TV1 cần làm tiếp — checklist theo tuần

### Tuần 1 — chốt nền móng
- [ ] Chạy được mục 2 trên máy mình; chụp màn hình dashboard, Spark UI, Kafka UI (dùng cho báo cáo/slide).
- [ ] Họp nhóm 30 phút: trình bày `docs/architecture.md`, `docs/data_contract.md`; chốt 10 trạm, 6 biến,
      split thời gian, target/horizon dự báo; ghi biên bản quyết định vào `docs/team_plan.md`.
- [ ] Tạo nhánh cho từng thành viên, bật branch protection (PR phải được TV1 duyệt), giao issue trên GitHub.
- [ ] Hướng dẫn TV2/TV3/TV4 chạy `make test` và đọc dữ liệu silver bằng pandas (mục 2 của `data_contract.md`).

### Tuần 2 — hoàn thiện streaming & vận hành
- [ ] Thử nghiệm khả năng chịu lỗi (mục 6, bước 4) và ghi lại kết quả (số dòng trước/sau, không mất/không trùng).
- [ ] Kiểm chứng xử lý dữ liệu trễ/trùng: đếm số bản ghi Kafka vs số dòng trong `measurements`
      (xem truy vấn SQL ở mục 7) và giải thích chênh lệch.
- [ ] (Tùy chọn, điểm cộng) Dead-letter: ghi message hỏng ra topic `wq.sensor.dlq` thay vì bỏ qua.
- [ ] (Tùy chọn) Job compaction gộp file nhỏ trong `data/lake/stream`.

### Tuần 3 — tích hợp & đo đạc
- [ ] Nhận model của TV3/TV4 (mục 5), chạy streaming với model thật, so sánh độ trễ ML với baseline.
- [ ] Chạy benchmark trên máy mạnh nhất nhóm (mục 7), ghi cấu hình phần cứng, vẽ biểu đồ.
- [ ] Bảng "độ chính xác online" (endpoint `/predictions/accuracy`) cho model thật vs baseline.

### Tuần 4 — báo cáo & bảo vệ
- [ ] Hoàn thiện `docs/report/ch1…ch6`, ghép báo cáo chung (LaTeX/Word), thống nhất hình/bảng.
- [ ] Slide 15–20 trang; tập kịch bản demo 5 phút (mục 6) ít nhất 2 lần; quay video dự phòng.

## 5. Tích hợp model của TV3 / TV4

TV3 tạo file `wq/models/imputers_tv3.py`:

```python
from wq.models.base import BaseImputer

class KalmanImputer(BaseImputer):
    name = "kalman"
    causal = True            # bắt buộc True nếu muốn chạy trong streaming

    def fit(self, df):
        ...                  # học tham số trên split train
        return self

    def transform(self, df):
        out = df.copy()
        ...                  # điền NaN theo từng station_id, chỉ dùng dữ liệu quá khứ
        return out
```

```python
# notebook của TV3
from wq.models.imputers_tv3 import KalmanImputer
KalmanImputer().fit(train_df).save("models/imputer_kalman.joblib")
```

TV4 tương tự với `BaseForecaster` (`predict(history)` trả về các cột `PREDICTION_COLUMNS`). Sau đó TV1 bật model:

```bash
echo "WQ_IMPUTER=/app/models/imputer_kalman.joblib" >> .env
```

```bash
echo "WQ_FORECASTER=/app/models/forecaster_xgb.joblib" >> .env
```

```bash
docker compose up -d --force-recreate stream-processor api
```

Kiểm tra: log `stream-processor` in `imputer=...joblib`; endpoint `/stations/{id}/forecast` trả `forecaster` mới;
`/predictions/accuracy` có dòng của model mới. Nếu model cần thư viện mới (vd. `torch`), thêm vào
`requirements.txt` (ghim version) rồi `docker compose build`.

## 6. Kịch bản demo khi bảo vệ (~5 phút)

1. `docker compose ps` — 10 container: Kafka, PostgreSQL, Spark master + 2 worker, streaming, producer, API, dashboard.
2. Kafka UI: topic `wq.sensor.raw`, 6 partition, message JSON. Spark Master UI: 2 worker, ứng dụng `wq-stream-processor`.
3. Dashboard: tab Giám sát (DO thay đổi theo chu kỳ ngày, điểm được điền khuyết, ngưỡng cảnh báo, dự báo),
   tab Dự báo (độ chính xác online), tab Pipeline (độ trễ p50/p95, throughput).
4. **Chịu lỗi:** `docker compose kill stream-processor` → đợi 20 giây (producer vẫn gửi, Kafka giữ dữ liệu) →
   `docker compose up -d stream-processor` → Spark đọc tiếp từ offset trong checkpoint, dashboard bắt kịp,
   không mất và không trùng dữ liệu (đếm bằng SQL ở mục 7).
5. Mở biểu đồ benchmark (`benchmarks/results/*.png`) và giải thích (mục 7).

## 7. Benchmark hiệu năng (phần TV1 trong Chương 5)

Đọc [`benchmarks/README.md`](../benchmarks/README.md). Tóm tắt lệnh:

```bash
make bench-batch
```

```bash
make bench-stream
```

```bash
make plots
```

Luôn ghi lại **cấu hình máy** (CPU, RAM, Docker resources) cạnh bảng kết quả; chạy mỗi cấu hình ≥ 3 lần nếu có
thời gian và báo cáo trung vị. Truy vấn kiểm tra tính đúng (dùng cho mục 6 và tuần 2):

```bash
docker compose exec postgres psql -U wq -d wq -c "SELECT count(*), count(DISTINCT (station_id, ts)), min(ts), max(ts) FROM measurements;"
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic wq.sensor.raw
```

## 8. Câu hỏi giảng viên có thể hỏi

| Câu hỏi | Gợi ý trả lời |
|---|---|
| Vì sao cần Kafka, sao không cho Spark đọc thẳng API USGS? | Kafka tách nguồn và bộ xử lý: lưu log bền vững nên đọc lại được khi Spark lỗi, nhiều consumer đọc độc lập (lake/serving/hourly), hấp thụ đột biến tải; API USGS là pull theo lô, không phải luồng. |
| Exactly-once đạt được thế nào? | Offset + state lưu trong checkpoint; batch lỗi sẽ chạy lại (at-least-once), nhưng mọi sink đều idempotent (upsert theo khóa chính, file sink có metadata log) ⇒ kết quả như exactly-once. Producer bật `enable.idempotence`. |
| Watermark 2 giờ chọn thế nào? Đánh đổi gì? | Đủ lớn để nhận dữ liệu trễ thường gặp (producer bơm trễ ≤ 3s thực ≈ 3h dữ liệu ở tốc độ 3600×, phần lớn < 2h); lớn hơn ⇒ state lớn và kết quả cửa sổ ra muộn hơn; nhỏ hơn ⇒ mất nhiều dữ liệu trễ. Có thể chứng minh bằng số dòng bị loại. |
| Dữ liệu này có phải "Big Data" không? | Ở mức đề tài: ~6 triệu giá trị thật/3 năm/10 trạm; kiến trúc mở rộng ngang (partition, executor, applyInPandas theo trạm); benchmark scale ×1→×4 (và hơn) cho thấy cách hệ thống mở rộng; dữ liệu cảm biến liên tục là bài toán velocity. Đồng thời trung thực: ở quy mô nhỏ pandas nhanh hơn (có số liệu). |
| Vì sao pandas nhanh hơn Spark ở dữ liệu nhỏ? | Spark tốn chi phí khởi động JVM, lập kế hoạch, shuffle, tuần tự hóa; lợi ích xuất hiện khi dữ liệu vượt RAM một máy hoặc khi tăng số core/máy. Kết quả benchmark của nhóm cho thấy điều này. |
| Vì sao chạy cluster trên 1 máy lại chậm hơn local? | Cùng số core vật lý nhưng thêm chi phí mạng giữa container, khởi tạo executor, shuffle qua network; cluster chỉ có lợi khi thêm máy thật. Đây là kết quả trung thực cần nêu. |
| Mô hình được triển khai vào streaming thế nào? | Interface `BaseImputer/BaseForecaster`, file `.joblib`, cấu hình bằng biến môi trường; chạy phân tán `groupBy(station).applyInPandas`, nạp model 1 lần mỗi worker. |
| Vì sao chỉ dùng imputer causal online? | Online không có dữ liệu tương lai; dùng phương pháp nhìn cả 2 phía sẽ gây rò rỉ thông tin (leakage) và không khả thi thời gian thực. |
| Hệ thống chịu lỗi thế nào nếu worker chết? | Spark lập lịch lại task trên worker còn lại; driver chết ⇒ khởi động lại từ checkpoint (`restart: on-failure`); Kafka giữ dữ liệu 7 ngày. |
| Làm sao mở rộng lên production? | Kafka nhiều broker (replication 3), Spark trên Kubernetes/YARN, lưu trữ S3/HDFS + Delta Lake, TimescaleDB, Prometheus/Grafana — trình bày ở Ch.6. |

## 9. Xử lý sự cố thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `toomanyrequests` khi pull image | Docker Hub giới hạn lượt pull ẩn danh: `docker login` hoặc chờ rồi chạy lại |
| Container bị `Killed`/OOM | Tăng RAM cho Docker; giảm `SPARK_WORKER_MEMORY` trong `.env`; tắt kafka-ui |
| Cổng đã bị dùng (5432, 8080…) | Tắt dịch vụ đang chiếm cổng hoặc đổi cổng bên trái trong `docker-compose.yml` |
| `scripts/reset_stream.sh: bad interpreter` (Windows) | File bị đổi sang CRLF: `git config core.autocrlf input`, checkout lại |
| Streaming không nhận dữ liệu mới sau khi phát lại | Checkpoint còn offset/watermark cũ ⇒ `./scripts/reset_stream.sh` |
| Không xóa được `data/checkpoints` (Linux) | File do container (root) tạo: dùng `./scripts/reset_stream.sh` hoặc `sudo rm -rf` |
| Dashboard báo không kết nối API | `docker compose logs api`; chờ postgres healthy; kiểm tra http://localhost:8000/health |
