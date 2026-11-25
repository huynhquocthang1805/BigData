# Kiến trúc hệ thống (TV1)

## 1. Tổng quan

Hệ thống theo kiến trúc **Lambda rút gọn**: *batch layer* (Spark ETL trên dữ liệu lịch sử → dữ liệu huấn luyện
cho TV3/TV4) và *speed layer* (Kafka + Spark Structured Streaming trên dữ liệu "thời gian thực"). Hai lớp dùng
**chung một data contract và chung code QC** (`wq/contracts.py`, `wq/spark_transforms.py`) nên dữ liệu
huấn luyện và dữ liệu phục vụ nhất quán với nhau.

```mermaid
flowchart TB
    USGS[(USGS Water Data API<br/>235 trạm, 15 phút)]
    subgraph ING[Thu nhận]
        DL[download_usgs.py] --> LAND[/landing zone<br/>CSV.gz trên đĩa/]
        LAND -->|ingest_hdfs.py| RAW
    end
    subgraph HDFS[HDFS: 1 NameNode + 2 DataNode, replication 2]
        RAW[/raw/] --> BR[/bronze<br/>Parquet long/] --> SV[/silver<br/>Parquet wide 15'/] --> GD[/gold/]
        LK[/stream Parquet/]
        CK[/checkpoint<br/>offset + state/]
    end
    subgraph SPARK[Spark cluster: 1 master + 2 worker]
        ETL[Batch ETL<br/>bronze → silver → gold]
        SSS[Structured Streaming<br/>QC → watermark 2h → dedup<br/>3 query]
    end
    subgraph SPEED[Luồng thời gian thực]
        PR[replay_producer.py] --> K{{Kafka KRaft<br/>wq.sensor.raw x6}}
    end
    subgraph SERVE[Phục vụ]
        PG[(PostgreSQL 16)] --> API[FastAPI] --> UI[Streamlit]
    end
    MODEL[Imputer TV3 + Forecaster TV4<br/>.joblib]
    USGS --> DL
    RAW -. đọc .-> ETL -. ghi .-> BR
    SV -->|split test| PR
    SV -->|split train/valid| MODEL
    K --> SSS
    SSS -->|lake_sink| LK
    SSS -->|serving_sink + ML online<br/>hourly_agg| PG
    SSS -.-> CK
    MODEL -. nạp .-> SSS
```

Hệ thống có **hai chế độ chạy cùng một mã nguồn**, chọn bằng biến môi trường:

| | Chế độ phát triển (mặc định) | Chế độ Big Data đầy đủ (`--env-file .env.bigdata`) |
|---|---|---|
| Dữ liệu | 10 trạm chuẩn (5,96 triệu giá trị) | 235 trạm USGS có đủ 5 thông số (mục tiêu ~118 triệu giá trị) |
| Lưu trữ data lake + checkpoint | Thư mục `./data` chia sẻ | **HDFS** (`hdfs://namenode:8020/wq/...`), replication 2 |
| Batch ETL | Spark `local[2]` | Spark **cluster** (`spark://spark-master:7077`) |
| Máy cần | ≥ 4 CPU, 8 GB RAM | ≥ 6 CPU, 12 GB RAM |
| Dùng cho | TV2/TV3/TV4 phát triển, thí nghiệm ML | Demo Big Data, benchmark hiệu năng (TV1) |

## 2. Thành phần và lý do lựa chọn

| Thành phần | Công nghệ | Vì sao chọn | Phương án thay thế đã cân nhắc |
|---|---|---|---|
| Nguồn dữ liệu | USGS OGC API (`/collections/continuous`) | Dữ liệu cảm biến thật, 15 phút, công khai, có khoảng thiếu tự nhiên (0,1–18%) | Bộ dữ liệu của bài báo (2) chỉ cung cấp theo yêu cầu |
| Message broker | Apache Kafka 4.3 (KRaft) | Log bền vững, đọc lại được (replay), phân vùng theo key → song song + giữ thứ tự theo trạm | MQTT (nhẹ nhưng không lưu log để replay) |
| Xử lý batch | PySpark 4.2 | Chung engine với streaming, chạy được local lẫn cluster | pandas (nhanh ở dữ liệu nhỏ — xem benchmark) |
| Xử lý stream | Spark Structured Streaming | Event-time, watermark, state, exactly-once với checkpoint, tái dùng code batch | Flink (mạnh hơn về độ trễ nhưng thêm 1 công nghệ) |
| Lưu trữ phân tán | HDFS 3.5 (1 NameNode, 2 DataNode) | Chuẩn của hệ sinh thái Hadoop; block + nhân bản chịu lỗi; Spark đọc/ghi trực tiếp; nơi đặt checkpoint streaming | S3/MinIO (đổi `WQ_LAKE_PATH` sang `s3a://`) |
| Data lake | Parquet (medallion bronze/silver/gold) | Nén cột, đọc chọn cột, phân vùng theo trạm | Delta Lake/Iceberg (hướng phát triển) |
| Serving DB | PostgreSQL 16 | Truy vấn theo trạm + khoảng thời gian, upsert `ON CONFLICT` | TimescaleDB (hướng phát triển) |
| API | FastAPI | Tự sinh tài liệu OpenAPI, nhẹ | Flask |
| Dashboard | Streamlit + Plotly | Làm nhanh bằng Python, đủ cho demo | React (tốn công frontend) |
| Triển khai | Docker Compose | 1 lệnh chạy toàn hệ thống, tái lập trên máy mọi thành viên | Kubernetes (quá mức cần thiết cho BTL) |

## 3. Luồng dữ liệu của một bản ghi

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant K as Kafka (partition = hash(station_id))
    participant S as Spark micro-batch (5s)
    participant DB as PostgreSQL
    participant M as Model workers (applyInPandas)
    participant D as Dashboard
    P->>K: JSON {station_id, event_time, 6 thông số, producer_ts}
    S->>K: đọc offset mới (maxOffsetsPerTrigger)
    S->>S: from_json, try_to_timestamp, QC phạm vi vật lý
    S->>S: watermark 2h + dropDuplicatesWithinWatermark
    S->>DB: upsert measurements + insert alerts (song song theo partition)
    S->>DB: đọc lịch sử 48h (JDBC, song song theo nhóm trạm)
    S->>M: groupBy(station).applyInPandas(impute)
    M-->>S: chuỗi đã điền khuyết + imputed_mask
    S->>M: groupBy(station).applyInPandas(forecast)
    M-->>S: dự báo +1h, +3h, +6h, +24h
    S->>DB: upsert measurements_imputed, predictions, batch_latency
    D->>DB: (qua FastAPI) đọc và vẽ
```

## 4. Quyết định thiết kế quan trọng (để viết báo cáo & bảo vệ)

1. **Key Kafka = `station_id`, 6 partition.** Mọi bản ghi của một trạm vào cùng partition ⇒ giữ thứ tự theo
   trạm; 6 partition ⇒ Spark đọc song song tối đa 6 task. Muốn mở rộng: tăng partition + thêm executor.
2. **Event-time + watermark 2 giờ.** Thời gian xử lý dựa trên thời điểm đo (`event_time`), không phải lúc nhận.
   Dữ liệu trễ ≤ 2h vẫn được tính; trễ hơn bị loại và state cũ được dọn ⇒ bộ nhớ không phình.
   Producer cố ý bơm **1% message trễ** và **0,5% message trùng** để chứng minh cơ chế hoạt động.
3. **Khử trùng lặp `dropDuplicatesWithinWatermark(station_id, event_time)`** (Spark ≥ 3.5).
4. **Exactly-once hiệu quả = checkpoint + sink idempotent.** Offset Kafka và state được lưu trong checkpoint;
   khi sự cố, Spark chạy lại micro-batch dở dang (at-least-once). Mọi lần ghi PostgreSQL là
   `INSERT … ON CONFLICT DO UPDATE/NOTHING` theo khóa chính ⇒ ghi lặp không sinh sai lệch.
   File sink Parquet có log `_spark_metadata` ⇒ exactly-once.
5. **Ghi DB từ executor (`foreachPartition`)**, không gom về driver ⇒ băng thông ghi tăng theo số executor.
6. **ML online phân tán theo trạm** bằng `groupBy("station_id").applyInPandas(...)`: mỗi trạm là một nhóm độc
   lập, Spark chia các nhóm cho executor ⇒ thêm trạm thì thêm máy. Mô hình được nạp **một lần mỗi Python worker**
   (cache), không nạp lại mỗi batch.
7. **Chỉ imputer *causal* được dùng online.** Streaming không biết tương lai ⇒ chỉ dùng phương pháp *filtering*
   (LOCF, Kalman filter, suy diễn tiến của DBN…). Phương pháp *smoothing* (nội suy 2 phía, MICE trên cả chuỗi)
   chỉ dùng offline. Interface có thuộc tính `causal` để kiểm soát.
8. **Một data contract, một code QC cho batch & streaming** ⇒ tránh *training/serving skew*.
9. **Bài học Spark 4:** ANSI mode bật mặc định ⇒ `to_timestamp('rác')` **ném lỗi làm sập query**.
   Dùng `try_to_timestamp` / `try_cast` (unit test `test_parse_messages_handles_nulls_and_garbage`).
10. **Parquet `TIMESTAMP_MICROS` (UTC)** thay vì INT96 mặc định ⇒ pandas/pyarrow của TV3/TV4 đọc đúng múi giờ.
11. **Small files problem:** sink Parquet streaming tạo nhiều file nhỏ ⇒ trigger 30s cho lake sink; hướng phát
    triển: job compaction định kỳ hoặc Delta Lake `OPTIMIZE`.
12. **Giám sát pipeline:** `StreamingQueryListener` ghi `numInputRows`, `processedRowsPerSecond`, watermark,
    số dòng state… vào bảng `stream_progress`; serving sink ghi độ trễ end-to-end vào `batch_latency`.

## 5. Sơ đồ triển khai (Docker Compose)

```mermaid
flowchart LR
    subgraph host[Máy chạy Docker]
        subgraph net[compose network]
            kafka[kafka :9092/:9094]
            pg[(postgres :5432)]
            sm[spark-master :7077 / UI 8080]
            w1[spark-worker-1 2 core 2g]
            w2[spark-worker-2 2 core 2g]
            sp[stream-processor<br/>Spark driver, UI 4040]
            pr[producer]
            di[data-init<br/>download + ETL]
            api[api :8000]
            ui[dashboard :8501]
        end
        vol[(./data bind mount<br/>raw, lake, checkpoints)]
    end
    pr --> kafka --> w1 & w2
    sp --> sm --> w1 & w2
    w1 & w2 --> pg
    w1 & w2 --- vol
    sp --- vol
    di --- vol
    api --> pg
    ui --> api
```

- **Driver** (`stream-processor`) chạy ở *client mode*, executor chạy trên 2 worker; `spark.driver.host`
  được đặt để executor kết nối ngược về driver.
- **`./data` là thư mục chia sẻ** giữa driver và mọi executor (checkpoint + data lake bắt buộc phải dùng chung).
  Trên cluster thật thay bằng HDFS/S3/MinIO (chỉ cần đổi `WQ_LAKE_PATH`, `WQ_CHECKPOINT_PATH`).
- **Một image dùng chung** cho mọi service Python/Spark ⇒ driver và executor có cùng phiên bản Python/thư viện
  (bắt buộc với PySpark UDF / `applyInPandas`).

## 6. Mô hình dữ liệu phục vụ (PostgreSQL)

```mermaid
erDiagram
    stations ||--o{ measurements : has
    stations ||--o{ measurements_imputed : has
    stations ||--o{ measurements_hourly : has
    stations ||--o{ predictions : has
    stations ||--o{ alerts : has
    stations { text station_id PK
               text name
               float latitude
               float longitude }
    measurements { text station_id PK
                   timestamptz ts PK
                   float water_temp_etc "6 thông số"
                   int qc_flags
                   timestamptz producer_ts }
    measurements_imputed { text station_id PK
                           timestamptz ts PK
                           int imputed_mask
                           text method }
    measurements_hourly { text station_id PK
                          text parameter PK
                          timestamptz window_start PK
                          float avg_min_max
                          float missing_rate }
    predictions { text station_id PK
                  text target PK
                  timestamptz issued_at PK
                  int horizon_min PK
                  text model PK
                  float y_pred }
    alerts { bigint alert_id PK
             text rule
             text severity }
```

Hai bảng giám sát: `stream_progress` (mỗi micro-batch mỗi query) và `batch_latency` (độ trễ end-to-end).
