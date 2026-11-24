# Chương 4. Thiết kế và hiện thực hệ thống — các mục do TV1 phụ trách (bản nháp)

> Các mục ETL (TV2), điền khuyết (TV3), dự báo (TV4) được chèn vào giữa theo thứ tự 4.1 → 4.8.
> Hình vẽ: xuất từ sơ đồ Mermaid trong `docs/architecture.md` (mermaid.live → PNG/SVG) và ảnh chụp màn hình.

## 4.1 Kiến trúc tổng thể

Hệ thống được tổ chức theo kiến trúc Lambda [6] rút gọn gồm năm lớp (Hình 4.1):

- **Lớp thu thập:** script tải dữ liệu từ USGS Water Data OGC API (collection *continuous*) theo trạm × thông số × năm,
  có retry và chia nhỏ khoảng thời gian khi chạm giới hạn trang; lưu CSV theo bố cục phân vùng kiểu Hive.
- **Lớp xử lý batch:** Spark chuyển dữ liệu qua các tầng bronze → silver → gold của data lake Parquet.
- **Lớp xử lý luồng (speed layer):** Kafka nhận dữ liệu cảm biến; Spark Structured Streaming [4] làm sạch, khử trùng lặp,
  tổng hợp theo cửa sổ thời gian, điền khuyết và dự báo trực tuyến.
- **Lớp lưu trữ:** data lake Parquet (lưu trữ phân tích, dữ liệu huấn luyện) và PostgreSQL (phục vụ truy vấn).
- **Lớp phục vụ:** FastAPI cung cấp REST API; Streamlit hiển thị giám sát, dự báo, cảnh báo và số liệu vận hành.

Hai lớp batch và streaming dùng chung một *data contract* (tên cột, đơn vị, phạm vi hợp lệ, quy ước bitmask,
cách chia dữ liệu) và chung mã kiểm tra chất lượng, nhờ đó dữ liệu dùng để huấn luyện và dữ liệu khi vận hành
được xử lý giống hệt nhau (tránh *training/serving skew*).

*Hình 4.1 — Kiến trúc tổng thể (docs/architecture.md, mục 1).*

**Bảng 4.1 — Công nghệ sử dụng:** Kafka 4.3 (KRaft), Spark 4.2.1 (PySpark, Python 3.12, Java 21), PostgreSQL 16,
FastAPI, Streamlit + Plotly, Docker Compose. Lý do lựa chọn và phương án thay thế: xem `docs/architecture.md` mục 2.

## 4.x Thu nhận dữ liệu thời gian thực với Kafka

**Mô phỏng nguồn cảm biến.** Bộ phát lại (*replay producer*) đọc dữ liệu silver của giai đoạn kiểm thử
(07–12/2025), sắp xếp theo thời gian đo, và gửi mỗi bản ghi (một trạm × một mốc 15 phút) thành một message JSON.
Các mốc mà mọi cảm biến đều thiếu bị bỏ qua để mô phỏng trạm mất kết nối. Tốc độ phát điều khiển theo hai chế độ:
*speedup* (ví dụ 3600 lần: 1 giờ dữ liệu mỗi giây thực) cho demo, và *rate* (số message/giây cố định) cho đo hiệu năng.
Để kiểm chứng khả năng xử lý dữ liệu "bẩn", producer chủ động làm trễ 1% message và gửi trùng 0,5% message.

**Thiết kế topic.** Topic `wq.sensor.raw` có 6 partition, khóa message là `station_id`: mọi bản ghi của một trạm
nằm trong cùng partition nên giữ đúng thứ tự theo trạm, trong khi các trạm khác nhau được đọc song song.
Producer bật *idempotence* để Kafka tự loại bản ghi trùng phát sinh do producer gửi lại; dữ liệu được nén LZ4
và gom lô (`linger.ms = 20`).

## 4.y Xử lý luồng với Spark Structured Streaming

**Làm sạch.** Mỗi micro-batch (5 giây), Spark đọc các offset mới (giới hạn `maxOffsetsPerTrigger` để kiểm soát
kích thước lô khi dữ liệu dồn về), giải mã JSON theo schema cố định, chuyển thời gian bằng `try_to_timestamp`
(ở Spark 4, chế độ ANSI mặc định khiến `to_timestamp` ném lỗi với chuỗi hỏng và làm dừng cả truy vấn — lỗi này được
phát hiện bằng unit test), và áp dụng kiểm tra phạm vi vật lý: giá trị bất khả thi bị thay bằng NULL đồng thời
bật bit tương ứng trong `qc_flags`.

**Thời gian sự kiện, watermark và khử trùng lặp.** Mọi phép tính theo thời gian dùng `event_time` (thời điểm đo).
Watermark 2 giờ cho phép nhận dữ liệu trễ tối đa 2 giờ; bản ghi trễ hơn bị loại và trạng thái (state) cũ được giải
phóng. Bản ghi trùng (cùng trạm, cùng mốc thời gian) bị loại bằng `dropDuplicatesWithinWatermark`.

**Ba truy vấn song song** dùng chung luồng đã làm sạch:
1. `lake_sink` — ghi Parquet phân vùng theo trạm và ngày vào data lake (trigger 30 giây để giảm số file nhỏ).
2. `serving_sink` — dùng `foreachBatch`: (a) một job thống kê lô; (b) một job ghi song song từ các executor vào
   PostgreSQL: upsert `measurements` và sinh `alerts` theo luật ngưỡng trong cùng một lượt duyệt dữ liệu;
   (c) điền khuyết và dự báo trực tuyến (mục 4.z); (d) ghi độ trễ end-to-end vào `batch_latency`.
3. `hourly_agg` — tổng hợp theo cửa sổ 1 giờ (trung bình, min, max, số quan trắc, tỉ lệ thiếu) ở chế độ append:
   kết quả của mỗi cửa sổ được phát ra một lần khi watermark vượt qua cuối cửa sổ.

**Đảm bảo tính đúng khi có sự cố.** Offset Kafka và state được lưu trong checkpoint. Khi tiến trình xử lý bị dừng
đột ngột, Spark khởi động lại từ checkpoint và chạy lại micro-batch dở dang (*at-least-once*); vì mọi thao tác ghi
vào PostgreSQL đều là `INSERT … ON CONFLICT` theo khóa chính, việc ghi lặp không làm sai kết quả, nên hệ thống đạt
ngữ nghĩa *exactly-once hiệu quả*. Sink Parquet dùng nhật ký `_spark_metadata` nên bản thân nó là exactly-once.

**Giám sát.** Một `StreamingQueryListener` ghi mỗi sự kiện tiến độ (số bản ghi vào, thông lượng xử lý, thời gian
micro-batch, watermark, số dòng state) vào bảng `stream_progress`, phục vụ dashboard và thực nghiệm chương 5.

## 4.z Tích hợp mô hình điền khuyết và dự báo

Pipeline giao tiếp với mô hình qua hai interface: `BaseImputer.transform(df)` và `BaseForecaster.predict(history)`.
Mô hình được lưu thành file `.joblib` và chọn bằng biến môi trường, nên thay mô hình không cần sửa mã tích hợp.
Trong mỗi micro-batch, Spark đọc 48 giờ lịch sử của các trạm vừa có dữ liệu (JDBC, song song theo nhóm trạm), sau
đó áp dụng `groupBy("station_id").applyInPandas(...)` hai lần — điền khuyết rồi dự báo — để mỗi trạm được xử lý
độc lập trên các executor. Mô hình được nạp một lần cho mỗi tiến trình Python worker và dùng lại cho các lô sau.
Chỉ các imputer có tính *nhân quả* (chỉ dùng dữ liệu quá khứ) được phép chạy trực tuyến; phương pháp làm trơn
hai phía chỉ dùng trong thực nghiệm offline. Mô hình mặc định là baseline (LOCF và seasonal-naive), được thay bằng
mô hình tốt nhất của TV3/TV4 sau thực nghiệm.

## 4.t Cơ sở dữ liệu phục vụ, API và dashboard

PostgreSQL gồm 8 bảng (Hình 4.x — ERD): `stations`, `measurements`, `measurements_imputed`, `measurements_hourly`,
`predictions`, `alerts`, `stream_progress`, `batch_latency`. Khóa chính tự nhiên (trạm, thời điểm, …) vừa phục vụ
truy vấn theo khoảng thời gian, vừa là điều kiện cho upsert idempotent.

FastAPI cung cấp 12 endpoint (tài liệu OpenAPI tự sinh tại `/docs`): danh sách trạm và giá trị mới nhất, chuỗi
thời gian thô/đã điền khuyết, tổng hợp theo giờ, dự báo mới nhất, chuỗi dự báo kèm giá trị thực, **độ chính xác
trực tuyến** (MAE/RMSE theo mô hình × tầm dự báo, tính bằng cách nối dự báo với giá trị thực đến sau), dự báo theo
yêu cầu, cảnh báo và số liệu vận hành. Dashboard Streamlit gồm 4 tab: Giám sát, Dự báo, Cảnh báo, Pipeline
(Hình 4.x — `docs/images/dashboard_monitoring.png`, `dashboard_forecast.png`, `dashboard_pipeline.png`, `api_docs.png`, `spark_master_ui.png`).

## 4.u Triển khai

Toàn bộ hệ thống gồm 12 service trong một tệp Docker Compose (Hình 4.x — sơ đồ triển khai): Kafka (KRaft, không
cần ZooKeeper), PostgreSQL, Spark master và 2 worker (mỗi worker 2 core, 2 GB), tiến trình streaming (Spark driver,
client mode), producer, job khởi tạo dữ liệu, API, dashboard và Kafka UI (tùy chọn). Một image duy nhất (Python 3.12,
Spark 4.2.1, connector Kafka và JDBC tải sẵn lúc build) dùng cho mọi service Python/Spark, bảo đảm driver và
executor có cùng môi trường — điều kiện bắt buộc khi dùng hàm Python trên executor. Thư mục `data/` được chia sẻ
giữa driver và executor để lưu checkpoint và data lake; khi lên cụm thật chỉ cần thay bằng HDFS/S3.
