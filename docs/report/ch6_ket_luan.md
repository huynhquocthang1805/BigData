# Chương 6. Kết luận (bản nháp — TV1)

> Cập nhật các con số `[...]` và kết luận của TV3/TV4 sau khi có kết quả cuối.

## 6.1 Kết quả đạt được

- Xây dựng thành công pipeline Big Data end-to-end cho bài toán giám sát, điền khuyết và dự báo chất lượng nước
  trên dữ liệu cảm biến thật của USGS ([10] trạm, [5,96] triệu giá trị đo, 2023–2025), triển khai bằng một lệnh
  Docker Compose với Kafka, cụm Spark (1 master, 2 worker), PostgreSQL, FastAPI và Streamlit.
- Luồng xử lý thời gian thực bằng Spark Structured Streaming xử lý theo thời gian sự kiện với watermark, khử trùng
  lặp, tổng hợp theo cửa sổ, ghi idempotent và khôi phục từ checkpoint; điền khuyết và dự báo được thực hiện trực
  tuyến, phân tán theo trạm. Độ trễ end-to-end ở tải demo: p50 ≈ [4] s, p95 ≈ [6] s (trigger 5 s).
- Thực nghiệm hiệu năng định lượng: ETL tăng tốc [2,65]× khi tăng từ 1 lên 4 core; streaming duy trì được
  [...] bản ghi/giây với độ trễ p95 [...] s trên máy [4] vCPU.
- [TV3] Phương pháp điền khuyết tốt nhất: [...], giảm RMSE [...]% so với LOCF ở tỉ lệ thiếu 20% liên tục.
- [TV4] Mô hình dự báo tốt nhất: [...]; dự báo trên dữ liệu đã điền khuyết tốt hơn dữ liệu còn thiếu [...]%.

## 6.2 Hạn chế

1. Cụm Spark và Kafka chạy trên **một máy vật lý** (các container chia sẻ CPU), nên kết quả mở rộng theo chiều ngang
   chỉ mang tính chỉ dẫn; ở quy mô dữ liệu hiện tại, pandas đơn tiến trình còn nhanh hơn Spark (đã nêu trung thực ở chương 5).
2. Dữ liệu thời gian thực được **mô phỏng bằng phát lại** dữ liệu lịch sử, chưa kết nối nguồn cảm biến trực tiếp.
3. Kafka một broker (replication factor 1) — chưa chịu được lỗi broker; PostgreSQL một nút.
4. Dự báo trực tuyến được phát hành theo từng micro-batch, nên tần suất phát hành phụ thuộc tốc độ phát lại.
5. Chưa có quản lý vòng đời mô hình (MLflow) và giám sát hạ tầng (Prometheus/Grafana).

## 6.3 Hướng phát triển

- Chuyển lên cụm nhiều máy: Kafka 3 broker (replication 3), Spark trên Kubernetes, lưu trữ S3/HDFS với Delta Lake
  hoặc Apache Iceberg (giao dịch ACID, gộp file nhỏ, time travel), TimescaleDB cho chuỗi thời gian.
- Kết nối nguồn thời gian thực (endpoint `latest-continuous` của USGS hoặc giao thức MQTT từ cảm biến).
- Huấn luyện lại định kỳ và theo dõi trôi dữ liệu (data drift), quản lý mô hình bằng MLflow.
- Mở rộng thông số liên quan vi khuẩn lam (chlorophyll, phycocyanin) theo hướng của bài báo [2].
- Cảnh báo đa kênh (email/Telegram) và chỉ số chất lượng nước tổng hợp (WQI).
