# Chương 6. Kết luận (bản nháp — TV1)

> Cập nhật các con số `[...]` và kết luận của TV3/TV4 sau khi có kết quả cuối.

## 6.1 Kết quả đạt được

- Xây dựng thành công pipeline Big Data end-to-end cho bài toán giám sát, điền khuyết và dự báo chất lượng nước
  trên dữ liệu cảm biến thật của USGS (10 trạm, 5,96 triệu giá trị đo, 2023–2025), triển khai bằng một lệnh
  Docker Compose với Kafka, cụm Spark (1 master, 2 worker), HDFS (NameNode, 2 DataNode, hệ số nhân bản 2),
  PostgreSQL, FastAPI và Streamlit. Cùng mã nguồn chạy được ở chế độ demo (ổ đĩa cục bộ) và chế độ Big Data
  (data lake, dữ liệu thô và checkpoint trên HDFS, ETL trên cụm Spark).
- Luồng xử lý thời gian thực bằng Spark Structured Streaming xử lý theo thời gian sự kiện với watermark, khử trùng
  lặp, tổng hợp theo cửa sổ, ghi idempotent và khôi phục từ checkpoint; điền khuyết và dự báo được thực hiện trực
  tuyến, phân tán theo trạm. Độ trễ end-to-end ở tải demo: p50 ≈ 4 s, p95 ≈ 6 s (trigger 5 s, có ML). Kiểm chứng
  với dữ liệu trễ/trùng và dừng đột ngột tiến trình xử lý: không mất, không trùng bản ghi, kể cả khi checkpoint
  đặt trên HDFS.
- Thực nghiệm hiệu năng định lượng (máy 4 vCPU): ETL tăng tốc 2,84× khi tăng từ 1 lên 4 core (23,8 triệu giá trị),
  xử lý 47,7 triệu giá trị trong 112 s; streaming bền vững tới 8.000 message/giây với độ trễ p95 4 s, thông lượng
  tối đa ≈ 18.000 bản ghi/giây.
- Chạy được trên dữ liệu thật quy mô lớn: 103 trạm USGS, 62 triệu giá trị thô, lưu trên HDFS và xử lý trên cụm
  Spark trong 238 s (Spark local 168 s). Ở khối dữ liệu này pandas hết bộ nhớ và không chạy xong (đã dùng 11 GB),
  trong khi Spark chạy trong giới hạn driver 6 GB; ở 34 triệu giá trị pandas còn nhanh hơn nhưng đã dùng 9,4 GB.
  Khoảng cách giữa cụm và local thu hẹp dần: 1,89 lần (6 triệu giá trị), 1,47 lần (34 triệu), 1,42 lần (62 triệu).
- Streaming có điền khuyết và dự báo không mất bản ghi nào với 10, 50 và 100 trạm; số trạm tăng 10 lần thì độ trễ
  p50 tăng 43% (5,5 → 7,9 s ở 1.000 msg/s).
- [TV3] Phương pháp điền khuyết tốt nhất: [...], giảm RMSE [...]% so với LOCF ở tỉ lệ thiếu 20% liên tục.
- [TV4] Mô hình dự báo tốt nhất: [...]; dự báo trên dữ liệu đã điền khuyết tốt hơn dữ liệu còn thiếu [...]%.

## 6.2 Hạn chế

1. Cụm Spark, HDFS và Kafka chạy trên **một máy vật lý** (các container chia sẻ CPU và ổ đĩa), nên kết quả mở rộng
   theo chiều ngang chỉ mang tính chỉ dẫn: cụm chậm hơn local, ghi HDFS chậm hơn ổ đĩa cục bộ 2,5 lần; ở quy mô dữ
   liệu chuẩn, pandas đơn tiến trình còn nhanh hơn Spark (đã nêu trung thực ở chương 5).
2. Lúc đo, bộ dữ liệu lớn mới có 103 trên 235 trạm do API của USGS giới hạn tần suất với người dùng không có API key;
   downloader tải tiếp được, cần chạy thêm vài giờ hoặc dùng API key.
3. Dữ liệu thời gian thực được **mô phỏng bằng phát lại** dữ liệu lịch sử, chưa kết nối nguồn cảm biến trực tiếp.
4. Kafka một broker (replication factor 1) — chưa chịu được lỗi broker; PostgreSQL một nút; HDFS một NameNode
   (chưa bật High Availability).
5. Data lake gồm nhiều tệp nhỏ (block trung bình dưới 1 MB so với block 128 MB của HDFS), chưa có bước gộp tệp.
6. Dự báo trực tuyến được phát hành theo từng micro-batch, nên tần suất phát hành phụ thuộc tốc độ phát lại.
7. Chưa có quản lý vòng đời mô hình (MLflow) và giám sát hạ tầng (Prometheus/Grafana).

## 6.3 Hướng phát triển

- Chuyển lên cụm nhiều máy: Kafka 3 broker (replication 3), Spark trên Kubernetes, HDFS ≥ 3 DataNode (hệ số nhân
  bản 3, NameNode HA) hoặc S3; dùng Delta Lake hoặc Apache Iceberg trên tầng này (giao dịch ACID, gộp tệp nhỏ,
  time travel); TimescaleDB cho chuỗi thời gian.
- Kết nối nguồn thời gian thực (endpoint `latest-continuous` của USGS hoặc giao thức MQTT từ cảm biến).
- Huấn luyện lại định kỳ và theo dõi trôi dữ liệu (data drift), quản lý mô hình bằng MLflow.
- Mở rộng thông số liên quan vi khuẩn lam (chlorophyll, phycocyanin) theo hướng của bài báo [2].
- Cảnh báo đa kênh (email/Telegram) và chỉ số chất lượng nước tổng hợp (WQI).
