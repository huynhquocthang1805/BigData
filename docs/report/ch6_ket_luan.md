# Chương 6. Kết luận (bản nháp — TV1)

## 6.1 Kết quả đạt được

- Xây dựng thành công pipeline Big Data end-to-end cho bài toán giám sát, điền khuyết và dự báo chất lượng nước
  trên bộ 235 trạm USGS giai đoạn 2023–2025 (24.725.760 dòng Silver trên lưới 15 phút), triển khai bằng một lệnh
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
- Chạy được trên dữ liệu thật quy mô lớn: đủ 235 trạm USGS (34 bang, Puerto Rico và British Columbia), 130 triệu
  giá trị thô, lưu trên HDFS và xử lý trên cụm Spark trong 542 s (Spark local 381 s). Từ 62 triệu giá trị trở lên
  pandas hết bộ nhớ và không chạy xong (đã dùng 11–11,8 GB), trong khi Spark chạy trong giới hạn driver 6 GB; ở
  34 triệu giá trị pandas còn nhanh hơn nhưng đã dùng 9,4 GB. Dữ liệu tăng 3,8 lần thì thời gian Spark tăng
  3,5–3,6 lần. Khoảng cách giữa cụm và local thu hẹp rồi ổn định: 1,89 lần (6 triệu giá trị), 1,47 lần (34 triệu),
  1,42 lần (62 và 130 triệu).
- Streaming có điền khuyết và dự báo không mất bản ghi nào với 10, 50 và 100 trạm; số trạm tăng 10 lần thì độ trễ
  p50 tăng 43% (5,5 → 7,9 s ở 1.000 msg/s).
- Trên tập test tháng 7–12/2025, khi che theo đoạn 20% DO, LOCF đạt RMSE 0,7700 mg/L, MAE 0,4000 mg/L và độ phủ 99,92% qua ba seed. Có 230/235 trạm đóng góp điểm điền khuyết ở mức này. [Kết quả test](validation_va_test_do.md)
- Ridge có kết quả dự báo trên 229/235 trạm. So với baseline tốt nhất từng khoảng, RMSE test giảm 23,08% ở 1 giờ, 39,21% ở 3 giờ, 21,61% ở 6 giờ và 5,11% ở 24 giờ. Trong thí nghiệm che thêm 10% và giữ cùng mô hình Ridge, LOCF cho RMSE thấp hơn Kalman ở cả bốn khoảng. Phép so sánh với để trống đầu vào có giao thức riêng ở mục TV4.4. [Kết quả dự báo và giao thức ba kịch bản](validation_va_test_do.md)

## 6.2 Hạn chế

1. Cụm Spark, HDFS và Kafka chạy trên **một máy vật lý** (các container chia sẻ CPU và ổ đĩa), nên kết quả mở rộng
   theo chiều ngang chỉ mang tính chỉ dẫn: cụm chậm hơn local, ghi HDFS chậm hơn ổ đĩa cục bộ 2,5 lần; ở quy mô dữ
   liệu chuẩn, pandas đơn tiến trình còn nhanh hơn Spark (đã nêu trung thực ở chương 5).
2. Bộ dữ liệu lớn (235 trạm, 410 MB nén) phải tải nhiều lượt trong khoảng 4 giờ vì API của USGS giới hạn tần suất
   với người dùng không có API key; muốn thêm trạm cần API key. Ở khối 130 triệu giá trị, thông lượng Spark giảm
   7–8% so với khối 62 triệu; số partition shuffle chưa được tinh chỉnh theo kích thước dữ liệu.
3. Dữ liệu thời gian thực được **mô phỏng bằng phát lại** dữ liệu lịch sử, chưa kết nối nguồn cảm biến trực tiếp.
4. Kafka một broker (replication factor 1) — chưa chịu được lỗi broker; PostgreSQL một nút; HDFS một NameNode
   (chưa bật High Availability).
5. Data lake gồm nhiều tệp nhỏ (15.680 tệp, block trung bình khoảng 0,1 MB so với block 128 MB của HDFS), chưa
   có bước gộp tệp.
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
