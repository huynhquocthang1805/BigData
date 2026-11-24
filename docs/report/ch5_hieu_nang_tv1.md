# Chương 5. Thực nghiệm — mục "Đánh giá hiệu năng hệ thống Big Data" (bản nháp — TV1)

> Số liệu dưới đây đo trên máy thử nghiệm **4 vCPU, 15 GB RAM, mọi container dùng chung 4 vCPU**. Nhóm chạy lại
> trên máy của nhóm bằng `benchmarks/` (lệnh trong `benchmarks/README.md`) rồi thay số liệu; nhận xét về xu hướng
> nhiều khả năng vẫn giữ nguyên. Các mục 5.1–5.3 (imputation, prediction) do TV3/TV4 viết.

## 5.4 Hiệu năng xử lý batch

**Mục đích.** Đánh giá khả năng mở rộng của ETL (đọc 180 file CSV → bronze → silver lưới 15 phút → gold) theo
(i) kích thước dữ liệu và (ii) số core; so sánh với cùng logic viết bằng pandas trên một tiến trình.

**Thiết lập.** Dữ liệu ×k được tạo bằng cách nhân bản số trạm k lần (×1 = 5,96 triệu giá trị đầu vào,
×8 = 47,7 triệu). Spark chạy ở chế độ local[n] (n = 1, 2, 4) và chế độ cluster (2 worker × 2 core, cùng máy).
Thời gian đo từ lúc đọc đến khi ghi xong Parquet.

**Bảng 5.x — Thời gian ETL (giây)**

| Cấu hình | ×1 (5,96 triệu) | ×2 (11,9 triệu) | ×4 (23,8 triệu) | ×8 (47,7 triệu) |
|---|---|---|---|---|
| pandas (1 tiến trình) | **17,3** | **21,6** | **43,6** (RAM đỉnh 5,9 GB) | không chạy (ước tính ~12 GB RAM) |
| Spark local[1] | 49,2 | 76,6 | 139,3 | — |
| Spark local[2] | 25,5 | 41,6 | 79,9 | — |
| Spark local[4] | 18,6 | 27,2 | 49,1 | 111,5 |
| Spark cluster, 2 core | 40,1 | 62,4 | 103,4 | — |
| Spark cluster, 4 core | 35,2 | 52,2 | 81,0 | — |

*Hình 5.x — `batch_throughput_vs_size.png`, `batch_speedup_vs_cores.png`, `batch_local_vs_cluster.png`.*

**Nhận xét.**
1. **Mở rộng theo số core:** ở quy mô ×4, tăng 1 → 2 → 4 core giảm thời gian 139 → 80 → 49 s, tức tăng tốc
   1,74× và **2,84×** (hiệu suất song song 71% ở 4 core). Không đạt tuyến tính vì phần tuần tự (lập kế hoạch, ghi
   file) và vì 4 vCPU của máy còn phục vụ Kafka, PostgreSQL và các container khác.
2. **Chi phí cố định được khấu hao khi dữ liệu lớn:** thông lượng Spark local[4] tăng từ 0,32 lên 0,49 triệu giá
   trị/giây khi dữ liệu tăng từ ×1 lên ×4; ở ×8 giảm nhẹ còn 0,43 do bắt đầu tràn bộ nhớ ra đĩa (driver giới hạn 6 GB).
3. **pandas nhanh hơn khi dữ liệu vừa RAM một máy** (17 so với 19 s ở ×1; 44 so với 49 s ở ×4), nhưng **bộ nhớ tăng
   tuyến tính** theo dữ liệu (5,9 GB ở 23,8 triệu giá trị), nên ở ×8 cần ~12 GB — sát giới hạn máy — trong khi Spark
   xử lý ×8 với bộ nhớ có giới hạn và có thể mở rộng thêm máy. Đây là ranh giới thực tế cho việc dùng Spark.
4. **Cluster trên cùng một máy chậm hơn local 1,65–1,9 lần** do chi phí trao đổi dữ liệu qua mạng giữa container,
   tuần tự hóa và khởi tạo executor; khoảng cách thu hẹp khi dữ liệu lớn hơn (1,89× ở ×1 xuống 1,65× ở ×4). Lợi ích
   của cluster chỉ xuất hiện khi bổ sung máy vật lý — kết quả này được nêu trung thực thay vì chỉ trình bày ưu điểm.

## 5.5 Hiệu năng xử lý luồng

**Mục đích.** Xác định thông lượng tối đa mà luồng Kafka → Spark → PostgreSQL duy trì được và độ trễ end-to-end.

**Thiết lập.** Cluster 2 worker, 4 core, trigger 2 giây, topic 6 partition. Với mỗi tốc độ R ∈ {500, 1000, 2000,
4000, 8000} message/giây, producer phát đúng R msg/s trong 45 giây vào topic mới (checkpoint mới, CSDL riêng).
Độ trễ end-to-end = thời điểm micro-batch ghi xong vào PostgreSQL − thời điểm producer gửi. Truy vấn đo:
`serving_sink` (làm sạch, QC, watermark, khử trùng lặp, upsert, cảnh báo), tắt ML để đo đường dữ liệu chính.

**Bảng 5.y — Thông lượng và độ trễ theo tốc độ phát**

| Tốc độ phát (msg/s) | Throughput (bản ghi/s) | Độ trễ p50 | Độ trễ p95 | Thời gian micro-batch | Đánh giá |
|---|---|---|---|---|---|
| 500 | 459 | 2,1 s | 3,0 s | 1,0 s | theo kịp |
| 1.000 | 936 | 2,4 s | 3,2 s | 1,3 s | theo kịp |
| 2.000 | 1.875 | 2,6 s | 3,2 s | 1,3 s | theo kịp |
| 4.000 | 3.822 | 2,6 s | 3,4 s | 1,5 s | theo kịp |
| 8.000 | 7.484 | 3,3 s | 4,0 s | 1,8 s | theo kịp |
| 16.000 | 14.008 | 12,0 s | 16,5 s | 8,3 s | quá tải |
| 24.000 | 17.886 | 24,7 s | 30,3 s | 14,0 s | quá tải (trần ≈ 18 nghìn bản ghi/s) |
| 1.000 + ML online | 936 | 3,5 s | 5,0 s | 3,1 s | theo kịp |

(Throughput = số bản ghi đã ghi vào PostgreSQL / (thời gian phát + thời gian xả backlog), nên thấp hơn tốc độ phát một
chút ngay cả khi hệ thống theo kịp; độ trễ là trung vị qua các micro-batch, bỏ batch khởi động.)

**Nhận xét.**
1. Với trigger 2 giây, độ trễ end-to-end ở tải thấp ≈ 2 s, gồm thời gian chờ đến trigger kế tiếp (trung bình ~1 s)
   cộng thời gian xử lý micro-batch (~1 s). Đến 8.000 msg/s độ trễ chỉ tăng nhẹ (p95 4,0 s): hệ thống **bền vững tới ít
   nhất 8.000 msg/s**.
2. Ở 16.000 msg/s, thời gian xử lý một micro-batch (8,3 s) vượt chu kỳ trigger, hàng đợi trong Kafka tích lũy, độ
   trễ tăng lên hàng chục giây — điểm bão hòa nằm giữa 8.000 và 16.000 msg/s; thông lượng xử lý tối đa đo được
   ≈ 18.000 bản ghi/s. Nhờ `maxOffsetsPerTrigger` và Kafka lưu dữ liệu bền vững, quá tải chỉ làm tăng độ trễ chứ
   không làm mất dữ liệu; hệ thống tự xả backlog khi tải giảm.
3. Điền khuyết và dự báo trực tuyến (baseline) làm tăng thời gian micro-batch ~1,8 s và độ trễ p50 thêm ~1,1 s ở
   1.000 msg/s; mô hình phức tạp hơn của TV3/TV4 cần được đo lại theo cùng quy trình.
4. Hướng mở rộng: tăng số partition Kafka và số executor; ghi PostgreSQL bằng `COPY` thay cho `INSERT` theo lô
   (khâu ghi chiếm phần lớn thời gian micro-batch).

*Hình 5.y — `stream_latency_cluster_4c.png`, `stream_throughput_cluster_4c.png`.*

## 5.6 Kiểm chứng tính đúng của luồng streaming

**Thiết lập.** Phát 3 ngày dữ liệu (01–03/07/2025, 10 trạm, 2.866 bản ghi) ở tốc độ 3600×, chủ động làm trễ 1%
message (0,5–3 giây thực, tương ứng ~30–180 phút dữ liệu) và gửi trùng 0,5% message. Sau 30 giây, dừng đột ngột
tiến trình Spark (`docker compose kill stream-processor`, tương đương mất điện), 20 giây sau khởi động lại. Đối chiếu
bằng `scripts/verify_stream.py`.

**Bảng 5.z — Kết quả kiểm chứng**

| Chỉ số | Giá trị |
|---|---|
| Bản ghi lẽ ra phải đến | 2.866 |
| Message trong Kafka | 2.876 (+10 bản trùng cố ý) |
| Dòng trong PostgreSQL / trùng khóa | 2.866 / 0 |
| Thiếu / thừa so với dữ liệu nguồn | 0 / 0 |
| Dòng trong data lake (Parquet) / phân biệt | 2.866 / 2.866 |
| Message trễ cố ý / bị loại bởi watermark | 32 / 0 |

**Nhận xét.**
1. Sau khi khởi động lại, Spark đọc offset và state từ checkpoint, **chạy lại micro-batch đang dở dang** rồi xử lý
   tiếp phần dữ liệu đã dồn trong Kafka. Vì mọi thao tác ghi là upsert theo khóa chính nên việc chạy lại không sinh
   bản ghi trùng: hệ thống không mất và không nhân đôi dữ liệu (exactly-once hiệu quả).
2. 10 message trùng bị loại bởi khử trùng lặp (và khóa chính); data lake không có bản ghi trùng nhờ nhật ký của file sink.
3. Không bản ghi trễ nào bị loại: watermark chỉ bảo đảm *không loại* dữ liệu trễ dưới 2 giờ; dữ liệu trễ hơn vẫn có thể
   được nhận nếu đến trước khi watermark của micro-batch kế tiếp vượt qua nó. Muốn loại chặt chẽ cần thêm bộ lọc theo
   thời gian xử lý — đánh đổi giữa tính đầy đủ và độ kịp thời.
4. Sink Parquet tạo 199 file cho 2.866 dòng (*small files problem*): cần job gộp file định kỳ hoặc dùng Delta Lake.
