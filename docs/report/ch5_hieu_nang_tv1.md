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

**Lặp lại trên HDFS.** Thí nghiệm được chạy lại ở chế độ Big Data, với checkpoint Spark và data lake đặt trên HDFS
(`WQ_CHECKPOINT_PATH`, `WQ_LAKE_PATH` trỏ tới `hdfs://namenode:8020/wq/…`), cùng kịch bản dừng đột ngột và khởi động
lại: kết quả giống hệt bảng 5.z (2.866/2.866 bản ghi, 0 thiếu, 0 trùng, 0 bản ghi trễ bị loại). Như vậy cơ chế khôi
phục không phụ thuộc thư mục chia sẻ cục bộ, điều kiện để chạy driver trên máy khác của cụm.

## 5.7 Lưu trữ HDFS so với ổ đĩa cục bộ

**Thiết lập.** Cùng job ETL raw → bronze → silver → gold trên bộ dữ liệu chuẩn (10 trạm, 5.956.594 giá trị thô,
1.052.160 dòng silver), Spark `local[4]`, chỉ đổi nơi ghi data lake.

**Bảng 5.w — Thời gian ETL theo nơi lưu data lake**

| Nơi ghi data lake | Thời gian (s) | Dung lượng logic | Dung lượng thực trên đĩa |
|---|---|---|---|
| Ổ đĩa cục bộ | 18,6 | 52,7 MiB | 52,7 MiB |
| HDFS, hệ số nhân bản 2 (NameNode + 2 DataNode) | 45,9 | 52,4 MiB | 104,7 MiB |

`hdfs fsck /wq/lake_core`: HEALTHY, 76 tệp, 73 block, hệ số nhân bản trung bình 2,0, không block nào thiếu bản sao.

**Nhận xét.**
1. Ghi lên HDFS chậm hơn 2,5 lần. Mỗi tệp phải xin NameNode cấp block rồi đẩy dữ liệu qua chuỗi 2 DataNode
   (*write pipeline*), tức ghi gấp đôi số byte và thêm nhiều lượt RPC; trên một máy, mọi DataNode dùng chung một ổ
   đĩa nên không có lợi ích đọc ghi song song.
2. Kích thước block trung bình chỉ 0,75 MB so với block 128 MB: data lake gồm nhiều tệp nhỏ, đúng loại tải HDFS xử
   lý kém nhất vì chi phí metadata tính theo tệp. Gộp tệp (`coalesce` trước khi ghi, hoặc compaction định kỳ) là
   cải tiến cần làm khi dữ liệu lớn hơn.
3. Cái giá 2,5 lần đổi lấy ba điều ổ đĩa cục bộ không có: một không gian tên chung cho mọi executor trên mọi máy,
   dữ liệu còn nguyên khi mất một DataNode, và checkpoint streaming khôi phục được từ máy khác (mục 5.6).

## 5.8 Dữ liệu thật quy mô lớn trên HDFS

**Thiết lập.** Bộ trạm lớn được chốt ở **56 trạm thật** thuộc 15 bang và Puerto Rico (52 trạm đủ 3 năm × 6 thông số; danh sách đầy đủ 235 trạm
chưa tải xong do USGS giới hạn tần suất), gồm 970 tệp CSV nén gzip, 108,6 MB, tương ứng **34.180.940 giá trị thô**
và 5.892.096 dòng silver trên lưới 15 phút. Nạp lên HDFS mất 57 giây (970 tệp, 207 MiB trên đĩa tính cả bản sao).
Cùng job ETL như mục 5.4, cùng máy 4 vCPU, 15,7 GB RAM; script `benchmarks/bench_large.py`, kết quả trong
`benchmarks/results/batch_large.csv`.

**Bảng 5.v — ETL trên 34,2 triệu giá trị thật của 56 trạm**

| Cấu hình | Dữ liệu thô đọc từ | Data lake ghi vào | Thời gian (s) | Thông lượng (giá trị/s) | Bộ nhớ |
|---|---|---|---|---|---|
| pandas, 1 tiến trình | ổ đĩa cục bộ | ổ đĩa cục bộ | **85,5** | 399.700 | đỉnh đo được **9,4 GB** |
| Spark `local[4]` | ổ đĩa cục bộ | ổ đĩa cục bộ | 104,8 | 326.200 | giới hạn driver 6 GB |
| Cụm Spark, 2 worker × 2 core | HDFS | HDFS | 153,7 | 222.400 | giới hạn 2 GB mỗi worker |

Sau khi chạy, HDFS chứa: dữ liệu thô 103,6 MiB, bronze 225,3 MiB, silver 66,1 MiB, gold 5,6 KiB (mỗi phần
chiếm gấp đôi trên đĩa do nhân bản). `hdfs fsck /wq`: HEALTHY, 1.537 tệp, 1.531 block, kích thước block trung bình
0,3 MB, không block nào thiếu bản sao.

*Hình 5.v — `benchmarks/results/batch_large.png`.*

**Nhận xét.**
1. pandas vẫn nhanh nhất nhưng đã dùng 9,4 GB (RSS đỉnh 9.651 MiB), tức 60% RAM của máy, cho 34 triệu giá trị. Bộ nhớ của pandas tăng
   gần tuyến tính theo dữ liệu (5,9 GB ở 23,8 triệu giá trị, mục 5.4), nên với đủ 235 trạm (ước tính khoảng
   143 triệu giá trị) pandas cần cỡ 40 GB và không chạy được trên máy này. Spark xử lý cùng job trong giới hạn
   6 GB vì đọc theo partition và tràn xuống đĩa khi thiếu bộ nhớ; đây là lý do dùng Spark cho tầng batch.
2. Cụm Spark đọc và ghi HDFS chậm hơn Spark local 1,47 lần, trong khi ở bộ chuẩn 6 triệu giá trị cụm chậm hơn
   local 1,89 lần (mục 5.4), dù lượt chạy trên cụm còn gánh thêm chi phí HDFS. Chi phí cố định của cụm (khởi động
   executor, lập lịch task, trao đổi dữ liệu qua mạng) được chia cho lượng dữ liệu lớn hơn, nên khoảng cách thu hẹp
   khi dữ liệu tăng.
3. Thông lượng trên dữ liệu thật thấp hơn dữ liệu nhân bản ở mục 5.4 (326 nghìn so với 485 nghìn giá trị/giây ở
   `local[4]`): phải giải nén gzip và mở 970 tệp nhỏ thay vì 180 tệp. Tệp gzip không chia nhỏ được nên mỗi tệp là
   một task; 970 tệp vẫn đủ để mọi core cùng bận. Benchmark trên dữ liệu thật cho thấy những chi phí mà dữ liệu
   nhân bản bỏ sót.
4. Block trung bình 0,3 MB, nhỏ hơn nhiều so với block 128 MB: vấn đề tệp nhỏ ở mục 5.7 càng rõ khi dữ liệu nhiều
   trạm, vì mỗi trạm × thông số × năm là một tệp.

## 5.9 Streaming với 56 trạm thật, bật điền khuyết và dự báo

**Thiết lập.** Như mục 5.5 (cụm Spark 4 core, trigger 2 s, Kafka 6 partition), nhưng producer phát lại dữ liệu silver
của 56 trạm đọc từ HDFS, và bật điền khuyết cùng dự báo trực tuyến (LOCF và seasonal-naive). Mỗi mức tốc độ phát
trong 45 giây; kết quả trong `benchmarks/results/stream_large.csv`.

**Bảng 5.u — Streaming có ML theo số trạm**

| Tốc độ phát | Số trạm | Bản ghi xử lý / đã phát | Throughput (bản ghi/s) | p50 | p95 | Micro-batch (trung vị) | Xả backlog |
|---|---|---|---|---|---|---|---|
| 1.000 msg/s | 10 | — | 936 | 3,5 s | 5,0 s | 3,1 s | — |
| 1.000 msg/s | 56 | 45.000 / 45.000 | 918 | 7,7 s | 11,6 s | 6,9 s | 4,0 s |
| 4.000 msg/s | 56 | 180.000 / 180.000 | 3.448 | 15,8 s | 23,7 s | 13,6 s | 7,2 s |

(Dòng 10 trạm lấy từ bảng 5.y.)

*Hình 5.u — `benchmarks/results/stream_large_latency.png`.*

**Nhận xét.**
1. Không mất bản ghi nào ở cả hai mức. Tốc độ xử lý của Spark (trung vị 1.289 và 4.424 bản ghi/s) cao hơn tốc độ
   phát, nên hệ thống vẫn theo kịp: phần dồn được xả trong 4–7 giây sau khi producer dừng.
2. Độ trễ tăng khoảng gấp đôi so với 10 trạm (p50 3,5 → 7,7 s ở 1.000 msg/s). Bước ML chạy một nhóm pandas cho mỗi
   trạm trong mỗi micro-batch (`groupBy(station_id).applyInPandas`), nên chi phí tăng theo số trạm chứ không chỉ theo
   số bản ghi. Micro-batch dài hơn chu kỳ trigger, Spark tự gom nhiều dữ liệu hơn vào mỗi lô; ở 4.000 msg/s tốc độ xử
   lý chỉ cao hơn tốc độ phát 11%, tức hệ thống đã gần ngưỡng khi bật ML trên máy 4 vCPU.
3. **Dữ liệu thật phát hiện một lỗi mà dữ liệu chuẩn không lộ ra.** Lần chạy đầu, truy vấn dừng ngay micro-batch đầu
   tiên: một trạm trong bộ 56 không có cảm biến DO, bộ dự báo trả khung rỗng, pandas 2.3 gán kiểu float64 cho mọi cột
   của khung rỗng, và Arrow không ép được float64 sang timestamp (kể cả khi 0 dòng). Đã sửa bằng khung rỗng đúng kiểu
   (`empty_predictions()` trong `streaming/stream_processor.py`) và thêm test hồi quy
   `test_forecast_udf_station_without_do`. Bộ 10 trạm chuẩn trạm nào cũng có DO nên lỗi chưa từng xuất hiện.
4. Muốn giảm độ trễ khi số trạm tăng: tăng số core cho bước ML, gom nhiều trạm vào một nhóm pandas, hoặc chạy dự báo
   theo chu kỳ riêng (ví dụ mỗi 15 phút dữ liệu) thay vì mỗi micro-batch.
