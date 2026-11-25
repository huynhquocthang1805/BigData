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

**Thiết lập.** Bộ trạm lớn (danh sách 235 trạm) được tải dần vì USGS giới hạn tần suất với người dùng không có
API key, nên benchmark được chạy trên ba lần chốt dữ liệu; lần chốt 3 có đủ 235 trạm:

| Lần chốt | Trạm có dữ liệu | Tệp CSV gzip | Dung lượng nén | Giá trị thô | Dòng silver |
|---|---|---|---|---|---|
| 1 | 56 (15 bang và Puerto Rico) | 970 | 108,6 MB | 34.180.940 | 5.892.096 |
| 2 | 103 (20 bang và Puerto Rico) | 1.824 | 196,3 MB | 61.955.481 | 10.837.248 |
| 3 | 235 (34 bang, Puerto Rico và British Columbia) | 4.206 | 410,1 MB | 129.664.155 | 24.725.760 |

Mỗi lần chốt: tạm dừng bộ tải để dữ liệu không đổi trong lúc đo, nạp lên HDFS (`batch.ingest_hdfs --overwrite`),
rồi chạy cùng job ETL như mục 5.4 trên cùng máy 4 vCPU, 15,7 GB RAM (`benchmarks/bench_large.py`, kết quả trong
`benchmarks/results/batch_large.csv`). Từ lần chốt 2, tiến trình pandas được giới hạn 12 GB bộ nhớ
(`--pandas-mem-gb 12`, xấp xỉ RAM còn trống khi Kafka, HDFS, Spark và PostgreSQL đang chạy): nếu thiếu bộ nhớ,
pandas dừng trong giới hạn đó thay vì kích hoạt OOM killer của hệ điều hành và làm sập các dịch vụ khác.

**Bảng 5.v — ETL trên dữ liệu thật**

| Cấu hình | 56 trạm, 34,2 triệu giá trị | 103 trạm, 62,0 triệu giá trị | 235 trạm, 129,7 triệu giá trị |
|---|---|---|---|
| pandas, 1 tiến trình | **85,5 s**, RSS đỉnh 9,4 GB | **không chạy được**: hết bộ nhớ sau 105 s (RSS 11,0 GB) | **không chạy được**: chạm giới hạn sau 88 s (RSS 11,8 GB) |
| Spark `local[4]`, đĩa cục bộ | 104,8 s (326.200 giá trị/s) | 168,1 s (368.500 giá trị/s) | 380,6 s (340.700 giá trị/s) |
| Cụm Spark 2 worker × 2 core, đọc/ghi HDFS | 153,7 s (222.400 giá trị/s) | 238,0 s (260.300 giá trị/s) | 541,7 s (239.300 giá trị/s) |
| Cụm / local | 1,47 lần | 1,42 lần | 1,42 lần |

*Hình 5.v — `benchmarks/results/batch_large.png`.*

Sau lần chốt 3, `hdfs fsck /wq`: HEALTHY, 15.680 tệp, 15.674 block, kích thước block trung bình 0,1 MB, không block
nào thiếu bản sao. Riêng dữ liệu thô là 4.206 tệp, 410,1 MB (820,2 MB trên đĩa với hệ số nhân bản 2).

**Nhận xét.**
1. **Đo được ranh giới của pandas trên dữ liệu thật.** Ở 34 triệu giá trị, pandas còn nhanh nhất nhưng đã dùng
   9,4 GB (RSS đỉnh 9.651 MiB), tức 60% RAM của máy. Ở 62 triệu giá trị, nó dừng vì hết bộ nhớ: đang giữ 11 GB thì
   không cấp phát nổi thêm một mảng 473 MiB. Với đủ 235 trạm (130 triệu giá trị), nó chạm giới hạn sau 88 giây và
   bị dừng với tín hiệu 11 (SIGSEGV) khi đang giữ 11,8 GB. Bộ nhớ của pandas tăng gần tuyến tính theo dữ liệu
   (khoảng 0,28 GB cho mỗi triệu giá trị), nên khối 130 triệu giá trị cần khoảng 36 GB, hơn gấp đôi RAM của máy.
2. **Spark chạy được ở mọi quy mô, thời gian tăng gần tuyến tính theo dữ liệu.** Dữ liệu tăng 3,79 lần (34 → 130
   triệu giá trị), thời gian Spark tăng 3,63 lần ở local và 3,52 lần ở cụm. Từ lần chốt 1 sang 2, thông lượng tăng
   (326 → 368 nghìn giá trị/giây ở local) vì chi phí cố định (khởi động job, lập lịch, liệt kê tệp) được chia cho
   nhiều dữ liệu hơn. Từ lần chốt 2 sang 3, thông lượng giảm 7–8% (368 → 341 nghìn ở local, 260 → 239 nghìn ở cụm).
   Giả thuyết, chưa kiểm chứng: số partition shuffle cố định là 16 nên mỗi partition lớn gấp đôi và phải tràn xuống
   đĩa nhiều hơn; thí nghiệm tiếp theo là tăng số partition theo kích thước dữ liệu. Spark đọc theo partition và
   tràn xuống đĩa khi thiếu bộ nhớ, nên driver 6 GB vẫn đủ cho khối dữ liệu mà pandas cần khoảng 36 GB.
3. **Khoảng cách giữa cụm và local thu hẹp rồi ổn định quanh 1,4 lần**: 1,89 lần ở 6 triệu giá trị (mục 5.4, cụm
   ghi đĩa cục bộ), 1,47 lần ở 34 triệu, 1,42 lần ở cả 62 và 130 triệu giá trị, dù lượt chạy trên cụm còn gánh thêm
   chi phí HDFS (mục 5.7). Trên một máy, cụm chưa thể nhanh hơn local vì mọi container dùng chung 4 vCPU; lợi ích
   của cụm là thêm được máy thật khi dữ liệu vượt khả năng một máy.
4. Thông lượng trên dữ liệu thật thấp hơn dữ liệu nhân bản ở mục 5.4 (326–368 nghìn so với 485 nghìn giá trị/giây
   ở `local[4]`): phải giải nén gzip và mở hàng nghìn tệp nhỏ (4.206 tệp ở lần chốt 3) thay vì 180 tệp. Tệp gzip
   không chia nhỏ được nên mỗi tệp là một task; số tệp vẫn đủ để mọi core cùng bận.
5. Block trung bình chỉ khoảng 0,1 MB so với block 128 MB: vấn đề tệp nhỏ ở mục 5.7 càng rõ khi dữ liệu nhiều
   trạm, vì mỗi trạm × thông số × năm là một tệp và mỗi tệp chiếm một mục metadata trong bộ nhớ của NameNode.

**Lỗi phát hiện khi nạp lại.** Lần chốt 2 làm lộ một lỗi của `--overwrite`: `copyFromLocalFile` vào một thư mục
HDFS đã có sẽ chép nguồn thành thư mục con (`/wq/raw/usgs/usgs_large/…`) và giữ nguyên dữ liệu cũ, nên lượt chạy
trên cụm sẽ âm thầm chỉ đọc 56 trạm cũ. Đã sửa (xóa bản cũ trước khi chép) và thêm test hồi quy
`test_ingest_overwrite_replaces_instead_of_nesting`; sau khi nạp lại, HDFS có đúng 1.824 tệp như landing, và ở lần
chốt 3 là đúng 4.206 tệp.

## 5.9 Streaming nhiều trạm thật, bật điền khuyết và dự báo

**Thiết lập.** Như mục 5.5 (cụm Spark 4 core, trigger 2 s, Kafka 6 partition), nhưng producer phát lại dữ liệu
silver của bộ trạm lớn đọc từ HDFS và bật điền khuyết cùng dự báo trực tuyến (LOCF và seasonal-naive). Mỗi lượt
phát 45.000 message trong 45 giây. Để tách riêng ảnh hưởng của số trạm, ba lượt được chạy trong cùng một phiên, cùng
cấu hình, chỉ đổi số trạm (`bench_streaming --max-stations N` giữ N trạm nhiều bản ghi nhất). Kết quả trong
`benchmarks/results/stream_stations.csv`.

**Bảng 5.u — Độ trễ theo số trạm (1.000 msg/s, bật ML, cùng phiên đo)**

| Số trạm | Bản ghi xử lý / đã phát | Throughput (bản ghi/s) | p50 | p95 | Micro-batch (trung vị) | Xả backlog |
|---|---|---|---|---|---|---|
| 10 | 45.000 / 45.000 | 938 | 5,5 s | 7,9 s | 5,1 s | 3,0 s |
| 50 | 45.000 / 45.000 | 918 | 6,3 s | 8,5 s | 5,6 s | 4,0 s |
| 100 | 45.000 / 45.000 | 898 | 7,9 s | 11,9 s | 7,8 s | 5,1 s |

*Hình 5.u — `benchmarks/results/stream_latency_by_stations.png`.*

Lượt đo đầu tiên, ở một phiên khác với bộ 56 trạm (51 trạm có dữ liệu trong khoảng thời gian được phát lại), cho
kết quả cùng mức: 1.000 msg/s có p50 7,7 s, p95 11,6 s; 4.000 msg/s xử lý đủ 180.000/180.000 bản ghi với p50
15,8 s, p95 23,7 s và tốc độ xử lý trung vị 4.424 bản ghi/s, chỉ cao hơn tốc độ phát 11%, tức đã gần ngưỡng khi bật
ML trên máy 4 vCPU (`benchmarks/results/stream_large.csv`).

**Nhận xét.**
1. Không mất bản ghi nào ở mọi lượt. Tốc độ xử lý của Spark (trung vị 1.165–1.261 bản ghi/s ở 1.000 msg/s) cao hơn
   tốc độ phát, nên hệ thống theo kịp; phần dồn được xả trong 3–5 giây sau khi producer dừng.
2. **Số trạm tăng 10 lần, độ trễ p50 tăng 43%** (5,5 → 7,9 s), p95 tăng 51%. Mỗi micro-batch, bước ML đọc lịch sử
   48 giờ của các trạm có mặt từ PostgreSQL rồi chạy một nhóm pandas cho mỗi trạm, nên khối lượng tăng theo số trạm.
   Nhưng phần lớn thời gian một micro-batch là chi phí cố định (lập lịch các Spark job đọc JDBC, `applyInPandas`,
   ghi PostgreSQL), nên độ trễ tăng chậm hơn nhiều so với số trạm.
3. **Micro-batch đầu tiên mất 18–21 giây ở mọi lượt** (khởi động Python worker trên executor, nạp mô hình, mở kết
   nối), tạo một khối dồn khoảng 19.000–22.000 bản ghi mà các batch sau phải xả dần. Với lượt đo chỉ 45 giây, phần
   khởi động này chiếm phần lớn độ trễ đo được. Khi chạy liên tục, độ trễ ổn định thấp hơn: trong demo chế độ Big
   Data (54 trạm, khoảng 200 msg/s), p95 giảm từ 14,8 s xuống 8,2 s sau khi xả xong phần dồn lúc khởi động.
4. Con số 3,5 s của bộ 10 trạm ở bảng 5.y được đo ở chế độ demo (data lake cục bộ, phiên đo khác), nên không so
   trực tiếp với bảng 5.u; so sánh theo số trạm phải dùng các lượt cùng phiên.
5. **Dữ liệu thật phát hiện một lỗi mà dữ liệu chuẩn không lộ ra.** Lần chạy đầu với bộ lớn, truy vấn dừng ngay
   micro-batch đầu tiên: một trạm không có cảm biến DO, bộ dự báo trả khung rỗng, pandas 2.3 gán kiểu float64 cho mọi
   cột của khung rỗng, và Arrow không ép được float64 sang timestamp (kể cả khi 0 dòng). Đã sửa bằng khung rỗng đúng
   kiểu (`empty_predictions()` trong `streaming/stream_processor.py`) và thêm test hồi quy
   `test_forecast_udf_station_without_do`. Bộ 10 trạm chuẩn trạm nào cũng có DO nên lỗi chưa từng xuất hiện.
6. Muốn giảm độ trễ khi số trạm tăng: khởi động trước (warm-up) Python worker, tăng số core cho bước ML, hoặc chạy
   dự báo theo chu kỳ riêng (ví dụ mỗi 15 phút dữ liệu) thay vì mỗi micro-batch.
