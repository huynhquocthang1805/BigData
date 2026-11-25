# Chương 2. Cơ sở lý thuyết — mục "Nền tảng Dữ liệu lớn" (bản nháp — TV1)

> Các mục về điền khuyết (TV3) và dự báo chuỗi thời gian (TV4) đặt sau mục này. Số trích dẫn tiếp nối danh sách
> ở Chương 1 ([1]–[8]); tài liệu mới bắt đầu từ [9].

## 2.1 Dữ liệu lớn và đặc trưng 5V trong quan trắc chất lượng nước

Dữ liệu lớn thường được mô tả qua năm đặc trưng. Bài toán của nhóm có đủ cả năm:

| Đặc trưng | Biểu hiện trong đề tài |
|---|---|
| **Volume** (khối lượng) | 235 trạm USGS × 6 thông số × 3 năm, lưới 15 phút: hàng chục đến hơn trăm triệu giá trị đo; toàn mạng USGS có hơn 900 trạm có cảm biến chất lượng nước, nên quy mô còn tăng tiếp |
| **Velocity** (tốc độ) | Mỗi trạm sinh 1 bản ghi / 15 phút, liên tục; hệ thống phải xử lý theo luồng, cảnh báo trong vài giây |
| **Variety** (đa dạng) | 6 thông số với đơn vị khác nhau, mã trạng thái phê duyệt (Approved/Provisional), qualifier, siêu dữ liệu trạm |
| **Veracity** (độ tin cậy) | Cảm biến bám bẩn, mất nguồn, lỗi truyền: tỉ lệ thiếu 0,1–18,5% tùy trạm/biến, giá trị ngoài phạm vi vật lý — chính là động cơ của bài toán điền khuyết |
| **Value** (giá trị) | Cảnh báo sớm thiếu oxy (DO < 2 mg/L), pH bất thường khi tảo/vi khuẩn lam nở hoa [2], hỗ trợ vận hành nguồn nước |

## 2.2 Hadoop và hệ thống file phân tán HDFS

MapReduce [9] đưa ra mô hình tính toán song song trên cụm máy thông dụng; hệ sinh thái Hadoop đi kèm hệ thống
file phân tán HDFS [10]. HDFS theo kiến trúc chủ–tớ:

- **NameNode** quản lý không gian tên (cây thư mục) và ánh xạ file → block → DataNode; chỉ giữ siêu dữ liệu.
- **DataNode** lưu các **block** (mặc định 128 MB) và định kỳ báo cáo (heartbeat, block report) về NameNode.
- Mỗi block được **nhân bản** (replication, mặc định 3) trên nhiều DataNode; khi một DataNode hỏng, NameNode tạo lại
  bản sao từ các bản còn lại. Việc ghi đi theo *pipeline* qua các DataNode; việc đọc ưu tiên bản sao gần nhất
  (*data locality*) để chuyển tính toán đến nơi có dữ liệu thay vì chuyển dữ liệu.

**Áp dụng.** Cụm HDFS của đề tài gồm 1 NameNode và 2 DataNode, hệ số nhân bản 2. HDFS chứa toàn bộ data lake
(bronze/silver/gold, dữ liệu streaming) và **checkpoint của Spark Structured Streaming** — đúng mô hình triển khai thực
tế: trạng thái luồng nằm trên bộ nhớ bền vững, chia sẻ được giữa mọi executor, chịu được hỏng một nút.

## 2.3 Apache Spark

Spark [3] tổ chức tính toán trên các tập dữ liệu phân tán bất biến (RDD) [14], ở mức cao hơn là DataFrame/Spark SQL [11].
Một ứng dụng gồm **driver** (lập kế hoạch) và nhiều **executor** (thực thi) do **cluster manager** cấp phát (ở đây là
Spark Standalone: 1 master, 2 worker).

- **Đánh giá lười (lazy evaluation) và DAG.** Các phép biến đổi chỉ ghi lại kế hoạch; khi gặp *action*, Spark tối ưu
  kế hoạch (Catalyst: đẩy bộ lọc xuống nguồn, cắt cột, chọn chiến lược join) rồi chia thành các **stage**.
- **Partition và task.** Mỗi stage gồm các task, mỗi task xử lý một partition trên một core — đơn vị song song.
- **Biến đổi hẹp và rộng.** `filter`, `withColumn` là biến đổi hẹp (không trao đổi dữ liệu); `groupBy`, `pivot`, `join`
  là biến đổi rộng, cần **shuffle** dữ liệu qua mạng giữa các executor — chi phí lớn nhất của job phân tán.
- **Spark SQL và Arrow.** Dữ liệu được chuyển sang pandas theo khối cột Arrow, cho phép chạy mã Python
  (`applyInPandas`) song song theo nhóm khóa trên các executor.

**Áp dụng.** ETL đọc 4.230 file CSV nén song song, `pivot` dữ liệu dài thành rộng (biến đổi rộng, có shuffle theo
`station_id`), ghi Parquet phân vùng theo `station_id` để các truy vấn theo trạm chỉ đọc đúng thư mục (*partition
pruning*). Điền khuyết và dự báo trực tuyến dùng `groupBy("station_id").applyInPandas(...)`: mỗi trạm là một nhóm
độc lập, nên thêm trạm thì chỉ cần thêm executor.

## 2.4 Apache Kafka

Kafka [5] là hệ thống nhật ký (log) phân tán theo mô hình publish–subscribe. Dữ liệu được ghi vào **topic**, mỗi topic
chia thành nhiều **partition**; mỗi partition là một log chỉ-ghi-thêm, mỗi bản ghi có **offset** tăng dần. Consumer tự
quản lý offset nên có thể **đọc lại** (replay) từ bất kỳ vị trí nào trong thời gian lưu giữ. Thứ tự chỉ được bảo đảm
*trong một partition*; các partition được đọc song song bởi các consumer trong cùng **consumer group**. Từ phiên bản 4,
Kafka chạy ở chế độ KRaft (tự quản lý metadata, không cần ZooKeeper).

**Áp dụng.** Topic `wq.sensor.raw` có 6 partition, khóa message là `station_id` ⇒ bản ghi của một trạm luôn vào cùng
partition (đúng thứ tự thời gian theo trạm), các trạm khác nhau được Spark đọc song song. Producer bật *idempotence*
để Kafka loại bản ghi trùng do gửi lại. Kafka giữ dữ liệu 7 ngày, nên khi tiến trình xử lý dừng, dữ liệu không mất mà
chờ được đọc tiếp.

## 2.5 Spark Structured Streaming

Structured Streaming [4] coi luồng dữ liệu là một **bảng không giới hạn** được nối thêm liên tục; truy vấn viết như
truy vấn batch và được thực thi theo từng **micro-batch** (trigger định kỳ).

- **Thời gian sự kiện và watermark.** Kết quả tính theo thời điểm đo (*event time*) chứ không theo thời điểm nhận.
  **Watermark** = thời gian sự kiện lớn nhất đã thấy trừ một độ trễ cho phép; dữ liệu trễ hơn watermark có thể bị
  loại, và trạng thái cũ hơn được dọn khỏi bộ nhớ. Mô hình này kế thừa Dataflow Model [12].
- **Toán tử có trạng thái.** Khử trùng lặp (`dropDuplicatesWithinWatermark`) và tổng hợp theo cửa sổ thời gian lưu
  trạng thái trong *state store*.
- **Chế độ xuất.** *Append* chỉ xuất kết quả đã chốt (cửa sổ đã qua watermark); *update* xuất các dòng thay đổi.
- **Chịu lỗi và exactly-once.** Offset nguồn và trạng thái được ghi vào **checkpoint** (write-ahead log). Khi khởi động
  lại, micro-batch dở dang được chạy lại; nếu nguồn đọc lại được (Kafka) và đích ghi **idempotent**, kết quả cuối cùng
  tương đương xử lý đúng một lần (*end-to-end exactly-once*).

**Áp dụng.** Watermark 2 giờ; khử trùng lặp theo (`station_id`, `event_time`); cửa sổ 1 giờ ở chế độ append;
`foreachBatch` ghi PostgreSQL bằng `INSERT … ON CONFLICT` (idempotent); checkpoint trên HDFS. Thực nghiệm ở mục 5.6
kiểm chứng: dừng đột ngột tiến trình xử lý giữa chừng vẫn cho 0 bản ghi thiếu, 0 bản ghi trùng.

## 2.6 Kiến trúc Lambda và Kappa

**Lambda** [6] tách *batch layer* (xử lý toàn bộ lịch sử, chính xác), *speed layer* (xử lý dữ liệu mới, độ trễ thấp)
và *serving layer* (hợp nhất kết quả để truy vấn). Nhược điểm kinh điển là phải duy trì hai bộ mã cho cùng một logic.
**Kappa** [15] chỉ dùng một luồng xử lý và tính lại lịch sử bằng cách phát lại log.

**Lựa chọn của nhóm.** Lambda rút gọn: batch cần thiết để huấn luyện mô hình trên 2 năm lịch sử, streaming cần thiết
để giám sát thời gian thực. Nhược điểm "hai bộ mã" được giảm bằng cách dùng **chung một engine (Spark), chung data
contract và chung hàm kiểm tra chất lượng** cho cả hai lớp.

## 2.7 Data lake, kiến trúc medallion và định dạng Parquet

Data lake lưu dữ liệu ở dạng gốc lẫn đã xử lý trên bộ nhớ rẻ, mở rộng được (HDFS, S3). Kiến trúc **medallion** chia
thành **bronze** (thô, đầy đủ), **silver** (đã làm sạch, chuẩn hóa) và **gold** (tổng hợp phục vụ phân tích). **Parquet**
là định dạng cột: nén tốt theo từng cột, chỉ đọc các cột cần (*column pruning*) và bỏ qua khối không thỏa bộ lọc nhờ
thống kê min/max (*predicate pushdown*). Ghi streaming tạo nhiều file nhỏ (*small files problem*), làm NameNode phải giữ
nhiều metadata và làm chậm truy vấn — cần gộp file định kỳ hoặc dùng định dạng bảng như Delta Lake/Iceberg.

## 2.8 Định lượng khả năng mở rộng

- **Speedup** S(N) = T(1) / T(N); **hiệu suất song song** E(N) = S(N) / N.
- **Định luật Amdahl** [13]: nếu phần p của công việc song song hóa được, S(N) = 1 / ((1 − p) + p/N), nên phần tuần tự
  giới hạn speedup tối đa ở 1/(1 − p).
- **Thông lượng** (bản ghi/giây) và **độ trễ** theo phân vị (p50, p95): hệ streaming **bền vững** ở tốc độ R nếu thời
  gian xử lý mỗi micro-batch nhỏ hơn chu kỳ trigger; vượt ngưỡng đó, hàng đợi tích lũy và độ trễ tăng không giới hạn
  (*backpressure* bằng `maxOffsetsPerTrigger` giữ cho mỗi batch có kích thước chặn trên).

Ví dụ từ thực nghiệm (mục 5.4): ETL đạt S(4) = 2,84 trên 4 core ⇒ theo Amdahl, phần song song hóa được p ≈ 0,86, nên
dù thêm vô hạn core trên cùng máy, speedup tối đa ≈ 7 lần — lý do cần mở rộng **ngang** (thêm máy) khi dữ liệu tăng.

## Tài liệu tham khảo bổ sung

[9] J. Dean, S. Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters," *Communications of the ACM*, vol. 51,
no. 1, pp. 107–113, 2008, doi: 10.1145/1327452.1327492.

[10] K. Shvachko, H. Kuang, S. Radia, R. Chansler, "The Hadoop Distributed File System," in *Proc. IEEE 26th Symp. Mass
Storage Systems and Technologies (MSST)*, 2010, pp. 1–10, doi: 10.1109/MSST.2010.5496972.

[11] M. Armbrust et al., "Spark SQL: Relational Data Processing in Spark," in *Proc. 2015 ACM SIGMOD Int. Conf.
Management of Data*, 2015, pp. 1383–1394, doi: 10.1145/2723372.2742797.

[12] T. Akidau et al., "The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in
Massive-Scale, Unbounded, Out-of-Order Data Processing," *Proc. VLDB Endowment*, vol. 8, no. 12, pp. 1792–1803, 2015,
doi: 10.14778/2824032.2824076.

[13] G. M. Amdahl, "Validity of the Single Processor Approach to Achieving Large Scale Computing Capabilities," in
*Proc. AFIPS Spring Joint Computer Conference*, 1967, pp. 483–485, doi: 10.1145/1465482.1465560.

[14] M. Zaharia et al., "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing,"
in *Proc. 9th USENIX Symp. Networked Systems Design and Implementation (NSDI '12)*, 2012, pp. 15–28.

[15] J. Kreps, "Questioning the Lambda Architecture," O'Reilly Radar, 2014.
