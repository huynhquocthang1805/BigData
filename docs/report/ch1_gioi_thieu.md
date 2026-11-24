# Chương 1. Giới thiệu (bản nháp — TV1)

> Ghi chú cho nhóm: các con số trong ngoặc vuông `[...]` cần cập nhật bằng kết quả cuối cùng.
> Trích dẫn theo danh sách tài liệu ở cuối chương.

## 1.1 Bối cảnh

Chất lượng nước mặt là chỉ báo trực tiếp về sức khỏe hệ sinh thái và an toàn nguồn nước sinh hoạt. Các
mạng lưới quan trắc hiện đại sử dụng cảm biến đo tự động (nhiệt độ nước, độ dẫn điện, pH, oxy hòa tan, độ đục…)
với chu kỳ vài phút đến 15 phút, liên tục nhiều năm và tại nhiều trạm. Dữ liệu sinh ra vì vậy mang đặc trưng
của dữ liệu lớn: **khối lượng** tích lũy nhanh, **tốc độ** đến liên tục, và **độ tin cậy** không đồng đều do
cảm biến bị bám bẩn, mất nguồn, lỗi truyền dẫn hoặc bảo trì — dẫn tới **dữ liệu thiếu** với tỉ lệ thay đổi
theo thời gian.

Dữ liệu thiếu ảnh hưởng trực tiếp đến các tác vụ phía sau: một mô hình dự báo oxy hòa tan (DO) — chỉ số then
chốt để cảnh báo tình trạng thiếu oxy hay hiện tượng tảo/vi khuẩn lam nở hoa — sẽ kém tin cậy nếu chuỗi đầu vào
bị đứt quãng. Pazo và cộng sự [2] cho thấy việc xử lý dữ liệu thiếu trong các kịch bản thiếu biến động bằng
mạng Bayes động (Dynamic Bayesian Network) giúp cải thiện khả năng dự báo và giám sát sự phát triển của vi
khuẩn lam. Ở góc độ luồng dữ liệu, Kimura và cộng sự [1] nghiên cứu bài toán khai thác và dự báo nhanh nhiều
luồng dữ liệu đồng tiến hóa (co-evolving data streams) trong lĩnh vực dịch tễ — một cấu trúc dữ liệu tương tự
mạng lưới nhiều trạm, nhiều thông số quan trắc.

## 1.2 Vấn đề đặt ra

1. **Thu thập và xử lý dữ liệu cảm biến liên tục ở quy mô lớn**: dữ liệu lịch sử nhiều năm (xử lý theo lô) và
   dữ liệu mới đến liên tục (xử lý theo luồng), có dữ liệu đến trễ và trùng lặp.
2. **Dữ liệu thiếu biến động**: tỉ lệ thiếu khác nhau theo trạm, theo thông số và theo thời gian
   (trong dữ liệu của nhóm: từ [0,1]% đến [18,5]%).
3. **Dự báo phục vụ cảnh báo sớm**: dự báo DO ở các tầm 1–24 giờ ngay khi dữ liệu mới đến.
4. **Vận hành ổn định**: hệ thống phải chịu được sự cố mà không mất hay nhân đôi dữ liệu, và có số liệu định
   lượng về hiệu năng.

## 1.3 Mục tiêu

- Xây dựng **pipeline Big Data end-to-end**: thu thập dữ liệu USGS → xử lý batch bằng Apache Spark → truyền dữ liệu
  thời gian thực qua Apache Kafka → xử lý luồng bằng Spark Structured Streaming → lưu trữ (data lake Parquet,
  PostgreSQL) → phục vụ qua REST API và dashboard.
- **Đánh giá các phương pháp điền khuyết** dưới nhiều tỉ lệ và kiểu thiếu dữ liệu (ngẫu nhiên, khối liên tục).
- **Đánh giá mô hình dự báo** trên dữ liệu gốc, dữ liệu còn thiếu và dữ liệu đã điền khuyết, nhằm trả lời câu hỏi:
  *xử lý dữ liệu thiếu có cải thiện chất lượng dự báo hay không?*
- **Đo hiệu năng hệ thống** bằng thực nghiệm định lượng: thời gian và thông lượng xử lý batch khi tăng dữ liệu
  và tài nguyên; thông lượng và độ trễ end-to-end của luồng streaming.

## 1.4 Phạm vi

- Dữ liệu: USGS Water Data API, 10 trạm sông tại Hoa Kỳ, 01/2023–12/2025, chu kỳ 15 phút, 6 thông số
  (nhiệt độ nước, độ dẫn điện riêng, pH, DO, độ đục, lưu lượng) — khoảng [5,96] triệu giá trị đo.
- Hạ tầng: triển khai bằng Docker Compose (Kafka, Spark standalone 1 master + 2 worker, PostgreSQL, FastAPI,
  Streamlit) trên một máy; kiến trúc được thiết kế để chuyển lên cụm nhiều máy chỉ bằng thay đổi cấu hình.
- Dữ liệu thời gian thực được **mô phỏng bằng cách phát lại** (replay) giai đoạn kiểm thử 07–12/2025 qua Kafka.

## 1.5 Đóng góp

1. Một hệ thống chạy được hoàn chỉnh bằng một lệnh, kết hợp batch và streaming trên cùng data contract.
2. Luồng streaming có xử lý event-time, watermark, khử trùng lặp, chịu lỗi nhờ checkpoint và ghi idempotent, kèm
   điền khuyết và dự báo trực tuyến phân tán theo trạm.
3. Bộ thực nghiệm so sánh phương pháp điền khuyết và mô hình dự báo theo quy trình chia dữ liệu theo thời gian,
   chống rò rỉ thông tin.
4. Bộ benchmark hiệu năng có thể tái lập (script + dữ liệu đầu ra), khắc phục hạn chế "đánh giá định tính"
   thường gặp ở các hệ thống tương tự.

## 1.6 Cấu trúc báo cáo

Chương 2 trình bày cơ sở lý thuyết và các công trình liên quan. Chương 3 mô tả bài toán và dữ liệu. Chương 4 trình
bày thiết kế và hiện thực hệ thống. Chương 5 trình bày thực nghiệm và kết quả. Chương 6 kết luận, nêu hạn chế và
hướng phát triển.

## Tài liệu tham khảo (dùng chung cho báo cáo)

[1] T. Kimura, Y. Matsubara, K. Kawabata, Y. Sakurai, "Fast Mining and Forecasting of Co-evolving Epidemiological
Data Streams," in *Proc. 28th ACM SIGKDD Conf. Knowledge Discovery and Data Mining (KDD '22)*, 2022, pp. 3157–3167,
doi: 10.1145/3534678.3539078.

[2] M. Pazo, S. Gerassis, M. Araújo, I. M. Antunes, X. Rigueira, "Enhancing water quality prediction for fluctuating
missing data scenarios: A dynamic Bayesian network-based processing system to monitor cyanobacteria proliferation,"
*Science of The Total Environment*, vol. 927, p. 172340, 2024, doi: 10.1016/j.scitotenv.2024.172340.

[3] M. Zaharia et al., "Apache Spark: A Unified Engine for Big Data Processing," *Communications of the ACM*,
vol. 59, no. 11, pp. 56–65, 2016, doi: 10.1145/2934664.

[4] M. Armbrust et al., "Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark," in
*Proc. 2018 Int. Conf. Management of Data (SIGMOD '18)*, 2018, pp. 601–613, doi: 10.1145/3183713.3190664.

[5] J. Kreps, N. Narkhede, J. Rao, "Kafka: A Distributed Messaging System for Log Processing," in *Proc. NetDB
Workshop*, 2011.

[6] N. Marz, J. Warren, *Big Data: Principles and Best Practices of Scalable Realtime Data Systems*. Manning, 2015.

[7] U.S. Geological Survey, "USGS Water Data APIs (OGC API – Features), collection *continuous*,"
https://api.waterdata.usgs.gov/ (truy cập 11/2026).

[8] U.S. Environmental Protection Agency, *Quality Criteria for Water 1986* (EPA 440/5-86-001), 1986.
