# Đánh giá tài liệu tham khảo và bài mẫu 9 điểm

## 1. Hai bài báo tham khảo (đã đối chiếu metadata qua Crossref)

### (1) Kimura et al., KDD 2022 — tài liệu BỔ TRỢ (dự báo luồng dữ liệu)

> T. Kimura, Y. Matsubara, K. Kawabata, Y. Sakurai. **Fast Mining and Forecasting of Co-evolving
> Epidemiological Data Streams.** *Proceedings of the 28th ACM SIGKDD Conference on Knowledge Discovery and
> Data Mining (KDD '22)*, pp. 3157–3167, 2022. DOI: [10.1145/3534678.3539078](https://doi.org/10.1145/3534678.3539078)

- **Nội dung:** khai thác và dự báo nhanh các luồng dữ liệu *dịch tễ học* đồng tiến hóa (co-evolving) —
  **không phải** chất lượng nước. Không được trình bày đây là nghiên cứu về water quality.
- **Dùng thế nào trong đề tài:** làm cơ sở cho *bài toán dự báo trên luồng dữ liệu đồng tiến hóa* — dữ liệu của
  nhóm cũng là nhiều chuỗi (10 trạm × 6 thông số) biến đổi cùng nhau và đến liên tục. Lập luận này hỗ trợ:
  (a) xử lý theo luồng (streaming) thay vì chỉ batch; (b) yêu cầu dự báo *nhanh, tăng dần* khi dữ liệu mới đến
  (mô hình được gọi mỗi micro-batch trong `streaming/stream_processor.py`); (c) khai thác tương quan giữa các chuỗi.
- **Người phụ trách viết:** TV4 (Chương 2, phần dự báo chuỗi thời gian). Cần đọc bản PDF để ghi đúng tên
  phương pháp, giả định mô hình và kết quả thực nghiệm của bài (Crossref không cung cấp abstract).

### (2) Pazo et al., Science of the Total Environment 2024 — CƠ SỞ KHOA HỌC CHÍNH

> M. Pazo, S. Gerassis, M. Araújo, I. Margarida Antunes, X. Rigueira. **Enhancing water quality prediction for
> fluctuating missing data scenarios: A dynamic Bayesian network-based processing system to monitor
> cyanobacteria proliferation.** *Science of The Total Environment*, vol. 927, 172340, 2024.
> DOI: [10.1016/j.scitotenv.2024.172340](https://doi.org/10.1016/j.scitotenv.2024.172340) (PII S0048969724024835)

- **Nội dung (theo tóm tắt nhóm đã đọc):** xử lý dữ liệu thiếu bằng *Dynamic Bayesian Network*, so sánh các
  phương pháp DI, EBDI và SEM, rồi phân tích sự phát triển của vi khuẩn lam; dữ liệu > 2,1 triệu điểm, 9,69% bị
  thiếu. ⚠️ Các con số và tên đầy đủ của DI/EBDI/SEM **cần TV3 đối chiếu lại trong bản PDF** trước khi đưa vào
  báo cáo (Crossref chỉ xác nhận tiêu đề, tác giả, tạp chí, số tập, số bài).
- **Dùng thế nào trong đề tài:** định hướng chính cho TV3 (imputation) và cho câu hỏi nghiên cứu trung tâm:
  *"xử lý dữ liệu thiếu có thực sự cải thiện chất lượng dự báo không?"*.
- **Lưu ý tái lập:** dữ liệu của bài báo chỉ cung cấp theo yêu cầu ⇒ nhóm **không tái lập nguyên thực nghiệm**
  mà áp dụng phương pháp luận trên dữ liệu USGS công khai.

### So sánh quy mô dữ liệu

| | Bài báo (2) | Đề tài (USGS) |
|---|---|---|
| Số điểm dữ liệu | > 2,1 triệu (cần xác nhận) | **5,96 triệu** giá trị thô; 1.052.160 dòng × 6 biến sau ETL |
| Tỉ lệ thiếu | 9,69% (cần xác nhận) | 0,1% – 18,5% tùy trạm/biến (bảng `data/lake/gold/missingness`) |
| Tần suất | (xem bài báo) | 15 phút |

## 2. Bài mẫu BKByte (Hệ phân bố CO3071, 9 điểm) — học gì, làm tốt hơn ở đâu

Bài mẫu là của **môn Hệ phân bố**, không phải Big Data ⇒ học *cách hiện thực và trình bày*, không sao chép
công nghệ (AWS EKS, Milvus, chatbot… không cần cho đề tài này).

| Điểm mạnh của BKByte (trang trong báo cáo) | Áp dụng cho nhóm |
|---|---|
| Sơ đồ kiến trúc tổng thể 4 lớp: Data Processing, Storage, Model Management, Serving (Hình 4.1, tr. 31) | `docs/architecture.md` mục 1, có thêm sơ đồ tuần tự và triển khai |
| Kiến trúc Lambda: Batch layer + Speed layer với Spark (Hình 4.3, tr. 37) | Batch ETL + Structured Streaming dùng chung contract và code QC |
| Streaming có event-time, watermark, window (tr. 41, Listing 4.2) | Watermark 2h, `dropDuplicatesWithinWatermark`, window 1h, **có kiểm chứng bằng dữ liệu trễ/trùng cố ý** |
| Thiết kế CSDL, ETL, backend, frontend, triển khai đầy đủ | PostgreSQL + FastAPI + Streamlit + Docker Compose |
| Phân công theo năng lực, mỗi người một mảng (Phụ lục A, tr. 90–91) | [`team_plan.md`](team_plan.md) |

| Hạn chế BKByte tự nhận | Nhóm làm tốt hơn bằng |
|---|---|
| Đánh giá hiệu năng chủ yếu **giả lập, định tính**, "chưa benchmark chi tiết với công cụ đo tải" (tr. 82–83, 85) | **Benchmark định lượng** có script tái lập: values/s theo kích thước dữ liệu × số core × local/cluster, so với pandas; throughput và độ trễ p50/p95 end-to-end theo tốc độ phát (`benchmarks/`) |
| Chỉ một bộ dữ liệu chuẩn | Dữ liệu cảm biến thật từ 10 trạm, thiếu dữ liệu tự nhiên |
| — | Unit test cho logic Spark (`tests/`), kiểm chứng khôi phục sau sự cố (checkpoint) |

## 3. Kết luận

- Lấy **bài (2) làm cơ sở khoa học chính** (imputation → prediction), **bài (1) làm tài liệu bổ trợ**
  cho dự báo trên luồng dữ liệu đồng tiến hóa.
- Điểm tạo khác biệt để hướng tới 9–10: (1) chứng minh bằng số liệu rằng xử lý dữ liệu thiếu cải thiện dự báo;
  (2) chứng minh hệ thống xử lý được dữ liệu cảm biến liên tục ở quy mô lớn bằng benchmark định lượng —
  đúng chỗ bài mẫu còn yếu.
