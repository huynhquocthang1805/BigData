# Nhật ký thực hiện TV3 — Điền khuyết và đánh giá

**Phụ trách:** Tài (TV3) · **Cập nhật:** 26/11/2026

Tài liệu ghi lại cách làm, số liệu đã kiểm chứng và tệp nguồn để viết báo cáo cuối kỳ. Cập nhật mục tương ứng sau mỗi đầu việc. Các thay đổi của TV3 đi vào nhánh `TV3`. Khi hoàn thành toàn bộ TV3, mở một PR từ `TV3` vào `thang/TV1`.

## 3.1 Giao thức che dữ liệu và chấm điểm

**Trạng thái:** Hoàn thành, đã kiểm chứng trên toàn bộ tập test của 10 trạm chuẩn.

### Dữ liệu và cách làm

- Dùng `wq.io.read_silver(stations="core", start=..., end=...)` của TV1. Dữ liệu theo lưới **15 phút UTC**. Dùng 5 biến chính từ `CORE_VARIABLES` gồm nhiệt độ nước, độ dẫn điện riêng, pH, oxy hòa tan (DO) và độ đục. Giữ nguyên mã trạm, thời gian, cờ QC và lưu lượng khi che dữ liệu.
- `make_mask` hỗ trợ che ngẫu nhiên và che theo khối, chọn được cột, tỷ lệ, độ dài khối và seed. Số ô che là tỷ lệ yêu cầu nhân với số ô đã quan sát của từng biến, làm tròn đến số nguyên gần nhất. Khối đi theo lưới thời gian trong từng trạm và dừng khi đứt mốc hoặc gặp ô thiếu tự nhiên. Mặc định dùng 4–96 mốc (1–24 giờ). Khối cuối hoặc đoạn quan sát ngắn có thể ngắn hơn để đủ số ô che. Khi TV2 có bảng phân bố khoảng thiếu, dùng bảng đó để hiệu chỉnh kịch bản theo khối.
- `apply_mask` thay các ô được chọn bằng `NaN` trên bản sao. Giữ riêng dữ liệu thật và dùng cùng mask cho tất cả mô hình trong một kịch bản.
- `score_imputation` tính MAE, RMSE và R² theo biến trên các ô bị che có đáp án. `n_masked` đếm số ô có đáp án và `n_scored` đếm số ô mô hình trả kết quả hữu hạn. Đọc độ bao phủ `n_scored / n_masked` cùng với sai số, vì hai mô hình có thể điền được các tập ô khác nhau. Với dưới hai cặp, R² không xác định. Nếu có ít nhất hai cặp và đáp án không đổi, R² bằng 1 khi điền đúng hoàn toàn và 0 trong trường hợp còn lại.

### Kiểm chứng trên dữ liệu thật

- Script tải USGS của nhóm lấy **60 CSV** (10 trạm × 6 thông số), gồm **989.331 bản ghi đo** trong khoảng `[2025-07-01, 2026-01-01)`. Spark ETL tạo **176.640 dòng silver** trong 10 tệp Parquet, đủ 10 trạm.
- Notebook đọc lại dữ liệu qua dataloader của TV1, chạy **8 kịch bản** gồm hai kiểu che × bốn tỷ lệ **5%, 10%, 20%, 30%** với seed **42**. Hai baseline có sẵn là **LOCF** và **Linear Interpolation**. Lượt chạy trả **80 dòng điểm** (8 kịch bản × 2 mô hình × 5 biến), độ bao phủ từ **99,49% đến 100%**, và biểu đồ giá trị DO thật so với giá trị điền.
- Notebook được thực thi từng cell bằng IPython vì môi trường chạy chặn kết nối socket của kernel Jupyter. Notebook lưu trong Git đã xóa output. Dữ liệu USGS và bản notebook đã chạy nằm trong thư mục dữ liệu cục bộ.

Ví dụ minh họa với DO, kiểu che theo khối ở mức 20%. MAE và RMSE tính bằng mg/L.

| Baseline | Ô bị che | Ô được chấm | MAE | RMSE | R² |
|---|---:|---:|---:|---:|---:|
| LOCF | 34.743 | 34.712 | 0,4422 | 0,8458 | 0,9041 |
| Linear Interpolation | 34.743 | 34.692 | 0,3750 | 0,7287 | 0,9288 |

Nguồn bảng là lượt chạy `notebooks/tv3_protocol.ipynb` trên tập test nửa cuối 2025 với seed 42. Đây là số kiểm chứng giao thức. Kết luận so sánh mô hình sẽ dựa trên benchmark nhiều seed ở 3.2.

### Kiểm thử và tệp nguồn

- **48 test của repo đạt**, trong đó 23 ca cho TV3.1. Test bao phủ tỷ lệ che, seed, cột được chọn, ranh giới trạm, mốc bị gián đoạn, ô thiếu sẵn, tính bất biến của dữ liệu gốc, công thức chấm bằng tay và độ bao phủ khi mô hình bỏ sót.
- Mã nguồn [wq/evaluation.py](wq/evaluation.py), kiểm thử [tests/test_evaluation.py](tests/test_evaluation.py) và notebook [notebooks/tv3_protocol.ipynb](notebooks/tv3_protocol.ipynb).
- Để tái lập, sinh silver bằng `ingestion.download_usgs` và `batch.etl_usgs` với `--start 2025-07-01 --end 2026-01-01`, rồi chạy notebook. Đặt `WQ_LAKE_PATH` nếu data lake nằm ngoài đường dẫn mặc định.

## 3.2 Thực nghiệm các phương pháp điền khuyết

**Trạng thái:** Chờ thực hiện. Hai baseline đã được kiểm chứng trong 3.1. Bổ sung KNN, Iterative Imputer và ứng viên Kalman hoặc DBN theo checklist. Chạy nhiều seed và cập nhật bảng CSV, hình RMSE và điều kiện tái lập tại mục này.

## 3.3 Xuất mô hình causal cho streaming

**Trạng thái:** Chờ thực hiện. Sau khi chọn mô hình causal, ghi lại cách fit trên train, lệnh xuất mô hình, kết quả kiểm tra và thời gian xử lý theo trạm.

## 3.4 Tổng quan dữ liệu thiếu và bài báo (2)

**Trạng thái:** Đã đọc và đối chiếu các thuật ngữ, chờ viết Chương 2. Pazo và cộng sự (2024) phân biệt MCAR, MAR, MNAR ở mục 2.3 và so sánh DI, EBDI, SEM ở mục 2.4. Nghiên cứu báo cáo 205.316 giá trị thiếu trên 2.118.844 điểm dữ liệu (9,69%). Các kịch bản che rải rác và theo khối của nhóm mô tả hình dạng khoảng thiếu. Chúng không tự xác định cơ chế thiếu của USGS. [Bài báo gốc](https://doi.org/10.1016/j.scitotenv.2024.172340)

## 3.5 Thiết kế và kết quả điền khuyết trong báo cáo cuối

**Trạng thái:** Chờ kết quả 3.2 và 3.3. Cập nhật nguồn bảng, biểu đồ, mô hình thắng theo kịch bản, lý do chọn mô hình online và số liệu cho slide.
