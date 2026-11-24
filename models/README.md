# Thư mục mô hình bàn giao (không commit file .joblib lớn lên git)

Mỗi mô hình bàn giao gồm:

| Thành phần | Ví dụ |
|---|---|
| Class kế thừa `BaseImputer` / `BaseForecaster` đặt trong `wq/models/` | `wq/models/imputers_tv3.py::KalmanImputer` |
| File mô hình đã fit trên split **train** | `models/imputer_kalman.joblib` |
| Notebook/script tái lập | `notebooks/imputation_kalman.ipynb` |
| Bảng kết quả (CSV) trên split **test** | `models/results_imputation.csv` |
| Mô tả 3–5 dòng ở bảng dưới | — |

Bật mô hình trong hệ thống: đặt `WQ_IMPUTER=/app/models/<file>.joblib` hoặc `WQ_FORECASTER=...` trong `.env`,
rồi `docker compose up -d --force-recreate stream-processor api` (xem `docs/TV1_GUIDE.md` mục 5).

| File | Người làm | Class | causal | Ghi chú (dữ liệu fit, siêu tham số, kết quả chính) |
|---|---|---|---|---|
| (baseline) `locf` | TV1 | `LOCFImputer` | ✅ | ffill ≤ 2h → cùng giờ hôm trước → ffill |
| (baseline) `seasonal_naive` | TV1 | `SeasonalNaiveForecaster` | — | y(t+h) = y cùng thời điểm ngày trước |
