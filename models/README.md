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
| `imputer_kalman.joblib` | TV3 | `KalmanImputer` | ✅ | Fit AR(1) theo trạm và biến trên train 2023–2024. Hạng RMSE trung bình 1,40 trong nhóm causal trên validation. Tạo lại bằng `python -m wq.models.imputers_tv3 --method kalman --out models/imputer_kalman.joblib`. |
| (baseline) `seasonal_naive` | TV1 | `SeasonalNaiveForecaster` | — | y(t+h) = y cùng thời điểm ngày trước |
| `forecaster_ridge_235.csv` (tên `ridge`) | TV4 | `RidgeForecaster` | Có | Fit StandardScaler và Ridge (alpha = 1) trên train 2023–2024 theo trạm và khoảng dự báo. Bản đi kèm có 932 bộ hệ số cho 233 trạm. |

## Ridge cho bộ 235 trạm

Cấu hình `.env.bigdata` nạp LOCF và Ridge cho cả `stream-processor` lẫn API. Tên `ridge` đọc hệ số CSV đi kèm repo. Mỗi hàng chứa hệ số chặn và năm hệ số đã quy đổi về đơn vị DO gốc, tương đương pipeline StandardScaler → Ridge đã chọn trên validation. Đặc trưng gồm DO hiện tại, các độ trễ 1, 3, 24 giờ và DO tại thời điểm đích lùi 24 giờ. Lịch sử đã điền được đặt lên đúng các mốc 15 phút. Ridge cần tối thiểu 97 mốc từ thời điểm phát lùi 24 giờ. Trạm thiếu mô hình hoặc đặc trưng sẽ bỏ qua dự báo tương ứng. [Hệ số dùng cho phục vụ](forecaster_ridge_235.csv) · [Kiểm tra qua Kafka và API](../docs/report/validation_va_test_do.md#kiểm-tra-ridge-trên-luồng-phục-vụ)

Khi HDFS, Kafka, PostgreSQL và Spark đang chạy, áp dụng cấu hình và xem kết quả trong PowerShell:

```powershell
docker compose --env-file .env.bigdata up -d --wait --wait-timeout 120 --no-deps --force-recreate stream-processor api
$base = 'http://127.0.0.1:8000'
$stations = (Invoke-RestMethod "$base/stations") | Where-Object { $_.last_ts }
$r = $null
foreach ($s in $stations) {
    $r = Invoke-RestMethod "$base/stations/$($s.station_id)/forecast" -ErrorAction Stop
    if ($r.predictions.Count -eq 4) {
        $r | Select-Object forecaster, history_points
        $r.predictions | Format-Table station_id, horizon_min, target_ts, y_pred
        break
    }
}
if ($null -eq $r -or $r.predictions.Count -ne 4) {
    Write-Host 'Chưa tìm thấy trạm có đủ lịch sử để dự báo. Kiểm tra producer và stream-processor.'
}
```

Compose đợi đến khi `/health` trả HTTP 200 sau khi truy vấn PostgreSQL thành công. Nếu lệnh hết thời gian chờ, xem `docker compose --env-file .env.bigdata logs --tail 80 api postgres`.

Nếu `/health` trả `ok` và `/stations/{id}/forecast` trả HTTP 404 với “Chưa có dữ liệu cho trạm này”, bảng `measurements` của PostgreSQL chưa có lịch sử cho mã đó. Danh mục 235 trạm và Silver trên HDFS có thể đã đầy đủ. Dùng ví dụ trên để tìm một trạm đang có dữ liệu phục vụ.

Để thử đúng trạm `USGS-01184000` khi watermark của `serving_sink` đã vượt qua tháng 7/2025, nạp lịch sử từ Silver vào PostgreSQL rồi gọi API. Lệnh bổ sung mốc đo còn thiếu và thông số còn trống đủ điều kiện QC của trạm trong khoảng từ 01/07 đến hết 03/07 UTC. Có thể chạy lại với cùng tham số. Trong bộ Silver của nhóm, khoảng này có 288 quan trắc DO. Biến `WQ_LAKE_PATH` trong `.env.bigdata` trỏ tới thư mục gốc `hdfs://namenode:8020/wq/lake`.

```powershell
docker compose --env-file .env.bigdata run --rm --no-deps data-init python -m batch.backfill_station_history --station USGS-01184000 --start 2025-07-01 --end 2025-07-04
$r = Invoke-RestMethod 'http://127.0.0.1:8000/stations/USGS-01184000/forecast' -ErrorAction Stop
$r | Select-Object forecaster, history_points
$r.predictions | Format-Table station_id, horizon_min, target_ts, y_pred
```

Để đo riêng đường đi Kafka → Spark → PostgreSQL trên luồng replay mới, phát dữ liệu theo thứ tự thời gian. `num_input_rows` đếm các bản ghi Spark đọc trước khi lọc theo watermark. Các sự kiện tháng 7 đến sau watermark tháng 12 sẽ bị loại ở bước khử trùng lặp theo event time.

```powershell
docker compose --env-file .env.bigdata run --rm --no-deps producer python -m ingestion.replay_producer --stations USGS-01184000 --start 2025-07-01 --end 2025-07-04 --rate 100
docker compose --env-file .env.bigdata logs --tail 30 stream-processor
```

Endpoint theo yêu cầu dùng lịch sử đã có trong PostgreSQL. Dự báo lưu trong `/predictions/latest` xuất hiện khi streaming xử lý bản ghi mới và đã có đủ lịch sử.

Để huấn luyện lại từ Silver trên HDFS, dùng lệnh bên dưới khi cụm Spark còn lõi trống. Lệnh nạp riêng split train, kiểm tra đủ danh mục, fit phân tán theo trạm và ghi file sau khi fit xong. Đổi `--out` thành `models/forecaster_ridge_235.csv` để tạo lại bản hệ số mặc định. Có thể cấu hình `WQ_FORECASTER=/app/models/forecaster_ridge_235.joblib` nếu dùng bản joblib và tạo lại hai service sau khi thay mô hình.

```powershell
docker compose --env-file .env.bigdata run --rm --no-deps data-init python -m wq.models.forecasters_tv4 --model ridge --stations all --master spark://spark-master:7077 --out models/forecaster_ridge_235.joblib
docker compose --env-file .env.bigdata run --rm --no-deps data-init python -m wq.models.validate models/forecaster_ridge_235.joblib --stations all
```
