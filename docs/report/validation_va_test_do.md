# Lựa chọn mô hình và đánh giá DO

Bộ mô hình chốt trước khi chạy test gồm **LOCF cho điền khuyết DO trong streaming** và **Ridge cho cả bốn khoảng dự báo 1, 3, 6 và 24 giờ**. Kết quả lựa chọn dùng tập validation tháng 1–6/2025. Tập test tháng 7–12/2025 dùng để báo cáo điểm của các mô hình đã chốt.

## Validation điền khuyết DO

Xét kịch bản che theo đoạn liên tục (`missing_type=block`) ở bốn mức thiếu. Với streaming, chỉ xét phương pháp causal. Tiêu chí chính là RMSE thấp, đối chiếu bằng MAE và độ phủ. RMSE và MAE lấy trung bình qua ba seed 42, 43 và 44. Độ phủ là tổng số điểm được chấm chia cho tổng số điểm bị che qua ba seed.

| Phương pháp | Causal | RMSE 5% | RMSE 10% | RMSE 20% | RMSE 30% |
|---|---|---:|---:|---:|---:|
| **LOCF** | Có | **0,5514** | **0,5666** | **0,6039** | **0,6382** |
| Kalman | Có | 0,7557 | 0,7644 | 0,7746 | 0,7882 |
| Iterative | Có | 1,1259 | 1,2051 | 1,3410 | 1,4515 |
| KNN | Có | 1,2390 | 1,3487 | 1,5058 | 1,6487 |
| Linear | Không | 0,5918 | 0,5758 | 0,5872 | 0,5931 |

Mỗi ô trong bảng sau ghi **MAE (mg/L) / độ phủ**.

| Phương pháp | Thiếu 5% | Thiếu 10% | Thiếu 20% | Thiếu 30% |
|---|---:|---:|---:|---:|
| **LOCF** | 0,2925 / 99,95% | 0,3047 / 99,92% | 0,3166 / 99,95% | 0,3275 / 99,94% |
| Kalman | 0,4002 / 99,95% | 0,4039 / 99,92% | 0,4055 / 99,95% | 0,4052 / 99,94% |
| Iterative | 0,7151 / 99,73% | 0,7751 / 99,44% | 0,8689 / 98,47% | 0,9580 / 96,75% |
| KNN | 0,7917 / 99,73% | 0,8637 / 99,44% | 0,9692 / 98,47% | 1,0733 / 96,75% |
| Linear | 0,2738 / 99,85% | 0,2736 / 99,81% | 0,2729 / 99,86% | 0,2712 / 99,88% |

Trong nhóm causal, LOCF có RMSE và MAE thấp nhất ở cả bốn mức thiếu, với độ phủ bằng Kalman. Linear dùng được trong xử lý offline và đạt RMSE thấp hơn LOCF ở mức thiếu 20% và 30%. Lựa chọn LOCF áp dụng cho DO bị thiếu theo đoạn trong streaming. [CSV validation](../../data/report/imputation-valid.csv) · [Bảng tổng hợp Excel](../../data/report/imputation-valid-do-block.xlsx)

## Validation dự báo DO

Chọn mô hình có RMSE thấp nhất tại mỗi khoảng dự báo và đối chiếu MAE. Mỗi ô dưới đây ghi **RMSE / MAE** theo mg/L. Số trạm và số điểm được chấm giống nhau cho cả ba mô hình ở từng khoảng dự báo. Benchmark tính MAE và RMSE gộp theo số điểm được chấm.

| Khoảng dự báo | Persistence | Seasonal Naive | Ridge | Số trạm | Số điểm chấm |
|---|---:|---:|---:|---:|---:|
| 1 giờ | 0,2253 / 0,1135 | 0,5438 / 0,3261 | **0,1608 / 0,0859** | 233 | 3.282.620 |
| 3 giờ | 0,5270 / 0,2825 | 0,5447 / 0,3267 | **0,2880 / 0,1611** | 233 | 3.278.940 |
| 6 giờ | 0,8626 / 0,4786 | 0,5469 / 0,3274 | **0,4039 / 0,2317** | 233 | 3.274.047 |
| 24 giờ | 0,5445 / 0,3263 | 0,5445 / 0,3263 | **0,5311 / 0,3248** | 233 | 3.249.540 |

Ridge có RMSE và MAE thấp nhất ở cả bốn khoảng.

| Khoảng dự báo | Baseline có RMSE thấp nhất | RMSE baseline (mg/L) | RMSE Ridge (mg/L) | Mức giảm RMSE |
|---|---|---:|---:|---:|
| 1 giờ | Persistence | 0,2253 | 0,1608 | 28,64% |
| 3 giờ | Persistence | 0,5270 | 0,2880 | 45,35% |
| 6 giờ | Seasonal Naive | 0,5469 | 0,4039 | 26,14% |
| 24 giờ | Persistence và Seasonal Naive | 0,5445 | 0,5311 | 2,46% |

Mức giảm được tính trên số liệu gốc trước khi làm tròn. [CSV validation](../../data/report/forecast-valid.csv) · [CSV theo trạm](../../data/report/forecast-valid-stations.csv)

## Bảng điền kết quả test tháng 7–12/2025

Chạy lần lượt hai lệnh trong PowerShell tại thư mục gốc `BigData`, với cụm Spark và HDFS đang hoạt động. Đợi lệnh thứ nhất ghi xong CSV rồi chạy lệnh thứ hai. `--split test` chọn khoảng từ 01/07/2025 đến trước 01/01/2026.

```powershell
docker compose --env-file .env.bigdata run --rm --no-deps data-init python -m benchmarks.bench_imputation_tv3 --stations all --master spark://spark-master:7077 --split test --seeds 42 43 44 --out data/report/imputation-test.csv --per-station-out data/report/imputation-test-stations.csv --plot data/report/imputation-test.png
docker compose --env-file .env.bigdata run --rm --no-deps data-init python -m benchmarks.bench_forecast --master spark://spark-master:7077 --split test --out data/report/forecast-test.csv --per-station-out data/report/forecast-test-stations.csv
Get-ChildItem .\data\report\*test*
```

Giữ nguyên lựa chọn LOCF và Ridge khi điền kết quả vào hai bảng dưới đây.

| Mô hình điền khuyết | Tỷ lệ thiếu theo đoạn | RMSE (mg/L) | MAE (mg/L) | Độ phủ |
|---|---:|---:|---:|---:|
| LOCF | 5% | — | — | — |
| LOCF | 10% | — | — | — |
| LOCF | 20% | — | — | — |
| LOCF | 30% | — | — | — |

Lấy số từ `data/report/imputation-test.csv`, lọc DO, `block` và `locf`, rồi tổng hợp ba seed như validation. Biểu đồ để đưa vào báo cáo nằm tại `data/report/imputation-test.png` sau khi chạy.

| Mô hình dự báo | Khoảng dự báo | RMSE (mg/L) | MAE (mg/L) | Số trạm | Số điểm chấm |
|---|---|---:|---:|---:|---:|
| Ridge | 1 giờ | — | — | — | — |
| Ridge | 3 giờ | — | — | — | — |
| Ridge | 6 giờ | — | — | — | — |
| Ridge | 24 giờ | — | — | — | — |

Lấy bốn dòng `ridge` từ `data/report/forecast-test.csv` tại các horizon 60, 180, 360 và 1440 phút.

## Đánh giá ảnh hưởng của điền khuyết lên dự báo

Validation dự báo hiện tại dùng chuỗi đầu vào được forward-fill tối đa 8 bước 15 phút. Để đo ảnh hưởng của việc đổi phương pháp điền khuyết, cần chạy thí nghiệm giữ nguyên Ridge, các khoảng dự báo, mặt nạ thiếu và các thời điểm chấm điểm, rồi thay đổi riêng phương pháp điền khuyết. [Mã benchmark dự báo](../../wq/models/forecast_experiment.py)
