# Việc của TV2, TV3, TV4: cần làm gì, đạt thế nào, đang ở đâu

File này là danh sách việc chính thức của **Quang (TV2)**, **Tài (TV3)** và **Nam (TV4)**. Mã việc (TV2.1, TV3.3…)
trùng với sơ đồ pipeline nhóm. Mỗi việc có mục **Kết quả cần đạt**: PR được merge khi đạt đủ các ô của việc đó.
Bảng trạng thái (mục 1) và nhật ký review (mục 8) được cập nhật mỗi khi một PR được merge (quy trình ở mục 6).

Đọc trước: [`team_plan.md`](team_plan.md) (phân công, lịch 4 tuần) · [`data_contract.md`](data_contract.md) (tên cột,
đơn vị, cách chia dữ liệu, interface mô hình) · [`../notebooks/README.md`](../notebooks/README.md).

Cập nhật lần cuối: 25/11/2026.

## 1. Bảng trạng thái

⬜ chưa bắt đầu · 🔄 có PR đang review · 🔧 cần sửa theo review · ⏸ chờ TV1 quyết · ✅ đã merge

| Mã | Việc | Người | Hạn | Chờ việc | Trạng thái | PR |
|---|---|---|---|---|---|---|
| TV2.1 | Dữ liệu chuẩn: kiểm chứng silver, bảng trạm, bảng thiếu dữ liệu | Quang | Cuối tuần 1 | — | ⬜ | |
| TV2.2a | QC nâng cao: qualifier, spike, flatline | Quang | Cuối tuần 2 | TV2.1 | ⬜ | |
| TV2.2b | Phân bố khoảng thiếu (gửi TV3) và EDA | Quang | Giữa tuần 2 | TV2.1 | ⬜ | |
| TV2.2c | WQI và luật cảnh báo có trích nguồn | Quang | Cuối tuần 2 | TV2.1 | ⬜ | |
| TV2.2d | Trang giám sát trên dashboard | Quang | Cuối tuần 2 | TV2.2c | ⬜ | |
| TV2.3 | Bản nháp Ch.3 và mục ETL của Ch.4 | Quang | Giữa tuần 3 | TV2.2 | ⬜ | |
| TV2.4 | Hoàn thiện Ch.3, rà soát số liệu và trích dẫn toàn báo cáo | Quang | Thứ 4 tuần 4 | TV2.3 | ⬜ | |
| TV3.1 | Giao thức che dữ liệu và metric (`wq/evaluation.py`) | Tài | Cuối tuần 1 | — | ⬜ | |
| TV3.2 | Cài và đánh giá ≥ 4 phương pháp điền khuyết | Tài | Cuối tuần 2 | TV3.1 | ⬜ | |
| TV3.3 | Xuất imputer causal tốt nhất | Tài | **Thứ 4 tuần 3** | TV3.2 | ⬜ | |
| TV3.4 | Ch.2: tổng quan điền khuyết và bài báo (2) | Tài | Cuối tuần 3 | — | ⬜ | |
| TV3.5 | Mục 4.x và 5.1: thiết kế và kết quả điền khuyết | Tài | Thứ 4 tuần 4 | TV3.2, TV3.3 | ⬜ | |
| TV4.1 | Giao thức đánh giá dự báo và baseline DO | Nam | Cuối tuần 1 | — | ⬜ | |
| TV4.2 | Đặc trưng thời gian, XGBoost, LSTM | Nam | Cuối tuần 2 | TV4.1 | ⬜ | |
| TV4.3 | Xuất forecaster tốt nhất | Nam | **Thứ 4 tuần 3** | TV4.2 | ⬜ | |
| TV4.4 | Thí nghiệm 3 kịch bản: gốc / còn thiếu / đã điền khuyết | Nam | Cuối tuần 3 | TV3.1, TV3.3, TV4.3 | ⬜ | |
| TV4.5 | Ch.2: dự báo và bài báo (1); mục 4.x và 5.2 | Nam | Thứ 4 tuần 4 | TV4.4 | ⬜ | |

**Đường găng:** TV3.2 → TV3.3 → TV4.4 → TV4.5, và TV4.3 → phần tích hợp của TV1. Imputer (TV3.3) và forecaster (TV4.3)
phải được merge trước **thứ 4 tuần 3**: trễ một ngày ở đây là cả nhóm trễ một ngày. Các việc khác của tuần 1–2 làm song
song được ngay, không phải chờ ai.

## 2. Đã có sẵn từ TV1 (dùng ngay, không làm lại)

| Thứ | Ở đâu | Ai dùng |
|---|---|---|
| Silver 10 trạm chuẩn, lưới 15 phút, UTC, 2023–2025 | `data/lake/silver/measurements`, tạo bằng `make data` rồi `make etl` | cả ba |
| Đọc dữ liệu, chia train / valid / test, đưa về lưới đều | `wq.io.read_silver`, `wq.timeseries.time_split`, `wq.timeseries.regularize` | TV3, TV4 |
| Data contract | `wq/contracts.py`, `docs/data_contract.md` | cả ba |
| Interface mô hình | `wq/models/base.py`: `BaseImputer`, `BaseForecaster`, `PREDICTION_COLUMNS` | TV3, TV4 |
| Baseline | `wq/models/baselines.py`: LOCF, nội suy tuyến tính, persistence, seasonal-naive | TV3, TV4 |
| Kiểm tra mô hình trước khi bàn giao | `python -m wq.models.validate <file .joblib>` | TV3, TV4 |
| Bộ dữ liệu lớn (danh sách 235 trạm, đã tải hơn 100 trạm) trên HDFS và cụm Spark | `docker compose --env-file .env.bigdata ...`, xem `docs/TV1_GUIDE.md` | TV2 |
| Streaming chạy mô hình online | biến `WQ_IMPUTER`, `WQ_FORECASTER` | TV1 tích hợp |
| API (12 endpoint) và dashboard 4 tab | `api/main.py`, `dashboard/app.py` | TV2 thêm trang giám sát |

```python
from wq.io import read_silver
from wq.timeseries import time_split
df = read_silver(stations="core")              # 10 trạm chuẩn, 1.052.160 dòng
train, valid, test = (time_split(df, k) for k in ("train", "valid", "test"))
```

## 3. TV2 · Quang — Dữ liệu, Spark batch, giám sát chất lượng nước

**Sở hữu:** `ingestion/download_usgs.py`, `batch/` (trừ `batch/ingest_hdfs.py`), `wq/wqi.py`, `ALERT_RULES` trong
`wq/contracts.py`, tab giám sát trong `dashboard/app.py`, `notebooks/tv2_*`, `tests/test_*_tv2.py`,
`docs/report/tables/tv2_*`, `docs/images/tv2_*`. **Báo cáo:** Ch.3 và mục ETL của Ch.4.

Môn là Big Data, nên mọi xử lý trên data lake viết bằng **Spark** (PySpark) và lấy đường dẫn từ
`get_settings().lake_path`, để cùng một code chạy được trên đĩa cục bộ lẫn HDFS.

### TV2.1 Dữ liệu chuẩn (cuối tuần 1)

Khung tải và ETL đã chạy. Việc của bạn là nhận phần này, kiểm chứng và lập bảng mô tả dữ liệu cho Ch.3.

Kết quả cần đạt:
- [ ] Chạy `make data` rồi `make etl` trên máy mình; silver có đúng 10 trạm × 105.216 mốc = 1.052.160 dòng (dán số đếm vào PR).
- [ ] Script Spark `batch/data_report_tv2.py` sinh lại được các bảng dưới đây từ data lake, chạy lại ra cùng số.
- [ ] `docs/report/tables/tv2_stations.csv`: mỗi trạm một dòng gồm mã, tên, bang, loại thủy vực (sông, hồ, cửa sông), các biến có đo, lý do chọn.
- [ ] `docs/report/tables/tv2_missingness.csv`: tỉ lệ thiếu theo trạm × biến, tách train / valid / test; hình `docs/images/tv2_missingness.png`.

### TV2.2 QC nâng cao, EDA, WQI, luật cảnh báo (tuần 2, nên tách 4 PR)

**TV2.2a QC nâng cao** (`batch/qc_tv2.py`, gọi trong `to_silver` của `batch/etl_usgs.py`):
- [ ] Thống kê các mã `qualifier` và `approval_status` thực tế trong bronze; loại giá trị có mã báo thiết bị hỏng, đóng băng, bảo trì… Danh sách mã dựa trên dữ liệu thật và có ghi lý do.
- [ ] Spike: lệch khỏi trung vị trượt quá k × MAD, hoặc tốc độ thay đổi vượt ngưỡng vật lý của từng biến. Flatline: cùng một giá trị lặp ≥ N mốc liên tiếp ở biến lẽ ra phải dao động. Ghi giá trị k, N trong code và trong Ch.3.
- [ ] Giá trị bị loại ⇒ đặt NULL và bật bit của biến trong `qc_flags`, đúng contract; không thêm cột vào silver; số dòng silver không đổi.
- [ ] `tests/test_qc_tv2.py`: chuỗi tổng hợp có spike và flatline bị gắn cờ, chuỗi bình thường không bị; trạm này không ảnh hưởng trạm khác.
- [ ] `docs/report/tables/tv2_qc_summary.csv`: số và tỉ lệ giá trị bị loại theo luật × biến.
- [ ] ETL có QC mới vẫn chạy trên bộ lớn (HDFS, cụm Spark); ghi thời gian chạy trước và sau trong PR. Máy không đủ RAM thì ghi trong PR để TV1 chạy.

**TV2.2b Phân bố khoảng thiếu và EDA** (gửi TV3 trước giữa tuần 2):
- [ ] `docs/report/tables/tv2_gap_lengths.csv` với cột `station_id, variable, gap_steps, count` (độ dài các đoạn thiếu liên tiếp) và độ dài lớn nhất của mỗi trạm × biến; hình `docs/images/tv2_gap_lengths.png`.
- [ ] `notebooks/tv2_eda.ipynb`: phân bố từng biến, chu kỳ ngày của DO và nhiệt độ, theo mùa, tương quan giữa các biến; hình lưu thành `docs/images/tv2_*.png`.

**TV2.2c WQI và luật cảnh báo:**
- [ ] `wq/wqi.py`: một chỉ số WQI có trích nguồn, tính từ các biến đang có. Có bản Spark ghi bảng gold `wqi_daily` (`station_id, date, wqi, wqi_class`); `tests/test_wqi_tv2.py` so với giá trị tính tay.
- [ ] Sửa `ALERT_RULES` trong `wq/contracts.py`: mỗi ngưỡng có nguồn, ghi trong comment và trong Ch.3.
- [ ] Giảm cảnh báo nhiễu. Luật hiện tại quá "ồn": trong lượt demo chế độ Big Data có 11.417 cảnh báo cho khoảng 26.666 bản ghi. SC_HIGH chiếm 6.028 cảnh báo ở 11 trạm ven biển (nước lợ vốn có độ dẫn điện cao), DO_LOW 3.475 ở 14 trạm, TEMP_HIGH 2.608 ở 7 trạm. Đề xuất ngưỡng theo loại trạm hoặc chỉ cảnh báo khi vi phạm kéo dài; phần giữ trạng thái trong streaming thì ghi đặc tả trong PR để TV1 làm.
- [ ] `docs/report/tables/tv2_alerts_before_after.csv`: số cảnh báo theo luật trên split test, trước và sau khi tinh chỉnh, tính bằng Spark với `wq.spark_transforms.alert_events`.

**TV2.2d Trang giám sát** (`dashboard/app.py`):
- [ ] WQI theo trạm và theo thời gian (tính bằng `wq.wqi` trên dữ liệu lấy từ API), bảng hoặc bản đồ trạm theo mức WQI, cảnh báo gần nhất, heatmap tỉ lệ thiếu.
- [ ] Dùng API có sẵn (`/stations`, `/measurements/hourly`, `/alerts`); cần endpoint mới thì mô tả trong PR, TV1 thêm vào `api/main.py`.
- [ ] Ảnh chụp `docs/images/tv2_dashboard_monitoring.png` cho báo cáo.

### TV2.3 Bản nháp Ch.3 và mục ETL của Ch.4 (giữa tuần 3)

- [ ] `docs/report/ch3_bai_toan_du_lieu_tv2.md`: bài toán; nguồn USGS và giới hạn tần suất; 10 trạm (bảng từ `tv2_stations.csv`); 6 biến (đơn vị, phạm vi hợp lệ); lưới 15 phút, UTC; cách chia train / valid / test; QC; thống kê thiếu dữ liệu; hình EDA.
- [ ] `docs/report/ch4_etl_tv2.md`: ETL medallion raw → bronze → silver → gold bằng Spark, QC, WQI, các bảng gold; chạy trên HDFS và cụm Spark (tham chiếu mục 4.v của TV1).
- [ ] Mọi số trong hai file khớp CSV trong `docs/report/tables/`; dưới mỗi bảng ghi tên file nguồn.

### TV2.4 Hoàn thiện Ch.3, rà soát toàn báo cáo (thứ 4 tuần 4)

- [ ] Ch.3 hoàn chỉnh, đã sửa theo góp ý.
- [ ] `docs/report/review_tv2.md`: danh sách số, hình, trích dẫn sai lệch trong mọi chương (file, vị trí, số đúng, file nguồn) và người cần sửa.
- [ ] Mọi bảng trong báo cáo trỏ đúng file kết quả; tài liệu tham khảo dùng chung một định dạng.

## 4. TV3 · Tài — Điền khuyết dữ liệu và đánh giá

**Sở hữu:** `wq/evaluation.py`, `wq/models/imputers_tv3.py`, `notebooks/tv3_*`, `tests/test_evaluation.py`,
`tests/test_imputers_tv3.py`, `models/results_imputation.csv`, `docs/report/*_tv3.md`, `docs/images/tv3_*`.
**Báo cáo:** Ch.2 phần điền khuyết (bài báo 2), mục 4.x và 5.1.

Thí nghiệm trên 10 trạm chuẩn và 5 biến core; fit trên train, chọn tham số trên valid, báo cáo trên test.

### TV3.1 Giao thức che dữ liệu và metric (cuối tuần 1)

- [ ] `wq/evaluation.py` có tối thiểu ba hàm:
  - `make_mask(df, rate, kind, *, variables, block_steps, seed) -> DataFrame[bool]`: `kind` là `"random"` hoặc `"block"`; chỉ che ô đang có giá trị; khối không vượt ranh giới trạm.
  - `apply_mask(df, mask) -> DataFrame`: đặt NaN ở các ô bị che.
  - `score_imputation(truth, imputed, mask, variables) -> DataFrame`: MAE, RMSE, R² theo biến, **chỉ trên ô bị che có giá trị thật**.
- [ ] Độ dài khối tạm lấy 1–24 giờ (4–96 mốc); khi có `tv2_gap_lengths.csv` của TV2.2b thì lấy mẫu theo phân bố thật.
- [ ] `tests/test_evaluation.py` kiểm tra: tỉ lệ che đúng (lệch ≤ 0,5 điểm phần trăm); chỉ ô có giá trị bị che; khối liên tục, không vượt trạm; cùng seed ra cùng mask; imputer "hoàn hảo" cho MAE = 0 và R² = 1; sửa ô không bị che không làm đổi điểm.
- [ ] `notebooks/tv3_protocol.ipynb` minh họa giao thức trên dữ liệu thật.

### TV3.2 Ít nhất 4 phương pháp điền khuyết (cuối tuần 2)

- [ ] LOCF và nội suy tuyến tính (có sẵn trong `wq/models/baselines.py`), KNN hoặc MICE (scikit-learn), Kalman hoặc DBN. Code đặt trong `wq/models/imputers_tv3.py`, không chỉ nằm trong notebook.
- [ ] `models/results_imputation.csv` với cột `method, causal, missing_type, missing_rate, variable, seed, n_masked, mae, rmse, r2, fit_s, transform_s`; đủ 4 phương pháp × 4 tỉ lệ (5, 10, 20, 30%) × 2 kiểu thiếu × 5 biến × ít nhất 3 seed.
- [ ] Hình `docs/images/tv3_rmse_by_rate.png`: RMSE theo tỉ lệ thiếu, mỗi phương pháp một đường, tách thiếu ngẫu nhiên và thiếu khối.
- [ ] Thư viện mới (statsmodels, pykalman…) ghim version trong `requirements.txt`; chạy lại ra cùng số.

### TV3.3 Xuất imputer causal tốt nhất (thứ 4 tuần 3, đường găng)

- [ ] Class trong `wq/models/imputers_tv3.py` kế thừa `BaseImputer`, `causal = True`, chỉ dùng dữ liệu quá khứ.
- [ ] Có lệnh tái lập `python -m wq.models.imputers_tv3 --method <tên> --out models/imputer_<tên>.joblib` (fit trên train). Không commit file `.joblib` (thư mục `models/` nằm trong `.gitignore`); bot và TV1 chạy lệnh này để tạo lại.
- [ ] `python -m wq.models.validate models/imputer_<tên>.joblib` cho kết luận **ĐẠT** (dán kết quả vào PR); thời gian ≤ 100 ms/trạm để không làm chậm micro-batch.
- [ ] `tests/test_imputers_tv3.py` dùng fixture `sample_df`: causal, giữ nguyên giá trị đã quan sát, số ô thiếu giảm.
- [ ] Thêm một dòng mô tả vào bảng trong `models/README.md`.

### TV3.4 Ch.2: tổng quan điền khuyết và bài báo (2) (cuối tuần 3, viết lúc nào cũng được)

- [ ] `docs/report/ch2_dien_khuyet_tv3.md`: cơ chế thiếu MCAR / MAR / MNAR; các nhóm phương pháp; vì sao streaming cần imputer causal; tóm tắt Pazo et al., 2024 (bài (2)).
- [ ] Số liệu và tên phương pháp DI / EBDI / SEM của bài (2) đã **đối chiếu với bản PDF** (xem lưu ý trong `docs/01_danh_gia_tai_lieu.md`).

### TV3.5 Mục 4.x và 5.1 (thứ 4 tuần 4)

- [ ] `docs/report/ch4_dien_khuyet_tv3.md`: thiết kế giao thức thí nghiệm và các phương pháp.
- [ ] `docs/report/ch5_dien_khuyet_tv3.md` (mục 5.1): bảng và hình từ `results_imputation.csv`; phương pháp tốt nhất cho từng kiểu và tỉ lệ thiếu; phương pháp được chọn chạy online và lý do.
- [ ] Số cho slide TV3 (RMSE thiếu khối 5, 10, 20, 30% × 4 phương pháp) ghi trong PR; TV1 điền vào slide.

## 5. TV4 · Nam — Dự báo chất lượng nước và đánh giá mô hình

**Sở hữu:** `wq/forecast_eval.py`, `wq/models/forecasters_tv4.py`, `notebooks/tv4_*`, `tests/test_forecast_eval.py`,
`tests/test_forecasters_tv4.py`, `models/results_baseline.csv`, `models/results_forecast.csv`,
`models/results_scenarios.csv`, `docs/report/*_tv4.md`, `docs/images/tv4_*`.
**Báo cáo:** Ch.2 phần dự báo (bài báo 1), mục 4.x và 5.2.

Target là oxy hòa tan (`dissolved_oxygen`); horizon 1, 3, 6 và 24 giờ (4, 12, 24, 96 mốc); 10 trạm chuẩn; chọn tham số
trên valid, báo cáo trên test.

### TV4.1 Giao thức đánh giá và baseline (cuối tuần 1)

- [ ] `wq/forecast_eval.py`: đánh giá cuốn chiếu (rolling origin) trên split test. Mỗi giờ phát một dự báo cho mọi trạm, chỉ dùng lịch sử đến thời điểm phát, so với DO thật tại `target_ts` (bỏ các mốc thật bị thiếu); tính MAE, RMSE (thêm MAPE nếu được) theo horizon.
- [ ] `tests/test_forecast_eval.py`: đổi dữ liệu sau thời điểm phát không làm đổi kết quả; số mốc được đánh giá đúng; một mô hình "biết trước" cho sai số 0 trên dữ liệu tổng hợp.
- [ ] `models/results_baseline.csv` với cột `model, scenario, horizon_min, station_id, n, mae, rmse, mape` (`station_id = ALL` cho dòng tổng hợp), cho persistence và seasonal-naive.

### TV4.2 Đặc trưng thời gian, XGBoost, LSTM (cuối tuần 2)

- [ ] Đặc trưng: lag DO, trung bình và độ lệch chuẩn trượt (1, 6, 24 giờ), giờ trong ngày (sin/cos), ngày trong năm, lag của các biến khác; code trong `wq/models/forecasters_tv4.py`.
- [ ] Mô hình xuất cho streaming chỉ được cần tối đa **192 mốc lịch sử** (48 giờ, `WQ_HISTORY_HOURS=48`). Cần dài hơn thì báo TV1 trước, vì độ trễ streaming sẽ tăng.
- [ ] XGBoost (`xgboost-cpu` đã có trong `requirements.txt`) và LSTM hoặc một mô hình học sâu khác. Thư viện như PyTorch đặt trong `requirements-dl.txt`, không đưa vào image chính, và chỉ chạy offline; nếu LSTM thắng rõ và cần chạy online thì báo TV1.
- [ ] `models/results_forecast.csv` (cùng cột với TV4.1): so với cả hai baseline ở cả 4 horizon. Hình `docs/images/tv4_forecast_<trạm>.png`: dự báo so với thực tế trong 1 tuần.

### TV4.3 Xuất forecaster tốt nhất (thứ 4 tuần 3, đường găng)

- [ ] Class trong `wq/models/forecasters_tv4.py` kế thừa `BaseForecaster`; `targets` và `horizons_steps` đúng contract; `min_history_steps ≤ 192`.
- [ ] `predict()` trả đúng cột `PREDICTION_COLUMNS`. Trạm không có DO hoặc thiếu lịch sử thì trả khung rỗng, không raise lỗi (lỗi kiểu này từng làm dừng cả luồng streaming).
- [ ] Có lệnh tái lập `python -m wq.models.forecasters_tv4 --model <tên> --out models/forecaster_<tên>.joblib`; không commit file `.joblib`.
- [ ] `python -m wq.models.validate models/forecaster_<tên>.joblib` cho kết luận **ĐẠT**, ≤ 100 ms/trạm.
- [ ] `tests/test_forecasters_tv4.py` dùng `sample_df`: đúng cột, đủ horizon, `target_ts = issued_at + horizon`, không có NaN, xử lý được trạm không có DO.
- [ ] Thêm một dòng vào bảng trong `models/README.md`.

### TV4.4 Thí nghiệm 3 kịch bản dữ liệu (cuối tuần 3)

- [ ] Ba kịch bản trên cùng split test, cùng 10 trạm, cùng seed:
  - **Gốc**: dữ liệu test như hiện có (chỉ có khoảng thiếu tự nhiên).
  - **Còn thiếu**: che thêm dữ liệu đầu vào bằng `wq.evaluation.make_mask` của TV3.1 (ít nhất 20% kiểu khối, tốt hơn là 10, 20 và 30%), không điền.
  - **Đã điền khuyết**: cùng dữ liệu "còn thiếu", điền bằng imputer của TV3.3 trước khi dự báo.
- [ ] Luôn chấm trên giá trị thật không bị che.
- [ ] `models/results_scenarios.csv` với cột `model, scenario, missing_type, missing_rate, horizon_min, station_id, n, mae, rmse`.
- [ ] Trả lời bằng số: điền khuyết có giúp dự báo tốt hơn không, và ở horizon nào.

### TV4.5 Ch.2 phần dự báo, mục 4.x và 5.2 (thứ 4 tuần 4)

- [ ] `docs/report/ch2_du_bao_tv4.md`: baseline, mô hình cây, LSTM; bài (1) Kimura et al., KDD 2022. Bài này nghiên cứu luồng dữ liệu **dịch tễ**, không phải chất lượng nước: chỉ dùng làm cơ sở cho bài toán dự báo nhiều chuỗi đồng tiến hóa trên luồng dữ liệu.
- [ ] `docs/report/ch4_du_bao_tv4.md` (thiết kế đặc trưng, huấn luyện, xuất mô hình) và `docs/report/ch5_du_bao_tv4.md` (mục 5.2: bảng TV4.2, bảng 3 kịch bản, biểu đồ dự báo).
- [ ] Số cho slide TV4 (RMSE của mô hình tốt nhất × 3 kịch bản × 4 horizon, kèm seasonal-naive) ghi trong PR; TV1 điền vào slide.

## 6. Nộp code và review tự động

### 6.1 Một lần khi bắt đầu

1. Gửi TV1 username GitHub để được mời làm collaborator, rồi chấp nhận lời mời (qua email hoặc github.com/notifications).
2. Clone repo và chạy thử. Nhánh mặc định là `thang/TV1`:

```bash
git clone https://github.com/huynhquocthang1805/BigData.git
```

```bash
cd BigData && docker compose build && make test
```

Toàn bộ test phải pass (hiện có 25). Windows không có `make`: chạy lệnh tương ứng trong `Makefile` bằng Git Bash hoặc WSL.

### 6.2 Mỗi việc một nhánh, một PR

```bash
git checkout thang/TV1 && git pull && git checkout -b tv3/3.1-giao-thuc-che
```

Code và viết test, chạy `make test`, rồi đẩy nhánh lên:

```bash
git push -u origin tv3/3.1-giao-thuc-che
```

- Mở PR vào **`thang/TV1`**. Tiêu đề bắt đầu bằng mã việc, ví dụ `[TV3.1] Giao thức che dữ liệu và metric`. Điền mẫu PR (tự hiện khi mở PR).
- Đang làm dở thì mở PR dạng **Draft** (bot bỏ qua Draft); xong thì bấm **Ready for review**.
- Mỗi PR một việc (hoặc một phần a, b, c, d). Nếu nhánh bị xung đột, tự gộp nhánh chính vào nhánh của mình bằng `git pull origin thang/TV1` rồi push lại.
- File dùng chung được phép sửa: `requirements.txt` (thêm thư viện, ghim version), `models/README.md` (thêm dòng), `tests/conftest.py` (thêm fixture, không sửa fixture cũ).

### 6.3 Kiểm tra tự động

1. **CI** (GitHub Actions) chạy `pytest` mỗi lần bạn push vào PR; xem kết quả ở tab Checks của PR.
2. **Bot review** (Claude Code, thay mặt TV1) xem các PR đang mở lúc 7:40, 10:40, 13:40, 16:40, 19:40 và 22:40 mỗi ngày
   (giờ Việt Nam). Bot chạy lại test trong Docker; với PR có mô hình, bot tạo lại file `.joblib` bằng lệnh tái lập và chạy
   `wq.models.validate` trên dữ liệu thật; rồi đối chiếu với mục "Kết quả cần đạt" của mã việc.
3. **Ổn** thì bot merge (merge commit, giữ nguyên các commit và tên tác giả của bạn), đánh dấu các ô đã đạt, cập nhật bảng
   trạng thái và nhật ký trong file này. **Chưa ổn** thì bot để lại review liệt kê từng điểm cần sửa; bạn push tiếp lên
   cùng nhánh, bot sẽ kiểm tra lại. Bot không push vào nhánh của bạn.

### 6.4 Những trường hợp bot không merge mà chuyển TV1 quyết

- CI hoặc test lỗi; xóa, skip hoặc nới lỏng test cũ để test qua.
- Sửa file ngoài phạm vi sở hữu mà không giải thích trong PR; sửa `streaming/`, `docker-compose.yml`, `docker/`, `.github/`,
  hoặc sửa chính file này (bot cập nhật file này; muốn đổi tiêu chí thì ghi trong PR để TV1 quyết).
- Đổi data contract (`wq/contracts.py`, `docs/data_contract.md`), trừ phần `ALERT_RULES` của TV2.
- Commit dữ liệu (`data/`), file `.joblib`, file lớn hơn 5 MB, notebook kèm output nặng, hoặc bí mật (`.env`, token, mật khẩu).
- Rò rỉ dữ liệu: fit trên valid hoặc test; chấm điền khuyết trên ô không bị che; mô hình causal dùng dữ liệu tương lai.
- Số trong báo cáo không khớp file CSV; ngưỡng hoặc luật không có nguồn; thư viện mới chưa ghim version.
- PR không vào `thang/TV1`, hoặc đến từ tài khoản ngoài nhóm.

## 7. Quy tắc chung

- Đọc dữ liệu bằng `wq.io.read_silver`, chia bằng `wq.timeseries.time_split`; không xáo trộn ngẫu nhiên.
- Cố định seed; mỗi bảng kết quả sinh lại được bằng một lệnh ghi trong PR.
- Logic đặt trong file `.py` có test; notebook chỉ gọi hàm và vẽ hình, xóa output lớn trước khi commit.
- Được commit: code, test, notebook gọn, CSV kết quả (`models/*.csv`, `docs/report/tables/*.csv`), hình PNG dưới 1 MB trong `docs/images/`.
- Báo cáo viết bằng Markdown tiếng Việt trong `docs/report/`, hình và bảng đánh số tạm (TV1 đánh số lại khi ghép); dưới mỗi bảng ghi file nguồn.
- Slide do TV1 giữ; số liệu cho slide ghi trong PR, TV1 cập nhật.

## 8. Nhật ký review

| Thời gian | PR | Mã việc | Kết quả | Ghi chú |
|---|---|---|---|---|
| — | — | — | — | Chưa có PR nào |
