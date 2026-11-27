# Data contract v1.0.0 (TV1 quản lý)

Nguồn sự thật duy nhất: [`wq/contracts.py`](../wq/contracts.py). Tài liệu này giải thích cho cả nhóm.
**Muốn thay đổi contract**: mở Pull Request sửa `wq/contracts.py` + tài liệu này, cả nhóm đồng ý,
tăng `CONTRACT_VERSION`, chạy lại `pytest`.

## 1. Biến đo (cột chuẩn)

| Cột | USGS code | Đơn vị | Phạm vi hợp lệ (QC) | Vai trò |
|---|---|---|---|---|
| `water_temp` | 00010 | °C | −5 … 45 | core |
| `specific_conductance` | 00095 | µS/cm @25°C | 0 … 100 000 | core |
| `ph` | 00400 | pH | 0 … 14 | core |
| `dissolved_oxygen` | 00300 | mg/L | 0 … 25 | core, **target dự báo** |
| `turbidity` | 63680 | FNU | 0 … 4 000 | core |
| `discharge` | 00060 | ft³/s | −1e6 … 5e6 | phụ trợ (3/10 trạm không có) |

Giá trị ngoài phạm vi ⇒ đặt NULL và bật bit tương ứng trong `qc_flags` (bit i = biến thứ i theo thứ tự bảng trên).
`imputed_mask` dùng cùng quy ước bit.

## 2. Định dạng DataFrame chuẩn (pandas) cho TV3/TV4

```
station_id: str | ts: datetime64[ns, UTC] (lưới đều 15 phút) | 6 cột biến: float64 (NaN = thiếu)
```

- Luôn **UTC**. Dùng `wq.timeseries.regularize(df)` để đưa về lưới đều (mốc thiếu ⇒ dòng NaN).
- Đọc dữ liệu silver (data lake local hoặc HDFS đều dùng một hàm):

```python
from wq.io import read_silver
from wq.timeseries import time_split
df = read_silver(stations="core")              # 10 trạm chuẩn, ~1,05 triệu dòng
train, valid, test = (time_split(df, k) for k in ("train", "valid", "test"))
```

- Hai bộ trạm: `config/stations.json` (10 trạm chuẩn — mọi thí nghiệm ML báo cáo trên bộ này) và
  `config/stations_large.json` (235 trạm — chế độ Big Data, chứa luôn 10 trạm chuẩn).

## 3. Chia dữ liệu theo thời gian (bắt buộc dùng chung)

| Split | Khoảng (nửa mở) | Dùng cho |
|---|---|---|
| train | 2023-01-01 → 2025-01-01 | huấn luyện |
| valid | 2025-01-01 → 2025-07-01 | chọn siêu tham số |
| test | 2025-07-01 → 2026-01-01 | báo cáo kết quả cuối + **streaming demo phát lại** |

Không xáo trộn ngẫu nhiên; không dùng thống kê của valid/test khi fit (chuẩn hóa, imputer…).

## 4. Kafka message (`wq.sensor.raw`, 6 partition, key = `station_id`)

```json
{"station_id":"USGS-05586300","event_time":"2025-07-01T00:15:00Z","water_temp":29.1,
 "specific_conductance":719.0,"ph":8.4,"dissolved_oxygen":9.0,"turbidity":28.05,"discharge":null,
 "producer_ts":1795551843255,"seq":12345}
```

- `event_time`: ISO-8601 UTC, thời điểm đo. `producer_ts`: epoch ms lúc gửi (để đo độ trễ). `null` = cảm biến thiếu.
- Mốc mà mọi cảm biến đều thiếu **không** được gửi (giả lập trạm mất kết nối).

## 5. Data lake (Parquet, phân vùng theo `station_id`)

| Đường dẫn | Schema |
|---|---|
| `data/raw/usgs/station_id=*/parameter_code=*/YYYY.csv` | time, value, approval_status, qualifier |
| `data/lake/bronze/usgs_long/` | station_id, parameter_code, variable, ts, value, approval_status, qualifier |
| `data/lake/silver/measurements/` | station_id, ts, 6 biến, qc_flags |
| `data/lake/gold/missingness/` | station_id, n_slots, `<biến>_missing_rate` |
| `data/lake/stream/measurements/` (streaming) | station_id, event_date, event_time, 6 biến, qc_flags, producer_ts, seq, kafka_ts |

## 6. Interface mô hình (`wq/models/base.py`)

```python
class BaseImputer:
    name: str; causal: bool          # causal=True mới được dùng trong streaming
    def fit(self, df) -> self        # df: DataFrame chuẩn (train)
    def transform(self, df) -> df    # cùng shape, NaN đã điền
    def save(self, path)             # joblib

class BaseForecaster:
    name: str; targets: list[str]; horizons_steps: list[int]; min_history_steps: int
    def fit(self, df) -> self
    def predict(self, history) -> df  # cột: station_id,target,issued_at,horizon_min,target_ts,y_pred,model
```

Bàn giao: lưu `models/imputer_<ten>.joblib`, `models/forecaster_<ten>.joblib`; class đặt trong
`wq/models/` để nạp lại được; **chạy `python -m wq.models.validate models/<file>.joblib` phải ĐẠT**;
TV1 bật bằng biến môi trường `WQ_IMPUTER` / `WQ_FORECASTER` (xem `.env.example`).

## 7. Mặc định dự báo & cảnh báo

- Target: `dissolved_oxygen`; horizon: 4, 12, 24, 96 bước = **1h, 3h, 6h, 24h**.
- Luật cảnh báo tuyệt đối (`ALERT_RULES`, dùng trong streaming; TV2 sở hữu, nguồn ghi trong `wq/contracts.py`):
  DO < 5 mg/L (warning, EPA 1986), DO < 2 mg/L (critical, hypoxia), pH < 6,5 hoặc > 9,0 (EPA 1986).
- Nhiệt độ, độ dẫn điện, độ đục không có tiêu chí số chung toàn quốc ⇒ cảnh báo khi vượt **p99 của chính trạm trên
  split train** (`TEMP_ANOM`, `SC_ANOM`, `TURB_ANOM`), và mọi cảnh báo gộp theo **sự kiện kéo dài ≥ 1 giờ**
  (`batch/alerts_tv2.py`; streaming chưa có, xem đặc tả trong PR TV2.2c).
- Chỉ số chất lượng nước: CCME WQI theo ngày, bảng gold `wqi_daily` (`wq/wqi.py`).
