# Notebooks (TV2/TV3/TV4)

Đặt tên: `<tv>_<chủ đề>.ipynb`, ví dụ `tv2_eda_missingness.ipynb`, `tv3_imputation_benchmark.ipynb`,
`tv4_forecast_xgboost.ipynb`. Chạy Jupyter trong container để có đúng môi trường:

```bash
docker compose run --rm --no-deps -p 8888:8888 data-init bash -c "pip install jupyterlab && jupyter lab --ip 0.0.0.0 --allow-root --no-browser"
```

Đọc dữ liệu và chia theo thời gian (bắt buộc dùng chung `wq.contracts.SPLITS`):

```python
import pandas as pd
from wq.timeseries import time_split
df = pd.read_parquet("/app/data/lake/silver/measurements")
df["station_id"] = df["station_id"].astype(str)
train, valid, test = (time_split(df, k) for k in ("train", "valid", "test"))
```

Quy tắc: không fit bất cứ thứ gì trên valid/test; chỉ đánh giá imputation trên giá trị thật bị che đi;
lưu bảng kết quả ra CSV để đưa vào báo cáo.
