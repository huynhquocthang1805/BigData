# Họp khởi động (TV1.1) — chương trình và biên bản

Thời lượng: 45 phút · Chủ trì: TV1 · Thành phần: TV1, TV2, TV3, TV4 · Tài liệu đọc trước: `README.md`,
`docs/architecture.md`, `docs/data_contract.md`, sơ đồ pipeline nhóm.

## 1. Chương trình

| Phút | Nội dung | Người trình bày | Kết quả cần có |
|---|---|---|---|
| 0–5 | Mục tiêu đề tài, tiêu chí môn **Big Data** (5V, xử lý phân tán, batch + streaming, đo hiệu năng) | TV1 | Cả nhóm hiểu "thế nào là đạt" |
| 5–15 | Demo hệ thống chạy thật: `docker compose up -d` → Kafka → Spark → dashboard; HDFS NameNode UI | TV1 | Mọi người thấy khung đã chạy end-to-end |
| 15–25 | Đọc và chốt **data contract v1.0** (mục 2 bên dưới) | TV1 | Biểu quyết từng quyết định |
| 25–35 | Phân công, đầu ra từng giai đoạn, điểm phụ thuộc (sơ đồ pipeline) | TV1 | Mỗi người nhận rõ việc tuần 1–2 |
| 35–45 | Mỗi người tự chạy checklist mục 3 trên máy mình; ghi lỗi gặp phải | Cả nhóm | 4/4 máy chạy được |

## 2. Các quyết định cần chốt (ghi ✅ / ✏️ sửa)

| # | Quyết định | Đề xuất | Chốt |
|---|---|---|---|
| D1 | Nguồn dữ liệu | USGS Water Data (collection `continuous`), dữ liệu 15 phút | ☐ |
| D2 | Bộ dữ liệu thí nghiệm ML (TV3, TV4) | 10 trạm chuẩn trong `config/stations.json` | ☐ |
| D3 | Bộ dữ liệu quy mô lớn (Big Data, TV1/TV2) | 235 trạm trong `config/stations_large.json`, chạy trên HDFS + cụm Spark | ☐ |
| D4 | Khoảng thời gian, cách chia | 2023–2025; train 2023–2024, valid 2025 H1, test 2025 H2 (không xáo trộn) | ☐ |
| D5 | Biến | 5 biến chất lượng nước (nhiệt độ, độ dẫn, pH, DO, độ đục) + lưu lượng (phụ trợ) | ☐ |
| D6 | Mục tiêu dự báo | DO, horizon 1h / 3h / 6h / 24h | ☐ |
| D7 | Thí nghiệm điền khuyết | Che 5/10/20/30%, ngẫu nhiên và khối liên tục; MAE/RMSE/R² chỉ trên ô bị che | ☐ |
| D8 | Interface mô hình | `BaseImputer.transform()`, `BaseForecaster.predict()`, bàn giao `.joblib`; imputer online phải `causal=True` | ☐ |
| D9 | Kiểm tra mô hình trước bàn giao | `python -m wq.models.validate <file>` phải ĐẠT | ☐ |
| D10 | Hạn bàn giao quan trọng | Imputer (TV3.3) và forecaster (TV4.3): thứ 4 tuần 3 | ☐ |
| D11 | Quy trình Git | Nhánh riêng mỗi người, PR vào `thang/TV1`, bot review thay TV1 theo `docs/TEAM_TASKS.md`, CI và `make test` phải pass | ☐ |
| D12 | Họp định kỳ | 2 lần/tuần, 15 phút | ☐ |

## 3. Checklist trên máy mỗi thành viên

- [ ] Docker Desktop chạy, cấp ≥ 4 CPU, ≥ 8 GB RAM (chế độ Big Data: ≥ 6 CPU, ≥ 12 GB)
- [ ] `git clone` repo và `docker compose build` thành công
- [ ] `docker compose up -d` → mở được http://localhost:8501 và http://localhost:8000/docs
- [ ] `make test` (hoặc lệnh pytest trong Makefile) → 20+ test pass
- [ ] Đọc được dữ liệu silver bằng pandas:

```python
from wq.io import read_silver
df = read_silver(stations="core", start="2023-01-01", end="2025-01-01")
```

## 4. Biên bản

| | |
|---|---|
| Ngày họp | |
| Có mặt | |
| Quyết định đã chốt | D1 … D12 (ghi thay đổi nếu có) |
| Việc phát sinh | |
| Người ghi | TV1 |
