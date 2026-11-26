# Chương 4. Thiết kế và hiện thực mô-đun điền khuyết

## 4.x Thiết kế thí nghiệm và tích hợp

### Dữ liệu và phân chia theo thời gian

Mô-đun dùng `read_silver(stations="core")` để đọc lưới 15 phút (UTC) của 10 trạm USGS. Năm biến đánh giá gồm nhiệt độ nước, độ dẫn điện riêng, pH, oxy hòa tan (DO) và độ đục. Mỗi dòng được xác định bằng cặp `station_id`, `ts`.

| Tập | Khoảng thời gian (UTC, cận cuối loại trừ) | Số dòng | Mục đích |
|---|---|---:|---|
| Train | 01/01/2023 đến 01/01/2025 | 701.760 | Fit mô hình và thống kê chuẩn hóa |
| Validation | 01/01/2025 đến 01/07/2025 | 173.760 | So sánh và chọn ứng viên causal |
| Test | 01/07/2025 đến 01/01/2026 | 176.640 | Báo cáo kết quả trên giai đoạn độc lập với train |

*Nguồn Bảng 4.x.* [Data contract](../data_contract.md) và số dòng do [lệnh benchmark](../../benchmarks/bench_imputation_tv3.py) đọc từ `silver`.

### Giao thức che và chấm điểm

1. Tạo mask cho từng biến bằng cách chọn từ các ô **đang có số đo**. Làm tròn số ô cần che từ tỷ lệ 5%, 10%, 20% hoặc 30%. Che ngẫu nhiên chọn ô không hoàn lại. Che theo khối chọn các đoạn liên tiếp trong từng trạm; độ dài mục tiêu là 4–96 mốc. Đoạn ngắn hoặc khối cuối có thể ngắn hơn để đạt số ô yêu cầu. Độ dài này là cấu hình mặc định trong lúc chờ thống kê của TV2.
2. Đặt ô được chọn thành `NaN` trên bản sao, giữ riêng đáp án. Tái sử dụng cùng mask cho cả năm phương pháp trong mỗi tổ hợp kiểu che, tỷ lệ và seed. Lặp với seed 42, 43 và 44. Lưu lượng làm biến phụ trợ ngoài phạm vi chấm.
3. Tính điểm riêng cho từng biến trên tập $S$ gồm các ô bị che có số đo thật và có giá trị dự đoán hữu hạn. Nếu một phương pháp để lại ô trống, số ô chấm giảm và được phản ánh qua độ bao phủ. Ô thiếu tự nhiên không tham gia tính sai số.

Với $e_i = \hat y_i - y_i$, $n=|S|$ và $\bar y$ là trung bình các đáp án trong $S$:

$$
\mathrm{MAE}=\frac{1}{n}\sum_{i\in S}|e_i|,\qquad
\mathrm{RMSE}=\sqrt{\frac{1}{n}\sum_{i\in S}e_i^2},\qquad
R^2=1-\frac{\sum_{i\in S}e_i^2}{\sum_{i\in S}(y_i-\bar y)^2}.
$$

Độ bao phủ bằng `n_scored / n_masked`, trong đó `n_masked` chỉ đếm ô bị che có đáp án hữu hạn. R² để trống nếu có dưới hai cặp chấm được. Với từ hai cặp có đáp án đồng nhất, quy ước R² bằng 1 nếu dự đoán khớp hoàn toàn và bằng 0 nếu có sai số. [Mã chấm điểm](../../wq/evaluation.py)

### Cấu hình các phương pháp

| Phương pháp | Thiết kế thí nghiệm | Dùng khi online |
|---|---|---|
| LOCF | Dùng số đo trước cho khoảng ngắn tối đa 8 mốc, sau đó thử cùng giờ ngày trước rồi tiếp tục truyền giá trị gần nhất. | Có |
| Linear Interpolation | Nội suy theo thời gian giữa hai quan sát cùng trạm. Đầu và cuối chuỗi có thể còn thiếu. | Chỉ dùng offline |
| KNN | Chuẩn hóa theo từng trạm trên train, chọn tối đa 1.024 dòng tham chiếu mỗi trạm bằng seed 42. Dùng 5 láng giềng, trọng số theo khoảng cách trên các biến cùng quan sát. | Causal theo thời gian |
| Iterative Imputer | Chuẩn hóa theo từng trạm trên train, luân phiên hồi quy Bayesian Ridge cho các biến, tối đa 10 vòng. | Causal theo thời gian |
| Kalman | Bộ lọc AR(1) độc lập theo trạm và biến. Ước lượng hệ số từ các cặp train liền kề, dự báo một bước trong khoảng thiếu, cập nhật trạng thái khi có quan sát. | Có |

*Nguồn Bảng 4.y.* [Baseline của TV1](../../wq/models/baselines.py) và [hiện thực TV3](../../wq/models/imputers_tv3.py). Tính causal của KNN và Iterative giả định các biến đồng thời được sử dụng đã có tại thời điểm xử lý. Cả hai giữ trống hàng không có biến đầu vào dùng được.

Các cấu hình được cố định cho lượt so sánh. Để chọn mô hình triển khai, tính RMSE trung bình qua ba seed trên validation, xếp hạng bốn ứng viên causal trong từng tổ hợp biến × kiểu che × tỷ lệ, rồi lấy trung bình các hạng. Xếp hạng riêng theo biến giúp tránh cộng trực tiếp các RMSE khác đơn vị.

### Bàn giao cho streaming

Model đã fit được lưu thành `models/imputer_kalman.joblib` và nạp qua interface `BaseImputer`. Bản triển khai giữ nguyên số đo hiện hữu. Pipeline tính `imputed_mask` theo bit của từng biến, bật bit khi ô thiếu trước xử lý có giá trị sau xử lý. Ô vẫn thiếu giữ bit tắt. Cờ này đi kèm tên phương pháp trong bảng `measurements_imputed`; mask thí nghiệm được giữ trong luồng chấm điểm. [Tích hợp streaming của TV1](ch4_thiet_ke_he_thong_tv1.md#4z-tích-hợp-mô-hình-điền-khuyết-và-dự-báo) · [Lệnh xuất mô hình](../../models/README.md)
