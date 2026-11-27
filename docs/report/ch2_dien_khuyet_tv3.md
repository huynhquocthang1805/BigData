# Chương 2. Cơ sở lý thuyết về điền khuyết dữ liệu quan trắc

## 2.x Cơ chế dữ liệu thiếu

Điền khuyết là ước lượng giá trị chưa quan sát được từ thông tin sẵn có. Trong dữ liệu quan trắc chất lượng nước, một ô trống có thể xuất hiện khi cảm biến ngừng đo, gói tin bị mất hoặc giá trị bị loại sau kiểm tra chất lượng. Gọi $Y_{\mathrm{obs}}$ là phần dữ liệu quan sát được, $Y_{\mathrm{mis}}$ là phần chưa quan sát được, và $R$ là chỉ báo thiếu. Cách xác suất của $R$ phụ thuộc vào dữ liệu dẫn đến ba cơ chế sau [TV3-1, TV3-2].

| Cơ chế | Điều kiện | Ví dụ giả định trong quan trắc |
|---|---|---|
| MCAR — Missing Completely At Random | Xác suất thiếu độc lập với cả $Y_{\mathrm{obs}}$ và $Y_{\mathrm{mis}}$. | Một gói tin bị loại ngẫu nhiên, độc lập với trạm, thời gian và số đo. |
| MAR — Missing At Random | Sau khi điều kiện hóa theo $Y_{\mathrm{obs}}$, xác suất thiếu không còn phụ thuộc vào $Y_{\mathrm{mis}}$. | Nếu lịch bảo trì đã được ghi và giải thích toàn bộ phần phụ thuộc của việc thiếu DO, dữ liệu có thể thiếu theo MAR. |
| MNAR — Missing Not At Random | Sau khi điều kiện hóa theo $Y_{\mathrm{obs}}$, xác suất thiếu vẫn phụ thuộc vào thông tin chưa quan sát. | Cảm biến bỏ trống khi DO thực tế xuống dưới giới hạn đo, ngay cả sau khi đã xét các biến được ghi nhận. |

*Nguồn Bảng 2.x.* Rubin (1976) và Pazo và cộng sự (2024), mục 2.3. Các ví dụ là giả định minh họa.

Trong thí nghiệm USGS, **che rải rác** và **che theo khối** mô tả cách tạo khoảng trống. Một đoạn thiếu liên tục có thể phát sinh do sự cố truyền tin hay lịch bảo trì. Hình dạng đoạn thiếu tự nó không xác định MCAR, MAR hay MNAR. Điểm thực nghiệm đo khả năng khôi phục trên những ô vốn có số đo và bị che theo thiết kế của nhóm.

## 2.y Các nhóm phương pháp điền khuyết

- **Dựa trên chuỗi thời gian.** LOCF truyền số đo trước đó về phía trước. Nội suy tuyến tính ước lượng từ hai điểm bao quanh khoảng trống theo khoảng cách thời gian. Bộ lọc Kalman duy trì trạng thái ước lượng và độ bất định, dự báo trạng thái khi gặp ô thiếu và hiệu chỉnh khi có quan sát mới.
- **Dựa trên quan hệ giữa các biến.** KNN dùng các mẫu tham chiếu có số đo tương tự trên những biến cùng quan sát. Chuẩn hóa giúp tránh để biến có thang đo lớn chi phối khoảng cách. Iterative Imputer luân phiên chọn từng biến có ô thiếu và hồi quy biến đó từ các biến còn lại. Trong hệ thống này, mô hình hồi quy là Bayesian Ridge.
- **Dựa trên mạng Bayesian.** Mạng biểu diễn quan hệ xác suất giữa các biến và suy ra phân phối của giá trị thiếu từ quan sát. Một mô hình động có thể biểu diễn quan hệ qua các thời điểm. SEM còn cập nhật cấu trúc mạng trong quá trình học từ dữ liệu thiếu [TV3-2].

## 2.z Nghiên cứu của Pazo và cộng sự

Pazo và cộng sự (2024) nghiên cứu chất lượng nước và sự phát triển của vi khuẩn lam tại lưu vực sông Limia, Tây Ban Nha. Nghiên cứu thu thập số đo theo chu kỳ 15 phút, với **2.118.844 điểm dữ liệu**, trong đó **205.316 giá trị thiếu (9,69%)**. Nhóm tác giả so sánh ba cách xử lý trong mạng Bayesian [TV3-2, mục 2.4].

| Phương pháp | Cách xử lý |
|---|---|
| DI — Dynamic Imputation | Gán giá trị khởi tạo rồi cập nhật các ô thiếu khi học cấu trúc mạng. |
| EBDI — Entropy-Based Dynamic Imputation | Dựa vào độ bất định của phân phối có điều kiện để ưu tiên thứ tự suy ra các giá trị thiếu. |
| SEM — Structural Expectation-Maximization | Luân phiên tính kỳ vọng trên phần chưa quan sát theo mô hình hiện tại và tìm cấu trúc mạng phù hợp hơn. |

*Nguồn Bảng 2.y.* Pazo và cộng sự (2024), mục 2.4.1–2.4.3.

Trong phép so sánh của bài báo, SEM đạt kết quả tốt nhất nhìn chung. Tác giả ghi nhận mức cải thiện xấp xỉ 50% ở một số trường hợp. Con số này thuộc dữ liệu và cấu hình của nghiên cứu Limia. Với dữ liệu USGS, đề tài đánh giá các phương pháp bằng một giao thức riêng. Kalman mô hình hóa trạng thái theo thời gian cho từng biến, còn Iterative Imputer dùng hồi quy theo từng biến. Cả hai đều khác với quy trình học cấu trúc mạng của SEM.

## 2.w Điền khuyết trong hệ thống streaming

Tại thời điểm $t$, pipeline trực tuyến chỉ sử dụng tham số đã fit và thông tin có sẵn đến $t$. Nội suy hai phía có thể là baseline offline mạnh vì khai thác cả điểm sau khoảng trống. Để triển khai online, nhóm đánh giá riêng các ứng viên causal. Điều kiện này được kiểm tra bằng cách thay số đo ở các mốc tương lai và xác nhận kết quả ở quá khứ giữ nguyên. Đầu ra cũng cần giữ số đo gốc và đánh dấu ô thật sự được điền.

## Tài liệu tham khảo

- [TV3-1] D. B. Rubin (1976). *Inference and Missing Data*. Biometrika, 63(3), 581–592. [DOI 10.1093/biomet/63.3.581](https://doi.org/10.1093/biomet/63.3.581)
- [TV3-2] M. Pazo, S. Gerassis, M. Araújo, I. Margarida Antunes và X. Rigueira (2024). *Enhancing water quality prediction for fluctuating missing data scenarios*. Science of the Total Environment, 927, 172340. [DOI 10.1016/j.scitotenv.2024.172340](https://doi.org/10.1016/j.scitotenv.2024.172340)
