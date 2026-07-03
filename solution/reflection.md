# Reflection (≤1 page)

Nguyễn Danh Thành - 2A202600581

**Which fault types were hardest to catch, and why?**

Các lỗi khó bắt nhất là lỗi tinh vi trong nhóm data checks, lineage và AI
infrastructure. Contract violation tương đối trực tiếp vì `contract_diff` trả về
vi phạm schema, type hoặc SLA. Ngược lại, nhiều lỗi private nằm rất gần vùng dao
động bình thường, nên chỉ dùng ngưỡng baseline tĩnh sẽ bỏ sót, còn nới ngưỡng quá
mạnh lại tăng false positive. Em dùng baseline làm lớp kiểm tra chính, bổ sung
weak signals cho data như độ lệch row count/mean amount/null rate/staleness và
running stats cho `std_amount`. Với lineage, em kết hợp runtime, upstream,
downstream và orphaned output. Với AI infra, em giữ feature skew khá chặt và dùng
thêm tín hiệu drift/staleness cho embedding khi có đủ bằng chứng.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

Ở bản cuối, em ưu tiên private score hơn việc giữ practice/public tuyệt đối. Mỗi
loại event vẫn chỉ gọi đúng toolkit method tương ứng, nhưng em thêm sampling
thích nghi cho feature và embedding khi budget còn ít và các batch gần đây sạch,
giúp giảm cost overage trên private. Tradeoff là một số rule subtle có thể làm
tăng false positive hoặc bỏ sót vài AI event ở public/practice. Nếu có thêm một
lượt, em sẽ cần thêm clean-stream statistics theo từng event type để tách lỗi
tinh vi khỏi nhiễu tốt hơn mà không phải đánh đổi giữa TPR, FPR và budget nhiều
như hiện tại.
