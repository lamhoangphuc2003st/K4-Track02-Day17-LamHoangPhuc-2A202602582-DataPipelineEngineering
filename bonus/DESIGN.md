# Bonus B2 — Brainstorm: pipeline dữ liệu cho SmartReceipt VN

**Học viên:** Lâm Hoàng Phúc — 2A202602582

## 1. Bài toán và ràng buộc thực

SmartReceipt VN nhận **ảnh hoá đơn** do người dùng chụp: hoá đơn siêu thị dài, hoá đơn
VAT điện tử in ra, bill quán viết tay. Hệ thống trích xuất dữ liệu có cấu trúc (người
bán, ngày, các dòng hàng, tổng tiền, VAT) bằng một VLM (Gemini Vision), rồi phân loại
chi tiêu (ăn uống, đi lại, mua sắm, …) bằng PhoBERT đã fine-tune.

**Người dùng:** cá nhân quản lý chi tiêu và đội kế toán của doanh nghiệp nhỏ. Kế toán
cần con số đúng đến từng đồng, còn người dùng cá nhân cần kết quả trong vài giây.

**Vì sao khó:**
- Đầu vào rất bẩn: ảnh mờ, nghiêng, bị cắt; tiếng Việt có dấu và viết tắt (“TT”, “CK”,
  “SL”); số tiền có nhiều định dạng (`1.250.000`, `1,250,000đ`, `1tr25`).
- Bước đắt và không tất định là một lời gọi VLM, giống bước LLM ở bonus B1, chỉ khác là
  đầu vào là ảnh.
- Hoá đơn chứa PII: tên, số điện thoại, mã số thuế cá nhân, đôi khi 4 số cuối thẻ.
- Nhãn đúng (category đúng, tổng tiền đúng) đến **muộn**, khi người dùng sửa trên app.
  Đây chính là nguồn dữ liệu cho flywheel.

## 2. Kiến trúc

```
App mobile ──upload──▶ Object store (ảnh gốc, bất biến, key = sha256(ảnh))
     │                        │
     │   event "receipt_uploaded" (Kafka/queue)
     ▼                        ▼
 [Bronze]  receipts_raw: (image_hash, user_id, uploaded_at, device, exif)    — append-only
     │
     ▼  extract (VLM)  — cache key = image_hash + model + prompt_version
 [Silver]  receipt_extractions  1 hàng / (image_hash, model, prompt_version)
           ├─ hợp lệ theo Pydantic + kiểm tra số học ──▶ receipts (1 hàng / receipt_id, MERGE)
           └─ sai schema / tổng ≠ Σ dòng hàng ─────────▶ extraction_quarantine ──▶ hàng chờ duyệt tay
     │
     ▼  classify (PhoBERT, model_version)            user_corrections (CDC từ DB app)
 [Gold]    gold_expense_lines  (theo ngày giao dịch)   ──▶ gold_training_snapshots (versioned, as-of)
           gold_monthly_report (overwrite-partition)  ──▶ eval set cố định (đã decontaminate)
```

## 3. Các câu hỏi then chốt và quyết định

### Q2 — Batch hay streaming?
**Quyết định:** micro-batch theo sự kiện (xử lý ngay khi có ảnh, qua hàng đợi) cho
bước trích xuất; batch hằng đêm cho Gold và báo cáo.
**Đánh đổi:** streaming đúng nghĩa (Flink/Kafka Streams) và hàng đợi + worker. Người dùng
chỉ cần kết quả trong khoảng 5–10 giây, và độ trễ bị chi phối bởi lời gọi VLM (2–4 giây)
chứ không bởi pipeline. Một hàng đợi có retry là đủ, trong khi Flink tăng chi phí vận
hành mà không cải thiện con số người dùng thấy. Báo cáo tháng không cần độ tươi theo
giây, nên chạy batch để rẻ và dễ chạy lại.

### Q4 — Hợp đồng và chất lượng: validate gì trước khi vào Gold?
**Quyết định:** hai lớp kiểm tra. (1) Schema: output VLM bắt buộc là JSON đúng Pydantic
model. (2) Ràng buộc nghiệp vụ: `Σ(số lượng × đơn giá) ≈ tổng trước thuế` (sai số ≤ 1%),
`tổng = trước thuế + VAT`, ngày không ở tương lai và không quá 2 năm. Dòng sai đi vào
`extraction_quarantine` kèm lý do; run không dừng. Khi tỉ lệ quarantine theo ngày vượt
P95 lịch sử thì cảnh báo, vì thường đó là dấu hiệu model hoặc prompt bị đổi.
**Đánh đổi:** kiểm tra nghiêm thì nhiều hoá đơn phải duyệt tay (tốn người), còn kiểm tra
lỏng thì số sai lọt vào báo cáo kế toán. Tôi chọn nghiêm cho tiền vì sai một con số làm
mất niềm tin của người dùng, còn chi phí duyệt tay giảm dần nhờ flywheel.

### Q8 — Failure semantics: chạy lại có idempotent không?
**Quyết định:** mọi bước đều có khoá. Ảnh được định danh bằng `sha256(bytes)`, nên upload
trùng thành no-op. Trích xuất được cache theo `image_hash + model + prompt_version`, nên
chạy lại không tốn tiền VLM; đổi prompt thì cố ý trích xuất lại và lưu song song theo
version. `receipts` dùng MERGE theo `receipt_id`, và bản sửa của người dùng luôn thắng
bản máy (điều kiện theo `updated_at` / LSN của CDC, giống LSN guard trong lab).
**Đánh đổi:** cache tốn dung lượng và làm việc xoá dữ liệu phức tạp hơn (xem Q10), nhưng
chạy lại 100k ảnh không cache sẽ tốn hàng trăm USD tiền VLM, nên cache là bắt buộc.

### Q5 — Train/serve parity và point-in-time
**Quyết định:** training snapshot cho PhoBERT được dựng **as-of** một ngày: chỉ dùng nhãn
người dùng đã sửa **trước** ngày đó, kèm text mà model serve thực sự nhìn thấy (output VLM
của prompt version lúc đó, không phải bản đã được người sửa). Nếu train trên text sạch
mà serve trên text VLM thì model sẽ đẹp offline nhưng kém khi chạy thật.
**Đánh đổi:** snapshot bất biến và có version giúp tái lập được, nhưng tốn chỗ và mâu
thuẫn với yêu cầu xoá. Tôi chấp nhận chi phí lưu trữ vì không tái lập được một model
thì cũng không debug được nó.

### Q10 — Bối cảnh Việt Nam: PII và Nghị định 13/2023
**Quyết định:** che PII ngay ở ranh giới Bronze → Silver (regex cho số điện thoại, MST,
số thẻ; NER tiếng Việt cho tên người). Ảnh gốc được mã hoá theo khoá riêng của từng
user, nên khi người dùng yêu cầu xoá thì chỉ cần hủy khoá (crypto-shredding): ảnh trong
Bronze, cache và các snapshot cũ đều không còn đọc được mà không phải viết lại file.
Chuẩn hoá Unicode NFC trước khi hash và trước khi đưa vào PhoBERT, vì cùng chữ “hoá”
có thể được mã hoá theo hai cách và sẽ làm vỡ cache.
**Đánh đổi:** crypto-shredding cần quản lý khoá (KMS) và phức tạp hơn việc xoá file,
nhưng nó là cách duy nhất làm cho "Bronze bất biến" và "quyền được xoá" cùng đúng.

## 4. Phương án bị loại

**OCR truyền thống (Tesseract/PaddleOCR) + regex/rule để trích xuất.** Phương án này rẻ
và tất định, nhưng với hoá đơn viết tay và hoá đơn siêu thị nhiều cột thì rule vỡ liên
tục. Mỗi mẫu hoá đơn mới cần một rule mới, nên chi phí con người tăng tuyến tính theo
số cửa hàng. VLM với output có cấu trúc, cộng cache theo hash và kiểm tra số học, cho
độ chính xác cao hơn, còn chi phí được kiểm soát nhờ cache. OCR vẫn được giữ làm
**fallback** khi VLM lỗi hoặc vượt ngân sách ngày, kết quả được đánh dấu `low_confidence`.

**Lambda architecture (một nhánh streaming và một nhánh batch tính cùng logic)** cũng bị
loại: hai code path cho cùng một con số sẽ lệch nhau. Tôi chọn một code path duy nhất
(giống `run_day` của lab), còn backfill chỉ là chạy lại code path đó theo từng ngày.

## 5. Prototype gắn với lab

Quyết định cốt lõi ở Q8 (cache theo `hash(input) + model + prompt_version`, output sai
schema đi vào quarantine) đã được hiện thực trong bonus B1 tại `pipeline/llm_label.py`.
Thay `ticket text` bằng `sha256(ảnh)` và `FakeLLM` bằng VLM là có ngay bước trích xuất
của SmartReceipt; `python -m scripts.bonus_llm` chứng minh chạy lại tốn 0 lời gọi và đổi
prompt thì cố ý gắn nhãn lại.
