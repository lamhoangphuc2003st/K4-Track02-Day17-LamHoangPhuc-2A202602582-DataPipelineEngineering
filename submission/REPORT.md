# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lâm Hoàng Phúc / 2A202602582
**Repo:** https://github.com/lamhoangphuc2003st/K4-Track02-Day17-LamHoangPhuc-2A202602582-DataPipelineEngineering
**Commit bài nộp:** `3ac8d40` (sửa 3 lỗi trong `pipeline/` + `submission/checksums.txt`)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc code, chạy baseline, đề xuất và áp dụng ba bản sửa, chạy kiểm chứng, soạn nháp REPORT. Tôi đã review từng dòng thay đổi và giải thích được chúng.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17, tài liệu Debezium PostgreSQL connector (định dạng change event).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 có 3 hàng `low/open`, `high/open`, `high/closed/bug` | verify: feature_daily lệch full recompute (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 có `(2, 0)` thay vì `(5, 1)`; `LOOKBACK_DAYS=0 < 3`; rerun3 FAIL | T-97 vẫn `is_deleted=False`, còn `u06`, subject, body (tên người); 1 hàng trong snapshot mới nhất, 2 chunk trong RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` → mỗi batch thêm hàng mới; dedup chỉ trong batch, không có khoá giữa các batch, không có điều kiện thứ tự | `LOOKBACK_DAYS = 0` là giả định ("event tới trong vài giây") chứ không đo; event u05 (event_time 08-12) tới Bronze 08-15 nhưng run 08-15 chỉ tính lại partition 08-15 | `ticket_changes_sql` lấy `ticket_id` từ `after`; với `op='d'` thì `after = null` → `ticket_id` NULL → bị `WHERE ticket_id IS NOT NULL` lọc bỏ, delete biến mất |
| **Cách sửa** | `pipeline/silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` = ceil(P99 = 3,00 ngày) đo bằng `main.py --lateness`; mỗi run xoá + tính lại `[day−3, day]` theo event date | `pipeline/staging.py`: `coalesce(after->>'ticket_id', before->>'ticket_id')`; tombstone Kafka (`value=null`) vẫn bị bỏ vì `_op IS NULL` |
| **Khái niệm trên slide** | Silver — có khoá; MERGE theo khoá + LSN guard (idempotent, replay batch cũ không ghi đè) | Data về muộn: event time vs ingest time, lookback = P99 đo từ Bronze, overwrite-partition | CDC log-based (`before/after/op`), tombstone, "xoá phải lan" xuống Gold |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 = 0, p95 = 2,90, max = 3, n = 43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f` (C0 = C1 = C2 = C3)
- `make parity`: **PARITY** (silver_tickets `3c15dfd43701`, gold_feature_daily `8630e04a61d1`)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể có khoá và trạng thái thay đổi nên cần upsert một hàng/khoá, LSN guard giúp replay batch 08-12 sau 08-16 thành no-op; feature là aggregate theo ngày nên xoá và tính lại cả partition `[day−3, day]` từ Silver vừa idempotent vừa hấp thụ event muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: hàng `is_deleted = true` (PII = null) giữ lại `_lsn` của delete, nên khi replay một batch cũ (LSN nhỏ hơn) ticket không "hồi sinh"; downstream (training, RAG) lọc theo cờ này. Đánh đổi: tombstone tồn tại mãi (có thể dọn sau thời gian giữ).
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: đảm bảo tái lập được (cùng version → cùng checksum) và point-in-time đúng (priority lúc tạo, feedback đã tới trước ngày đó), tránh rò rỉ tương lai vào dữ liệu huấn luyện.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu vài chục bản ghi/ngày chạy trên một máy, DuckDB đọc Parquet trực tiếp, có MERGE/QUALIFY, zero-infra; dbt cho `merge` + `microbatch` + test/contract khai báo; Spark chỉ thêm chi phí cluster khi chưa có vấn đề quy mô.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs quyền được xoá:** bất biến là bảo đảm kỹ thuật, còn quyền xoá là nghĩa vụ pháp lý nên quyền xoá thắng. Tôi giữ nguyên tắc "không sửa snapshot âm thầm" bằng cách: khi có yêu cầu xoá thì phát hành lại các version bị ảnh hưởng (ví dụ `v2026-08-12-r1`) đã loại T-97, ghi lý do vào nhật ký version, rồi thu hồi/xoá vật lý bản cũ (kể cả Bronze, dùng crypto-shredding theo khoá mã hoá của từng user nếu không thể ghi lại file). Model đã huấn luyện trên snapshot cũ được đánh dấu cần train lại theo lịch. Ngoài ra snapshot chỉ nên lưu `ticket_id` + văn bản đã che, để giảm phạm vi phải xoá.
2. **Chốt PII cho tên người:** đặt chốt ở ranh giới Bronze → Silver (mọi cột free-text rời Bronze đều qua đó), thêm bước NER tiếng Việt (ví dụ underthesea/PhoBERT-NER hoặc Presidio với recognizer tiếng Việt) thay `Nguyễn Văn An` bằng `<PERSON>`, kết hợp danh sách tên đã biết từ bảng user. Đo bằng một tập vàng có gán nhãn PII: recall (ưu tiên, mục tiêu ≥ 0,95) và precision; thêm một contract trong verify quét Silver/Gold bằng detector độc lập và fail run nếu còn PII, đồng thời theo dõi số lần che/ngày để phát hiện drift.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell bằng các lệnh tương đương trong SUBMISSION.md, trên commit `3ac8d40`.

```text
$ .\.venv\Scripts\python.exe -m scripts.verify          # make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest                  # make test
..................................                                       [100%]
34 passed in 3.89s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check     # make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness         # make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\python.exe main.py --land-only; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17   # make dbt
15:12:47  Running with dbt=1.12.5
15:12:47  Registered adapter: duckdb=1.11.0
15:12:51  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
15:12:54  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.15s]
15:12:54  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
15:12:54  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
15:12:54  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.19s]
15:12:54  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.12s]
15:12:54  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.06s]
15:12:55  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
15:12:55  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
15:12:55  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
15:12:55  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
15:12:55  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
15:12:55  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.04s]
15:12:55  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
15:12:55  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
15:12:55  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
15:12:55  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
15:12:55  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.09s]
15:12:55  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.04s]
15:12:55  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.04s]
15:12:55  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
15:12:55  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
15:12:55  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
15:12:55  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.34s]
15:12:55  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
15:12:55  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
15:12:55  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
15:12:55  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 4.15 seconds (4.15s).
15:12:55  Completed successfully
15:12:55  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity          # make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Ghi chú: output dbt ở trên đã lược các dòng `START ... [RUN]` để gọn; mọi dòng kết quả được giữ nguyên.

### Triệu chứng ban đầu (bản seed chưa sửa)

```text
RESULT: 8/18 checks — FAILURES ABOVE
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', ...), (False, 'u06', ...)])
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9 chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build
pytest: 9 failed, 25 passed
```
