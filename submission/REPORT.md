# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lê Tuấn Anh / 2A202602952
**Repo:** https://github.com/anhltsefpt/K4-Track02-Day17-LeTuanAnh-2A202602952-DataPipelineEngineering
**Commit bài nộp:** `6a688f3` (sửa lỗi: `3901aec`, `1070cd7`, `1daa472`)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc code, chỉ ra vị trí 3 lỗi, đề xuất bản sửa và giải thích từng bước; soạn nháp REPORT. Tôi đã chạy lại mọi lệnh, đọc và hiểu từng dòng sửa.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17; tài liệu Debezium (định dạng change event), dbt docs (`merge`, `microbatch`).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | *24 rows for 12 tickets*; T-91 có 3 hàng (`low/open` … `high/closed/bug`) | `gold_feature_daily` lệch full recompute (`c50b…` ≠ `8630…`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)`; rerun FAIL | T-97 `is_deleted = False`, còn `u06` + body có tên; còn trong snapshot mới nhất (1 hàng) và RAG index (2 chunk) |
| **Nguyên nhân gốc** | `INSERT` (append), không áp khoá → mỗi batch thêm hàng, chạy lại thêm hàng trùng | `LOOKBACK_DAYS = 0` (giả định event tới ngay); event 08-12 của u05 tới ngày 08-15 nhưng run 08-15 chỉ tính lại partition 08-15 | Khoá đọc từ `after`; delete có `after = null` → khoá NULL → bị `WHERE ticket_id IS NOT NULL` loại |
| **Cách sửa** | `silver.py`: `MERGE … ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `ELSE INSERT` | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) | `staging.py`: khoá lấy từ `key` của Kafka record → MERGE ghi tombstone, Gold lọc `NOT is_deleted` |
| **Khái niệm** | Silver có khoá; MERGE + chặn phiên bản (idempotent) | Data về muộn; event time; lookback = P99 đo từ Bronze | CDC log-based (`before/after/op`); "Xoá phải lan" |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY**

## 3. Lựa chọn công cụ / kỹ thuật

- **MERGE cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:** ticket là thực thể có khoá và phiên bản (LSN) nên MERGE có chặn LSN giữ một hàng và không để batch cũ ghi đè; feature là bảng tổng hợp nên tính lại cả partition từ Silver là cách idempotent đơn giản nhất.
- **Tombstone thay vì xoá hẳn:** giữ `is_deleted` + `_lsn` (PII = NULL) để chạy lại batch cũ không "hồi sinh" T-97; đánh đổi là hàng tombstone nằm lại mãi.
- **Snapshot dựng lại từ Bronze "as of", không sửa bản cũ:** Bronze bất biến nên mỗi `v<ngày>` tái lập được với cùng checksum và không rò dữ liệu tương lai (T-91 vẫn là `low`).
- **DuckDB / dbt thay vì Spark:** vài chục bản ghi/ngày vừa một máy; DuckDB chạy in-process, có `MERGE`; dbt có sẵn `merge`/`microbatch` + test — Spark chỉ thêm chi phí cluster.

## 4. Hai câu hỏi suy ngẫm

1. Quyền được xoá thắng snapshot bất biến. Snapshot chỉ nên lưu `ticket_id` + đặc trưng, văn bản join từ Silver lúc đọc (hoặc mã hoá theo khoá từng user rồi xoá khoá — crypto-shredding); với snapshot cũ, tạo bản redact mới có ghi log, xoá bản gốc, đánh dấu model cần train lại, và xử lý cả Bronze.
2. Đặt chốt NER (vd. Presidio/underthesea) + từ điển tên đã biết ở ranh giới Bronze → Silver, cạnh `mask_pii`; quét lại ở Gold trước khi embed/train, sai thì quarantine. Đo bằng recall/precision trên tập vàng có gán nhãn và một contract đếm tên còn sót (mục tiêu = 0).

## 5. Output (dán nguyên văn)


```text
$ make verify
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

$ make test
..................................                                       [100%]
34 passed in 0.98s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /Users/anh/Desktop/learn-ai/ai-thuc-chien/0.learn/17-day/lab/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
01:28:24  Running with dbt=1.12.5
01:28:24  Registered adapter: duckdb=1.11.0
01:28:24  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
01:28:24  
01:28:24  Concurrency: 1 threads (target='dev')
01:28:24  
01:28:24  1 of 19 START sql view model main.stg_events ................................... [RUN]
01:28:24  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.04s]
01:28:24  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
01:28:24  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.01s]
01:28:24  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
01:28:24  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.06s]
01:28:24  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
01:28:24  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.06s]
01:28:24  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
01:28:24  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.07s]
01:28:24  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
01:28:24  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
01:28:24  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
01:28:24  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
01:28:24  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
01:28:24  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
01:28:24  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
01:28:24  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.01s]
01:28:24  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
01:28:24  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.01s]
01:28:24  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
01:28:24  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
01:28:24  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
01:28:24  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
01:28:24  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
01:28:24  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
01:28:24  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
01:28:24  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
01:28:24  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
01:28:24  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
01:28:24  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
01:28:24  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
01:28:25  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.01s]
01:28:25  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.01s]
01:28:25  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.01s]
01:28:25  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.01s]
01:28:25  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.01s]
01:28:25  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
01:28:25  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.01s]
01:28:25  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.10s]
01:28:25  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
01:28:25  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
01:28:25  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
01:28:25  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
01:28:25  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
01:28:25  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
01:28:25  
01:28:25  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.57 seconds (0.57s).
01:28:25  
01:28:25  Completed successfully
01:28:25  
01:28:25  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

```
