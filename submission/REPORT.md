# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Xuân Trường / 2A202602761
**Repo:** https://github.com/truongapep/K4-Track02-Day17-NguyenXuanTruong-2A202602761-DataPipelineEngineering
**Commit bài nộp:** fbf7454
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code — hỗ trợ xác định 3 lỗi có chủ đích (staging/silver/config), đối chiếu logic với `dbt_project/`, gợi ý lệnh kiểm tra; mọi dòng sửa do người học tự xác nhận và kiểm chứng bằng `verify`/`rerun`/`parity`.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17 (Bronze/Silver/Gold, CDC log-based, Data về muộn, Chạy lại & Backfill), docs `CHECKPOINTS.md`/`RUBRIC.md`/`RULES.md`.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ, checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `verify`: `silver_tickets` có 24 hàng cho 12 ticket; T-91 có 3 hàng (low/open, high/open, high/closed/bug); `gold_doc_chunks` 22 hàng / 9 chunk; rerun3 FAIL. | `verify`: u05 ngày 08-12 chỉ có (2 events, 1 click, 0 down) thay vì (5, 3, 1); checksum `gold_feature_daily` c50b8851affe khác full recompute 8630e04a61d1; `LOOKBACK_DAYS=0 < 3`. | `verify`: T-97 `is_deleted=False`, còn user/subject/body và 2 hàng trong Silver; còn 1 hàng ở snapshot v2026-08-16 và 2 chunk trong RAG. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ INSERT: không khoá, không dedup trong batch, không guard LSN nên mỗi batch thêm hàng mới và batch cũ chạy lại có thể ghi đè trạng thái mới. | `LOOKBACK_DAYS = 0` nên mỗi run chỉ tính lại đúng ngày `day`; event của 08-12 đến Bronze ngày 08-15 (trễ 3 ngày) không bao giờ được gộp lại vào partition 08-12. | `ticket_changes_sql` lấy `ticket_id` chỉ từ `after`; với `op='d'` thì `after=null` nên `ticket_id` NULL và dòng delete bị `WHERE ticket_id IS NOT NULL` loại bỏ. |
| **Cách sửa** (file, vài dòng) | `silver.py`, `upsert_silver_tickets`: dedup trong batch bằng `QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC) = 1`, rồi `MERGE ON ticket_id`, chỉ UPDATE khi `s._lsn > t._lsn`. | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) đo từ Bronze (p50=0, p95=2.9, p99=3.0, max=3); `build_feature_daily` xoá và ghi lại cửa sổ `[day-3, day]`. | `staging.py`, `ticket_changes_sql`: `coalesce(after->>'ticket_id', before->>'ticket_id')`; Silver ghi tombstone (`is_deleted=true`, PII null); Gold lọc `_op <> 'd'` và `NOT is_deleted`. |
| **Khái niệm trên slide** | *Silver — có khoá*, *Bốn cách viết idempotent*, LSN là thứ tự thật. | *Data về muộn*: event time khác ingest time, lookback = ceil(P99), overwrite-partition. | *CDC log-based* (`op='d'`, `after=null`), *Xoá phải lan*. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

Đánh đổi của lookback: P99 = max = 3 nên không có biên an toàn, event trễ từ 4 ngày trở lên sẽ bị sót; đổi lại mỗi run phải tính lại 4 partition.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể có vòng đời nên cần upsert theo khoá và LSN guard, còn feature là fact theo event time nên tính lại cả cửa sổ lookback là đơn giản và idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khoá và LSN nên replay batch cũ không hồi sinh ticket, đồng thời để Gold lọc và lan thao tác xoá.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: ticket dựng từ Bronze `upto=day`, feedback lấy từ Silver với `_batch_id <= day`, nên tái lập được và đổi nội dung sẽ báo `SnapshotImmutableError`.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu chỉ vài chục dòng, chạy zero-key; dbt chứng minh cùng Bronze cho cùng checksum (parity).

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   Đề chủ đích giữ snapshot cũ bất biến để tái lập; chỉ snapshot mới nhất và RAG index phải loại T-97. Thực tế tôi sẽ đặt retention cho snapshot cũ, khi có yêu cầu xoá thì rebuild có kiểm soát và ghi lineage, đồng thời xử lý cả Bronze và xoay cache embedding. Bất biến phục vụ tái lập, không miễn nghĩa vụ xoá.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

   Giữ Bronze nguyên bản, đặt PII gate ở Silver (trước khi xuống Gold). Thêm NER tiếng Việt: độ tin cậy cao thì che, thấp thì gắn `pii_suspect` để review. Đo bằng precision/recall trên tập gán nhãn (có "Nguyễn Văn An"), theo dõi tỉ lệ `pii_suspect` theo batch, và thêm contract test quét tên người ở Silver/Gold.

## 5. Output (dán nguyên văn)

```text
$ python --version
Python 3.11.6

$ .\.venv\Scripts\python.exe -m scripts.verify
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

$ .\.venv\Scripts\python.exe -m pytest
..................................                                                                                                                                    [100%]
34 passed in 2.41s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
$ Push-Location dbt_project; try { ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
03:27:22  Running with dbt=1.12.5
03:27:22  Registered adapter: duckdb=1.11.0
03:27:23  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:27:23  
03:27:23  Concurrency: 1 threads (target='dev')
03:27:23  
03:27:23  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:27:23  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.06s]
03:27:23  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:27:23  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
03:27:23  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:27:23  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.10s]
03:27:23  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:27:23  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.10s]
03:27:23  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:27:23  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.08s]
03:27:23  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:27:23  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
03:27:23  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:27:23  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
03:27:23  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:27:23  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
03:27:23  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:27:23  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
03:27:23  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:27:23  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
03:27:23  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:27:23  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
03:27:23  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:27:23  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
03:27:23  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:27:23  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
03:27:23  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:27:23  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
03:27:23  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:27:23  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
03:27:23  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:27:23  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:27:23  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.01s]
03:27:23  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:27:23  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.05s]
03:27:23  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:27:23  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
03:27:23  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:27:24  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
03:27:24  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:27:24  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
03:27:24  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:27:24  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
03:27:24  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:27:24  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
03:27:24  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.20s]
03:27:24  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:27:24  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
03:27:24  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:27:24  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
03:27:24  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:27:24  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
03:27:24  
03:27:24  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.95 seconds (0.95s).
03:27:24  
03:27:24  Completed successfully
03:27:24  
03:27:24  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

> Lưu ý PowerShell: `python` trỏ tới Python hệ thống (không có `duckdb`) nên phải dùng `.\.venv\Scripts\python.exe` cho mọi lệnh pipeline; lệnh dbt phải chạy **từ trong `dbt_project/`** (`Push-Location dbt_project; …; Pop-Location`) vì `sources.yml` dùng `../lake/bronze/...` tương đối.

Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.

