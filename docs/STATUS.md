# Status — Phase 0

**As of:** 2026-10-25 · branch `feat/lease-fence` at `f48f84e` · nothing in production

Phase 0 = scaffold + consistency prototype ([design/implementation-plan.md](design/implementation-plan.md),
scenarios in [design/consistency.md §13](design/consistency.md#13-phase-0-prototype--must-pass-before-phase-1)).
Everything below was checked against the code, the tests and `git log` on 2026-10-24.

`prototype/node_side/` is the **reference implementation of Node's side** (Mongo): it proves the
protocol end to end in Python; Node ports it to TypeScript in Phase 2. `src/` never imports it.

## 0. Resume here (read this first after a break or a context compaction)

**Branch:** `feat/lease-fence` (dataservice) · last code commit `f48f84e` · 212 tests (126 unit,
86 integration). Backend design docs: backend branch `docs/data-service-hybrid-edits` (last
`22a20996` = decisions D11–D13, **not pushed**); `docs/design/` here is an identical copy
(synced `170e2bb`) — change both together.

**Scope (2026-10-25, user):** this service handles **data only** and **performance comes first**.
Login, quota, edits, undo, unique-ID guard stay in Node's existing modules; `prototype/node_side/`
is a test-only spec (no new Node logic). **D11** old Mongo forms stay as they are; **D12** new
forms' import (xlsx first), type detection (one classifier per distinct value), `:validate` and
derived fields (computed at read, never stored) live here; **D13** no merge with the analysis
service for now.

**Just done (2026-10-25):** the reader reworked for 10 GB bases (package `storage/reader/`),
hardened after a 55-agent adversarial review (every finding fixed with a test; 12,000 random
cases old-vs-new reader: 0 differences), benchmarked on 10M synthetic rows (§1 Performance).

**Next, in order:**

1. Data-Service-side Phase 0 scenarios (C13, C14, C16, C19, C21, C40, C43 done):
   C25 / C26 → C1 / C36 / C37 → C9 / C12 (§2). Node-side ones (C4, C8, C17, C22–C24, C27, C30, C33, C38) run
   against Node itself.
2. Compaction file by file (keeps row-id clustering when patches exist, bounds memory to one
   file) — §2.
3. Real files (Phase 0.2 / 0.10): **ask the user** for the largest CSV, a `.sav`, an `.rds`, an
   `.RData`, an xlsx with a codebook.
4. Proposed to the user (waiting for a nod): start Node Phase 1 (DataStore abstraction) in
   parallel — first a read-only audit of every Record / Q / M / D caller.

**Run:** Docker Desktop → `docker compose up -d` (Mongo replica set on 27018);
`uv run python -m scripts.install_duckdb_extensions` once (httpfs, aws);
`uv run python -m pytest` (Windows: always `python -m`). Benchmark:
`uv run python probes/bench_reader.py 10000000 --patches 0,100000 [--threads 8 --memory 2GB]`
(builds under `.bench/`, git-ignored). AWS: the backend `.env` testing bucket (`AWS_S3_*`) is
allowed for probes — never print keys, write under `dataservice-probe/<id>/`, clean up.

**Commits:** hooks check `<emoji> <type>: <subject>` (types in `scripts/check_commit_msg.py`);
no Co-Authored-By; print the branch first. Patch scripts: write them as files (shell heredocs
mangle backslashes on this box).

## 1. Built

| What | Where | Commits |
|---|---|---|
| Project: uv on Python 3.12, ruff, strict mypy, ty, import-linter, pytest; git hooks with the backend's commit rules; CI with a Mongo replica set | `pyproject.toml`, `.githooks/`, `.github/workflows/ci.yml` | `5a0b71c`, `e082236`, `53a849d` |
| Service skeleton: FastAPI on Granian, the backend's layers, `/health` | `src/dataservice/main.py`, `modules/health/` | `a15a439` |
| Settings that refuse unsafe configs (`AWS_S3_ALLOW_UNSAFE_RENAME`, short service secret) | `config/settings.py` | `a15a439`, `2513017` |
| Service-token auth on every route (scaffold HMAC header; the design's scoped token comes with the first internal endpoint) | `common/auth.py`, `common/utils/service_token.py` | `a15a439` |
| Node-style response envelope + one `ErrorCode` catalog; deny-by-default errors | `common/responses.py`, `common/errors.py`, `common/error_codes.py` | `13677e2` |
| The one Delta **write** gate: `txn("emr-writer", token)` + metadata (`token`, `ops = opId:attempt,…`, `opTables`, `part`) on append / overwrite / fence / restore | `storage/fenced_commit.py` | `a15a439`, `5828d77`, `071c4cd`, `618f1fd` |
| No DynamoDB lock: S3 conditional writes coordinate commits — C0 on moto in CI, on the real AWS test bucket on demand | `tests/integration/test_c0_concurrent_commits.py`, `probes/c0_real_s3.py` | `484027c`, `a14618e` |
| Writer lease: `LeaseAuthority` contract + reference Mongo store (CAS on the expected previous token, `$$NOW`, publish writes the lease row) | `storage/lease.py`, `prototype/node_side/lease_store.py` | `712b6e4` |
| Fence every table, then reconcile (roll forward the committed prefix, fenced `RESTORE` of the rest) | `storage/reconcile.py` | `071c4cd` |
| The one **reader**: Delta base at its version + Mongo patches `from_seq < seq ≤ to_seq` (rows, row state with `_deleted_at`, cells); architecture test forbids Delta data reads elsewhere | `storage/reader/`, `storage/snapshot.py`, `storage/mongo_patches.py` | `a03f86c`, `8b50d2d`, `618f1fd` |
| **Reader at 10 GB scale**: DuckDB reads only the needed columns of the pinned version's Parquet files (`union_by_name`, typed NULLs for columns no file holds, decoded paths, guards for column mapping / deletion vectors / partitions); `Query{fields, filters, sort, limit, offset, include_deleted}`, `count`, streamed `scan`, `explain`; untouched fields stay plain columns (filter pushdown, late materialization); patched cells + row state in ONE join; two-pass pages (row ids first, then only those rows via a `page_ids` semi join) when patches touch the page or the page is deep; streaming compaction | `storage/reader/{model,sql,read}.py`, `storage/compaction.py` | `c6a9520`, `eaf3620`, `3da3973`, `f48f84e` |
| **Shared DuckDB engine**: one database per process with one budget (`DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, `DUCKDB_TEMP_DIRECTORY`), a cursor per read; S3 secret from storage options or AWS env / role (`credential_chain`), scoped per bucket; extensions installed at image build (`scripts/install_duckdb_extensions.py`), never during a read; requests validated before any I/O | `storage/reader/engine.py`, `config/settings.py`, `main.py` | `3da3973` |
| Synthetic benchmark (10M rows × 50 fields, per-case process, best of 3, peak memory) | `probes/bench_reader.py` | `eaf3620`, `f48f84e` |
| **Hybrid small edits** — reference Node edit transaction: idempotency, revision CAS, patches (with `at`), change log, unique claims, revision manifest, `Form.dataVersion`, event outbox; an in-transaction `check` hook; `batchId` (mutationId) apart from the request's idempotency key | `prototype/node_side/edit_store.py` | `8b50d2d`, `264c4c0`, `f1679c8` |
| **Compaction**: fold a revision's patches into the base (refuses a base rewritten since, a dropped or retyped column); Node publishes it with `fromSeq = W`, the same `dataVersion`, no staleness; patch purge respects pins | `storage/compaction.py`, `prototype/node_side/compaction_store.py` | `618f1fd` |
| **Undo** per cell from `dataset_changes`: preview (entries, losses, excluded, `lossSetHash`), apply as a new small edit (`undo:<batchId>`, redo = undo of the undo), losses re-checked inside the transaction | `prototype/node_side/undo_store.py` | `f1679c8` |
| **Delta-job operations**: `data_operations` with every status move a CAS on opId + status + token + attempt; cancel only before `committed` | `prototype/node_side/operation_store.py` | `3a2b96f` |
| **Publish + writer**: `OperationAuthority` contract; the writer runs `append_rows` (running → fenced commit → committed with its commit record → publish); Node's publish (idempotent, lease row written, rebase on `touchedFieldIds` + row ops, `dataVersion`, staleness for the form's analyses, event) | `storage/operations.py`, `modules/writer/service.py`, `prototype/node_side/publish_store.py`, `prototype/node_side/authority.py` | `ad351a3` |
| **Crash recovery** on takeover: fence + reconcile, rolled-back attempts made runnable again, committed operations rolled forward (published, never re-run); Node's door refuses any writer call whose token is not the live lease | `modules/writer/service.py` (`recover`), `prototype/node_side/authority.py` | `191c255` |
| **Queue drain + poison operations**: only the head (lowest unfinished `seq`) runs; a failing attempt rolls its own commits back and is reported retryable or not (a wrong payload fails at once); Node retries until `max_attempts`, then fails it (C6); the sweeper dispatches operations whose wake-up was lost (C5); a lost lease (`FenceError` at Node's door) stops the drain without touching the operation | `modules/writer/service.py` (`run_operation`, `drain`), `prototype/node_side/operation_store.py` | `0254320` |
| **Entry quota** (spec of Node's side): imports reserve with their operation (no room → `QUOTA_EXCEEDED`, no operation), publish settles (re-validated if released), final failure / cancel release, the sweeper releases only for finished or vanished operations (C7), the heartbeat renews every unfinished reservation; small edits charge inserts / restores and credit deletes; abandoned `running` heads are taken over | `prototype/node_side/quota_store.py`, `operation_store.py`, `authority.py` | `e03afa2` |
| **Old versions**: fenced `vacuum` (token on VACUUM START / END, keeps the files of the versions Node reports pinned, never below the table's own file retention); `retention_configuration(days)` for every new table (log + removed files kept `DELTA_RETENTION_DAYS`, default 60); Settings refuse a retention not longer than `RESTORE_WINDOW_DAYS` (C14, C16) | `storage/fenced_commit.py`, `config/settings.py` | `40f4f95` |
| Library-facts probe (delta-rs / DuckDB), run in CI and on push | `probes/delta_api_probe.py` | `a15a439` |

### Tests (2026-10-25, `f48f84e`, all passing with the local Mongo up)

**199 tests: 117 unit** (architecture 3, commit message 8, compaction stream 2, config 3,
errors 7, fenced commit 7, health 3, mongo patches 4, reader 9, reader delta 7, reader edge
cases 22, reader engine 8, reader query 22, reconcile 12) and **82 integration** (C0 1,
compaction 6, drain 7, edits 7, lease 9, operations 12, publish 9, quota 13, reader on S3 2,
recovery 4, undo 12).

Reviews: the quota work had a 3-lens adversarial review (2 bugs fixed). The reader rework had a
**55-agent adversarial review** (6 lenses, 2 skeptics per finding, a completeness critic): 24
findings, every reachable one fixed with a test; a differential fuzz of the old (`e03afa2`) vs
the new reader over **12,000 random cases** found 0 differences on well-formed input. Every rule
is **mutation-tested** (the reader: 26 of 26 mutants killed).

| Scenario | Test file | Store |
|---|---|---|
| C0 | `test_c0_concurrent_commits.py`, `probes/c0_real_s3.py` | moto S3 / real AWS S3 |
| C2, C2b, C3 (Delta level), C3b | `test_reconcile.py` | local filesystem Delta |
| C3 (full path, both ways), C20 | `test_recovery.py` | Mongo replica set + Delta |
| C10, C11, C35 | `test_undo.py`, `test_edits.py` | Mongo + Delta through the reader |
| C15 | `test_publish.py` | Mongo + Delta |
| C18, C29 | `test_lease.py` | Mongo replica set |
| C5, C6 | `test_drain.py`, `test_operations.py` | Mongo + Delta |
| C7 | `test_quota.py` | Mongo + Delta |
| C28, C31 (jobs), C32 | `test_operations.py` | Mongo replica set |
| C31 (edits), C34, C44 | `test_edits.py` | Mongo + Delta |
| C39, C41, C42 | `test_compaction.py`, `test_compaction_stream.py` | Mongo + Delta, local Delta |
| C13, C19, C40, C43 | `test_reads.py` | Mongo + Delta through the reader |
| C14, C16 | `test_retention.py` | local filesystem Delta |
| C21 (the reader's side: each revision read with its own schema; a schema that does not match its base is refused) | `test_reader_delta.py` | local filesystem Delta |

### Performance (2026-10-25, synthetic 10M rows × 50 fields = 1.1 GB Parquet, 22-core dev box)

`probes/bench_reader.py`; each case in its own process, best of 3; peak = the process's peak
working set. "before" = the first rework (`c6a9520`, one pass, a join per patched field), "now"
= `3da3973` / `f48f84e` with the engine at DuckDB's defaults.

| Case | Patches | Before | Now |
|---|---|---|---|
| Grid page, sorted by a plain field | 0 | 304 ms · 202 MB | 463 ms · 212 MB |
| Grid page, sorted by a plain field | 100k | 2,234 ms · 1,701 MB | 1,030 ms · 315 MB |
| Grid page, sorted by a patched field | 100k | 1,824 ms · 1,709 MB | 819 ms · 340 MB |
| Grid page, filtered (~2%) | 100k | 641 ms · 1,021 MB | 563 ms · 303 MB |
| Grid page, filtered on a patched field | 100k | 1,432 ms · 1,717 MB | 819 ms · 344 MB |
| Deep page (offset 500k) | 0 | 5,079 ms · 2,964 MB | 1,482 ms · 720 MB |
| Count all / filtered | 100k | 317 / 208 ms | 362 / 188 ms |
| Full read, streamed | 100k | 16,012 ms · 2,557 MB | 8,392 ms · 2,163 MB |
| Compaction (fold 100k patches) | 100k | 38,100 ms · 4,107 MB | 42,129 ms · 4,062 MB unordered (`3da3973`) |
| The same 10M revision streamed into a new table (the compaction write, no patches) | 0 | 24.7 s · 3,973 MB unordered | 23.2 s · 2,969 MB ordered (`f48f84e`) |

With `--threads 8 --memory 2GB` pages are ~1.5× slower and compaction 24 s; the memory peak of
a full read / compaction stays ~1.7–3 GB because part of it is outside DuckDB's budget (the
Arrow batches in flight and delta-rs's writer, ~1 GB flat whatever the row count — measured).
Findings that shaped the code: the old reader loaded the whole base; one join per patched field
dominated; delta-rs writes ~1M-row row groups (one per file); unordered streaming scattered row
ids across every row group of a compacted base.

## 2. Not built yet

**Phase 0 scenarios** still to automate (15 of 45): C1, C4, C8, C9, C12, C17, C22–C27, C30,
C33, C36–C38 — the Data-Service-side ones first (C25, C26, C1, C36, C37, C9, C12); C21's Node
side (the `dataset_schemas` snapshot in the publish, restore bringing the schema back) and C27
are Node's publish transaction; Node-side ones run against Node itself once it is built (scope decision, §0).

**Phase 0 features:**

- The BullMQ wake-up (dispatcher) and the sweeper's schedule; a refused publish after `committed`
  (C4: the drain stops and the operation stays at the head); payload encryption / S3 offload; quota reservations;
  `dataset_schemas`; field freeze; group commit for Delta jobs.
- Reader: table and follow-up scopes, the `files` value type, child tables, caches, `_row_id`;
  a patch cache per revision (a revision's patch range never changes — fetch 100k patches from
  Mongo once, not per page; not measured yet).
- **Compaction file by file**: with patches, the edits join (a parallel hash join) breaks the
  base's row order into ~5k-row pieces, so a compacted base loses row-id clustering (correct rows,
  slower `_rid` pruning for two-pass pages). Folding each base file on its own keeps each file's
  id range and bounds memory to one file.
- The vacuum **job** (ask Node for pinned revisions, map them to per-table versions, call
  `fenced_commit.vacuum` per table) and ingest creating tables with `retention_configuration`.
- Design-doc facts to correct (backend + copy together): consistency.md still names DuckDB's
  `delta_scan` (the reader uses core `read_parquet` over the Delta log's files) and says moto
  cannot serve Parquet reads (DuckDB reads moto fine; only delta-rs's DataFusion reads stall).
- Open performance questions for the real-file benchmark: row-group size (delta-rs writes ~1M
  rows per group; 122,880 made no clear difference on synthetic data), `PAGE_ROWS_MAX` /
  `DEEP_OFFSET` thresholds, the engine budget per host.
- Undo of row ops (insert / delete batches); change-log trimming; retention pins + fenced vacuum;
  whole-dataset restore; pinned pagination.
- The production Node client (`LeaseAuthority`, `OperationAuthority` over HTTP) and the design's
  scoped service token ([design/api.md §2](design/api.md#2-internal-api-node--data-service)).
- Query API endpoints, jobs on Lambda / Fargate, AWS infrastructure (plan 0.4), OpenAPI contract
  (0.7), metrics and tracing (0.6), benchmarks on real files (0.10 — needs sample files).

**Phases 1–8:** not started ([design/implementation-plan.md](design/implementation-plan.md)).

## 3. Problems found and how we solved them

| Date | Problem | Solution |
|---|---|---|
| 2026-10-24 | Windows Application Control blocks the `.venv\Scripts\*.exe` launchers and some pyarrow DLLs (`_dataset`, `_compute`); DuckDB's delta extension sometimes | Run tools as `uv run python -m <tool>`; no `pyarrow.compute` in `src/` (compaction checks types instead of casting); CI on Linux runs everything |
| 2026-10-24 | Timezone-aware timestamps need a tz database Windows lacks | `tzdata` dependency |
| 2026-10-24 | MinIO image gone from Docker Hub; Docker Hub CDN EOF | S3 = in-process **moto**; Mongo from the local `mongo:7` image |
| 2026-10-24 | A DynamoDB lock table was planned | Not needed: delta-rs 1.6 commits with S3 conditional writes (C0 on moto and real S3) — dropped |
| 2026-10-24 | moto cannot serve delta-rs Parquet data reads | moto tests assert on the Delta log; read-path tests use local filesystem Delta |
| 2026-10-24 | FastAPI `ORJSONResponse` deprecated; DuckDB 2.0 relations have no `fetchone`; `execute()` → Arrow copies rows | Pydantic return types; `con.execute(...).fetchone()`; `con.sql(...).to_arrow_table()` |
| 2026-10-24 | DuckDB 2.0 wheels only for CPython 3.11 / 3.12; `httpx` stale | Python 3.12; `httpx2` |
| 2026-10-24 | `AT` is a DuckDB keyword (the patch column `at`) | Quoted `"at"` in the reader's SQL |
| 2026-10-24 | DataFusion reads Delta strings as `string_view`; DuckDB names list items `item` | Compaction compares against the Delta table's own schema, list child names ignored |
| 2026-10-24 | A fence commit moves a table's version, so "version unchanged" would refuse every compaction after a takeover | The rebase check counts **data** commits since the base (fence / reconcile commits carry none) |
| 2026-10-24 | The architecture regex matched `set.update(...)` | Narrowed to delta-rs's own arguments |
| 2026-10-24 | Assumed a `RESTORE` lowers the fence (a mutation test proved it wrong) | A `RESTORE` keeps the latest txn; it still carries the token for races and audit |
| 2026-10-24 | `alter.set_table_properties` refused `emr.fence` | `raise_if_not_exists=False`; the fence lives in the txn |
| 2026-10-24 | Edit records first used their own names | Aligned to [design/data-model.md](design/data-model.md) (`264c4c0`); the docs are the spec |
| 2026-10-24 | `batchId` and the idempotency key were one value, so a save in parts could not be undone as one | Separated (`f1679c8`): parts share `batchId` (mutationId), each request has its own key |
| 2026-10-24 | A roll-forward publish was refused: the operation was committed under the old holder's token | The request carries `commitToken`; the lease is checked against the publisher (`191c255`) |
| 2026-10-24 | Node accepted a stale writer's `running` (only the Delta fence stopped it) | Every writer call checks the live lease at Node's door (`191c255`) |
| 2026-10-24 | Compaction would have moved `Form.dataVersion` (needless bulk-download recompute) | `dataVersion` = latest value-changing revision; compaction keeps it |
| 2026-10-25 | A failed attempt that wrote nothing still got a fence commit from the rollback | Roll back only when the attempt has data commits since the manifest |
| 2026-10-25 | The C18 takeover test failed about once in twenty full runs | Windows `time.monotonic()` ticks every ~15 ms (two instants compared equal) → `perf_counter()` (`61156d5`) |
| 2026-10-25 | Review: the poison path in `running()` released a **committed** operation's reservation on a late duplicate call (decided from a stale read) | Single CASes on the current document; release only after a real move to `failed` (`e03afa2`) |
| 2026-10-25 | Review: an operation whose writer died before its first Delta commit stayed `running` forever (reservation held, queue stuck) | A head `running` under an older token is ready for the new holder and is taken over (`e03afa2`) |
| 2026-10-25 | `prototype/node_side/` re-implements Node logic in Python (~1,435 lines) — duplication | Scope decision (§0): no more Node logic here; Node extends its existing modules; this folder is deleted once Node passes the scenario tests |
| 2026-10-25 | The reader loaded the whole base (`select * from base`) — impossible at 10 GB | DuckDB `read_parquet` over the pinned version's files (from the Delta log), projection + filter pushdown, paging / count in DuckDB (`c6a9520`) |
| 2026-10-25 | 10M bench: pages with patches took 1.4–2.2 s / 1.7 GB — one hash join per patched field over every base row, full width | All patched cells + row state in ONE grouped join; two-pass pages (`eaf3620`) |
| 2026-10-25 | Review: a schema-evolved base (column added by merge-append or `add_columns`) crashed every read | `union_by_name` + typed NULL columns from the Delta schema (`3da3973`) |
| 2026-10-25 | Review: delta-rs hands out percent-encoded paths (`%20`), DuckDB found no files | Decoded; glob characters refused (`3da3973`) |
| 2026-10-25 | Review: S3 reads by DuckDB had no credentials under an IAM role (delta-rs had them) | AWS env merged with storage options; `credential_chain` without static keys (`3da3973`) |
| 2026-10-25 | Review: a fresh DuckDB per call = its own 80%-of-RAM budget and all cores each; `INSTALL httpfs` during requests | One shared engine with a Settings budget, cursor per read; extensions installed at build (`3da3973`) |
| 2026-10-25 | Review: an untimed delete came back to life after compaction (its `_deleted_at` became NULL); a patch without a row id added a NULL row | Both refused by the reader (Node always writes them) (`3da3973`) |
| 2026-10-25 | Review critic: unordered streaming scattered row ids across a compacted base's row groups | Ordered streaming — also faster and lighter on 10M rows (`f48f84e`); file-by-file compaction pending (§2) |
| 2026-10-25 | Shell heredocs on this box turn a backslash-n into a real newline inside patch scripts | Patch scripts are written as files with the editor tool, never heredocs with backslashes |

## 4. Design decision log

| Date | Decision |
|---|---|
| 2026-10-21 | Separate Python Data Service, new repo; old forms stay on MongoDB; Delta on S3 + DuckDB / Polars; max file 10 GB; full feature parity (PRD D1–D7) |
| 2026-10-24 | DynamoDB lock dropped — S3 conditional writes |
| 2026-10-24 | **Hybrid edits** — small edits are Mongo `dataset_patches` in one Node transaction; big data and big changes in Delta; one reader; compaction folds patches; the service reads only `dataset_patches`, read-only |
| 2026-10-24 | Same cell: the later commit wins (Google Docs style); the earlier value stays in history and undo |
| 2026-10-24 | Multi-user editing: small edits serialize on the revision CAS; group commit only for Delta jobs |
| 2026-10-24 | `dataVersion` counts value changes; compaction keeps it |
| 2026-10-24 | Undo is per cell with consented losses, re-checked inside the transaction; undo of an undo is a redo |
| 2026-10-24 | Job results split `changedFieldIds` (staleness) from `touchedFieldIds` (rebase); an append conflicts with no edit |
| 2026-10-24 | `committed` stores the commit record; a new holder rolls forward without re-running |
| 2026-10-24 | Service token: scaffold HMAC until the scoped token is built with the first internal endpoint |
| 2026-10-25 | Failed attempts: a wrong payload (`AppError`) fails at once; anything else retries until `max_attempts` (Node config), then the operation is a poison one and the queue moves on |
| 2026-10-25 | **Scope:** this service = data + performance; Node keeps login, quota, edits, undo in its existing modules; no new Node logic in Python |
| 2026-10-25 | **D11–D13** (backend `22a20996`): old Mongo forms stay; new forms' import / detection / `:validate` / derived (at read) in Python; no merge with the analysis service for now; Polars dropped (DuckDB only unless a benchmark needs it) |
| 2026-10-25 | The reader reads Delta files with DuckDB's core `read_parquet` (file list from delta-rs at the pinned version) — not DuckDB's delta extension (blocked on the dev box) nor DataFusion (no late materialization through our joins) |
| 2026-10-25 | One DuckDB engine per process with one budget from Settings; a cursor per read |
| 2026-10-25 | A delete without `at` and a patch without a row id are refused, not guessed |
| 2026-10-25 | A full stream keeps the base's order (compaction, export) |
