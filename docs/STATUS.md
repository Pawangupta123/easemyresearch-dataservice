# Status — Phase 0

**As of:** 2026-10-25 · branch `feat/lease-fence` at `e03afa2` · nothing in production

Phase 0 = scaffold + consistency prototype ([design/implementation-plan.md](design/implementation-plan.md),
scenarios in [design/consistency.md §13](design/consistency.md#13-phase-0-prototype--must-pass-before-phase-1)).
Everything below was checked against the code, the tests and `git log` on 2026-10-24.

`prototype/node_side/` is the **reference implementation of Node's side** (Mongo): it proves the
protocol end to end in Python; Node ports it to TypeScript in Phase 2. `src/` never imports it.

## 0. Resume here (read this first after a break or a context compaction)

**Branch:** `feat/lease-fence` (dataservice) · last code commit `e03afa2` · CI green · 136 tests.
Backend design docs live on the backend branch `docs/data-service-hybrid-edits` (last
`2094a8ae`, **not pushed**); `docs/design/` here is an identical copy — change both together.

**Scope decision (2026-10-25, user):** this service handles **data only** — storage, the one
reader, compaction, ingest, queries — and **performance is the top priority**. Login, quota,
edits, undo, unique-ID guard stay in Node's existing modules. `prototype/node_side/` is a
test-only spec of Node's side: **add no more Node logic to it**; when Node builds its side it
extends its existing modules (`records-core-quota.service`, `revision-batch-*`,
`records-core-unique-identifier`), the scenario tests run against Node's real endpoints, and
`prototype/node_side/` is deleted.

**Next step — performance of the data path:**

1. The reader today loads the **whole base** into memory (`read_base` = `select * from base`)
   and merges in DuckDB. That cannot serve a 10 GB dataset. Rework it to push the work into
   DuckDB / delta-rs: read only the needed columns (projection), filter / sort / page inside
   the query, apply only the patches of the rows on the page, count without materialising.
2. A benchmark harness on **synthetic** data (1M / 10M / 30M rows × ~50 columns) for: grid page
   (sorted + filtered), count, patch overlay with 1k / 100k patches, compaction time and peak
   memory — numbers recorded in this file.
3. Then the real files (big CSV, `.sav`, `.rds`) — **ask the user for them** when step 2 is done.

**Run:** Docker Desktop must be running → `docker compose up -d` (Mongo replica set on 27018);
`uv run python -m pytest` (Windows: always `python -m`, the `.exe` launchers are blocked).
AWS: the backend `.env` testing bucket (`AWS_S3_*`) is allowed for probes — never print keys,
write under `dataservice-probe/<id>/`, clean up.

**Commits:** branch-prefixed hooks add the emoji; no Co-Authored-By; print the branch first.

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
| The one **reader**: Delta base at its version + Mongo patches `from_seq < seq ≤ to_seq` (rows, row state with `_deleted_at`, cells); architecture test forbids Delta data reads elsewhere | `storage/reader.py`, `storage/snapshot.py`, `storage/mongo_patches.py` | `a03f86c`, `8b50d2d`, `618f1fd` |
| **Hybrid small edits** — reference Node edit transaction: idempotency, revision CAS, patches (with `at`), change log, unique claims, revision manifest, `Form.dataVersion`, event outbox; an in-transaction `check` hook; `batchId` (mutationId) apart from the request's idempotency key | `prototype/node_side/edit_store.py` | `8b50d2d`, `264c4c0`, `f1679c8` |
| **Compaction**: fold a revision's patches into the base (refuses a base rewritten since, a dropped or retyped column); Node publishes it with `fromSeq = W`, the same `dataVersion`, no staleness; patch purge respects pins | `storage/compaction.py`, `prototype/node_side/compaction_store.py` | `618f1fd` |
| **Undo** per cell from `dataset_changes`: preview (entries, losses, excluded, `lossSetHash`), apply as a new small edit (`undo:<batchId>`, redo = undo of the undo), losses re-checked inside the transaction | `prototype/node_side/undo_store.py` | `f1679c8` |
| **Delta-job operations**: `data_operations` with every status move a CAS on opId + status + token + attempt; cancel only before `committed` | `prototype/node_side/operation_store.py` | `3a2b96f` |
| **Publish + writer**: `OperationAuthority` contract; the writer runs `append_rows` (running → fenced commit → committed with its commit record → publish); Node's publish (idempotent, lease row written, rebase on `touchedFieldIds` + row ops, `dataVersion`, staleness for the form's analyses, event) | `storage/operations.py`, `modules/writer/service.py`, `prototype/node_side/publish_store.py`, `prototype/node_side/authority.py` | `ad351a3` |
| **Crash recovery** on takeover: fence + reconcile, rolled-back attempts made runnable again, committed operations rolled forward (published, never re-run); Node's door refuses any writer call whose token is not the live lease | `modules/writer/service.py` (`recover`), `prototype/node_side/authority.py` | `191c255` |
| **Queue drain + poison operations**: only the head (lowest unfinished `seq`) runs; a failing attempt rolls its own commits back and is reported retryable or not (a wrong payload fails at once); Node retries until `max_attempts`, then fails it (C6); the sweeper dispatches operations whose wake-up was lost (C5); a lost lease (`FenceError` at Node's door) stops the drain without touching the operation | `modules/writer/service.py` (`run_operation`, `drain`), `prototype/node_side/operation_store.py` | `0254320` |
| **Entry quota** (spec of Node's side): imports reserve with their operation (no room → `QUOTA_EXCEEDED`, no operation), publish settles (re-validated if released), final failure / cancel release, the sweeper releases only for finished or vanished operations (C7), the heartbeat renews every unfinished reservation; small edits charge inserts / restores and credit deletes; abandoned `running` heads are taken over | `prototype/node_side/quota_store.py`, `operation_store.py`, `authority.py` | `e03afa2` |
| Library-facts probe (delta-rs / DuckDB), run in CI and on push | `probes/delta_api_probe.py` | `a15a439` |

### Tests (2026-10-25, `e03afa2`, all passing with the local Mongo up — 3 full runs in a row)

**136 tests: 56 unit** (architecture 3, commit message 8, config 3, errors 7, fenced commit 7,
health 3, mongo patches 4, reader 9, reconcile 12) and **80 integration** (C0 1, compaction 6,
drain 7, edits 7, lease 9, operations 12, publish 9, quota 13, recovery 4, undo 12). The quota
work also had an **adversarial review** (3 lenses, each finding verified by a skeptic): two real
bugs found and fixed with regression tests that failed first. Every rule was also
**mutation-tested**: the code was broken on purpose and the matching test failed.

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
| C39, C41, C42 | `test_compaction.py` | Mongo + Delta |

## 2. Not built yet

**Phase 0 scenarios** still to automate: C1, C4, C8, C9, C12–C14, C16, C17, C19, C21–C27,
C30, C33, C36–C38, C40, C43 — the Data-Service-side ones first (performance, C14, C16, C40, C43);
Node-side ones run against Node itself once it is built (scope decision, §0).

**Phase 0 features:**

- The BullMQ wake-up (dispatcher) and the sweeper's schedule; a refused publish after `committed`
  (C4: the drain stops and the operation stays at the head); payload encryption / S3 offload; quota reservations;
  `dataset_schemas`; field freeze; group commit for Delta jobs.
- Reader: table and follow-up scopes, the `files` value type, child tables, caches, `_row_id`.
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
