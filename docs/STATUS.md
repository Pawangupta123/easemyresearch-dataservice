# Status — Phase 0

**As of:** 2026-10-24 · branch `feat/lease-fence` at `a03f86c` · nothing in production

Phase 0 = scaffold + consistency prototype ([design/implementation-plan.md](design/implementation-plan.md),
scenarios in [design/consistency.md §13](design/consistency.md#13-phase-0-prototype--must-pass-before-phase-1)).
Everything below was checked against the code, the tests and `git log` on 2026-10-24.

## 1. Built

| What | Where | Commit (2026-10-24) |
|---|---|---|
| Project: uv on Python 3.12, ruff, strict mypy, ty, import-linter, pytest; git hooks; CI | `pyproject.toml`, `.githooks/`, `.github/workflows/ci.yml` | `5a0b71c`, `e082236` |
| Service skeleton: FastAPI app on Granian, the backend's layers, `/health` | `src/dataservice/main.py`, `modules/health/` | `a15a439` |
| Settings that refuse unsafe configs: `AWS_S3_ALLOW_UNSAFE_RENAME` forbidden, service secret ≥ 32 characters, `storage_mode` `s3` \| `local` | `config/settings.py` | `a15a439`, `2513017` |
| Service-token auth on every route: `x-service-token: <issuedAt>.<HMAC-SHA256>`, max age from settings (default 300 s), 30 s clock skew | `common/auth.py`, `common/utils/service_token.py` | `a15a439` |
| Node-style response envelope + one `ErrorCode` catalog; deny-by-default error handler (only user-safe messages shown) | `common/responses.py`, `common/errors.py`, `common/error_codes.py` | `13677e2` |
| Fenced commit gate — the only place Delta is written: `txn("emr-writer", token)` + metadata (`token`, `ops = opId:attempt,…`, `opTables`, `part`), `max_commit_retries=0`, fence check; a group of operations per commit | `storage/fenced_commit.py` | `a15a439`, `5828d77`, `071c4cd` |
| No DynamoDB lock: S3 conditional writes coordinate commits | settings, compose, `.env.example` | `2513017` |
| C0 concurrent commits on S3 — moto in CI (10 writers); the real AWS test bucket by hand (20 writers: every success kept, the rest failed loudly) | `tests/integration/test_c0_concurrent_commits.py` | `484027c` |
| Writer lease contract (`LeaseAuthority`) + the **reference Node side** on a real Mongo replica set: CAS on the expected previous token, Mongo clock (`$$NOW`), publish guard that writes the lease row | `storage/lease.py`, `prototype/node_side/lease_store.py` | `712b6e4`, `53a849d` |
| Fence every table, then reconcile: roll forward the committed prefix, fenced `RESTORE` of the rest (tagged with what it removed), stop on a log that breaks the protocol | `storage/reconcile.py` | `071c4cd` |
| The **one reader** (hybrid edits): Delta base at its version + patches with `from_seq < seq ≤ to_seq`, merged in DuckDB (rows, row state, cells); field ids validated before SQL; architecture test forbids Delta data reads elsewhere | `storage/reader.py`, `storage/snapshot.py` | `a03f86c` |
| Library-facts probe (delta-rs / DuckDB behaviour the protocol relies on), run in CI and on push | `probes/delta_api_probe.py` | `a15a439` |

### Tests (2026-10-24, `a03f86c`, all passing with the local Mongo up)

61 tests: **51 unit** (`tests/unit`: architecture 3, commit message 8, config 3, errors 7,
fenced commit 7, health 3, reader 8, reconcile 12) and **10 integration** (`tests/integration`:
C0 1, lease 9).

| Scenario | Test | Store |
|---|---|---|
| C0 | `test_c0_concurrent_commits.py` | moto S3 (asserts on the Delta log) |
| C2, C2b, C3, C3b | `test_reconcile.py` | local filesystem Delta |
| C18, C29 | `test_lease.py` | local Mongo replica set |
| Reader merge rules (insert / delete / restore / set / clear / revision range — main scope) | `test_reader.py` | in memory + local filesystem Delta |

C3b's test also checks the attempt rule C20 relies on (a rolled-back attempt's commit is never
counted) at the reconcile level; C20 itself, with Node's operation states, is not built.

## 2. Not built yet

**Phase 0 scenarios** still to automate: C1, C4–C17, C19–C28, C30–C44 (C19 and C42 only have
the reader's merge rules so far, before any compaction exists).

**Phase 0 features:**

- Node side beyond the lease: `data_operations` + status machine + payloads + outbox +
  sweeper + poison handling; the publish transaction; quota reservations; `dataset_schemas`.
- Hybrid edits: the Mongo edit transaction, a Mongo `PatchSource` for `dataset_patches`
  (only the `PatchSource` protocol exists), `dataset_changes`, `dataset_unique_claims`,
  `dataset_idempotency`, compaction, patch retention.
- Reader: table and follow-up scopes, the `files` value type, child tables, caches.
- Retention pins + fenced vacuum; per-cell undo; pinned pagination.
- Production `LeaseAuthority` (Node client); service token with the design's claims
  (`datasetId`, `scope`, `rowScope`, `exp ≤ 60 s` — [design/api.md §2](design/api.md#2-internal-api-node--data-service)).
- Query API endpoints, jobs (Lambda / Fargate), AWS infrastructure (plan 0.4), OpenAPI
  contract (0.7), metrics and tracing (0.6), benchmarks (0.10).

**Phases 1–8:** not started ([design/implementation-plan.md](design/implementation-plan.md)).

## 3. Problems found and how we solved them

| Date | Problem | Solution |
|---|---|---|
| 2026-10-24 | Windows Application Control on the dev box blocks the `.venv\Scripts\*.exe` launchers, and sometimes pyarrow's `_dataset` DLL; DuckDB's delta extension is blocked there too | Run every tool as `uv run python -m <tool>`; the probe's DuckDB-delta check runs on Linux (CI) |
| 2026-10-24 | The MinIO image is gone from Docker Hub, and Docker Hub's CDN failed with EOF | S3 = in-process **moto** server in the integration tests; Mongo from the local `mongo:7` image (`docker-compose.yml`) |
| 2026-10-24 | A DynamoDB lock table was planned for S3 commits | Not needed: delta-rs 1.6 commits with S3 conditional writes (zero DynamoDB requests on moto; C0 on moto and on the real AWS test bucket) — lock dropped |
| 2026-10-24 | moto's server cannot serve delta-rs Parquet data reads (requests stall) | Integration tests on moto assert on the Delta log; read-path tests use the local filesystem (or real S3) |
| 2026-10-24 | FastAPI's `ORJSONResponse` is deprecated | Routes declare Pydantic return types (`ApiResponse[T]`); FastAPI serialises them in Pydantic's Rust core |
| 2026-10-24 | DuckDB 2.0 relations have no `fetchone` | `con.execute(...).fetchone()` in the probe; the reader streams with `con.sql(...).to_arrow_table()` |
| 2026-10-24 | DuckDB 2.0 wheels exist only for CPython 3.11 / 3.12 | Python **3.12** (`.python-version`, `requires-python`) |
| 2026-10-24 | `httpx` is stale | `httpx2` in the dev dependencies |
| 2026-10-24 | The architecture test's write-call regex matched a plain `set.update(...)` | Pattern narrowed: `.update(` / `.delete(` count only with delta-rs's own arguments |
| 2026-10-24 | Assumption that a `RESTORE` lowers the fence was wrong (a mutation test proved it) | Corrected: a `RESTORE` keeps the table's latest txn; it still carries the token for the race check and the audit trail |
| 2026-10-24 | `alter.set_table_properties` refused the custom `emr.fence` property | `raise_if_not_exists=False`; the fence itself lives in the txn (a `RESTORE` drops custom properties set after its target) |

## 4. Design decision log

| Date | Decision |
|---|---|
| 2026-10-21 | Separate Python Data Service, new repository; old forms stay on MongoDB; Delta Lake on S3 + DuckDB / Polars (not MongoDB, not Databricks); max file 10 GB; full feature parity (PRD D1–D7) |
| 2026-10-24 | **DynamoDB lock dropped** — S3 conditional writes coordinate Delta commits (C0 passed) |
| 2026-10-24 | **Hybrid edits (option B)** — small edits (≤ 500 rows) are Mongo `dataset_patches` written by Node in one transaction; big data and big changes stay in Delta; one reader for every read; compaction folds patches into Delta; the service reads only `dataset_patches`, read-only ([design/consistency.md §14–§15](design/consistency.md#14-small-edits--one-mongo-transaction)) |
| 2026-10-24 | **Same cell: the later commit wins** (Google Docs style, server order); the earlier value stays in history and undo |
| 2026-10-24 | **Multi-user editing** — any number of users edit one dataset at once; small edits serialize on the revision compare-and-set; group commit only for Delta jobs |
