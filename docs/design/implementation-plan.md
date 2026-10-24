# Data Service — Implementation Plan

**Status:** Proposed · Phase 0 in progress — in `easemyresearch-dataservice` (branch `feat/lease-fence`, 2026-10-24): scaffold, fenced commit gate, writer lease + reference Node side, fence-every-table + reconcile, the one reader; C0 passed
**Owner:** Pawan Gupta
**Last updated:** 2026-10-24
**Back to:** [prd.md](prd.md) · [tech-design.md](tech-design.md) · [data-model.md](data-model.md) · [api.md](api.md)

Each phase ships on its own, behind the `DATA_SERVICE_ENABLED` flag, and leaves the product
working. Old (`mongo`) forms are untouched throughout. "Done" means: every step merged, the
acceptance checks pass, and the phase report is written.

---

## Phase 0 — Decisions, foundations and the consistency prototype

**Goal:** answer the open questions, have an empty but deployable service, and prove the
protocol in [consistency.md](consistency.md) on real Delta on S3 before anything else is built.

| # | Step |
|---|---|
| 0.1 | Decided: Delta + DuckDB, new repo, 10 GB max, full feature parity (PRD D4–D7). Still to answer: real dataset sizes + sample files (Q3), AWS budget (Q5) |
| 0.2 | Collect 5 real files: largest CSV, a typical `.sav`, an `.rds`, an `.RData` with several frames, an xlsx with a codebook sheet. Measure R in-memory size multiple |
| 0.3 | New repository `easemyresearch-dataservice` (FastAPI, Python 3.12, uv, ruff, mypy, pytest, architecture check), same CI gates as the existing Python service. **Pin minimum delta-rs and duckdb-delta versions** that provide `txn` app transactions, `transaction_version`, `restore` / `vacuum(keep_versions)` and `delta_scan(version =>)` (consistency, facts list) |
| 0.4 | AWS: S3 prefix `datasets/` (SSE-KMS, lifecycle rules for aborted multipart, raw and payload retention), ECR, ECS cluster, Fargate task + service definitions, Lambda container function, IAM roles (least privilege), VPC private networking, CloudWatch |
| 0.5 | Service-to-service auth: Node signs short-lived service tokens; HMAC for callbacks; secrets (incl. the payload encryption key) in the existing secret store |
| 0.6 | Observability: OpenTelemetry in both services, Prometheus metrics, dashboards for jobs, queries, operation queue age and lease state |
| 0.7 | Contract file (OpenAPI) for the internal API; generate Pydantic and Zod types from it |
| 0.8 | **Consistency prototype**, minimal code on real Delta on S3: revision manifests (Delta versions + patch range), S3 conditional-write commits, fenced commit wrapper, lease + fence-every-table + reconciliation, durable operations + outbox + sweeper + poison handling, publish transaction, quota reservations; **hybrid edits**: the Mongo edit transaction (`dataset_patches`, revision CAS, unique claims, idempotency, quota, staleness, `dataset_changes`, outbox), the one reader, compaction + patch retention; per-cell undo, typed nested targets, pinned pagination, retention pins + fenced vacuum |
| 0.9 | Run **all scenarios C0–C44** ([consistency §13](consistency.md#13-phase-0-prototype--must-pass-before-phase-1)) as automated tests; two Node instances, two query-service instances + a job runner, with fault injection between every protocol step |
| 0.10 | **Benchmarks** on real sample files (Q3): ingest time for the largest file, compression ratio, grid p50/p95 with a **cold cache**, **10 concurrent users**, a **growing patch window** and compaction time, edit latency (**reader read + Mongo edit transaction**, CAS retries under 20 typing users), job commit latency (**S3 conditional write + Mongo publish transaction**), Mongo storage of patches and change log, cost per dataset |

**Acceptance:** `GET /health` of the query service reachable from Node with a valid token and
rejected without; a hello-world job runs on Lambda and on Fargate and calls back to Node;
**C0–C44 all pass** and stay in CI; the benchmark report replaces the PRD targets with
measured numbers (or the design is revised before Phase 1).

---

## Phase 1 — DataStore abstraction in Node (MongoStore only)

**Goal:** every feature reads/writes row data through `DataStore`; behaviour unchanged.

| # | Step |
|---|---|
| 1.1 | Add `src/v2/common/datastore/` with the interface ([tech-design §4.2](tech-design.md#42-the-interface)) and `MongoStore` wrapping Q / M / D |
| 1.2 | Route the six bypasses through it: analysis stats, graphs, field variance, subset count, screening `$lookup`, raw `Q.aggregate` callers |
| 1.3 | Move every service caller (submit, write, row lifecycle, bulk, grid read, export, import, transforms, unique identifier, type migration, column archive, revision adapter, AI record tools) to `DataStore`, keeping today's row scope per operation ([tech-design §4.3](tech-design.md#43-row-scope--as-the-code-does-it-today)) |
| 1.4 | Add `form.storage` (default `'mongo'`, no backfill) and a resolver `dataStoreFor(form)` |
| 1.5 | **Storage guard per feature and per form:** a feature not yet ported answers a `dataservice` form with a clear user error instead of touching Mongo; the guard list shrinks phase by phase |
| 1.6 | Return `WriteResult` from writes and drive quota / `dataVersion` / staleness from it (same values as today) |
| 1.7 | Lint rule: importing `Record` model or Q/M/D outside `datastore/` is an error |

**Tests:** full unit suite green; new unit tests on `MongoStore` per method; e2e on grid,
bulk edit, column paste, undo, import, export against the test DB — results identical to
before (snapshot the responses before the refactor and diff).

**Acceptance:** zero behaviour change in production; every row access goes through
`DataStore` (enforced by lint).

---

## Phase 2 — Publish path, ingest and read-only grid

**Goal:** a new import of CSV / xlsx / Parquet lands in Delta **as a published revision** and
is browsable. Ingest is a write, so the publish path is built first, inside this phase.

| # | Step |
|---|---|
| 2.1 | **Publish path, before any ingest:** `data_operations` + payload/outbox, `dataset_leases` + heartbeat (port of the reference `prototype/node_side`), the single fenced commit wrapper (CI check), fence-every-table + reconciliation, Node publish transaction with every critical effect, idempotent publish, BullMQ dispatcher (`jobId = datasetId`), sweeper, poison-operation handling ([consistency §4–§6](consistency.md#4-writer-lease-fencing-and-reconciliation)) |
| 2.2 | Data Service: Delta writer (`deltalake`) through the wrapper, dataset layout ([data-model.md](data-model.md)), `_row_id` allocation from the manifest |
| 2.3 | `detect` job: full-file type detection, porting today's Node rules (numeric/date/MCQ, multi-value cells, corrections) to Python with **shared golden tests** so both give the same answer on the same file |
| 2.4 | `ingest` job: streaming CSV (delimiter / encoding / header-row detection), xlsx (streaming reader), Parquet; files written outside the lease, one fenced commit + publish at the end; quota reserved with the operation; progress + cancel |
| 2.5 | Query service: `rows:query`, `rows:count`, `rows:ids`, `rows/{id}`, `cells:read`, `rows:stream`, `fields:stats`, `:sample` — all through the **one reader** (Delta **at the manifest's versions** + `dataset_patches` of the revision's range, read-only), filter-tree compiler, `(table, version)` snapshot cache, row scope in every cache key ([consistency §15](consistency.md#15-one-reader-and-compaction)) |
| 2.6 | Node: `dataset_uploads`, `datasets`, `dataset_revisions`, `data_jobs` models; upload (multipart presign) routes; dataset/preview/confirm routes; job orchestration + callback route; `DataServiceStore` read methods |
| 2.7 | Node: grid read for `dataservice` forms through `DataServiceStore` (same response shape; revision inside the cursor; `columns` param) |
| 2.8 | **FE changes (acknowledged, not "FE unchanged"):** a new multipart uploader with progress/resume for large files (today's 10 MB Excel upload cannot carry GB files); accept `.csv` / `.parquet`; preview reused; paged schema for wide files; grid sends `columns` |

**Tests:** C0–C44 re-run against the production code; golden files (CSV encodings and
delimiters, quoted newlines, BOM, huge column counts); detection parity Node vs Python; filter
compiler fuzz tests (no injection); load test against the Phase 0 measured targets.

**Acceptance:** with the flag on for test users, the largest sample file is browsable end to
end within the measured target; old forms unaffected.

---

## Phase 3 — Editing on the Data Service

| # | Step |
|---|---|
| 3.1 | `dataset_patches` with typed nested targets + the **edit transaction** (revision CAS, patches, revision, `dataVersion`, staleness, `dataset_changes`, `RevisionBatch`, idempotency, quota, audit, event outbox); the one reader merges every scope (`fromSeq < seq ≤ toSeq`) ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)) |
| 3.2 | Insert row, cell edit, bulk edit, column paste (`ROW_GONE`), soft delete / restore as small edits (≤ `BULK_EDIT_MAX_ROWS`), beyond the cap as set-based Delta jobs; unique identifier by `dataset_unique_claims` + reader check; **derived fields recomputed** by Node from the merged row inside the same edit (set-based recompute for column-wide changes) |
| 3.3 | Schema operations: add / rename / delete field, column archive / restore, type migration + unique reindex — form updated in the publish transaction |
| 3.4 | Change log `dataset_changes` (+ job change files); per-row history and batch undo on it. **Batch-undo contract mapping:** `mutationId ≡ batchId`; preview keeps `entries`, `excluded`, `losses`, `lossSetHash`, `stateHashes`, `coverage`; result keeps `restoredCount`, `skipped`, `undoMutationId`; FE copy for per-cell `losses` reviewed ([consistency §8.2](consistency.md#82-todays-contracts-on-the-change-log)) |
| 3.5 | Compaction outside the lease (trigger — patch count, idle time, maximum frequency — from config) publishing a new revision with `patches.fromSeq = W` ([consistency §15.2](consistency.md#152-compaction)); explicit whole-dataset restore as a separate, audited action |
| 3.6 | **Retention / vacuum job:** pinned revisions from Node → per-table `keep_versions`, fenced `vacuum`, payload cleanup, patch deletion only when no retained or pinned revision needs them ([consistency §15.3](consistency.md#153-patch-retention)), `dataset_changes` trim, trash purge after 45 days ([consistency §11](consistency.md#11-retention)) |
| 3.7 | Child tables for table fields and follow-ups (needed for CRF forms created on the Data Service) |

**Tests:** concurrency tests (two Node instances editing, two writers, same dataset); undo after compaction; quota
reconciliation; e2e parity — the same scripted edits on a `mongo` form and a `dataservice`
form give identical grid output (batch undo compared under the per-cell rule).

---

## Phase 4 — More formats and bigger files

| # | Step |
|---|---|
| 4.1 | SPSS `.sav`: `pyreadstat` chunked; value labels → field options; variable labels → field labels; user-missing → null |
| 4.2 | R `.rds` / `.RData`: `pyreadr`; factor levels → options; object picker for `.RData`; memory guard → `R_FILE_TOO_LARGE_FOR_MEMORY` |
| 4.3 | Codebook for CSV / xlsx (separate file or second sheet) → code → label mapping with preview |
| 4.4 | Compute escalation Lambda → Fargate (thresholds from config); SPSS fan-out by row ranges |
| 4.5 | Raise file-size limit to 10 GB (config), FE copy and validation |

**Tests:** label/missing-code fixtures per format; memory-limit behaviour; a 6 GB `.sav`
fan-out run.

---

## Phase 5 — Analyses on Data Service datasets

| # | Step |
|---|---|
| 5.1 | Existing Python analysis service: `RowSource` seam; `DataServiceRowSource` asking the Data Service (`rows:stream`) for today's record shape (projected columns, filters pushed down) — it never reads Delta or patches itself |
| 5.2 | Node gateways pass `storage` + `datasetId` + the published revision manifest alongside `formId`; same `user_id` row scope as today. Open point: today's bulk-download cache split compares `Form.dataVersion`, so a compaction revision would make it recompute — decide the fix here ([consistency §15.2](consistency.md#152-compaction)) |
| 5.3 | Parity suite: every analysis kind on the same data in Mongo and Delta → identical results (descriptive, inferential, paired, correlation, agreement, regression, ROC, diagnostic, survival, charts, bulk report) |
| 5.4 | Long analyses as jobs (beyond the 30 s synchronous limit) |
| 5.5 | Columnar engines for the heaviest analyses, one at a time, behind the parity suite |

---

## Phase 6 — Transforms and cleaning

| # | Step |
|---|---|
| 6.1 | Rule catalogue (config/DB): range, allowed values, missing codes, regex, duplicates, trim/case, coercion, unit conversion, recode, outlier flag |
| 6.2 | Spec validation, sample preview, full run → new revision + report |
| 6.3 | Existing transform catalogue (derived fields, conversions) executed by the Data Service for `dataservice` forms |
| 6.4 | Revisions UI: list, compare counts, restore |
| 6.5 | Lineage: which upload / transform / cleaning run produced each column |

---

## Phase 7 — Export, lifecycle, admin, AI

| # | Step |
|---|---|
| 7.1 | Export: synchronous below a size threshold (config); above it an export job (xlsx / csv / sav / parquet) with presigned download |
| 7.2 | Delete: soft delete with the 45-day window → purge job (S3 objects + Delta) |
| 7.3 | Admin counts and plan quotas include `datasets.rowCount` / `storageBytes` |
| 7.4 | AI record agent tools on `DataStore` (they already go through core services — verify caps and selection scope) |
| 7.5 | Screening export and doc-build subset counts on `DataStore` |

---

## Phase 8 — Hardening and cut-over

| # | Step |
|---|---|
| 8.1 | Load and soak tests at target sizes; chaos (kill a job mid-run, query service restart) |
| 8.2 | Cost dashboard (per dataset storage, job compute, query service) and alerts |
| 8.3 | Runbooks: stuck job or operation, poison operation, failed compaction, Lambda OOM, restore a revision, purge |
| 8.4 | Security review (token scope, presigned URLs, PHI in logs) |
| 8.5 | Cut-over: new forms and imports default to `dataservice` (flag), staged: internal → beta users → all |
| 8.6 | Docs: database-models, architecture overview, feature docs, swagger |

---

## Rough cost (to be refined with Q3 sizes; check AWS pricing for ap-south-1)

| Item | Basis | Order of magnitude |
|---|---|---|
| S3 storage | ~1–2 GB per 6 GB CSV (Parquet) + raw upload | cents to a few dollars per large dataset per month |
| Query service | 1–2 Fargate tasks, 2 vCPU / 8 GB, always on | ~US$100–200 / month |
| Ingest / jobs | Lambda + occasional Fargate tasks, pay per run | a few dollars per large dataset ingested |
| MongoDB | Metadata + small edits (`dataset_patches`, `dataset_changes`) for new data | grows with editing, bounded by compaction and retention — not with dataset size |

## Risks

| Risk | Mitigation |
|---|---|
| Two stores drift in features | Phase 1 lint rule; every new feature built on `DataStore`; parity e2e suite |
| Detection rules differ between Node and Python | Shared golden tests; later Node calls Python detection for all imports |
| Large R files exceed memory | Config limit + Fargate + Parquet/CSV guidance (U8) |
| Patch window grows on heavy editing | Configured compaction trigger + alerts; growing-patch benchmark in Phase 0 |
| Mongo storage for patches and change log (Atlas quota) | Patch retention rule, undo trim, compaction; measured in Phase 0 |
| Consistency bugs (lost edits, wrong undo, stale analysis) | Protocol prototyped and C0–C44 automated in Phase 0, kept in CI afterwards |
| Stuck or poison operation blocks a dataset | Sweeper re-dispatch; poison op failed after N attempts; alert on queue age |
| Old Delta versions unreadable after log cleanup | Retention properties ≥ longest window + margin; fenced vacuum with pins (C14, C16) |
| Analysis memory on huge datasets | Projected columns, then columnar engines (Phase 5.5); analysis jobs |
| Data Service outage blocks new forms | Health checks, autoscaling, clear user errors; old forms unaffected |
