# Data Service — Tech Design

**Status:** Proposed · not built
**Owner:** Pawan Gupta
**Last updated:** 2026-10-25
**PRD:** [prd.md](prd.md) · **APIs:** [api.md](api.md) · **Plan:** [implementation-plan.md](implementation-plan.md) · **Protocol:** [consistency.md](consistency.md)

---

## 1. Summary

A new **Python Data Service** stores and computes over dataset rows. **Node** stays the only
public API: it authenticates, checks form permissions and quotas, writes the audit trail,
keeps form/schema metadata in MongoDB, orchestrates jobs, and is the **only publisher** of
dataset revisions. The uploaded dataset lives as **Delta Lake tables on S3**, queried with
**DuckDB**; **small edits** (cells, rows, bulk-edit parts up to 500 rows) are Mongo
documents (`dataset_patches`) that Node writes in one transaction, and **one reader** in the
Data Service merges both for every read (hybrid edits, decided 2026-10-24).
Short jobs run on **Lambda**, heavy jobs on **Fargate**, and the interactive grid is served by
a small **always-on query service**. Existing forms stay on MongoDB with today's Node code
(D11); for new forms the Data Service owns import, type detection, validation and derived
fields (D12, §7.1–§7.2); a **DataStore** interface hides which store a form uses.

```
            ┌──────────────────────── Browser (FE) ───────────────────────┐
            │  grid (rows+cols virtualised) · import wizard · transforms  │
            └──────────────┬──────────────────────────────▲───────────────┘
                           │ REST (JWT)                   │ presigned S3 multipart
                           ▼                              │
   ┌──────────────────── Node API (orchestrator) ─────────┴──────────────┐
   │ auth · permissions · quota · audit · form/schema metadata (Mongo)   │
   │ DataStore ──► MongoStore (old forms)                                │
   │          └──► DataServiceStore ── internal HTTP (signed) ──┐        │
   │ small edits: one Mongo txn → dataset_patches + revision    │        │
   │ data_operations · dispatcher + jobs (BullMQ) · publish ◄───┤        │
   └─────────────────────────────────────────────────────────────┼───────┘
                                                                 ▼
   ┌───────────────────────── Python Data Service ────────────────────────┐
   │ Query API (always-on, ECS Fargate service): rows window, count,      │
   │   distinct, stats → the ONE reader (Delta + patches) + local cache   │
   │ Writer (lease holder): drains data_operations, fenced Delta commits  │
   │ Jobs: ingest/convert · transform · clean · export · compact          │
   │   ├─ Lambda (≤10 GB RAM, ≤15 min)                                    │
   │   └─ Fargate task (≤120 GB RAM, no time limit)                       │
   └───────────────┬───────────────────────────────┬──────────────────────┘
                   ▼                               ▼ read-only
        S3: raw uploads (immutable) ·        Mongo: dataset_patches
        Delta tables (conditional-write commits) · exports
```

## 2. Why this shape

| Concern | Choice | Reason |
|---|---|---|
| Row storage | Delta Lake (Parquet + transaction log) on S3 | Columnar (reads only needed columns), compressed (target: 6 GB CSV ≈ 1–2 GB), versioned (readers pin a published revision; undo is change-log based, [consistency §8](consistency.md#8-history-and-undo)), supports MERGE/UPDATE/DELETE for jobs and compaction, and **Databricks reads it natively** — no migration if we move there later |
| Engine | DuckDB (SQL), with delta-rs and pyarrow | Single-node, out-of-core, seconds on tens of millions of rows (target); no cluster to run or pay for when idle. Polars is **not** a dependency — optional, only if a benchmark shows DuckDB is insufficient |
| Writer | `deltalake` (delta-rs) ≥ 1.6 | Writes Delta without Spark/JVM; small enough for Lambda; concurrent commits coordinated by S3 conditional writes — no lock table ([consistency §3](consistency.md#3-s3-commit-coordination-conditional-writes)) |
| Metadata + small edits | MongoDB (existing) | Forms, fields, permissions, operations, revisions, jobs — and small edits (`dataset_patches`, change log) — are small, transactional, already there; an edit commits in one Mongo transaction instead of a Delta commit ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)) |
| Orchestration | Node + BullMQ (existing, standard — no Pro) | One public API, existing auth/audit/quota |
| Compute | Lambda + Fargate + one small always-on service | Pay per use for jobs; low latency for the grid |

### 2.0 Why not MongoDB, Databricks, Postgres or ClickHouse (decision D4)

| Option | Fit for ≤ 10 GB files, interactive grid, editing, analysis | Verdict |
|---|---|---|
| MongoDB (bigger tier) | Row-per-document → 10–50× size, slow column scans for statistics, large always-on tier | ✗ |
| Databricks | Strong for 100 GB+ / ETL; per-query latency and per-hour cost for a grid; small-edit file churn | ✗ now (same Delta files work there later) |
| Postgres | Good editing; 10 GB file → ~20–30 GB rows + indexes on an always-on instance; column scans slower than columnar; schema per dataset is awkward with thousands of columns | runner-up |
| ClickHouse | Very fast grid/analytics; updates are heavy "mutations"; an always-on cluster to run | ✗ for edit-heavy use |
| **Delta Lake on S3 + DuckDB** | Columnar + compressed (target: 10 GB → ~2–3 GB), seconds on tens of millions of rows, versioned, MERGE for edits, pay-per-use compute | **chosen** |

### 2.1 Databricks — considered, deferred

Databricks fits datasets of hundreds of GB and heavy ETL. For interactive grids it adds
latency (a SQL warehouse query per scroll/sort, cold starts), a per-hour cost that grows with
concurrent users, and the small-file problem on cell edits. Because tables are Delta, moving a
dataset to Databricks later means pointing Databricks at the same S3 path (it must then use
the same commit coordination, [consistency §3](consistency.md#3-s3-commit-coordination-conditional-writes)).
**Recommended: not in v1; revisit when a single dataset exceeds ~100 GB or jobs exceed one
Fargate task.**

### 2.2 New service, not the existing Python service (Q2 — recommended)

The existing service (`EASEMYRESEARCH_PYTHONMS_V1`, FastAPI) is a stateless
statistics-over-Mongo API: no file parsing, no upload handling, none of
duckdb / pyarrow / deltalake / pyreadstat / pyreadr, no job queue, synchronous
handlers under a 30 s timeout, one process deployed by systemd. The Data Service needs long
jobs, large memory, Lambda/Fargate packaging and its own scaling, so it is a **separate
repository and deployable**. It reuses from the existing service: the auth pattern
(`x-api-key` → upgraded to signed tokens), the S3 helpers, and the cohort filter semantics
(`app/features/shared/cohort/`), which become the grid filter compiler's reference.

**Merge deferred (D13, 2026-10-25).** Merging the two Python services later — one repository
with workload roles (api / jobs / Lambda) — was analysed and deferred; it is not planned. Until
then the analysis service reads Data Service rows at a pinned revision through the Data Service
(§4.6).

### 2.3 Size estimates (6 GB CSV, ~200 columns, ~30 M rows — targets until Phase 0 / Q3 data)

| Store | Size | Import | Crosstab on 2 columns |
|---|---|---|---|
| MongoDB, today's record shape | ~400–500 GB + indexes | hours–days | minutes |
| MongoDB, slim record shape | ~100–150 GB + indexes | ~1–3 h on a large tier | minutes |
| **Delta on S3 + DuckDB** | **~1–2 GB** | **~5–20 min** | **seconds** |

Every latency, throughput, size and cost number in this document is a **target** until the
Phase 0 benchmarks replace it ([consistency §13](consistency.md#13-phase-0-prototype--must-pass-before-phase-1)).

---

## 3. Components

### 3.1 Node (orchestrator) — responsibilities

1. Public REST API for datasets, uploads, grid reads, edits, transforms, cleaning, exports.
2. Permission check on **every** call (today's row-scope rules, §4.3).
3. Quota and plan limits (rows, storage, file size) from configuration.
4. Audit trail: every write goes through `auditScope()` like any other route.
5. Mongo metadata and small edits: `Form` (+ `storage` flag), fields/schema, `datasets`,
   `dataset_revisions`, `dataset_patches`, `dataset_changes`, `dataset_unique_claims`,
   `dataset_leases`, `data_operations`, `data_jobs` (see [data-model.md](data-model.md)).
6. **Small edits**: one Mongo transaction per edit that writes the patches and publishes the
   revision with every effect ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)).
7. Operations and jobs: insert `data_operations`, wake the writer through the BullMQ
   dispatcher, start jobs on the right compute (Lambda/Fargate), receive callbacks.
8. **Publish** of a Delta job: the one Mongo transaction that moves the revision pointer and
   applies every critical effect ([consistency §6](consistency.md#6-publish--one-mongo-transaction-every-critical-effect)).
9. The **DataStore** interface (§4) that every feature uses for row data.

Node never parses a data file and never holds a full dataset in memory.

### 3.2 Python Data Service

Two deployables built from one codebase:

| Deployable | Runs on | Does |
|---|---|---|
| `query-api` | ECS Fargate service, always on (1–2 tasks, autoscale) | Rows window, count, distinct, column stats, cell reads, schema reads at a published revision — all through the **one reader**, derived columns computed at read (§7.2); the synchronous import `:validate` (§7.1); hosts the **writer** (`:drain`) that takes the lease and commits Delta-job operations. Keeps hot datasets cached on local disk |
| `jobs` | Lambda (container image) **or** Fargate task, chosen per job by Node | Stage + detect (§7.1), ingest/convert, transforms, cleaning, compaction, export, purge — heavy work outside the lease, short fenced commit at the end; inputs read through the one reader |

Both talk to S3 (data) and back to Node (internal endpoints). Their only Mongo access is the
reader's **read-only** access to `dataset_patches` ([consistency §15.1](consistency.md#151-the-reader));
they never write Mongo and never publish.

### 3.3 Compute selection (per job, by Node)

| Job | Default | Escalate to Fargate when |
|---|---|---|
| Stage + detect, ingest xlsx / CSV / Parquet | Lambda (streaming) | file > configured size or estimated time > 12 min |
| Ingest SPSS `.sav` | Lambda, chunked; parallel fan-out by row ranges for big files | single chunk cannot finish in time |
| Ingest R `.rds` / `.RData` | Lambda if file ≤ configured limit | always Fargate above it (R files load fully into memory; RAM need is a multiple of file size) |
| Transform / clean | Lambda | dataset rows × columns above threshold |
| Compaction (small, frequent — [consistency §15.2](consistency.md#152-compaction)) | Lambda | the files holding patched rows exceed the configured size |
| Export | Fargate | — |

Thresholds are configuration (`DATA_SERVICE_*` env / config collection), never constants in code.

---

## 4. DataStore abstraction

### 4.1 Where row data is touched today (inventory, 2026-10-21)

Almost all row access goes through three repositories in `src/v2/modules/record/`:
`record-query.repository.ts` (Q), `record-mutation.repository.ts` (M) and
`record-deleted-query.repository.ts` (D). Six places bypass them and must be routed through
the DataStore first:

| Bypass | File |
|---|---|
| `Record.aggregate` (MCQ / number stats) | `record/analysis/analysis.repository.ts:44,83` |
| `Record.aggregate` (graphs) | `record/graphs/graphs.repository.ts:34,91,146` |
| `Record.aggregate` (field variance) | `analysis/ai/document-build/document-field-variance.service.ts:46,74` |
| `countDocuments` | `analysis/ai/document-build/builders/document-subset-title.service.ts:48` |
| `$lookup from: 'records'` | `record/screening/screening.repository.ts:268-300` |
| Raw `Q.aggregate()` with caller-built pipelines | transforms, follow-up schedule, conversion |

Python analysis reads `forms` and `records` straight from MongoDB (Node sends only
`formId` + optional `recordIds`) — see §4.6.

### 4.2 The interface

One interface, two implementations, chosen by `form.storage` and guarded **per feature**: a
feature flag per form decides `mongo` vs `data-service`, and a feature not yet ported refuses
`data-service` forms with a clear error instead of reading the wrong store.

Today several writes are atomic Mongo transactions (form update with row changes, transforms,
revision restore, bulk delete + quota). The interface keeps that with an **operation
context**: on `MongoStore` it is a session; on `DataServiceStore` a small edit is **one edit
transaction** (patches + every effect, [consistency §14](consistency.md#14-small-edits--one-mongo-transaction))
and a bigger compound change is **one `data_operation`** (a Delta job); every write carries
`opId` (a small edit: its `batchId`) and `baseRevision`.

```ts
// src/v2/common/datastore/datastore.types.ts (shape, not final code)
interface OpContext { opId: string; baseRevision: number; actor: Actor; batchId?: string }
interface DataStore {
  // ── reads (rows are RecordDocument-shaped, §4.5) ──────────
  getRow(formId, rowId, scope): Promise<RecordDocument | null>;
  getRowMeta(formId, rowId): Promise<RowMeta | null>;                     // creator, status, timestamps
  listRows(formId, q: GridQuery, scope): Promise<{ rows: RecordDocument[]; next?: Cursor; total?: number }>;
  listDeleted(formId, q, scope): Promise<{ rows: RecordDocument[]; next?: Cursor }>;
  countRows(formId, filter, scope): Promise<number>;
  findRowIds(formId, q: { filter; order; cap }, scope): Promise<RowId[]>;   // paste, doc-build, stats subsets
  streamRows(formId, q: { filter; fields }, batch): AsyncIterable<RecordDocument[]>; // export, migrations
  sampleFieldValues(formId, fieldIds, n): Promise<Map<FieldId, unknown[]>>;
  optionUsage(formId, fieldIds): Promise<Map<OptionId, number>>;
  fieldStats(formId, req: FieldStatsRequest): Promise<FieldStats>;          // MCQ counts, numeric stats, graph axes
  rowIndexStats(formId, parentFieldIds): Promise<RowIndexStats>;           // table-field sub-row counts
  columnDrift(formId, fieldIds): Promise<ColumnDrift>;                     // archived vs current values
  exists(formId): Promise<boolean>;
  uniqueValueExists(formId, value, exceptRowId?): Promise<boolean>;
  // ── global counts (Data Service: from datasets.rowCount aggregates) ──
  countActiveByUser(userId): Promise<number>;                              // quota
  countRecordsGroupedByForm(formIds): Promise<Map<FormId, number>>;
  countUserRecords(userId): Promise<number>;
  countByFilter(adminFilter): Promise<number>;                             // admin
  // ── writes (all take OpContext) ─────────────────────────
  insertRows(ctx, formId, rows): Promise<WriteResult>;                     // submit, add-row, import
  updateRow(ctx, formId, rowId, entries): Promise<WriteResult>;            // cell edit, follow-up merge
  replaceRow(ctx, rowId, snapshot): Promise<WriteResult>;                  // per-row revision restore
  applyBatch(ctx, formId, batch: CellPatch[]): Promise<WriteResult>;       // bulk edit, column paste
  softDelete(ctx, formId, rowIds): Promise<WriteResult>;
  restore(ctx, formId, rowIds): Promise<WriteResult>;
  purgeDataset(ctx, formId): Promise<WriteResult>;
  // ── schema-level row operations ──────────────────────────
  migrateFieldType(ctx, formId, fieldId, from, to): Promise<WriteResult>;
  reindexUniqueIdentifier(ctx, formId, fieldId): Promise<WriteResult>;
  runTransform(ctx, formId, spec): Promise<JobRef | WriteResult>;          // sync for Mongo, job for Data Service
  archiveColumn / restoreColumn(ctx, formId, fieldIds): Promise<WriteResult>;
  // ── history ─────────────────────────────────────────────
  undoBatch(ctx, formId, batchId): Promise<WriteResult>;
}
interface WriteResult { rowsChanged: number; rowsAdded: number; rowsRemoved: number;
                        changedFieldIds: string[]; storageBytesDelta: number;
                        batchId?: string; revision?: number }
```

- `MongoStore` wraps today's Q/M/D repositories and the six bypasses — **no behaviour change**.
  The Node logic around `mongo` forms — import, derived-field evaluators, Mongo grid filters,
  type conversion — keeps serving them unchanged and is frozen (D11); it is not re-used for
  `dataservice` forms (§7.1–§7.2).
- `DataServiceStore` reads through the Data Service internal API ([api.md §2](api.md#2-internal-api-node--data-service)),
  writes small edits to Mongo itself, and submits Delta jobs as operations.
- `replaceRow` is keyed by row id only (today's per-row revision restore), so it needs a
  `_rid → dataset` lookup.
- Column archives copy values into Mongo today; on the Data Service the archived column stays
  in Delta (hidden), and `columnDrift` compares it with the current one.
- `scope` carries the caller's row scope (§4.3). The Data Service stores `_created_by` per row.

### 4.3 Row scope — as the code does it today

Verified against the current routes and repositories; the Data Service reproduces it exactly.

| Operation | Rows in scope today |
|---|---|
| Grid list, record ids, export | All rows of the form (not scoped by creator) |
| Single get / edit / delete / restore of a record | The record's creator only (`userId`) — even for the form owner |
| Bulk edit | Owner: all rows; others: their own |
| Bulk delete | The caller's own rows only |
| Python analysis | `records` with `formId`, `userId ==` requesting user, `status` active |

For parity the Data Service row source keeps `_created_by` and applies the same filter per
operation. Changing any of these rules is a **product decision outside this migration**.

### 4.4 What stays in Node around every write (both stores)

The DataStore returns a `WriteResult`; the calling service keeps the same effects. For
`dataservice` forms they move into the protocol:

| Concern | Today | Data Service |
|---|---|---|
| Entry quota | `Form.entryCount` + `Payment.used_entries` deltas | Small edit: guarded `$inc` inside the edit transaction; Delta job: reserved in the **same transaction as the operation insert**, settled at publish ([consistency §7](consistency.md#7-quota-reservations)) |
| `Form.dataVersion` | `incrementFormDataVersion` in the write transaction | = the latest value-changing revision (`manifest.dataVersion`), set in the edit or publish transaction; a compaction leaves it |
| Analysis staleness | `markAllDirtyForForm` / `incrementVersionForColumns` | From the changed fields plus the derived fields that depend on them (Node's derived dependency graph, §7.2), in the edit or publish transaction |
| Audit | `auditScope()` / activity log | Unchanged; the atomic audit record is written in the edit or publish transaction |
| Revision / undo | Revision layer snapshots (Mongo) | `RevisionBatch` + `dataset_changes` in the same transaction; change-log undo ([consistency §8](consistency.md#8-history-and-undo)) |

For `dataservice` forms there are two write paths:

- **Small edits** (at most `BULK_EDIT_MAX_ROWS` = 500 rows per request or save part): the
  Mongo transaction still wraps the row write — Node writes the patches, the new revision and
  every effect above in **one edit transaction**, and answers the FE when it commits
  ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)).
- **Delta jobs** (ingest, transforms, set-based edits beyond the cap, …): the write is a
  **durable operation** (`data_operations` with an immutable encrypted payload, created before
  any data is touched). The writer commits it to Delta under a fenced lease and marks it
  `committed`; then **Node publishes** it in one Mongo transaction, strictly in revision order,
  and the operation becomes `applied`. **Node answers the FE only when the operation is
  `applied`** — never on `committed`, and the FE does not poll for it. Full protocol:
  [consistency §5](consistency.md#5-durable-operations), [§6](consistency.md#6-publish--one-mongo-transaction-every-critical-effect).

### 4.5 Grid compatibility

The FE keeps calling `POST /forms/:formId/records` with the same body and gets the same
response shape: `DataServiceStore` returns **`RecordDocument`-shaped rows, label-resolved**
as today. `RecordsCoreReadService` picks the store:

| Grid path today | Data Service equivalent |
|---|---|
| Fast path (default sort, offset/cursor) | `rows:query`, default order `createdAt desc, _id desc`; child rows by `rowIndex` |
| Column sort (reads every row, sorts in memory) | `rows:query` with `sort` — done in DuckDB, not in Node |
| Filter / search pipeline (`buildRecordGridPipeline`) | `rows:query` with the same filter tree, compiled to SQL |

- The existing cursor carries the revision **inside** it; the FE decodes it and its format is
  unchanged ([consistency §12](consistency.md#12-caches-and-pinned-pagination)).
- `limit` keeps today's API maximum (**1000**, `listRecordsRequestSchema`); one additive
  request field, `columns` (only visible field ids).
- Filter tree: today's operators (text contains/equals; number equals/gt/lt/between; date
  equals/before/after/between; choice in/equals) map one-to-one, including the
  `rowIndex` / follow-up scope and search parity; `isEmpty` is new.
- **Export:** below a configured row threshold, today's synchronous `arraybuffer` response is
  kept; above it, a new async export route (job + S3 link). The FE changes only for the large
  path.

### 4.6 Python analysis on Data Service datasets

This is the **existing, live** Python analysis service (not the new Data Service, which is not
built yet). It stays a separate service (D13). It has **two readers** of `records`: the
analysis shared repository (`app/features/shared/repository.py`, `db.records.find(...)` → full
list in memory) and `bulk_report` (which also filters `deletedAt`). Both read `_id`,
`uniqueIdentifierValue`, `parentFieldId`, `followUpNumber`, `rowIndex`. Plan:

1. Introduce a **RowSource** at both seams: `MongoRowSource` (today) and `DataServiceRowSource`,
   which **asks the Data Service** for the rows of the published revision Node passes in —
   projected to the variables the request uses, filters pushed down, the same row-scope filter
   as §4.3, derived columns included (computed at read, §7.2). The analysis service never reads
   Delta files or `dataset_patches` itself; the Data Service's one reader merges them
   ([consistency §15.1](consistency.md#151-the-reader)).
2. Phase 1 of this: `DataServiceRowSource` yields the **same in-memory record shape and fields**
   the engines expect (`data[] = {fieldId, value, optionIds, rowIndex, …}`) — zero engine
   changes, correct results, fine up to ~1 M rows (target).
3. Phase 2: engines move to columnar frames of the needed columns (pandas, already a
   dependency) one analysis at a time, behind parity tests, for very large datasets.
4. Long analyses move from the 30 s synchronous call to a job (the Data Service job runner),
   with the existing recompute flow polling its result. Analyses store `computedAtRevision`.

### 4.7 Nested data (table fields, follow-ups)

Records hold table-field sub-rows (`rowIndex`, `parentFieldId`) and follow-up visits
(`followUpNumber`, `followUpStatus`) inside one document. Imported flat files have neither;
CRF forms created on the Data Service will. Model as child Delta tables keyed to the parent
row: `table_<fieldId>` keyed by (`_row_id`, `parentFieldId`, `rowIndex`, `followUp`) and
`followups(_row_id, follow_up, status, f_<fieldId>…)`, with `followUp = 0` the main visit
([consistency §9](consistency.md#9-cell-addressing-and-values)). Grid, export and analysis
join them on `_row_id` where requested.

### 4.8 Write concurrency and uniqueness

An in-process lock does not work across Node instances, query-service instances and job
runners, and a Delta commit is not a uniqueness constraint.

**Small edits** serialize in Mongo: the edit transaction compare-and-sets
`datasets.currentRevision` from n to n+1, so of two concurrent edits one commits and the other
re-reads at n+1 and commits next; on the same cell the later commit wins
([consistency §14.3](consistency.md#143-concurrency--edits-serialize-on-the-revision)).

**Delta jobs** (ingest, transforms, set-based edits, compaction, dataset restore) share one
model:

- **Queue:** `data_operations` is the ordered per-dataset queue. A Node BullMQ dispatcher adds
  a job with `jobId = datasetId`, `removeOnComplete`; the writer drains pending operations in
  `seq` order and re-checks pending before releasing the lease ([consistency §4.4](consistency.md#44-ordering-and-wake-ups-standard-bullmq)).
- **Lease:** in a separate `dataset_leases` collection, so the heartbeat never conflicts with
  the publish transaction ([consistency §4.1](consistency.md#41-lease)).
- **Fencing:** every Delta write goes through **one wrapper** that adds the `txn` token
  ([consistency §4.2](consistency.md#42-every-commit-is-fenced)); concurrent commits are
  coordinated by S3 conditional writes ([consistency §3](consistency.md#3-s3-commit-coordination-conditional-writes)).
- **Reconcile:** a new lease holder fences every table, rolls forward `committed` operations,
  and `RESTORE`s the rest before writing ([consistency §4.3](consistency.md#43-acquire--fence-every-table-then-reconcile)).
- **Heavy work** runs outside the lease and commits briefly, retrying if any table moved
  ([consistency §4.5](consistency.md#45-heavy-work-is-done-outside-the-lease)).

The unique identifier: a small edit claims every value it makes live in
`dataset_unique_claims` (unique index) and checks revision n through the reader, inside its
transaction ([consistency §14.4](consistency.md#144-unique-identifier)); a Delta job checks the
whole column before its fenced commit.

---

## 5. Storage layout (S3)

```
s3://<bucket>/datasets/<datasetId>/
  raw/<uploadId>/<original-file-name>          # immutable upload, kept per retention policy
  staged/<uploadId>.parquet                     # the upload converted once (§7.1); kept with raw/
  table/                                        # Delta table: base data
     _delta_log/…                               # versions (pinned by published revisions)
     part-*.parquet
  table_<fieldId>/, followups/                  # Delta tables: nested data (§4.7)
  changes/<jobId>.parquet                       # before/after of one big Delta job (undo)
  exports/<jobId>/<file>
  reports/<jobId>.json                          # cleaning / transform reports
ops/<datasetId>/<opId>.json.gz                  # large operation payloads (SSE-KMS)
```

- Every row has an **internal** `_row_id` (int64, allocated from `manifest.maxRowId` by the
  Delta writer, never reused once published — a row a small edit inserted gets it when
  compaction folds it) and a **public id `_rid` (ObjectId string)**, so existing
  `objectIdSchema` validators and ObjectId-based revision models keep working
  ([consistency §10](consistency.md#10-allocation)). APIs, edits, undo and the grid address
  rows by `_rid`, never by position; `_row_id` never leaves the Data Service.
- Small edits are not on S3: they are `dataset_patches` in Mongo (§6). No table is read "at
  latest": every read resolves the dataset's **published revision**, which pins the version of
  every table and the patch range ([consistency §2](consistency.md#2-revision-manifest)).
- Column names in the Delta table are **field ids** (`f_<fieldId>`), not user labels, so a
  rename is a metadata change only. Labels live in Mongo. Derived fields have **no** Delta
  column: they are computed at read (§7.2).
- Values are stored in **typed nullable columns** (`v_str`, `v_num`, `v_date`, `v_opts`,
  `v_files`): choice values are option id strings (`v_opts`); file values, today arrays of URL
  strings, become `v_files list<struct{url, key?, name?, size?}>` with the original URL kept
  ([consistency §9](consistency.md#9-cell-addressing-and-values)).
- Coded columns keep the code in the table; the code → label map is metadata (value labels).
- Encryption: S3 SSE-KMS. The bucket policy lets only the Data Service role read or write the
  Delta tables, so nothing can read Delta without the one reader; Node only presigns uploads
  into `raw/` and downloads from `exports/`.

## 6. Edits without rewriting the dataset

A cell edit on a 30-million-row table must not rewrite Parquet files — and, with hybrid edits
(decided 2026-10-24), it does not touch Delta at all. Model:

1. **Patches in Mongo** (`dataset_patches`): `{ datasetId, seq, target, op, value, opId,
   batchId, actor, at }`, where `target` addresses main, table-row and follow-up cells and
   `value` is typed `{ t, v }` ([consistency §9](consistency.md#9-cell-addressing-and-values)).
   `seq` comes from the `datasets.patchSeq` counter.
2. **Edit transaction** — Node reads the current values of the targets at revision n through
   the one reader, then in **one Mongo transaction**: compare-and-set `currentRevision` n → n+1,
   insert the patches, the revision n+1 (same Delta versions, `patches.toSeq` moved),
   `Form.dataVersion`, analysis staleness, change-log entries, `RevisionBatch`, idempotency
   record, quota, audit and the event outbox. No Delta commit, no lease, no queue. A concurrent
   edit makes the CAS miss once and re-runs at n+1, so edits serialize and the later commit
   wins on the same cell ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)).
3. **Read contract** — the one reader reads the Delta tables at the manifest's versions and the
   patches with **`patches.fromSeq < seq ≤ patches.toSeq`** (those ≤ `fromSeq` are already in
   base), in one DuckDB query:
   1. **Rows** = base rows **∪** rows created by `insert_row` patches (a new row exists only in
      patches until compaction — a plain `base LEFT JOIN patches` would hide it).
   2. **Row state** = the latest `delete_row` / `restore_row` patch per row (else the row's
      own `_deleted_at`); deleted rows are dropped unless the trash is being read.
   3. **Cells** = the latest `set` patch per cell target (by `seq`), else the row's value.
   Grid, analysis, export and every job read this way, so they see identical rows for one
   revision ([consistency §15.1](consistency.md#151-the-reader)). An edit publishes a new
   revision, so the grid sees it at once.
4. **Compaction** (a Delta job, small and frequent, triggered by configuration) reads a pinned
   revision through the reader outside the lease, writes the Delta files folding patches up to
   its watermark `W` (that revision's `patches.toSeq`), then commits briefly under the lease and
   publishes `patches.fromSeq = W`. Values never change, so nothing turns stale. The commit is
   refused and re-prepared if any later revision rewrote base or changed the schema
   ([consistency §4.5](consistency.md#45-heavy-work-is-done-outside-the-lease), [§15.2](consistency.md#152-compaction)).
   Patches are deleted from Mongo later, only when no retained or pinned revision still reads
   them ([consistency §15.3](consistency.md#153-patch-retention)).
5. **Undo** reads the change log `dataset_changes` (Mongo, kept through compaction) and writes
   the batch's before-values per cell as a new small edit, with conflict detection when a later
   batch touched the same cell — never a whole-dataset restore
   ([consistency §8](consistency.md#8-history-and-undo)). Restoring a whole revision is a
   separate, explicit action.
6. Bulk edit / column paste = one edit transaction per save part (≤ `BULK_EDIT_MAX_ROWS`, 500
   rows). Bigger set-based changes (e.g. a rule over all rows) skip patches and run as a Delta
   job producing a new revision; they write their before-values to a change file
   (`changes/<jobId>.parquet`) for undo within the retention window.

### 6.1 Editing at 10 GB scale (decision D7)

The size of the dataset does not change the cost of an edit; the size of the **change** does.

| Edit | Rows touched | How | Speed (target) | Undo |
|---|---|---|---|---|
| One cell / one row | 1 | Mongo patches, one edit transaction | < 300 ms (target) | Targeted undo from `dataset_changes` |
| Bulk edit / paste (selected rows) | ≤ `BULK_EDIT_MAX_ROWS` (500) per save part — today's cap; a paste of up to 5,000 rows goes in parts | One edit transaction per part | < 2 s per part (target) | Targeted batch undo, conflicts shown |
| "Set column X where <filter>" / paste over all filtered rows | beyond the cap, up to all rows | **Set-based Delta job**: `UPDATE … WHERE` compiled from the filter → new revision; before-values written to its change file | seconds–minutes (target; job with progress) | Targeted undo of that column/filter (only the cells this job changed) |
| Add / delete / rename column | schema | Metadata (rename) or Delta schema evolution (add); delete = hide, purged after retention | < 2 s (target) | Revision of the form schema |
| Create / change a derived field | schema only | Schema-only revision (new `schemaRef`); nothing written to Delta or patches (§7.2) | as an edit (one Mongo transaction) | Revision of the form schema |
| Change column type | all rows of one column | Synchronous duplicate check first for unique fields; field enters a **migration-pending** state; job casts into a new column, keeps the old until confirmed; derived columns that read it follow at read (§7.2) — nothing to recompute | job | Old column kept for the undo window |
| Add rows / delete rows | n (≤ the cap; more → Delta job) | Patches `insert_row` / `delete_row` / `restore_row` | as patches | Targeted undo |

Whole-dataset restore to an earlier revision exists, but only as an explicit, separate
action — never as the implementation of undo.

The grid never waits on a job: a job's result appears as a new revision, and the UI shows
"applying…" with progress for that column/filter until it lands.

## 7. Import, derived fields, transforms and cleaning

- Transforms and cleaning rules are **declarative specs** stored in Mongo, from catalogues
  (the existing transform/derivation catalogue is extended, not duplicated).
- The Data Service compiles a spec to DuckDB SQL expressions — **never** `eval` of user input.
- Each transform or cleaning run reads revision N (pinned) and publishes revision N+1 through the
  normal operation path; the report (rule → rows affected, examples, before/after counts) is
  saved to `reports/`. Derived fields differ: nothing is stored (§7.2).
- Lineage: each derived field records its inputs and spec id.
- Rule catalogue (initial): range, allowed values, missing codes, regex/format, duplicates on
  key, trim/case, type coercion, unit conversion, recode map, outlier flag (flag only).

### 7.1 Import and type detection (new forms)

For `dataservice` forms every import format runs in the Data Service (D12): xlsx first, then
CSV and Parquet, later `.sav` / `.rds` (Phase 4). Node's import module keeps serving `mongo`
forms only (D11).

1. **Stage once.** The `detect` job converts the upload once to `staged/<uploadId>.parquet`;
   detection, validation and ingest read that file, never the original again.
2. **One classifier per distinct value.** Per column, DuckDB groups the staged values
   (`GROUP BY value` with its count) and one classifier runs on every **distinct** value,
   emitting a **value map** `value → (class, parsed value)`. Type detection (text / number /
   MCQ / date, multi-value cells), MCQ / typo / ordinal option suggestions, notices and the
   optional AI refine are computed from these maps — over the whole file, not a sample (P1).
3. **Validate on confirm.** Node sends the user's edited fields to
   `POST /v1/datasets/{id}:validate` (synchronous, [api §2.5](api.md#25-import-validation)),
   which returns the issues (today's message text), the normalized fields and the final **form
   draft**: types, deduped and ordered options, and the correction map. Node does auth, Zod
   shape, quota and job status / cancel, and persists exactly that draft to `forms`; it keeps no
   thresholds or option logic for new forms.
4. **Ingest by the same map.** The `ingest` job casts every column by joining the staged values
   on the same value map (with the draft's corrections), so the preview the user confirmed is
   exactly what is committed.

Today's Node rules (`import-type-*.helper.ts`, `import-column-stats.helper.ts`,
`import-options-canonical.helper.ts`) are the **reference**: golden test vectors are built from
them and the Python classifier must pass them. They are not re-implemented in Node for new forms
and not kept "in parity" as a second engine.

### 7.2 Derived fields (new forms)

- **One spec compiler** in the Data Service turns a derived field's spec (formula, score,
  conditional, field-pick — today's catalogue) into a DuckDB expression.
- **Computed at read.** The one reader adds the derived columns to every read of a revision,
  per table (base, `followups`, `table_<fieldId>`), for every consumer: grid (sort, filter,
  counts), export, field stats, and `rows:stream` for the analysis service. They are **never
  stored or patched**: no Delta column, no `dataset_patches`, no backfill, and no
  `derived_recompute` job for `dataservice` forms; compaction has nothing derived to fold.
- **Create / change** a derived field = a **schema-only revision**: a new `schemaRef` holding the
  spec, Delta versions and patch range copied from n, published by Node with the edit
  transaction's revision CAS ([consistency §2.1](consistency.md#21-schema-of-a-revision)); that
  field and the derived fields depending on it are marked stale.
- **An edit does no derived work.** It writes only the source cells; Node marks the dependent
  derived fields stale from its dependency graph in the same transaction.
- **Node keeps only metadata:** the spec as form data (never evaluated in Node), source field
  ids, dedupe keys and the staleness dependency graph. The TypeScript evaluators keep serving
  `mongo` forms (D11).
- **Follow-ups.** Golden vectors from the TypeScript evaluators come first (they have no unit
  tests today); they pin the score `missingPolicy: 'zero'` case where every component is empty
  (today it scores 0) and the numeric-range default. A spec-hash-keyed materialisation at
  compaction is a documented escape hatch, used only if benchmarks show read-time computing is
  too slow.

## 8. Grid queries

- Request: `{ offset | cursor, limit ≤ 1000, columns: [visible field ids], sort: [...],
  filter: <filter tree>, search }` — the same limit as today's API.
- The service validates field ids against the dataset schema, builds parameterised DuckDB SQL,
  and returns rows keyed by `_rid`. Unknown columns or operators are rejected (no SQL injection).
- Every query reads **one published revision**; the revision travels inside the cursor, and
  column-paste id lists send it back (pinned for the session; targets deleted since →
  `ROW_GONE`).
- Separate caches, all on the published revision and `rowScope`: **count**
  `(dataset, revision, rowScope, filter, search)`, **order** (+ `sort`) holding the ordered
  row ids; **pages are not cached** — a page is a slice of the cached order plus the projected
  columns fetched by id. Owner and collaborator never share an entry, and every write
  publishes a new revision, so nothing outlives an edit
  ([consistency §12](consistency.md#12-caches-and-pinned-pagination)).
- Hot-dataset cache: the query service keeps recently used table files on local disk; S3 reads
  only on a miss.
- Sorting tens of millions of rows: target ~1–3 s cold; to be benchmarked in Phase 0.

## 9. Security

1. Data Service is private (VPC); only Node reaches it, with a signed service token per
   request (short-lived, includes `datasetId`, `userId`, scope).
2. Node checks form permissions before issuing the token; the Data Service re-checks that the
   token's `datasetId` matches the path.
3. File parsing never executes code: `pyreadr` / `pyreadstat` read data only; no R process.
4. No row data in logs; logs carry dataset id, job id, counts and timings.
5. Presigned upload URLs are scoped to one key, short-lived, size-capped.
6. Operation payloads are encrypted (inline AES-GCM, or S3 SSE-KMS for large ones).
7. The Data Service's Mongo user can only **read** `dataset_patches`; Delta prefixes are
   reachable only by the Data Service role (§5), and only through its one reader.

## 10. Observability

- Metrics: ingest rows/s, job duration by type and compute, query p50/p95 by operation,
  cache hit ratio, patch-window size (`patchSeq − patches.fromSeq`) and compaction lag,
  edit-transaction duration and CAS retries, Lambda→Fargate escalations, failures by reason,
  operation age by status, reconciliations (roll forward / roll back).
- Traces: Node span → Data Service span (W3C trace context propagated).
- Logs: structured JSON, job id + dataset id + op id on every line.

## 11. Failure handling

| Failure | Behaviour |
|---|---|
| Upload interrupted | Multipart resumes; abandoned uploads expire by S3 lifecycle rule |
| Crash inside a small-edit transaction | Mongo aborts it; nothing is written. The browser's retry with the same idempotency key runs it once |
| Crash after a small edit committed, before the answer | The edit is published; the retry with the same key returns the stored result; events are delivered by the sweeper |
| Small edit keeps losing the revision CAS (very busy dataset) | Re-read and retry up to a configured limit, then `DATASET_BUSY` (retryable) |
| Ingest / compaction / transform crashes before its commit | Heavy work was outside the lease; nothing is published. The job retries from the pinned revision; after N attempts → `failed` with reason |
| Heavy job's short commit finds a table moved | Commit refused (`max_commit_retries=0` + version check); the job retries from the new revision |
| Crash after some commits, before `committed` | Next lease holder `RESTORE`s every table to the manifest version (fenced) and retries the operation from its payload |
| Crash after `committed`, before publish | Next lease holder rolls it forward: Node publishes it before anything else |
| Lambda timeout / OOM | Node re-dispatches the job to Fargate once |
| Writer paused past its lease | Fence commit makes its commit lose (`ConcurrentTransaction`) or be visible to reconciliation; Node rejects its publish |
| Publish rejected or response lost | Writer re-fetches the manifest and reconciles; publishing an `applied` op again is an idempotent 200 |
| Poison operation | After N attempts: rolled back, `failed` (user-visible), the queue moves on |
| Crash before dispatch | Outbox sweeper re-wakes the dataset |
| Query service down | Grid and small edits show a retryable error (an edit reads its before-values through the reader); Delta-job operations stay `pending` and are never lost |

Full rules: [consistency §4.3](consistency.md#43-acquire--fence-every-table-then-reconcile),
[§4.5](consistency.md#45-heavy-work-is-done-outside-the-lease),
[§5](consistency.md#5-durable-operations).
