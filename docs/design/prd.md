# Data Service — PRD

**Status:** Proposed · design under review · not built
**Owner:** Pawan Gupta
**Last updated:** 2026-10-24
**Tech design:** [tech-design.md](tech-design.md) · **Consistency:** [consistency.md](consistency.md) · **Plan:** [implementation-plan.md](implementation-plan.md)

**Vocabulary.** A **revision** is a published state of a whole dataset (`datasets.currentRevision`);
every write publishes one. `Form.dataVersion` is the latest revision that changed values (a
compaction publishes a revision but changes no value, so it leaves `dataVersion`). A Delta **version** is the commit number of a
single Delta table; a revision lists one version per table plus the range of small edits
(Mongo **patches**) it includes. Users only ever see revisions.

---

## 1. Problem

Researchers bring their data to EaseMyResearch to clean it and analyse it. Today the only way
in is an Excel file of at most **10 MB**, and every row becomes a MongoDB document. That
design has reached its limits:

- **Size.** Users have datasets of several GB (one user has a 6 GB file). A row-per-document
  store turns a 6 GB file into an estimated 100–500 GB of documents and indexes, and imports
  at 60–170 rows per second (measured 2026-09-28) — days for tens of millions of rows.
- **Formats.** Research data arrives as CSV, SPSS `.sav` and R `.rds` / `.RData`, not only
  `.xlsx`. SPSS and R files carry value labels (1 = Male) that are lost if the user has to
  re-export to Excel by hand.
- **Analysis speed.** Statistics read two or three columns across every row. A document store
  reads whole rows to do that; a columnar store reads only the columns asked for.
- **Cleaning.** Real data needs cleaning (ranges, missing codes, recodes, duplicates) before
  analysis. Today that is done cell by cell, or outside the app.

## 2. Goal

Let a user bring a dataset of **any size we support (target: up to 10 GB per file)** in
**xlsx, csv, sav, rds/RData or parquet**, see it, clean it, transform it and analyse it with
the same experience they have today for small files — and pay for storage and compute only
in proportion to what they use.

### Success looks like

*(Every number below, and every performance or cost figure in these docs, is a **target**
until the Phase 0 benchmarks — cold cache, concurrent users, a growing patch window, the Mongo
edit transaction on the edit path, S3 commit + Mongo publish on the job path — replace it with
a measured value.)*

- Target: a 6 GB CSV is uploaded, converted and browsable in the grid in **under 15 minutes**.
- Target: scrolling, sorting and filtering the grid on 30 million rows answers in **under 2
  seconds** (p95), and the first screen in under 1 second.
- No edit is ever lost or applied twice; undo reverts exactly the cells of the batch undone
  (per cell — later edits to other cells of the row are kept).
- An SPSS file imports with its value labels: coded columns appear as their labels.
- Every cleaning rule run produces a report, and the raw upload is never modified.
- Existing forms keep working exactly as today, with **no data migration**.

### Non-goals (v1)

- Moving existing forms off MongoDB. Old forms stay where they are (see §5).
- Real-time multi-user co-editing of the same cell.
- Files above 10 GB (decision D6), and SAS / Stata in v1 (Stata `.dta` comes nearly free
  later — same library as SPSS).
- Running user-written code (R or Python scripts) on the server.

---

## 3. Users and use cases

| User | Use case |
|---|---|
| Thesis student | Uploads a 2 MB SPSS file from a statistician; wants labels, not codes |
| Clinical researcher | Uploads a 6 GB CSV export of hospital records (e.g. CPT codes); filters to the cohort; cleans; runs tables |
| Statistician | Uploads an R `.rds` of a cleaned data frame; runs analyses in the app |
| Data-entry team | Keeps entering CRF records one by one into an existing form (unchanged) |

## 4. Requirements

### 4.1 Upload and formats

| ID | Requirement |
|---|---|
| U1 | Accept `.xlsx`, `.csv`, `.sav`, `.rds`, `.RData`/`.rda`, `.parquet` |
| U2 | Upload straight to S3 (multipart, resumable); the API server never holds the file |
| U3 | Max file size is configuration, not code; initial 10 GB |
| U4 | CSV: detect delimiter (`,` `;` tab `\|`), encoding (UTF-8, UTF-8 BOM, Windows-1252) |
| U5 | SPSS: keep value labels, variable labels, user-missing codes (e.g. 99 → missing) |
| U6 | R: keep factor levels as labels; `.RData` with several data frames → user picks one |
| U7 | Show progress and allow cancel for every long step |
| U8 | Large R files beyond the conversion memory limit get a clear message: export as Parquet or CSV |
| U9 | Header-row detection (CSV / xlsx): find the header row below title or blank rows; the user can override it in the preview |

### 4.2 Preview and schema

| ID | Requirement |
|---|---|
| P1 | Type detection (text, number, MCQ, date) over the **whole** file, not a sample, with the same correction rules as today's import |
| P2 | Coded columns: show code → label mapping; allow a separate codebook file for CSV/xlsx |
| P3 | The user can change a column's type before confirming; suggestions follow the chosen type |
| P4 | Thousands of columns must render: column virtualisation in the grid, and a **paged schema** (preview, type editing and field lists load columns in pages, never the whole schema at once) |

### 4.3 Grid (records view)

| ID | Requirement |
|---|---|
| G1 | Rows and columns are both virtualised; the server returns only the visible window |
| G2 | Sort by any column, multi-column sort |
| G3 | Filter by any column (equals, in, range, contains, is empty), combined with AND/OR |
| G4 | Search across text columns |
| G5 | Counts (filtered row count) shown and consistent with what the grid lists |

### 4.4 Editing

| ID | Requirement |
|---|---|
| E1 | Edit a cell; edit many cells (bulk edit); paste a column into all rows (today's column paste) |
| E2 | Add / delete rows; add / delete / rename columns |
| E3 | Every save is undoable, singly and as a batch (today's revision contracts; undo is per cell, with consent for `losses` — [consistency §8.2](consistency.md#82-todays-contracts-on-the-change-log)) |
| E4 | Edits on 30 million rows stay fast: an edit must not rewrite the dataset (small edits are Mongo patches, D8) |
| E5 | Deleting a column is undoable: the column is archived through the undo window and restore brings it back |

### 4.5 Transform and cleaning

| ID | Requirement |
|---|---|
| T1 | Derived columns, recodes, binning, date differences — the existing transform catalogue |
| T2 | Cleaning rules declared as data (catalogue/config), never hard-coded: range, allowed values, missing codes, duplicates, type coercion, trimming, unit conversion |
| T3 | Each run produces a report: rule, rows affected, examples, before/after |
| T4 | Raw data is immutable; every transform/cleaning run publishes a new **revision**; the user can go back |
| T5 | Lineage: every column records what produced it — which upload (and sheet / object), or which transform / cleaning run with its inputs and rule |

### 4.6 Analysis, export, lifecycle

| ID | Requirement |
|---|---|
| A1 | All existing analyses run on Data Service datasets with the same results as on MongoDB forms |
| A2 | Export the current revision to xlsx / csv / sav / parquet |
| A3 | Delete a dataset: soft delete flags the form only (as today), restore within today's window, then purge after it |
| A4 | Quotas (rows, storage) enforced per plan, from configuration |

### 4.7 Non-functional

| ID | Requirement |
|---|---|
| N1 | Tenant isolation: a dataset is reachable only through Node, which checks form permissions |
| N2 | PHI: data encrypted at rest (S3 SSE) and in transit; no row data in logs |
| N3 | Every write is audited through the existing audit trail |
| N4 | Cost scales with use: no always-on cluster sized for the largest dataset |
| N5 | Observability: metrics, traces and logs for every job, query and operation |

### 4.8 Configuration

All of these are configuration, not code. Defaults already set by today's product are kept;
the rest are fixed by the Phase 0 prototype and benchmarks.

| Setting | Value |
|---|---|
| Writer lease TTL / heartbeat interval | TBD in Phase 0 |
| Sweeper: age of a pending operation before re-dispatch | TBD in Phase 0 |
| Poison operation: failed attempts before `failed` | TBD in Phase 0 |
| Export: size above which export is an async job | TBD in Phase 0 |
| Small-edit cap (rows per edit transaction) | `BULK_EDIT_MAX_ROWS` = 500 — today's constant |
| Small edit: revision CAS retries before `DATASET_BUSY` | TBD in Phase 0 |
| Compaction trigger: patch count, idle time, maximum frequency | initially ~1,000 patches · ~5 min idle · max frequency TBD in Phase 0 |
| Idempotency keys | 24 h |
| Undo window | today's rule — newest 10 batches per row, older ones while < 30 days |
| Mongo patches (`dataset_patches`) | until no retained or pinned revision needs them |
| Trash (soft-deleted rows) | 45 days |
| Explicit dataset restore window | 30 days |
| Delta log / deleted-file retention | ≥ longest window + margin (initially 60 days) |
| Raw uploads, operation payloads | until purge / until `applied` + undo window |
| Max file size | 10 GB (U3) |

Retention mechanics: [consistency §11](consistency.md#11-retention).

## 5. Coexistence with today's data — no migration

- Every form gets a storage flag: `mongo` or `dataservice`.
- Forms created before the cut-over stay `mongo` **for life**, including new records and
  follow-ups entered into them later. One form's data is never split across two stores.
- Forms created after the cut-over, and every new import, are `dataservice`.
- All features reach row data through one **DataStore** interface with two implementations,
  so a feature works the same on both. See [tech-design.md §4](tech-design.md#4-datastore-abstraction).
- Optional later: move an old form on demand (user action or size threshold). Not required.

## 6. Decisions taken

| # | Decision | Date |
|---|---|---|
| D1 | A separate Python Data Service owns row storage and compute; Node orchestrates | 2026-10-21 |
| D2 | Old data stays in MongoDB; only new forms/imports go to the Data Service | 2026-10-21 |
| D3 | SPSS and R parsing is done in Python (pyreadstat / pyreadr) | 2026-10-21 |
| D4 | Neither MongoDB nor Databricks for row data: **Delta Lake on S3 + DuckDB/Polars** (Mongo keeps metadata) | 2026-10-21 |
| D5 | The Data Service is a **new repository** | 2026-10-21 |
| D6 | Max file size **10 GB** | 2026-10-21 |
| D7 | **Every feature that exists today works on the Data Service**, including cell editing on large datasets — see [feature-parity.md](feature-parity.md) | 2026-10-21 |
| D8 | **Hybrid edits:** small edits (≤ 500 rows) are Mongo patches written by Node in one transaction — no Delta commit, lease or queue; the uploaded dataset and every big or set-based change stay in Delta; one Data Service reader merges both for every read; compaction folds patches into Delta. The Data Service reads only `dataset_patches`, read-only — [consistency §14–§15](consistency.md#14-small-edits--one-mongo-transaction) | 2026-10-24 |
| D9 | Many users edit one dataset at once; on the same cell the later commit wins (Google Docs style), the earlier value stays in history and undo | 2026-10-24 |
| D10 | No DynamoDB lock table: Delta commits on S3 are coordinated by S3 conditional writes (C0 passed) | 2026-10-24 |

## 7. Open questions

| # | Question | Needed by |
|---|---|---|
| Q3 | Typical dataset size (rows × columns) of real users, and 2–3 sample files | Phase 0 |
| Q5 | AWS budget; region (ap-south-1 assumed) — the design works without it, Phase 0 infra waits for it | Phase 0 infra |
