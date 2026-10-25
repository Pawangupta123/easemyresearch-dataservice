# Data Service — Data Model

**Status:** Proposed · not built
**Last updated:** 2026-10-25
**Back to:** [tech-design.md](tech-design.md) · [consistency.md](consistency.md) · [api.md](api.md)

MongoDB keeps **metadata and the small edits**; the uploaded dataset (big data) lives in Delta
tables on S3. Node is the only publisher: every Mongo document below that a write changes is
changed in Node's edit transaction ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction))
or publish transaction ([consistency §6](consistency.md#6-publish--one-mongo-transaction-every-critical-effect)).
The Data Service reads only `dataset_patches`, read-only.

---

## 1. Changes to existing collections

### `forms` — new fields

| Field | Type | Notes |
|---|---|---|
| `storage` | `'mongo' \| 'dataservice'` | Set once at creation, never changed (except by an explicit future move). Missing = `'mongo'` so no backfill is needed |
| `datasetId` | ObjectId \| null | Set when `storage = 'dataservice'` |
| `dataVersion` | number | Existing field; for `dataservice` forms = the current manifest's `dataVersion`: the latest revision that changed values (a compaction leaves it) |

Fields (`form.fields`) stay the **current** schema for both stores: label, type, options,
value labels, order. The Delta column for a field is `f_<fieldId>`. A schema change carried by
an operation (type cast, column add/archive) is applied to the form in the same publish
transaction and captured as an immutable post-change snapshot in `dataset_schemas` (not the
revision layer's optional capture), which the new revision references as `schemaRef`; pinned
revisions are read with their own snapshot ([consistency §2.1](consistency.md#21-schema-of-a-revision)).

For `dataservice` forms (PRD D12) the imported fields are the form draft returned by the Data
Service's `:validate` (types, deduped and ordered options, correction map), persisted as is
([api §2.5](api.md#25-import-validation)). A derived field keeps its spec and metadata here —
source field ids, dedupe keys, the dependency graph used for staleness — but its values are
never stored: the one reader computes them at read
([tech design §7.2](tech-design.md#72-derived-fields-new-forms)).

### `payments` — quota counters

| Field | Notes |
|---|---|
| `entries_limit` | Existing — plan limit |
| `used_entries` | Existing — live rows already published (moved directly by small-edit inserts / restores / deletes; settled reservations of Delta jobs) |
| `reserved_entries` | **New** — sum of `held` reservations of Delta-job operations not yet settled or released |

Guard on every reservation and every small-edit insert: `used_entries + reserved_entries + n ≤ entries_limit`
([consistency §7](consistency.md#7-quota-reservations), [§14.2](consistency.md#142-the-edit-transaction)).

## 2. New collections

### `datasets`

| Field | Type | Notes |
|---|---|---|
| `_id` | ObjectId | |
| `formId` | ObjectId | 1:1 with the form |
| `ownerId` | ObjectId | Denormalised for quota queries |
| `s3Prefix` | string | `datasets/<id>/` |
| `currentRevision` | number | The published revision readers resolve; every small edit and every publish moves it by compare-and-set ([consistency §2](consistency.md#2-revision-manifest)) |
| `rowCount` | number | Set by the edit transaction (`$inc`) or to `manifest.rowCount` at publish — never computed elsewhere |
| `patchSeq` | number | `$inc` counter that assigns `dataset_patches.seq`; never goes back |
| `opSeq` | number | `$inc` counter that assigns `data_operations.seq` |
| `freezeEpoch` | number | `$inc` whenever a field freeze is set or lifted, so an edit in flight re-reads ([consistency §14.3](consistency.md#143-concurrency--edits-serialize-on-the-revision)) |
| `lastEditAt` | Date | Last small edit — the compaction idle trigger ([consistency §15.2](consistency.md#152-compaction)) |
| `storageBytes` | number | Delta files on S3; drives the storage quota |
| `status` | `'ingesting' \| 'ready' \| 'failed' \| 'deleted'` | |
| `deletedAt` | Date \| null | Soft delete (trash); purge job after the window |
| `createdAt`, `updatedAt` | Date | |

The patch window that triggers compaction is `patchSeq − ` the current revision's
`patches.fromSeq`. The writer lease is **not** on this document (see `dataset_leases`), so
heartbeats never conflict with the edit or publish transaction. Indexes: `{ formId: 1 }`
unique, `{ ownerId: 1, status: 1 }`.

### `dataset_patches`

The small edits ([consistency §14.6](consistency.md#146-dataset_patches)) — immutable, written
by Node's edit transaction, read (read-only) by the Data Service's one reader.

| Field | Type | Notes |
|---|---|---|
| `datasetId` | ObjectId | |
| `seq` | number | From `datasets.patchSeq`; a revision reads `patches.fromSeq < seq ≤ patches.toSeq` |
| `target` | `{ row: _rid, field?, scope: { kind: 'main' \| 'table' \| 'followup', parentField?, rowIndex?, followUp } }` | Main, table-row and follow-up cells; `field` absent for row ops ([consistency §9](consistency.md#9-cell-addressing-and-values)) |
| `op` | `'set' \| 'insert_row' \| 'delete_row' \| 'restore_row'` | |
| `value` | `{ t: 'null' \| 'str' \| 'num' \| 'date' \| 'opts' \| 'files', v }` | `set` only |
| `opId`, `batchId` | string | `batchId ≡` today's `mutationId` |
| `actor`, `at` | | |

Index: `{ datasetId: 1, seq: 1 }` unique. Deleted only by the retention rule once no retained or
pinned revision needs them ([consistency §15.3](consistency.md#153-patch-retention)).

### `dataset_changes`

The change log ([consistency §8.1](consistency.md#81-change-log)): one document per changed
target `{ datasetId, revision, batchId, target, before, after, actor, at }` (`before` / `after`
are typed `{ t, v }` values), written in the transaction that publishes the change. A Delta job
that changes many cells writes one entry for its batch pointing to its change file
(`changes/<jobId>.parquet`). Survives compaction; trimmed only by the undo rule (`keep: 10`
batches per row, older ones while < 30 days). Indexes: `{ datasetId: 1, batchId: 1 }`,
`{ datasetId: 1, 'target.row': 1, revision: -1 }` (per-row history).

### `dataset_unique_claims`

`{ datasetId, fieldId, value, row, seq }` — a value a small edit made live for the
unique-identifier field ([consistency §14.4](consistency.md#144-unique-identifier)). Inserted
and deleted inside the edit transaction; claims with `seq ≤ W` are deleted by the publish that
moves `patches.fromSeq` to `W`. Index: `{ datasetId: 1, fieldId: 1, value: 1 }` unique.

### `dataset_idempotency`

`{ actor, idempotencyKey, payloadHash, datasetId, revision, result, expiresAt }` — the caller's
idempotency key for a small edit, inserted in the edit transaction
([consistency §14.5](consistency.md#145-idempotency)). Indexes: `{ actor: 1, idempotencyKey: 1 }`
unique, TTL on `expiresAt` (initially 24 h).

### `dataset_leases`

`{ datasetId, owner, token, expiresAt, lastPublishedOpId }` — one row per dataset; acquire,
renew and publish are all compare-and-set writes on it (server time `$$NOW`), so a takeover and
a publish can never both commit. `token` is written into every Delta commit as
`txn("emr-writer")` ([consistency §4.1](consistency.md#41-lease)). Small edits never touch it.
Index: `{ datasetId: 1 }` unique.

### `dataset_revisions`

One document per **published revision** — the manifest that binds the version of every Delta
table and the patch range into one snapshot. Inserted in the edit or publish transaction
together with the `datasets.currentRevision` move.

| Field | Type | Notes |
|---|---|---|
| `datasetId` | ObjectId | |
| `revision` | number | Monotonic per dataset; n+1 only on top of n |
| `tables` | `{ base: {version}, followups: {version} \| null, ['table_<fieldId>']: {version} }` | Exact Delta version of every table |
| `patches` | `{ fromSeq, toSeq }` | The revision's `dataset_patches`: `fromSeq < seq ≤ toSeq`; `fromSeq` = already folded into the Delta tables |
| `maxRowId` | number | `_row_id` high-water mark ([consistency §10](consistency.md#10-allocation)) |
| `schemaRef` | ObjectId | → `dataset_schemas`; mandatory post-change snapshot the data matches; never deleted while a retained revision references it |
| `rowCount` | number | Live rows in this revision (copied to `datasets.rowCount`) |
| `dataVersion` | number | The latest revision number that changed values; equal to `revision` except after a compaction, which copies its predecessor's (copied to `Form.dataVersion`) |
| `kind` | operation type (job-type enum below), `edit`, or `schema` (schema-only revision, e.g. a derived-field change — [consistency §2.1](consistency.md#21-schema-of-a-revision)) | `edit` and `compact` never appear in the user-facing version list |
| `opId` \| `batchId` | string | Producing operation (Delta job) or small-edit batch |
| `writerToken` | number \| null | Lease token that produced it; `null` for a small edit or a schema-only revision |
| `specId`, `reportKey` | | For transform / clean |
| `pinnedUntil` | Date \| null | Retention, running jobs, open pagination ([consistency §11](consistency.md#11-retention)) |
| `createdBy`, `publishedAt` | | |

Indexes: `{ datasetId: 1, revision: -1 }` unique, `{ datasetId: 1, pinnedUntil: 1 }` (vacuum
and patch-retention pins).

### `dataset_schemas`

`{ _id, datasetId, hash, fields | { s3Key, sha256 }, sizeBytes, createdAt }` — immutable,
content-addressed post-change field list (types, options, value labels, order, derived-field
specs). Written by the first ingest and by every schema-changing publish (reused when the hash
matches); large ones live in S3 and are uploaded and verified before the publish transaction.
Not the revision layer's optional capture ([consistency §2.1](consistency.md#21-schema-of-a-revision)). Index:
`{ datasetId: 1, hash: 1 }` unique.

### `data_operations`

Durable, ordered per-dataset queue of every **Delta-job** write, inserted before any data is
touched ([consistency §5](consistency.md#5-durable-operations)). Small edits are not operations.

| Field | Type | Notes |
|---|---|---|
| `opId` | string | Unique; internal idempotency key |
| `idempotencyKey`, `payloadHash` | string | From the caller's `Idempotency-Key`; same key + same hash → same operation, different hash → `409` ([consistency §5.1](consistency.md#51-payload-and-outbox)) |
| `seq` | number \| null | From `datasets.opSeq`, assigned only when eligible — `null` while `held` behind a field freeze |
| `datasetId`, `formId`, `actor`, `type` | | `type` = a writing job type below |
| `payload` | `{ inline (AES-GCM, platform key) } \| { s3Key, sha256 }` | Immutable full input; large ones under `ops/<datasetId>/<opId>.json.gz` (SSE-KMS) |
| `status` | `'held' \| 'pending' \| 'dispatched' \| 'running' \| 'committed' \| 'applied' \| 'failed' \| 'rolled_back' \| 'cancelled'` | Every transition is a CAS on `(opId, expectedStatus, token, attempt)`; `committed` = all its table commits exist; `applied` = published ([consistency §5.2](consistency.md#52-status-machine)) |
| `token` | number | Lease token that committed it (checked on roll-forward) |
| `committedTableVersions` | `{ <table>: version }` | Sent with `committed` |
| `commit` | `{ baseRevision, tableVersions, result }` \| null | Sent with `committed`: what a later lease holder publishes when it rolls the operation forward — it never re-runs it ([consistency §5.3](consistency.md#53-boundaries)) |
| `appliedRevision` | number \| null | Set by publish |
| `attempt` | number | Incremented on every move to `running`; carried in every commit's metadata; poison after a configured limit → `failed` |
| `rolledBackAttempts` | number[] | Attempts reconciliation rolled back — their commits never count ([consistency §5.3](consistency.md#53-boundaries)) |
| `result` | `{ rowsAdded, rowsRemoved, rowsChanged, changedFieldIds, touchedFieldIds, batchId }` \| null | `changedFieldIds` → staleness; `touchedFieldIds` (fields whose existing values the job rewrote) → the publish rebase check |
| `error` | `{ code, message }` \| null | User-safe |
| `createdAt`, `committedAt`, `appliedAt` | Date | |

Indexes: `{ opId: 1 }` unique, `{ datasetId: 1, seq: 1 }` unique (partial: `seq` set),
`{ actor: 1, idempotencyKey: 1 }` unique, `{ status: 1, updatedAt: 1 }` (sweeper).

### `dataset_freezes`

`{ datasetId, jobId, fieldIds | 'all', expiresAt }` — an active field freeze
([consistency §4.6](consistency.md#46-field-freeze-starvation-guard)). Set and lifted in a
transaction that also `$inc`s `datasets.freezeEpoch`. Small edits touching frozen fields wait;
new operations touching them are inserted `held`; lifting the freeze (commit, failure, cancel
or `expiresAt`) runs the waiting edits and assigns the held operations their `seq` in arrival
order. Index: `{ datasetId: 1 }`.

### `dataset_events`

Post-publish outbox, inserted **in the edit or publish transaction**:
`{ datasetId, revision, opId | batchId, changedFieldIds, consumers: { <name>: 'pending' | 'done' }, pending: boolean, attempts, createdAt }`.
A Node dispatcher delivers to each consumer (recompute, FE notification, …) with retries and
marks it `done`; a sweeper re-delivers stale ones. Indexes: `{ datasetId: 1, revision: 1 }`
unique, `{ pending: 1, createdAt: 1 }` (sweeper; `pending` is false once every consumer is `done`).

### `entry_reservations`

`{ ownerId, opId, entries, status: 'held' | 'settled' | 'released', expiresAt }` — for
Delta-job operations only, inserted **in the same transaction as the operation**, with `$inc`
of `Payment.reserved_entries` under the guard above; renewed by the writer heartbeat for all
pending operations of a draining dataset; settled in the publish transaction (moves `entries`
from `reserved_entries` to `used_entries`; re-validated if no longer `held`); released by the
sweeper only once the operation is `failed` / `rolled_back` or missing.

### `data_jobs`

| Field | Type | Notes |
|---|---|---|
| `datasetId`, `opId` | ObjectId, string | A job that writes data runs as an operation |
| `type` | `'ingest' \| 'compact' \| 'transform' \| 'clean' \| 'bulk_edit' \| 'bulk_delete' \| 'restore_rows' \| 'column_archive' \| 'column_restore' \| 'type_migration' \| 'reindex_unique' \| 'export' \| 'vacuum' \| 'purge'` (+ read-only `'detect'`: stages the upload to Parquet and builds the value maps) | Writing types carry `opId`; `bulk_edit` / `bulk_delete` / `restore_rows` are the set-based forms beyond the small-edit cap. No derived-recompute type: derived columns are computed at read |
| `compute` | `'lambda' \| 'fargate'` | Chosen by Node ([tech design §3.3](tech-design.md#33-compute-selection-per-job-by-node)) |
| `status` | `'queued' \| 'running' \| 'succeeded' \| 'failed' \| 'cancelled'` | |
| `progress` | `{ done, total, unit: 'rows' \| 'bytes' \| 'chunks' }` | From callbacks (progress only — effects come from publish) |
| `attempts` | number | |
| `input` | object | Upload key, format options, spec id, … (no row data) |
| `output` | object | `appliedRevision` (from publish), report key, export key |
| `error` | `{ code, message }` \| null | User-safe message + internal code |
| `cancelRequested` | boolean | Checked by the job between chunks |
| `startedAt`, `finishedAt` | Date | |

Indexes: `{ datasetId: 1, createdAt: -1 }`, `{ status: 1, updatedAt: 1 }` (stuck-job sweep).

### `dataset_uploads`

| Field | Type | Notes |
|---|---|---|
| `datasetId` \| null | ObjectId | Null until confirmed |
| `userId` | ObjectId | |
| `s3Key`, `uploadId` | string | S3 multipart upload |
| `fileName`, `format`, `sizeBytes` | | `format`: `xlsx \| csv \| sav \| rds \| rdata \| parquet` |
| `status` | `'uploading' \| 'uploaded' \| 'aborted'` | |
| `expiresAt` | Date | TTL index — abandoned uploads disappear |

### `transform_specs` / `cleaning_specs`

Declarative, catalogue-driven specs (one collection with `kind` is also fine):

| Field | Type | Notes |
|---|---|---|
| `datasetId` | ObjectId | |
| `kind` | `'transform' \| 'clean'` | |
| `steps` | array | Each step = `{ rule: <catalogue key>, params: {...}, target: fieldId \| new field }` |
| `createdBy`, `createdAt` | | |

`rule` must exist in the catalogue (config/DB); unknown rules are rejected at validation.

## 3. Delta tables (S3)

Every table is written through the fenced wrapper ([consistency §4.2](consistency.md#42-every-commit-is-fenced))
and read only through the one reader, at the version its manifest lists, merged with the
revision's patches ([consistency §15.1](consistency.md#151-the-reader)). There is no Delta
patch table and no Delta change log: small edits are `dataset_patches`, the change log is
`dataset_changes` (§2).

### Base table `table/`

| Column | Type | Notes |
|---|---|---|
| `_row_id` | int64 | Internal id, `manifest.maxRowId + k`, allocated by the Delta writer (ingest, or compaction for rows a small edit inserted); never reused once published |
| `_rid` | string (ObjectId hex) | **Public** row id used by every API, validator and revision model; never changes |
| `_created_by` | string | User id of the row's creator — collaborator row scope (today's `userId`) |
| `_created_at`, `_updated_at` | timestamp | Default grid order is `_created_at, _rid` (today: `createdAt, _id` — `_rid` is that ObjectId, and it orders rows that exist only in patches too) |
| `_deleted_at` | timestamp \| null | Soft delete (today `status: 'inactive'` + `deletedAt`); hidden from reads, purged after the trash window |
| `uniqueIdentifierValue` | string \| null | Denormalised unique-identifier value (as today); uniqueness checked by the edit transaction (claims + reader) and by Delta jobs before their commit |
| `f_<fieldId>` | per field type | See value types below. Derived fields have no column (computed at read) |

**Value types** (same `t` as patches, [consistency §9](consistency.md#9-cell-addressing-and-values)):
number → float64, text → string (`""` kept), date → date, choices → `list<string>` of option
ids (today `optionIds` ObjectIds), files → `list<struct{url, key?, name?, size?}>` (today an
array of URL strings; `url` kept as is).

Partitioning: none by default (single-dataset tables); Z-ordering/clustering on columns that
are filtered most, decided per dataset by usage later.

### Nested data

| Table | Key | Holds |
|---|---|---|
| `table_<fieldId>/` | `(_row_id, parentFieldId, rowIndex, followUp)` | Sub-rows of a table field per visit (`followUp = 0` main visit); columns `f_<childFieldId>` |
| `followups/` | `(_row_id, followUp)` | One row per follow-up visit: `followUpStatus` (`pending \| completed`), `submitted_at`, `screening_excluded` flag, `f_<fieldId>` columns |

Created only for forms that have table fields / follow-ups (`followups: null` in the manifest
otherwise). Imported flat files have neither. Patches address their cells by the full target
(`scope.kind` `table` / `followup`) and the reader merges them the same way as base cells.

### Job change files `changes/<jobId>.parquet`

Not a Delta table: the before/after values of one Delta job that changed many cells
(set-based edit, transform, cleaning), written with the job's output and referenced by its
`dataset_changes` entry; read by that batch's undo ([consistency §8.1](consistency.md#81-change-log)).

## 4. What stays in MongoDB for `storage = 'mongo'` forms

Everything as today (`records`, revisions, etc.), served by today's Node code — import,
derived-field evaluators, Mongo grid filters, type conversion — unchanged and frozen (PRD D11).
Nothing in this document applies to them.
