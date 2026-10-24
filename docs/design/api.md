# Data Service — APIs

**Status:** Proposed · not built
**Last updated:** 2026-10-24
**Back to:** [tech-design.md](tech-design.md) · [consistency.md](consistency.md) · [data-model.md](data-model.md)

Three layers: the **public API** on Node (the FE only ever talks to Node), the **internal API**
Node → Python Data Service, and the **writer endpoints** Data Service → Node (Node is the only
publisher). Small edits have no internal write endpoint at all: Node writes them to Mongo
itself ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)). Shapes are
indicative; final DTOs are Zod (Node) and Pydantic (Python) schemas generated from one
contract file.

---

## 1. Public API (Node, `/api/v2`, JWT)

### 1.1 Unchanged — routed by `form.storage`

Every existing records route keeps its path, request and response shape (paths below are the
real ones in `src/v2/modules/record` and `src/v2/modules/revision`). Node dispatches to
`MongoStore` or `DataServiceStore`. A **small edit** (at most `BULK_EDIT_MAX_ROWS` = 500 rows
per request or save part) is one Mongo edit transaction, and Node answers once it has committed
([consistency §14.2](consistency.md#142-the-edit-transaction)); anything bigger is a **Delta
job** operation, and Node answers only once it is **`applied`** ([consistency §5.3](consistency.md#53-boundaries)).

**Retries:** every write carries an `Idempotency-Key` header (bulk edit's existing `mutationId`
serves as its key). Same key + same payload → the same write and its result; same key +
different payload → `409 IDEMPOTENCY_MISMATCH`. A timed-out request is not a cancelled one —
retrying with the key returns the committed edit or picks the operation up
([consistency §14.5](consistency.md#145-idempotency), [§5.1](consistency.md#51-payload-and-outbox)).

| Route | Notes for `dataservice` forms |
|---|---|
| `POST /forms/:formId/records` (grid list) | Additive body fields: `columns?: string[]` (visible field ids), higher `limit` cap from config. The revision rides **inside** the existing cursor (format unchanged), so later pages read the same snapshot |
| `GET /forms/:formId/records/:recordId`, `…/:recordId/view` | `recordId` = the row's public `_rid` (an ObjectId string), so existing validators keep working |
| `POST /forms/records` (submit, `formId` in the body) · `POST /forms/:formId/records/add-row` | Small edit: `insert_row` + `set` patches in one edit transaction; quota checked in it |
| `POST /forms/records-followup` (`formId`, `recordId`, `followUpNumber` in the body) | Small edit: patches on the follow-up cells (`scope.kind: 'followup'`, or `'table'` with its `followUp`) |
| `POST /forms/:formId/records/:recordId` | Row edit → one edit transaction (only the changed cells) |
| `PATCH /forms/:formId/records/bulk-edit` | One edit transaction per request / save part (also the target of **column paste**); today's 500-row cap (`BULK_EDIT_MAX_ROWS`) is the small-edit cap; bigger rule-based changes are set-based Delta jobs |
| `POST /forms/:formId/record-ids` | Grid-order ids for column paste; carries its revision, targets deleted since → `ROW_GONE` |
| `DELETE /forms/:formId/records/:recordId` · `DELETE /forms/:formId/records/bulk-delete` | Small edit: `delete_row` patches (bulk delete ≤ 100 ids today) |
| `PATCH /forms/:formId/records/restore` · `GET /forms/:formId/records/deleted` | Restore from trash: `restore_row` patches, counted against quota like an insert (a selection above the cap → `restore_rows` Delta job) · trash list |
| `GET /forms/:formId/records/check-unique` | Checked through the reader against the current revision; the edit transaction re-checks (claims + reader, [consistency §14.4](consistency.md#144-unique-identifier)) |
| `POST /forms/:formId/records/import` | An append Delta job (operation); large files use §1.2–1.3 |
| `POST /forms/:formId/columns` · `POST /forms/:formId/fields/toggle-hidden` | Column add = schema change applied in the publish transaction · display only |
| `GET /forms/:formId/export-records` | Becomes an `export` **job** for large datasets (returns job id; file link when done) |
| `GET /revisions/batches` · `GET /revisions/batches/:mutationId/restore-preview` · `POST /revisions/batches/:mutationId/restore` | Batch = `dataset_changes.batchId` (`≡ mutationId`); same response shapes, per-cell undo; the restore is a small edit ([consistency §8.2](consistency.md#82-todays-contracts-on-the-change-log)) |
| `GET /revisions/:entityType/:entityId` · `…/restore-preview` · `POST …/restore` · `GET …/:version` · `GET …/:version/changes` | Per-row history (`entityType = record`) built from `dataset_changes` for that `_rid` |

### 1.2 New — uploads (S3 multipart, direct from the browser)

| Method · Path | Body / query | Returns |
|---|---|---|
| `POST /data-uploads` | `{ fileName, sizeBytes, format }` | `{ uploadId, key, partSize, parts: n }` (checks plan file-size limit) |
| `POST /data-uploads/:uploadId/parts` | `{ partNumbers: number[] }` | Presigned PUT URLs (short-lived) |
| `POST /data-uploads/:uploadId/complete` | `{ parts: [{ partNumber, eTag }] }` | `{ status: 'uploaded' }` |
| `DELETE /data-uploads/:uploadId` | — | Aborts the multipart upload |

### 1.3 New — datasets and ingest

| Method · Path | Purpose |
|---|---|
| `POST /datasets` | `{ uploadId, formName, options: { delimiter?, encoding?, sheet?, rObject?, codebookUploadId? } }` → creates form (`storage: 'dataservice'`) + dataset, starts the read-only **detect** job |
| `GET /datasets/:datasetId/preview` | Detected schema: columns, types, value labels, corrections, notices (same shape as today's import preview) |
| `POST /datasets/:datasetId/confirm` | Final column types/options (same body as today's import confirm) → starts the **ingest** job |
| `GET /datasets/:datasetId` | Status, row count, current revision, storage bytes |
| `GET /datasets/:datasetId/revisions` | User-facing history (kinds other than `edit` and `compact`: ingest, transform, clean, restore, …) |
| `POST /datasets/:datasetId/revisions/:revision/restore` | Explicit whole-dataset restore (new revision, audited, confirmation required; only inside the retention window). Not used for undo |

### 1.4 New — jobs

| Method · Path | Purpose |
|---|---|
| `GET /data-jobs/:jobId` | `{ type, status, progress, error, output }` |
| `POST /data-jobs/:jobId/cancel` | Sets `cancelRequested`; job stops at the next chunk |

### 1.5 New — transforms and cleaning

| Method · Path | Purpose |
|---|---|
| `GET /data-rules/catalogue` | Available transform / cleaning rules with parameter schemas |
| `POST /datasets/:datasetId/specs:validate` | Validates a spec against the schema and catalogue |
| `POST /datasets/:datasetId/specs:preview` | Runs the spec on a sample → before/after + affected-row estimate |
| `POST /datasets/:datasetId/specs:run` | Runs on the whole dataset → job → new revision + report |
| `GET /datasets/:datasetId/reports/:jobId` | Cleaning / transform report |

### 1.6 New — export

| Method · Path | Purpose |
|---|---|
| `POST /datasets/:datasetId/exports` | `{ format: 'xlsx' \| 'csv' \| 'sav' \| 'parquet', columns?, filter? }` → job |
| `GET /datasets/:datasetId/exports/:jobId` | Presigned download URL when done |

All new write routes carry `audit(action)` right after auth (audit catalogue entries added
per route), and `checkFormPermission` like the records routes.

---

## 2. Internal API (Node → Data Service)

Private network only. Every request carries `Authorization: Bearer <service token>` — a
short-lived JWT signed by Node with `{ sub: userId, datasetId, scope: 'read' | 'write' | 'job',
rowScope: 'all' | <userId>, exp ≤ 60 s }`. The Data Service rejects a token whose `datasetId`
differs from the path. `x-request-id` and W3C `traceparent` are propagated. (The Phase 0
scaffold verifies a simpler token so far — `x-service-token: <issuedAt>.<HMAC-SHA256>`, maximum
age from configuration; the claims above are still to be added.)

### 2.1 Query service (synchronous, read-only)

Every query names a **published revision**; Node resolves it (current, or the one in the
grid cursor) and passes its manifest. Nothing is read "at latest". Every endpoint answers
through the **one reader**: the Delta tables at the manifest's versions merged with the
`dataset_patches` of the manifest's range, which the reader reads from Mongo read-only
([consistency §15.1](consistency.md#151-the-reader)).

| Method · Path | Body | Returns |
|---|---|---|
| `POST /v1/datasets/{id}/rows:query` | `{ revision, manifest, columns, filter, sort, search, offset \| cursor, limit }` | `{ revision, rows: [{ _rid, values: {fieldId: typedValue} }], next? }` |
| `POST /v1/datasets/{id}/rows:count` | `{ revision, manifest, filter, search }` | `{ revision, count }` (cache key: dataset, revision, row scope, filter, search) |
| `POST /v1/datasets/{id}/rows:ids` | `{ revision, manifest, filter, sort, cap }` | `{ revision, ids: [...] }` |
| `POST /v1/datasets/{id}/rows/{rid}:get` | `{ revision, manifest }` | One row incl. child tables and follow-ups |
| `POST /v1/datasets/{id}/cells:read` | `{ revision, manifest, targets, unique?: { fieldId, values, exceptRids } }` | `{ revision, cells: [{ target, value }], rows: [{ _rid, state, createdBy }], uniqueTaken: [...] }` — the `before` values, row state / scope and unique check a small edit reads before its transaction ([consistency §14.2](consistency.md#142-the-edit-transaction)) |
| `POST /v1/datasets/{id}/rows:stream` | `{ revision, manifest, columns, filter }` | Rows of a revision (row scope from the token) for the existing Python analysis service and exports (projected columns, filters pushed down) — that service never reads Delta or patches itself |
| `POST /v1/datasets/{id}/fields/{fieldId}:sample` | `{ revision, manifest, n }` | Sample values |
| `POST /v1/datasets/{id}/fields:stats` | `{ revision, manifest, fields, kind: 'mcq' \| 'number' \| 'answered' \| 'distinct' \| 'graph', filter? }` | Stats |
| `POST /v1/datasets/{id}/batches/{batchId}:undo-preview` | `{ revision, manifest }` | `{ entries, excluded, losses, lossSetHash, stateHashes, coverage }` |

### 2.2 Writes — operations and the drain wake-up

Writes are **not** sent to the Data Service as requests. Small edits never reach it: Node
writes them to `dataset_patches` in its edit transaction ([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)).
For a Delta job Node inserts the `data_operations` document (payload inline-encrypted or in S3,
quota reservation in the same transaction) and the dispatcher wakes the writer
([consistency §4.4](consistency.md#44-ordering-and-wake-ups-standard-bullmq)):

| Method · Path | Body | Notes |
|---|---|---|
| `POST /v1/datasets/{id}:drain` | `{}` | `202`. The writer acquires the lease, fences and reconciles, then pulls pending operations in `seq` order via §3 and commits each |

The operation types are the writing job types of §2.3 (beyond-cap bulk edits, deletes and
restores among them). Node waits for the operation to reach **`applied`** — signalled by its own
publish transaction (§3), not by polling the Data Service — and only then answers the FE.

**Filter tree** (same operators as today's grid filters):
```json
{ "all": [ { "field": "f1", "op": "between", "value": [18, 60] },
           { "any": [ { "field": "f2", "op": "in", "value": ["opt1", "opt2"] },
                      { "field": "f3", "op": "isEmpty" } ] } ] }
```
Field ids are validated against the dataset schema; values are bound parameters — never
string-concatenated into SQL.

### 2.3 Jobs (asynchronous)

| Method · Path | Body | Notes |
|---|---|---|
| `POST /v1/jobs` | `{ jobId, opId?, type, datasetId, compute, input, revision, callbackUrl }` | Node chooses `compute`; the Data Service starts the Lambda / Fargate task and returns `202`. Heavy work runs outside the lease and commits briefly under it ([consistency §4.5](consistency.md#45-heavy-work-is-done-outside-the-lease)). Node starts `compact` when the configured trigger fires ([consistency §15.2](consistency.md#152-compaction)) |
| `POST /v1/jobs/{jobId}:cancel` | — | Cooperative cancel |

Job types: `ingest`, `compact`, `transform`, `clean`, `bulk_edit`, `bulk_delete`,
`restore_rows`, `column_archive`, `column_restore`, `type_migration`, `reindex_unique`,
`derived_recompute`, `export`, `vacuum`, `purge` — every one that writes data carries an
`opId` and publishes through §3. Plus the read-only `detect` (upload preview).

### 2.4 Job callbacks (Data Service → Node) — progress only

`POST /api/v2/internal/data-jobs/{jobId}/events` — HMAC-signed body:

```json
{ "eventId": "<jobId>:<attempt>:<seq>", "attempt": 2, "seq": 17,
  "event": "progress" | "succeeded" | "failed" | "cancelled",
  "progress": { "done": 1200000, "total": 30000000, "unit": "rows" },
  "output":   { "reportKey": "…", "exportKey": "…" },
  "error":    { "code": "R_FILE_TOO_LARGE_FOR_MEMORY", "message": "…" } }
```

Callbacks only update `data_jobs` (status, progress, report/export keys) and notify the FE.
They **never** apply effects: revision, `dataVersion`, `rowCount`, staleness, quota and audit
happen only in the publish transaction (§3).

Ordering and dedupe: idempotent per `eventId`. Node applies a callback only if its `attempt`
is the job's current attempt and its `seq` is higher than the last applied one, so progress
never moves backwards. A terminal event (`succeeded` / `failed` / `cancelled`) is final: later
callbacks of that attempt are ignored, and callbacks of an older attempt are always ignored.

**Job status vs data.** For a writing job the Data Service's `succeeded` callback only reports
that its output is ready; Node marks the job `succeeded` **in the publish transaction**, when
its operation becomes `applied`. `POST /api/v2/jobs/:id/cancel` is a CAS on the operation: it
wins only before `committed` (the writer then rolls back and the reservation is released);
after that it returns `409 TOO_LATE` and the job finishes as `succeeded`
([consistency §5.2](consistency.md#52-status-machine)).

## 3. Internal API — Data Service → Node (writer endpoints)

All are HMAC-signed, reachable only from the Data Service network, and token-guarded: Node
rejects a request whose `token` is not the current lease token. Every operation transition is a
CAS on `(opId, expectedStatus, token, attempt)`, so a delayed call from an older attempt can
never complete, fail or publish a newer one
([consistency §5.2](consistency.md#52-status-machine)).

| Method · Path (Node) | Body | Purpose |
|---|---|---|
| `POST /internal/datasets/:id/lease` | `{ action: 'acquire' \| 'renew' \| 'release', owner, maxTxnSeen? }` | `dataset_leases` → `{ token, expiresAt }`; acquire is a CAS on the expected previous token, `newToken = max(p, maxTxnSeen) + 1` ([consistency §4.1](consistency.md#41-lease)). Renew also renews the quota reservations of all pending operations |
| `GET /internal/datasets/:id/manifest` | — | Current revision manifest (re-fetched after any publish rejection) |
| `GET /internal/datasets/:id/operations?status=pending` | — | Pending operations in `seq` order |
| `GET /internal/operations/:opId` | — | `{ status, seq, token, attempt, rolledBackAttempts, committedTableVersions }` (used in reconciliation) |
| `GET /internal/operations/:opId/payload` | — | Decrypted payload (inline or from S3, SHA-256 checked); writer role only |
| `POST /internal/operations/:opId/running` | `{ token }` | `dispatched → running`, `attempt + 1` → returns `{ attempt }`. Repeated with the same token while `running` → same `attempt`, no increment. Refused if the operation is `cancelled` |
| `POST /internal/operations/:opId/committed` | `{ token, attempt, committedTableVersions, commit: { baseRevision, tableVersions, result } }` | Only after **all** the operation's table commits succeeded; guard `running` + same token + same attempt; refused if cancelled |
| `POST /internal/operations/:opId/failed` | `{ token, attempt, error, rolledBack: boolean }` | `failed` / `rolled_back`; guard `running` + same token + same attempt; reservation released |
| `POST /internal/operations/:opId/rolled-back` | `{ token, attempt }` | Reconciliation removed this attempt's commits: record it in `rolledBackAttempts` and set the operation `dispatched` again; only for an attempt of an older token that never reached `committed` |
| `POST /internal/datasets/:id/publish` | `{ ops: [{ opId, attempt, result }], token, baseRevision, manifest, commitToken? }` (`commitToken` on a roll-forward of an operation an earlier holder committed) | One Delta-job operation or a group commit ([consistency §4.7](consistency.md#47-many-users-on-one-dataset)), published as one revision. One Mongo transaction: guard (revision, `committed` with this token + attempt, form active, lease CAS, rebase check against small edits since `W`), new revision (Delta versions + `patches` range), `rowCount`, `dataVersion`, mandatory schema snapshot, staleness, quota settle, `RevisionBatch` + change-log entry, audit, `applied`, job `succeeded`, event outbox ([consistency §6](consistency.md#6-publish--one-mongo-transaction-every-critical-effect)). Already applied → `200 { appliedRevision, manifest }` |
| `POST /internal/datasets/:id/freeze` | `{ token, jobId, fieldIds \| 'all', timeoutMs }` · `DELETE` to lift | Field freeze for a starving job ([consistency §4.6](consistency.md#46-field-freeze-starvation-guard)) |
| `GET /internal/datasets/:id/pins` | — | Pinned revisions with their per-table versions, for vacuum ([consistency §11](consistency.md#11-retention)) |

## 4. Error codes (user-safe messages via the friendly-messaging layer)

| Code | Meaning |
|---|---|
| `UNSUPPORTED_FORMAT` | Extension / magic bytes not accepted |
| `FILE_TOO_LARGE` | Above the plan or system limit |
| `R_FILE_TOO_LARGE_FOR_MEMORY` | R file exceeds the conversion memory limit → export as Parquet/CSV |
| `ENCODING_UNDETECTED` | CSV encoding could not be determined → user picks one |
| `RDATA_MULTIPLE_OBJECTS` | `.RData` has several data frames → user picks one |
| `SCHEMA_CONFLICT` | Patch / spec references a field not in the schema, or a value type the field does not allow |
| `DATASET_BUSY` | Queue backlog above the configured limit, or a small edit lost the revision compare-and-set more often than the configured retries; retry |
| `DUPLICATE_UNIQUE_ID` | Unique identifier already present in the revision being written, or claimed by a concurrent edit |
| `ROW_GONE` | A target row was deleted after the revision the request was based on |
| `REVISION_EXPIRED` | A pinned revision is past its retention / session TTL; reload |
| `UNDO_CONFLICT` | Later edits touched the same cells; consent (`lossSetHash`) required |
| `QUOTA_EXCEEDED` | Rows or storage over plan (also when publish re-validation fails) |
| `OPERATION_FAILED` | Poison operation failed after the configured attempts |
| `IDEMPOTENCY_MISMATCH` | The same idempotency key was sent with a different request |
| `TOO_LATE` | Cancel arrived after the operation committed; it completes normally |
| `FORM_DELETED` | The form was deleted before the operation published; nothing was written |
