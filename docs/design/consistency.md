# Data Service — Consistency Protocol (v3)

**Status:** Proposed (v3.3, 2026-10-24: hybrid edits — small edits as Mongo patches in one Node transaction, one reader, compaction and patch retention, change log in Mongo, unique-identifier claims; v3.2: token CAS, attempt-guarded transitions, watermark + rebase rules, mandatory schema snapshots, held-op freeze, caller idempotency, cancel vs publish, dataset restore invariants) · must be prototyped in Phase 0 before any other build
**Last updated:** 2026-10-24
**Back to:** [tech-design.md](tech-design.md) · [data-model.md](data-model.md) · [api.md](api.md)

v3 answers three reviews (2026-10-21 ×2, 2026-10-24). v1 trusted the Mongo pointer to hide
unpublished Delta commits; it cannot — the next writer builds on them. v2 fenced commits
with Delta `txn` actions, but only in tables the new writer had already touched, said
nothing about the S3 commit lock, and could publish a half-written retry after a `RESTORE`.
v3 closes those: S3 commit coordination (conditional writes, §3), a fence commit in **every** table, an explicit all-tables
`committed` marker, retries that only look past the manifest, heavy work done outside the
lease, and a retention contract the tools actually honour.

v3.3 (2026-10-24, approved "hybrid edits", option B) moves **small edits out of Delta**: they
are Mongo documents in `dataset_patches`, written by Node in one transaction with every other
effect of the edit — no Delta commit, no lease, no queue (§14). Big data stays in Delta on S3.
Every read of a revision goes through **one reader** that merges both (§15).

**Two write paths.**

| Path | What | Protocol |
|---|---|---|
| **Small edit** | Cell edit, row edit, add row, delete / restore rows, bulk edit and column paste parts up to `BULK_EDIT_MAX_ROWS` (500) rows, undo of such a batch | One Node Mongo transaction: patches + revision + every effect (§14) |
| **Delta job** | Ingest, import, transforms, cleaning, set-based edits beyond the cap, type migration, unique reindex, column archive / restore, compaction, dataset restore | Durable operation, lease, fence, reconcile, publish (§4–§6) |

Facts the protocol relies on (checked against delta-rs and DuckDB sources/docs, 2026-10-24):

- A Delta commit covers one table; the log is linear; every commit builds on the latest
  committed version, published or not.
- delta-rs (`CommitProperties(app_transactions=[Transaction(app_id, version)], custom_metadata=…)`)
  writes `txn` actions and custom commit metadata; `DeltaTable.transaction_version(app_id)`
  reads the latest txn version; its conflict checker returns `ConcurrentTransaction` when two
  racing commits carry the same app id (it compares app ids, not versions).
- delta-rs **1.6** commits each `_delta_log/…N.json` with an **S3 conditional write**: S3
  refuses a second writer of the same version (412) and delta-rs retries on the next one. It
  never contacts a DynamoDB lock table, and with conditional writes disabled it refuses to
  commit at all. Verified 2026-10-24 (§3).
- `DeltaTable.restore(target, commit_properties=…)`, `vacuum(..., keep_versions=[…])` and
  `DeltaTable(uri, version=n)` exist (minimum delta-rs version pinned in Phase 0).
- `alter.set_table_properties` needs `raise_if_not_exists=False` to set a custom property such
  as `emr.fence` (found while prototyping, 2026-10-24).
- A `RESTORE` keeps the table's latest `txn` — it does **not** lower the fence — but it still
  carries the writer's token, for the race check and the audit trail (2026-10-24, proved by a
  mutation test).
- A `RESTORE` drops custom table properties set after its target version, so the fence lives
  in the `txn` action, never in the `emr.fence` property (2026-10-24).
- moto's S3 server cannot serve delta-rs Parquet **data** reads (requests stall), while writes
  and log reads work: integration tests on moto assert on the Delta log; read-path tests use
  the local filesystem or real S3 (2026-10-24).
- DuckDB reads a Delta table at a version with `delta_scan(path, version => n)` (duckdb-delta
  ≥ the version pinned in Phase 0) or `ATTACH … AT (VERSION => n)`.
- `delta.logRetentionDuration` (default 30 days) deletes old log entries at checkpoints; a
  version whose log is gone cannot be read even if its Parquet files remain.
- MongoDB multi-document transactions need a replica set (Atlas is one) and have a lifetime
  limit (default 60 s); a write to a document a transaction is modifying causes a write
  conflict and a transient abort.
- The repository uses standard BullMQ; per-group concurrency is a BullMQ **Pro** feature —
  not used.

---

## 1. Roles

| Role | Who | Does |
|---|---|---|
| **Editor** | Node | Writes a small edit as `dataset_patches` in one Mongo transaction that also publishes its revision (§14) |
| **Dispatcher** | Node worker | Wakes the writer for a dataset with pending Delta-job operations (§4.4) |
| **Writer** | Data Service process holding the dataset's lease | Fences, reconciles, commits Delta-job operations one group at a time — never small edits |
| **Publisher** | **Node only** | Moves the revision pointer and applies every critical effect in one Mongo transaction: the edit transaction (§14.2) or the publish transaction (§6) |
| **Reader** | The Data Service's one reader | Reads a published revision: the Delta tables at the manifest's versions plus the revision's patch range from `dataset_patches` (§15) |

The Data Service reads exactly one Mongo collection, **`dataset_patches`, read-only** (a
database role with read access to that collection only). Everything else it needs from Mongo
comes through Node's internal endpoints ([api.md §3](api.md#3-internal-api--data-service--node-writer-endpoints)).
It never publishes.

## 2. Revision manifest

```
dataset_revisions {
  datasetId, revision: n,
  tables: {                                         // Delta tables, each at an exact version
    base:              { version },
    followups:         { version } | null,
    'table_<fieldId>': { version } …
  },
  patches: { fromSeq, toSeq },                      // Mongo dataset_patches with fromSeq < seq ≤ toSeq
  maxRowId,                                         // _row_id high-water mark (§10)
  schemaRef,                                        // immutable form-structure snapshot (§2.1)
  rowCount, kind, opId | batchId, writerToken, pinnedUntil, publishedAt
}
datasets.currentRevision = n
```

- `patches.fromSeq` (exclusive) is the watermark already folded into the Delta tables (the
  old `compactedSeq`); `patches.toSeq` (inclusive) is the newest patch of the revision (the
  old `maxPatchSeq`).
- A small-edit revision copies `tables`, `maxRowId` and `schemaRef` from n and moves only
  `patches.toSeq`; it has `batchId` and `writerToken: null` (no lease is involved).
- A Delta-job revision lists the new table versions; a job that folds patches into base
  (compaction, every base rewrite, dataset restore) also moves `patches.fromSeq` (§4.5, §15.2).

The old Delta `patches` table and `changeLog` table are **gone**: patches live in Mongo, the
change log in `dataset_changes` (§8.1). A reader reads **every table at the version listed**
and the patches with `fromSeq < seq ≤ toSeq`, merged by the one reader (§15). Nothing is read
"at latest". Readers cache a loaded snapshot per `(table, version)` so paging does not replay
the log.

### 2.1 Schema of a revision

`form.fields` is only the **current** schema. A pinned revision (job, pagination session,
undo preview, dataset restore) must be read with the schema it was written under, so every
revision carries `schemaRef` — the id of an immutable document in **`dataset_schemas`**.

The revision layer's `revisionCaptureService.capture()` is **not** used for this: it is
optional history — it returns `null` when revisions are disabled, the content is unchanged or
serialisation fails, stores `snapshot: null` above its size limit, and records the
**pre-image**. A dataset schema must always exist and be the **post-change** state.

- `dataset_schemas { _id, datasetId, hash, fields, sizeBytes }`, unique `(datasetId, hash)`;
  `fields` is the canonical projection of the post-change field list (types, options, value
  labels, order). Above the configured inline size it goes to S3 (`schemas/<hash>.json.gz`,
  SSE-KMS) and the document keeps `{ s3Key, sha256 }`, uploaded **before** the transaction.
- **Initial ingest** creates the first snapshot; the first revision references it.
- An operation that changes the schema writes its snapshot **in the publish transaction**,
  reusing an existing one when the hash matches. Other revisions — every small edit among
  them — inherit their predecessor's `schemaRef`.
- **Mandatory:** if the snapshot cannot be written or (for S3) verified readable, the publish
  aborts — a revision never carries a dangling `schemaRef`.
- Readers resolve field types, options and value labels from `schemaRef`, never from the live
  form. A snapshot referenced by any retained revision is never deleted.

## 3. S3 commit coordination (conditional writes)

- Commits are coordinated by **S3 conditional writes** — no DynamoDB lock table. Scenario C0
  passed on 2026-10-24:
  - moto: 10 concurrent writers, zero DynamoDB requests, 44 refused log PUTs (412), every
    successful commit present;
  - the real AWS test bucket: 20 concurrent writers — every successful commit kept, the rest
    failed loudly (`CommitFailedError` after retries), nothing overwritten.
- C0 runs in CI (`easemyresearch-dataservice/tests/integration/test_c0_concurrent_commits.py`),
  so an upgrade that changes this behaviour fails the build.
- `AWS_S3_ALLOW_UNSAFE_RENAME` is forbidden (startup check refuses it). The storage must
  support conditional writes — AWS S3 does; any other S3-compatible store must be checked
  with C0 first.
- Real contention is rare by design — one writer per dataset (§4), and small edits never
  commit to Delta at all (§14); conditional writes are the backstop.
- Any other engine that ever writes these tables (e.g. Databricks later) must use the same
  commit coordination.

## 4. Writer lease, fencing and reconciliation

Everything in §4–§7 is the **Delta-job** path. Small edits take none of it (§14).

### 4.1 Lease

`dataset_leases { datasetId, owner, token, expiresAt }` — a separate collection, so the
heartbeat never writes the `datasets` document. Every change to it is a compare-and-set:

- **Acquire:** read the row (`token = p`), compute
  ```
  newToken = max(p, highest txn("emr-writer") version in any of the dataset's tables) + 1
  ```
  then `updateOne({ datasetId, token: p, $or: [{ expiresAt: { $lt: now } }, { owner }] },
  { $set: { owner, token: newToken, expiresAt: now + ttl } })` (insert with a unique index on
  first use). The filter on the **expected previous token** makes allocation a true CAS: a
  delayed acquire that computed its token from an older `p` matches nothing and starts again,
  so two holders can never share a token. Taking the max with the Delta logs means a Mongo
  restore that lowered the stored token can never strand the dataset.
- **Renew:** `updateOne({ datasetId, owner, token, expiresAt: { $gt: now } }, { $set: { expiresAt } })`;
  no match → the lease is lost and the writer stops before its next commit.
- **Publish** writes the same row inside its transaction (§6 step 1), so publish and a
  takeover can never both succeed: they write one document, and the loser aborts on the write
  conflict. A read-only token check would not be enough — a transaction reads a snapshot and
  does not conflict with a write it only read.

`now` is the Mongo server time (`$$NOW`), never a process clock.

### 4.2 Every commit is fenced

All Delta writes go through **one wrapper** that injects
`CommitProperties(app_transactions=[Transaction("emr-writer", token)],
custom_metadata={ops: "opId:attempt,…", opTables, part})` (one or more operations, §4.7) — appends, merges, updates, `RESTORE`, `OPTIMIZE`,
`VACUUM`, `alter`, table creation. A commit without the txn is a blind append that never
conflicts; CI fails on any delta-rs write call that bypasses the wrapper. Before committing,
the wrapper checks `transaction_version("emr-writer") ≤ token`; a higher value aborts.
`delta.setTransactionRetentionDuration` is never set (expiring txn ids would erase the fence).

### 4.3 Acquire = fence every table, then reconcile

1. **Fence.** Write a data-free fenced commit (`alter.set_table_properties({"emr.fence": token},
   raise_if_not_exists=False)`) to **every** Delta table of the dataset — base, followups,
   each `table_<fieldId>`. The fence is the commit's `txn`, not the property (a `RESTORE` drops
   the property but keeps the txn, Facts). A stale writer's commit is now either before the
   fence (visible to step 2) or races it and loses (`ConcurrentTransaction`, then its fresh
   check sees the higher token and aborts).
2. **Reconcile.** For each table, look at commits after the manifest's version and before the
   fence:
   - commits whose `(opId, attempt)` is the attempt Node has marked **`committed`** (§5.3)
     and whose txn token equals the token recorded on that operation → **roll forward**: ask
     Node to publish it before anything else;
   - every other commit → **roll back** with a fenced `RESTORE` to the manifest's version. A
     `RESTORE` writes a **new** commit (it does not bring back the old version number), so it
     carries `custom_metadata = { kind: 'reconcile', rolledBack: [{ opId, attempt }] }`, and
     Node records the attempt in the operation's `rolledBackAttempts` before it is retried
     (with `attempt + 1`, from its payload, §5.1) or failed.
3. Only then start new operations.

Reconciliation also runs **after any rejected or failed publish**, before the next operation,
not only on acquisition.

### 4.4 Ordering and wake-ups (standard BullMQ)

- `data_operations` (Mongo) is the ordered per-dataset queue of **Delta-job** commits; `seq`
  comes from `datasets.opSeq` (`$inc`). Only **short commits** enter it: a heavy job prepares
  first, as a `data_job` against a pinned revision, and enqueues its commit operation only
  when its output is ready (§4.5). Small edits never enter it (§14), so a cell edit never waits
  behind a job.
- An operation receives its `seq` only when it is **eligible**. An operation touching a frozen
  field (§4.6) is stored with status `held` and **no `seq`**, so it can never sit at the head
  of the queue and block the operations behind it.
- The **dispatcher** is a Node BullMQ worker. On a new operation Node adds a job with
  `jobId = datasetId`, `removeOnComplete: true`, `removeOnFail: true`; the dispatcher calls
  the Data Service writer (`POST /v1/datasets/{id}:drain`), which acquires the lease and drains
  pending operations in `seq` order.
- Before releasing the lease the writer re-reads pending operations; any that arrived during
  the drain are processed, so no wake-up is lost to `jobId` de-duplication.
- A sweeper re-dispatches datasets whose pending operations are older than a configured age.
- **Poison operation:** after a configured number of failed attempts the writer rolls it back,
  Node marks it `failed` (user-visible error), and the queue moves on.

### 4.5 Heavy work is done outside the lease

Compaction, transforms, cleaning, ingest and set-based edits read a pinned revision (their
`baseRevision`) through the one reader (§15) and write their new Parquet files **without** the
lease. Their commit operation then runs like any other, briefly under the lease (fenced,
`max_commit_retries=0`).

**Watermark rule.** Every job that rewrites `base` (compaction, transform, cleaning,
set-based edit, dataset restore) reads base **plus** all patches with
`fromSeq < seq ≤ W`, where `W = patches.toSeq` of its `baseRevision`, writes the merged
result, and publishes `patches.fromSeq = W`. Patches up to `W` are therefore folded **exactly
once**: a transform that turned a patched 2 into 20 has folded the patch, and the patch can
never be re-applied over the 20. Only patches with `seq > W` keep applying on top.

**Rebase rule** — at commit the writer classifies every revision published since
`baseRevision` (small-edit revisions are found by their patches, `seq > W`, in `dataset_patches`),
and Node repeats the check inside the publish transaction (§6 step 1):

| Later revision | Compatible? |
|---|---|
| Small edits (cell edits, inserts, deletes/restores) not touching the job's input or output fields or its row scope | **Yes** — their patches have `seq > W` and stay on top of the new base |
| Small edits touching the job's input or output fields, or inserting / deleting rows in its row scope | No |
| **Any** base rewrite (another compaction, a transform, a dataset restore) | No |
| A schema change (type cast, options, column add/archive) | No |

A compaction has no input/output fields of its own, so only the last three rows apply to it.
Any incompatible revision → the job's commit is refused and it **re-prepares** from the newest
revision. Commit = the new base files plus the published `patches.fromSeq = W`; nothing else.

### 4.6 Field freeze (starvation guard)

After a configured number of re-prepares the job asks Node for a **field freeze** on its
input/output fields (a whole-dataset freeze for compaction and restore). Setting or lifting a
freeze is a Mongo transaction that also `$inc`s `datasets.freezeEpoch`, so an edit transaction
in flight conflicts with it and re-reads (§14.3). While it holds:

- a **small edit** touching a frozen field waits in Node until the freeze lifts, then runs as
  usual (§14.3); edits to other fields commit at once;
- a new **operation** touching frozen fields is stored as `held`, with **no `seq`** (§4.4);
  operations on other fields get their `seq` and run as usual;
- the job re-prepares once more; nothing can conflict with it now;
- when the job commits, fails, is cancelled or the freeze reaches its hard timeout, Node lifts
  the freeze, runs the waiting edits and gives the held operations their `seq` in arrival
  order. They then run on top of whatever was published, and their callers get the normal
  answer.

The freeze timeout is shorter than the FE request timeout, so a waiting edit is answered within
one request.

### 4.7 Many users on one dataset

Any number of users (owner, collaborators) edit at the same time. Their **small edits** are
Mongo transactions that serialize on the revision compare-and-set (§14.3): each commits in
milliseconds and publishes its own revision; nobody waits for another user's job.

**Group commit — Delta jobs only.** An S3 commit costs tens of milliseconds, so the writer
does not commit one Delta-job operation at a time. It takes every eligible pending operation up
to a configured size (`groupMaxOps`, `groupMaxBytes`) in `seq` order and writes them as **one
commit per table and one published revision**:

- each operation keeps its own `opId`, `attempt`, idempotency key, result and change-log entry;
  the commit metadata lists them (`ops = "opId:attempt,…"`);
- validation (types, unique identifier, row gone, quota) runs **before** the commit, per
  operation, in `seq` order — a failing operation is answered with its error and left out;
  the rest of the group proceeds;
- the commit and the publish are all-or-nothing for the group: `committed` is marked for every
  operation of the group together, publish applies them in one transaction, and a crash rolls
  the whole group back for a retry (§4.3), each operation with `attempt + 1`.

Small edits need no grouping: they never commit to Delta, and Mongo transactions serialize them.

**Cell-level writes.** Every edit writes only the cells it changes (patches by target, §9) —
never the whole row. Two users changing *different* cells of the same row both keep their
values. (Today a row edit replaces the whole `data` array, so a concurrent bulk edit and row
edit can silently drop one of them — `updateRecordDataForUser`; this design removes that.)

**Same cell — the later commit wins, by server order (Google Docs style).** The edit that
committed later has the higher `seq` and is the value. Nothing is lost: `dataset_changes`
keeps every before/after with its actor and time (the later edit's `before` is the earlier
edit's `after`), so the overwritten value is visible in the row's history and comes back
through the existing undo / restore (batch restore and per-row history, §8.2), with the usual
`losses` consent if someone edited the cell after it.

**Seeing each other's edits.** Each revision emits a `notify` event (§6 step 10, §14.2); other
open grids learn the dataset moved to revision n+1 and refresh what they show (polling or push
— decided with the FE in Phase 3). Pages already open keep their pinned revision until then
(§12).

## 5. Durable operations

Delta-job writes only. A small edit is not an operation: its durability is its Mongo
transaction (§14).

### 5.1 Payload and outbox

1. In **one Mongo transaction** Node writes the immutable payload reference, inserts the
   `data_operations` document (`pending`) and — for inserts — the quota reservation (§7).
   Small payloads are stored inline, encrypted with the platform key (AES-GCM, key from the
   secret store; only Node and the writer's role can decrypt); larger ones go first to S3
   (SSE-KMS) under `ops/<datasetId>/<opId>.json.gz`, with their SHA-256.
2. Node then wakes the dataset (§4.4) and sets `dispatched`. The sweeper re-wakes stale
   `pending`/`dispatched` operations, so a crash between insert and enqueue only delays.

**Caller retries.** Every write request from the browser carries a client-generated
**idempotency key** (`Idempotency-Key` header; bulk edit already sends `mutationId`, which is
used as the key). `data_operations` stores `{ actor, idempotencyKey, payloadHash }` with a
unique index on `(actor, idempotencyKey)` (small edits keep the same contract in
`dataset_idempotency`, §14.5):

- same key, same payload hash → the **existing** operation: Node waits for it and returns its
  result (or its error) — a lost response never causes a second write;
- same key, different payload → `409 IDEMPOTENCY_MISMATCH`;
- a request timeout or a closed connection does **not** cancel the operation; the retry with
  the same key picks it up. Keys are kept for a configured window (initially 24 h).

### 5.2 Status machine

`held → pending → dispatched → running → committed → applied`, with `failed`,
`rolled_back` and `cancelled`. Every transition is a compare-and-set on
`(opId, expectedStatus, token, attempt)`:

| Transition | Guard | Duplicate request |
|---|---|---|
| → `running` | status `dispatched`, or `running` with an expired previous token | same token while already `running` → returns the current `attempt`, **no increment** |
| → `committed` | status `running`, same `token`, same `attempt` | same values again → 200; anything else → rejected |
| → `failed` | status `running`, same `token`, same `attempt` | rejected for any other attempt |
| publish → `applied` | status `committed`, same `token`, same `attempt`, lease CAS (§6) | already `applied` → idempotent 200 |
| → `cancelled` | status `held`, `pending`, `dispatched` or `running` — **not** `committed` | already `cancelled` → 200 |

A delayed callback from an older attempt or an older token therefore can never complete, fail
or publish a newer attempt. Only the lease holder calls these, through Node's internal
endpoints.

**Cancel vs publish.** Cancel and the `committed` transition are both CAS writes on the same
document, so exactly one wins:

- cancel wins → the writer's `committed` call is refused; it rolls its commits back
  (reconciliation, §4.3); the reservation is released; the job is `cancelled`;
- `committed` wins → cancel answers `409 TOO_LATE`; the operation publishes; the UI shows the
  result, never "cancelled".

A writing job is `succeeded` **only when its operation is `applied`** — Node sets both in the
publish transaction; a Data Service `succeeded` callback alone never marks it.

**Deleted forms.** Deleting a form cancels its `held` / `pending` / `dispatched` / `running`
operations in the delete transaction, and the publish guard (§6 step 1) also requires the form
to be active; an operation that reached `committed` first is rejected at publish, rolled back,
and its reservation released (`FORM_DELETED`). The edit transaction checks the same (§14.2).

### 5.3 Boundaries

- **`committed`** — the writer calls `POST /internal/operations/:opId/committed
  { token, attempt, committedTableVersions }` only after **all** of the operation's table commits
  succeeded. Every commit also carries `custom_metadata.opTables` and `part`, so the log alone
  shows whether an operation is complete.
- **`applied`** — Node's publish transaction (§6) succeeded. **Node answers the FE only on
  `applied`**; a `committed` operation may still be rolled back.
- **Attempts.** Node increments `data_operations.attempt` each time it moves an operation to
  `running`; every commit of that run carries it (§4.2). "Already committed" means *a commit
  with this `opId` and the **current** `attempt`* — commits of rolled-back attempts (listed in
  `rolledBackAttempts` and in the reconcile `RESTORE`'s metadata) never count, wherever they
  sit in the log. `committedTableVersions` records the **effective** version of each table —
  the commit of the current attempt.
- **Idempotent publish.** Publishing an `opId` that is already `applied` returns 200 with its
  `appliedRevision` and stored manifest — a lost response never triggers a rollback. After any
  publish rejection the writer re-fetches the manifest from Node before reconciling.

## 6. Publish — one Mongo transaction, every critical effect

`POST /internal/datasets/:id/publish { ops: [{ opId, attempt, result }], token, baseRevision: n, manifest }`,
run with `withTransaction` (retries transient errors), budgeted well under a second. `ops` is
one Delta-job operation or a group commit (§4.7); every step below applies to each of them, and
the group publishes as **one** revision. (Small edits publish through the edit transaction,
§14.2, which applies the same effects.)

1. Guard: same `opId` already applied → idempotent 200 (§5.3). Otherwise the operation is
   `committed` with this `token` and `attempt`, the form is active,
   `datasets.currentRevision == n`, and the **lease row is written**:
   `updateOne({ datasetId, token, expiresAt: { $gt: $$NOW } }, { $set: { lastPublishedOpId: opId } })`
   must match one document (§4.1) — no match aborts the transaction.
   **Rebase check, again:** small edits do not pass through the writer, so one can publish
   between the job's commit and its publish. Node repeats the rebase check (§4.5) here, inside
   the transaction, over the patches with `seq > W`; an incompatible one rejects the publish,
   reconciliation rolls the job's commits back, and the job re-prepares (or freezes, §4.6). A
   compatible edit published since the writer's manifest only makes the `currentRevision` check
   miss: the writer re-fetches the manifest and publishes on top of the newest revision.
2. Insert `dataset_revisions` n+1 with the new table versions and `patches.toSeq` = n's;
   `patches.fromSeq` = `W` for a job that folded patches, else n's. A revision that moves
   `fromSeq` to `W` also deletes the unique-identifier claims with `seq ≤ W` (§14.4).
   `datasets.currentRevision = n+1`; `datasets.rowCount = manifest.rowCount`.
3. `Form.dataVersion = n+1` (it is the revision — monotonic by construction).
4. A schema change carried by the operation (type cast, column add/archive) is applied to the
   form, and its mandatory `dataset_schemas` snapshot is written or reused (§2.1) — failure
   aborts the publish — so form and data never disagree.
5. Analysis staleness for `result.changedFieldIds` — bounded `updateMany` by form and field
   (statistical tables, survival, analyses). Compaction has none (§15.2).
6. Quota: settle the reservation; re-validate it if it is no longer `held` (§7).
7. Revision timeline: `RevisionBatch` for the operation (§8.2), and its `dataset_changes` entry.
8. Audit record (policy `atomic`).
9. Operation `applied`, `appliedRevision = n+1`.
10. **Event outbox:** insert `dataset_events { datasetId, revision, opId, changedFieldIds,
    consumers: { recompute, notify, … : 'pending' } }`.

After the transaction a Node dispatcher delivers each pending event to each consumer, with
retries and backoff, and marks that consumer `done` (per-consumer acknowledgement). A sweeper
re-delivers events still pending after a configured age, so a crash right after the commit
only delays recompute or notification — it never loses them. Consumers are idempotent per
`(datasetId, revision, consumer)`. Staleness itself is already in the transaction (step 5), so
even a delayed recompute never shows a stale table as fresh. The `recompute` consumer is
today's auto-recompute (debounced, `ANALYSIS_AUTO_RECOMPUTE_DELAY_MS`, 60 s by default).
Revisions publish strictly in order: n+1 only on top of n — whichever path publishes it.

## 7. Quota reservations

Reservations belong to Delta-job operations (ingest, imports, `restore_rows` beyond the
small-edit cap). A small edit needs none: its edit transaction moves `used_entries` directly
under the same guard (§14.2).

- Created **in the same transaction as the operation** (§5.1), with `$inc` on the owner's
  quota document (`Payment.reserved_entries`) guarded by
  `used_entries + reserved_entries + n ≤ entries_limit`.
- While a dataset is draining, the writer's heartbeat renews the reservations of **all** its
  pending operations, not only the running one.
- The sweeper releases a reservation only after the operation is resolved: `committed` →
  settled at publish, never released; running with a live heartbeat → kept; `failed` /
  `rolled_back`, or no operation exists → released.
- Publish re-validates: a reservation that is no longer `held` re-checks capacity atomically;
  without capacity the publish is refused, reconciliation rolls the commits back, and the user
  gets `QUOTA_EXCEEDED`.
- Restoring soft-deleted rows counts like an insert; deleting rows frees capacity when the
  deletion publishes.

## 8. History and undo

### 8.1 Change log

The change log is the Mongo collection **`dataset_changes`** (no longer a Delta table):
`{ datasetId, revision, batchId, target, before, after, actor, at }`, one document per changed
cell or row state, written **in the transaction that publishes the change** — the edit
transaction (§14.2) or the publish transaction (§6 step 7). It is never compacted and survives
compaction (compaction changes no value, §15.2); it is trimmed only by the undo window (§11).

A Delta job that changes many cells (set-based edit, transform, cleaning) does not write one
Mongo document per cell: it writes its before/after values to a Parquet file
`datasets/<datasetId>/changes/<jobId>.parquet` (SSE-KMS) next to its other output, and its
publish adds one `dataset_changes` entry for the batch that points to that file. Its undo is a
set-based job reading that file.

Undo reads `dataset_changes` (and a job's change file) — the before/after values are stored
there — and never time-travels to an older Delta version. Undo therefore does not depend on
`logRetentionDuration`: a batch kept by the "newest 10 per row" rule stays undoable however old
it is, because its change entries are live data that `VACUUM` and log cleanup never remove.

### 8.2 Today's contracts, on the change log

| Today (Mongo) | On the Data Service |
|---|---|
| Batch timeline (`RevisionBatch`, `mutationId`) | `RevisionBatch` written in the edit / publish transaction; `mutationId ≡ batchId` |
| Batch restore preview: `entries`, `excluded`, `losses`, `lossSetHash`, `stateHashes`, `coverage` | Same response shape, built from `dataset_changes` and the current values read through the one reader |
| Batch restore result: `restoredCount`, `skipped`, `undoMutationId` | Same, as a new small edit (a batch is at most `BULK_EDIT_MAX_ROWS` rows, as today) writing the batch's before-values |
| Per-row history (`/revisions/record/:id`, `restore-preview`, `/:version/changes`) | Built from `dataset_changes` for that `_rid`; restore to a version writes that row's values as of that point (a small edit) |

**Semantics.** Today a batch undo restores whole-row pre-images, so later edits to the same
row revert too. On the Data Service the undo is **per cell**: later edits to *other* cells of
the row are kept, and later edits to the *same* cells are the `losses` the user consents to.
This is a deliberate improvement; the FE copy that explains losses is reviewed in Phase 3.

### 8.3 Whole-dataset restore

Restoring to revision r is a separate, explicit, audited **Delta job** (a base rewrite, so
§4.5 applies, with a whole-dataset freeze). Invariants:

- It publishes a **new** revision n+1 — history is never rewound. Each Delta table (base,
  followups, `table_<fieldId>`) gets a fenced `RESTORE` to r's version, i.e. a new commit, and
  r's patches (`r.fromSeq < seq ≤ r.toSeq`) are folded into it exactly as compaction folds
  them (§15.2). n+1 lists those new versions with `patches = { fromSeq: n.toSeq, toSeq: n.toSeq }`,
  so no patch written after r applies on top.
- **High-water marks never regress:** `maxRowId` of n+1 = n's, and `datasets.patchSeq` is a
  counter that never goes back, so ids and seqs allocated after r are never reused.
- **History is kept:** `dataset_changes` is **not** restored; the restore appends one batch
  `{ kind: 'dataset_restore', from: n, to: r }`. Undo of the restore = a restore to n.
  Per-cell undo of older batches keeps working through its normal conflict checks.
- **Schema:** n+1 uses r's `schemaRef`, and the live form is set to that snapshot in the publish
  transaction.
- **Counts and quota:** `rowCount` = r's live rows; `used_entries` moves by the difference — an
  increase is reserved first and refused (`QUOTA_EXCEEDED`) without capacity.
- **Unique identifier:** the claims with `seq ≤ n.toSeq` are deleted in the publish (§14.4);
  r's values are now in base, where the reader check finds them.
- **Staleness:** every analysis on the form is marked stale (all fields changed).

## 9. Cell addressing and values

```json
{ "row": "<_rid>", "field": "<fieldId>",
  "scope": { "kind": "main",     "followUp": 0 }
         | { "kind": "table",    "parentField": "<fieldId>", "rowIndex": 2, "followUp": 0 }
         | { "kind": "followup", "followUp": 3 } }
```

`followUp = 0` is the main visit, so a table field in visit 1 and visit 2 are different
targets — today's `parentFieldId + rowIndex + followUpNumber`. The same target addresses a
`dataset_patches` document, a `dataset_changes` entry and a Delta cell.

Values are typed. In Delta they are stored in **typed nullable columns** (Parquet has no union
type); in `dataset_patches` and `dataset_changes` the value is `{ t, v }`, with `v` of the
type `t` names:

| `t` | Delta column | Meaning |
|---|---|---|
| `null` | — | Cell cleared (`v` absent) |
| `str` | `v_str` (may be `""`) | Text; `""` is an explicit empty answer |
| `num` | `v_num` (double) | Number |
| `date` | `v_date` | Date-only |
| `opts` | `v_opts` (list<string>) | Option ids from `form.options` (radio/select 1, checkbox many) |
| `files` | `v_files` (list<struct{url, key?, name?, size?}>) | Every file of the field, original URL kept |

The field's type decides which `t` is legal; anything else is `SCHEMA_CONFLICT`.

## 10. Allocation

- `_row_id` (internal int64): allocated **only by the Delta writer** — ingest / import and
  compaction (for rows a patch inserted) take `manifest.maxRowId + k`; the manifest records the
  new high mark. Ids of rolled-back commits may be reused — they were never published. A row
  inserted by a small edit has no `_row_id` until compaction folds it; until then it is
  addressed, ordered and cached by `_rid`, like every row outside the Data Service.
- `_rid` (public ObjectId string) is generated with the row — by Node in the edit transaction
  for a small-edit insert, by the job for ingest — and never changes.
- Patch `seq`: `datasets.patchSeq`, `$inc` inside the edit transaction (§14.2); it only ever
  grows, so a published `seq` is never reused (an aborted transaction rolls its `$inc` back
  with everything else).
- `data_operations.seq`: `datasets.opSeq` counter; processing order defines semantics.

## 11. Retention

| What | Kept for | Mechanism |
|---|---|---|
| Undo | Today's rule `{ keep: 10, days: 30 }`: the newest 10 batches per row are always kept; older ones also while younger than 30 days | `dataset_changes` documents only — the trim job deletes entries outside the rule; no old Delta version is needed (§8.1). A Delta job's single entry (and its change file) is kept by the 30-day part of the rule |
| Patches (`dataset_patches`) | Until no retained or pinned revision needs them | §15.3 |
| Unique-identifier claims | Until their patch is folded into base | Deleted in the publish that moves `patches.fromSeq` past them (§14.4) |
| Trash (soft-deleted rows / form) | 45 days, rows and form alike (`TRASH_RESTORE_WINDOW_MS`) | `_deleted_at` / the latest `delete_row` patch, purge after the window |
| Explicit dataset restore | 30 days | `pinnedUntil` on revisions |
| Running jobs, open pagination sessions | job duration / session TTL | pins renewed by heartbeat |
| Operation payloads | until `applied` + the undo window | S3 lifecycle / TTL |
| Small-edit idempotency keys | configured window (initially 24 h) | TTL index on `dataset_idempotency` |
| Raw uploads | until the dataset is purged (configurable shorter) | S3 lifecycle on `raw/` |

Old Delta **versions** are needed only by time-travel readers: explicit dataset restore (30
days), running jobs and pagination sessions (short). Table properties on every table:
`delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` ≥ the longest of those
plus a margin (initially 60 days); a startup check refuses a configuration where a pin window
exceeds them. `vacuum(keep_versions)` keeps data files only — it does not keep log entries, which
is why the pin windows must fit inside the log retention rather than rely on `keep_versions`.
The vacuum job asks Node for pinned revisions, maps them to per-table versions and calls
`vacuum(retention_hours=…, keep_versions=[…], enforce_retention_duration=True)` per table
(fenced). Compaction frequency is bounded by configuration, because each compaction rewrites
the files that hold patched rows and the restore window keeps the old ones.

## 12. Caches and pinned pagination

| Cache | Key | Value |
|---|---|---|
| Count | `(datasetId, revision, rowScope, filter, search)` | number |
| Order | `(datasetId, revision, rowScope, filter, search, sort)` | ordered `_rid` list (spilled to disk when large) — `_rid`, because a row a small edit inserted has no `_row_id` before compaction (§10) |
| Page | **not cached** | slice of the cached order + projected columns fetched by id |

`rowScope` follows the per-operation scope table ([tech-design §4.3](tech-design.md#43-row-scope--as-the-code-does-it-today)).
Every write publishes a new revision. The grid's existing cursor carries the revision
**inside** it (format unchanged), so later pages read the same snapshot; column paste carries
its revision and rejects targets deleted since (`ROW_GONE`). A compaction publishes a revision
with the same rows and values (§15.2), so a session pinned before it stays correct.

## 13. Phase 0 prototype — must pass before Phase 1

| # | Scenario | Pass when |
|---|---|---|
| C0 | Concurrent commits to one table on S3 | every successful commit kept, the rest fail loudly, nothing overwritten — **passed 2026-10-24** (moto + real S3) |
| C1 | Two Delta-job operations (e.g. two imports) insert the same unique identifier | exactly one succeeds (small edits: C44) |
| C2 | Writer pauses past its lease, another writer fences and commits, first resumes | first's commit rejected; nothing lost or duplicated |
| C2b | Stale writer commits to a table the new holder has not touched yet | fence makes it visible to reconciliation, or it loses the race |
| C3 | Commit to Delta, crash before `committed` / after `committed`, before publish | incomplete op rolled back and retried; committed op rolled forward exactly once |
| C3b | Op partly committed, rolled back by `RESTORE`, then retried | retry re-writes every table; no half-published op |
| C4 | Commit, publish rejected (lease lost), next writer appends | rejected data never in a later revision |
| C5 | Crash after operation insert, before enqueue | sweeper dispatches; applied once |
| C6 | Publish of rev 11 fails, rev 12 attempted; poison op | 12 waits; poison op fails after N; `rowCount` / `dataVersion` / staleness never regress |
| C7 | Long import whose reservation expires while a second import starts | limit never exceeded |
| C8 | Cell edits during compaction (compaction outside the lease) | every edit visible after; none lost; edit latency unaffected |
| C9 | Owner vs collaborator on the same filter; page 1 vs page 2 | no cache leakage; pages correct |
| C10 | Batches A and B on different rows; compaction; undo A | B intact, A reverted |
| C11 | Batches A and B on the same cell; undo A | conflict reported; consent required |
| C12 | Table field edited in follow-up 1 and follow-up 2 | distinct targets; undo of one leaves the other |
| C13 | Pagination across a concurrent write | pages consistent with the pinned revision |
| C14 | VACUUM while a job is pinned to an old revision | job completes; files intact |
| C15 | Publish response lost, writer retries | idempotent 200; no rollback |
| C16 | Read a version older than one checkpoint interval after log cleanup | readable within the retention window |
| C17 | Publish concurrent with lease heartbeat and recompute writes | publish completes (retries transient errors) |
| C18 | Lease expires mid-publish; a new writer acquires while the old publish transaction is open | exactly one of publish / acquire commits; no stale revision published |
| C19 | Insert rows, read before any compaction; delete + restore one of them | inserted rows visible at once; delete / restore applied |
| C20 | Attempt 1 commits, reconcile `RESTORE`s it, attempt 2 retries | attempt-1 commits never counted; attempt 2 re-writes every table |
| C21 | Change a field's type / options, then read a revision pinned before the change | old revision read with its own `schemaRef`; dataset restore brings schema back too |
| C22 | Undo a batch older than the log retention (row with < 10 batches) | undo succeeds from `dataset_changes` alone |
| C23 | Cell edits while a transform prepares; one edit touches the transform's input field | other edits publish without waiting; conflicting one makes the transform re-prepare; freeze after N |
| C24 | Crash right after the publish commit, before event delivery | recompute and notification delivered by the sweeper, once |
| C25 | Compaction prepared on rev 10; a transform rewrites base in rev 11; compaction commits | compaction refused and re-prepared; transform result intact |
| C26 | Cell patched to 2; transform turns it into 20; read after | 20 — the folded patch is never re-applied |
| C27 | Schema snapshot write fails (or S3 copy unreadable) during a schema-changing publish | publish aborts; no revision with a dangling `schemaRef`; first ingest has a snapshot |
| C28 | Retry inside one lease: a delayed `committed` / `failed` from attempt 1 arrives during attempt 2; duplicate `running` | rejected; attempt 2 unaffected; duplicate `running` does not increment |
| C29 | Delayed lease acquire with a token computed from an old `p` | CAS fails; no two holders share a token |
| C30 | Field freeze active; small edits and operations to frozen and other fields arrive; freeze times out | edits / operations on other fields publish at once; edits to frozen fields wait and commit after the freeze lifts, within one request; held operations get `seq` after it |
| C31 | Browser save succeeds, response lost, same request retried; same key with another payload | one write, same result; mismatch → 409 (small edits and operations alike) |
| C32 | Cancel races `committed`; form deleted with a committed operation pending | exactly one wins; reservation settled or released; `FORM_DELETED` rollback |
| C33 | Whole-dataset restore to r, then insert rows and undo an older batch | new revision; ids/seqs after r not reused; history kept; counts, quota, staleness and schema match r |
| C34 | 10 users edit different cells of the same rows at the same time (concurrent edit transactions) | every value kept; edits serialize through the revision CAS — one revision each, CAS retries within the configured limit |
| C35 | Two users edit the same cell concurrently; undo the later edit | the later commit wins; `dataset_changes` shows both with actors; undo brings the earlier value back |
| C36 | A Delta-job group commit contains one invalid operation (duplicate unique identifier) | that operation alone is rejected; the rest publish together |
| C37 | Crash after a Delta-job group's commits, before `committed` | the whole group rolls back and every operation retries once; no partial group published. (Not an edit scenario: a crash inside an edit transaction leaves nothing — Mongo aborts it) |
| C38 | 20 users typing continuously for a minute (benchmark) | edit-to-visible latency within the PRD target; edit-transaction time and CAS retry rate measured |
| C39 | Compaction equivalence | every cell, row state and row count identical through the reader before and after compaction |
| C40 | An analysis reads while a compaction commits and publishes | the analysis reads its one pinned revision only — never base of one revision with patches of another |
| C41 | Patch deletion after compaction while an older revision is pinned (job, session, restore window) | patches deleted only when no retained or pinned revision has `fromSeq` below them; the pinned revision stays readable |
| C42 | Rows inserted by small edits only, some deleted / restored, before and after compaction | correct rows, states and values both before and after; each gets a `_row_id` once, at compaction |
| C43 | Grid, analysis and export read the same revision | identical rows and values from all three |
| C44 | Two users insert the same unique identifier concurrently, and a value already in base | exactly one insert succeeds; the base value is refused (`DUPLICATE_UNIQUE_ID`) |

Numbers in the PRD (ingest time, p95, compression, cost, edit latency) stay **targets** until
the Phase 0 benchmarks (cold cache, concurrent users, a growing patch window, the reader read +
Mongo edit transaction on the edit path, S3 commit + Mongo publish transaction on the job path)
replace them.

## 14. Small edits — one Mongo transaction

### 14.1 What is a small edit

Cell edit, row edit, follow-up submit, add row (CRF submit, add empty row), delete / restore
rows, bulk edit, column paste and the undo of such a batch — each request (or each save part)
touching at most **`BULK_EDIT_MAX_ROWS` = 500** rows, today's constant
(`src/v2/common/constants`). Column paste keeps today's flow: up to `RECORD_ORDER_MAX_IDS`
(5,000) grid-order ids, saved in parts of at most 500 rows; each part is its own edit
transaction and its own undo batch, as today.

Anything bigger — a rule over many rows, a paste or delete beyond the cap, ingest, transforms,
cleaning, type migration, unique reindex, dataset restore — is a Delta job (§4–§6, §8.3).

### 14.2 The edit transaction

Node, for one small edit:

1. **Read (outside the transaction).** `n = datasets.currentRevision` and `e =
   datasets.freezeEpoch`; then, from the one reader at revision n (§15): the current value of
   every target (the `before`), the state and `_created_by` of every row touched (row scope,
   `ROW_GONE`), and — for a unique-identifier field — whether the new value already exists in
   revision n. Derived fields are recomputed by Node from the merged row it read, and become
   patches of the same edit.
2. **`withTransaction`** (retries transient errors), budgeted well under a second:
   1. **CAS** `datasets.findOneAndUpdate({ _id, currentRevision: n, freezeEpoch: e, status: 'ready' },
      { $set: { currentRevision: n+1, lastEditAt: $$NOW }, $inc: { patchSeq: k, rowCount: Δ } })` — `k` patches
      get `seq = patchSeq − k + 1 … patchSeq`. No match → abort and retry (§14.3). The form must
      be active, else `FORM_DELETED`. No active freeze may cover the edit's fields (§4.6).
   2. Insert the `k` **`dataset_patches`** (§14.6).
   3. **Unique identifier:** insert / delete the `dataset_unique_claims` (§14.4).
   4. Insert **`dataset_revisions` n+1**: `tables`, `maxRowId`, `schemaRef` copied from n;
      `patches = { fromSeq: n.fromSeq, toSeq: patchSeq }`; `rowCount` = n's + inserted +
      restored − deleted; `kind: 'edit'`, `batchId`, `writerToken: null`.
   5. `Form.dataVersion = n+1`.
   6. **Analysis staleness** for the changed fields — the same bounded `updateMany` as §6 step 5.
   7. **Change log:** one `dataset_changes` entry per changed target `{ datasetId, revision: n+1,
      batchId, target, before, after, actor, at }` (§8.1).
   8. **`RevisionBatch`** for the batch (`mutationId ≡ batchId`).
   9. **Idempotency record** in `dataset_idempotency` (§14.5).
   10. **Quota:** inserts and restores `$inc` `Payment.used_entries` under the guard
       `used_entries + reserved_entries + n ≤ entries_limit` (no match → abort,
       `QUOTA_EXCEEDED`); deletes decrement it.
   11. **Audit** record (policy `atomic`).
   12. **Event outbox:** `dataset_events { datasetId, revision: n+1, batchId, changedFieldIds,
       consumers: { recompute, notify } }` — delivered after commit exactly as in §6
       (`recompute` = today's debounced auto-recompute, `ANALYSIS_AUTO_RECOMPUTE_DELAY_MS`, 60 s).
3. **Answer** the FE once the transaction has committed — the edit is published and visible to
   every reader of revision n+1. There is no `committed` / `applied` gap for small edits.

A crash anywhere before the commit leaves nothing (Mongo aborts the transaction); a crash after
it loses nothing (the retry with the same idempotency key returns the stored result, the event
sweeper delivers the events).

### 14.3 Concurrency — edits serialize on the revision

- Two edits that both read revision n race on the CAS: one commits n+1; the other's CAS
  matches nothing, so it **re-reads at n+1** (new `before` values, fresh unique check) and
  commits n+2. Concurrent edits therefore serialize; each publishes its own revision. Mongo
  write conflicts on the `datasets` document are transient errors that `withTransaction`
  retries; a CAS miss re-runs step 1. After a configured number of misses the edit answers
  `DATASET_BUSY` (retryable).
- **Same cell:** the later commit has the higher `seq` and wins (§4.7); the earlier value is
  the later entry's `before` in `dataset_changes`.
- **Delta jobs:** a job's publish (§6) also moves `currentRevision`, so an edit in flight
  retries once; an edit never waits for a job that is still preparing (it prepares outside the
  lease). A job whose rebase check finds an incompatible small edit re-prepares (§4.5).
- **Freezes:** setting or lifting a freeze `$inc`s `datasets.freezeEpoch` in its own
  transaction, so the CAS of an edit that read the old epoch misses and the edit re-reads. An
  edit touching a frozen field waits until the freeze lifts (bounded by the freeze timeout,
  shorter than the FE request timeout, §4.6) and then runs.

### 14.4 Unique identifier

Today: a partial unique index on `(formId, uniqueIdentifierValue)` over active records. On the
Data Service, inside the edit transaction:

- every value a patch makes live for the unique-identifier field (a `set`, an `insert_row`, a
  `restore_row` of a row holding one) is **claimed** in `dataset_unique_claims` with a unique
  index on `(datasetId, fieldId, value)`; a duplicate-key error aborts the edit with
  `DUPLICATE_UNIQUE_ID`;
- a patch that changes a claimed value away, or deletes its row, deletes that claim;
- the **reader check** of step 1 refuses a value already present in revision n (base values
  included). The CAS guarantees n is still the current revision at commit, so the check is
  exact; the claims make the same rule a database constraint.

Claims cover values introduced by patches that are not folded yet. A revision that moves
`patches.fromSeq` to `W` (compaction, base rewrites, dataset restore) deletes the claims with
`seq ≤ W` in its publish transaction — those values are in base now, where the reader check
finds them. Base itself changes only through Delta jobs, under the rebase and freeze rules
(§4.5, §4.6); a job that changes unique-identifier values (reindex, type migration, transform)
checks uniqueness over the whole column before its commit (C1).

### 14.5 Idempotency

Every small-edit request carries the caller's `Idempotency-Key` (bulk edit: its `mutationId`).
The edit transaction inserts `dataset_idempotency { actor, idempotencyKey, payloadHash,
revision, result, expiresAt }` with a unique index on `(actor, idempotencyKey)`:

- the same key with the same payload hash → the stored result is returned, nothing is written
  again (the insert's duplicate-key error aborts the second transaction);
- the same key with another payload → `409 IDEMPOTENCY_MISMATCH`;
- keys expire after the configured window (initially 24 h, TTL index) — the same window as
  operations (§5.1).

### 14.6 `dataset_patches`

```
dataset_patches {
  datasetId, seq,                                   // seq from datasets.patchSeq ($inc)
  target: { row: <_rid>, field: <fieldId>,          // field absent for row ops
            scope: { kind: 'main' | 'table' | 'followup', parentField?, rowIndex?, followUp } },
  op: 'set' | 'insert_row' | 'delete_row' | 'restore_row',
  value: { t: 'null' | 'str' | 'num' | 'date' | 'opts' | 'files', v },   // set only (§9)
  opId, batchId,                                    // batchId ≡ mutationId
  actor, at
}
unique index (datasetId, seq)
```

Patches are immutable: written once by the edit transaction, read by the one reader, deleted
only by the retention rule (§15.3).

## 15. One reader and compaction

### 15.1 The reader

Every read of a revision — grid list, count, ids, single row, field stats and samples, export,
undo preview, the `before` values of an edit (§14.2), the existing Python analysis service, and
the input of every Delta job including compaction — goes through **one reader** in the Data
Service. For revision n it reads the Delta tables at `n.tables` versions and the
`dataset_patches` with `n.patches.fromSeq < seq ≤ n.patches.toSeq`, and merges them:

1. **Rows** = base rows **∪** rows created by `insert_row` patches (a plain base join would hide
   a row that exists only in patches).
2. **Row state** = the latest `delete_row` / `restore_row` patch of the row by `seq`, else the
   row's own `_deleted_at`; deleted rows are dropped unless the trash is being read.
3. **Cell** = the latest `set` patch of the target by `seq` (`t: 'null'` clears it), else the
   base value. Child tables (`table_<fieldId>`, `followups`) merge the same way, by full target.

Rules that keep this the only path:

- The existing Python analysis service never reads patches or Delta itself: it asks the Data
  Service for the rows of a revision ([tech-design §4.6](tech-design.md#46-python-analysis-on-data-service-datasets)).
- Delta files are private: the bucket policy lets only the Data Service role read the Delta
  prefixes, and an architecture test in the Data Service repository fails on any Delta data read
  outside the reader module (`tests/unit/test_architecture.py`, built 2026-10-24).
- The Data Service reads `dataset_patches` with a read-only role (§1); it never writes Mongo.

### 15.2 Compaction

Compaction folds patches into the Delta tables so the patch window stays small. It is a Delta
job (`compact`) and uses the existing protocol — lease, fence, reconcile, heavy work outside the
lease, rebase (§4):

- **Trigger — configuration, never constants:** the patch window (`patchSeq − currentRevision's
  patches.fromSeq`) above a threshold (initially ~1,000), the dataset idle for a configured time
  after its last edit (initially ~5 min), or before a Delta job that needs a clean base. Small
  and frequent; the maximum frequency is configured too (§11).
- **Fold:** pin base revision b, `W = b.patches.toSeq`; read b through the reader; write the
  folded files (only the files holding patched rows change; rows a patch inserted get their
  `_row_id`, §10) outside the lease; commit briefly under the lease.
- **Publish:** revision n+1 on top of the current n with the new table versions,
  `patches = { fromSeq: W, toSeq: n.patches.toSeq }`, the new `maxRowId`, the same `rowCount`
  and `schemaRef`, `changedFieldIds = []`, and the claims with `seq ≤ W` deleted (§14.4). Small
  edits published after b have `seq > W` and stay on top (§4.5, C8).
- **Values never change** (C39), so compaction marks no analysis stale and triggers no
  recompute, and it writes no `dataset_changes`. `Form.dataVersion` still moves, because it is
  the revision; consumers that compare `Form.dataVersion` itself — today the bulk-download cache
  split — would see a new version after a compaction (open point for Phase 5).

### 15.3 Patch retention

A patch with `seq = s` is needed by every retained or pinned revision R with
`R.patches.fromSeq < s ≤ R.patches.toSeq` — a revision in the explicit-restore window, a job's
pinned base revision, an open pagination session. After a compaction to `W`, patches with
`seq ≤ W` are deleted **only when no retained or pinned revision has `fromSeq < W`**; the
retention job deletes `seq ≤ min(fromSeq of every retained or pinned revision, the current one
included)` (C41).
`dataset_changes` is separate and follows the undo rule (§11).
