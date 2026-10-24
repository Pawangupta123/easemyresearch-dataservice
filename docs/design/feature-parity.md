# Data Service — Feature Parity

**Status:** Proposed · not built
**Last updated:** 2026-10-24
**Back to:** [prd.md](prd.md) · [tech-design.md](tech-design.md) · [consistency.md](consistency.md) · [implementation-plan.md](implementation-plan.md)

Decision D7: **every feature that works on a `mongo` form today works on a `dataservice`
form**, with the same API and UI. This table is the checklist; a phase is not done until its
rows pass the parity e2e suite (same scripted actions on both stores → same result).

Source of the "today" column: record-access inventory of 2026-10-21 (tech design §4.1),
re-checked against `src/v2` on 2026-10-24. Every write on the Data Service publishes a new
**revision**: a small edit (≤ `BULK_EDIT_MAX_ROWS` = 500 rows) in one Node Mongo transaction
([consistency §14](consistency.md#14-small-edits--one-mongo-transaction)), anything bigger as a
durable Delta-job operation that Node publishes ([consistency §5–§6](consistency.md#5-durable-operations));
every read goes through the Data Service's one reader ([consistency §15](consistency.md#15-one-reader-and-compaction)).
Rows marked **Δ** change behaviour on purpose.

---

## 1. Row scope (who sees which rows)

Kept exactly as the code does it today — see [tech-design §4.3](tech-design.md#43-row-scope--as-the-code-does-it-today);
the scope is part of every cache key ([consistency §12](consistency.md#12-caches-and-pinned-pagination)).

| Operation | Today (Mongo) | On the Data Service |
|---|---|---|
| Grid list, counts, `record-ids` (grid order), export | **unscoped** — every row of the form (pipeline filters `formId` only) | same — no `_created_by` filter |
| Single get / edit / delete / restore, trash list | scoped to the **creator** (`userId` = requesting user) — **even for the owner** (`*ForUser`) | `_created_by` = requesting user, from the service token |
| Bulk edit | owner → all rows; collaborator → own rows (`recordScopeUserId`) | same rule |
| Bulk delete | creator only (`bulkSoftDeleteForUser`), owner included | same rule |
| Python analyses | Python reads records with `userId` = requesting user (`user_id` in every gateway payload) | `DataServiceRowSource` asks the Data Service for the rows, with the same `_created_by` filter |

## 2. Data entry and records

| Feature | Today (Mongo) | On the Data Service | Phase |
|---|---|---|---|
| CRF form submit, add empty row | `M.createRecord` | small edit: `insert_row` + `set` patches in one edit transaction | 3 |
| Cell / row edit | merge + `M.updateRecordDataForUser` (replaces the whole `data` array — a concurrent bulk edit + row edit can drop one) | **Δ** cell-level patches in one edit transaction; concurrent edits serialize on the revision CAS; different cells never collide; same cell = the later commit wins, earlier value kept in history + undo ([consistency §4.7](consistency.md#47-many-users-on-one-dataset), [§14.3](consistency.md#143-concurrency--edits-serialize-on-the-revision)) | 3 |
| Follow-ups (submit / read / schedule) | entries with `followUpNumber` in the record | `followups` child table; patches with target `scope.followUp` ([consistency §9](consistency.md#9-cell-addressing-and-values)) | 3 |
| Table fields (sub-rows) | entries with `rowIndex`, `parentFieldId` | `table_<fieldId>` child table; target `parentField + rowIndex + followUp` | 3 |
| File-type fields | **array** of S3 URLs per field (incoming + uploaded, max-files check); signed read | `v_files` list of `{url, key?, name?, size?}` — every URL preserved; same signed read | 3 |
| Derived fields on write | `recomputeMultiFieldDerived` on submit / edit / bulk edit | recomputed by Node from the merged row (read through the reader) and written as patches of the same edit | 3 |
| Unique identifier (guard, check-unique) | partial unique index + `uniqueIdentifierValue` | small edits: `dataset_unique_claims` (unique index) + reader check inside the edit transaction (C44); Delta jobs: whole-column check before commit (C1) | 3 |
| Screening (inclusion / exclusion) | `ScreeningAttempt` collection in Mongo | unchanged (Mongo); only the export join reads rows | 3 / 7 |
| Hidden fields for non-owners | dropped in grid read | same, on the projected columns | 2 |

## 3. Grid

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| Paged list (offset / cursor) | `findFilteredPaged` / cursor | `rows:query` at a pinned revision; revision carried inside the cursor | 2 |
| Column sort | reads every row, sorts in Node memory | DuckDB `ORDER BY`, cached order per revision | 2 |
| Filters / search | Mongo pipeline / regex `$or` | same filter tree → SQL / `ILIKE` | 2 |
| Follow-up number filter | `data.followUpNumber $in` | join `followups` | 3 |
| Totals / counts | `countByFilter` | `rows:count` (cached per revision + scope) | 2 |
| Row + column virtualisation | FE virtualiser, 50 rows per page | same + `columns` param, larger pages | 2 |

## 4. Bulk operations and history

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| Bulk edit (≤ 500 rows / part, 100 parts) | `M.bulkWrite`, pre-images captured first | one edit transaction per part (same 500-row cap); rule-based changes beyond it → set-based Delta job outside the lease | 3 |
| Column paste incl. rows not loaded | grid-order ids (≤ 5000) + bulk edit | `rows:ids` + one edit transaction per part, carrying its revision; targets deleted since → `ROW_GONE` | 3 |
| Bulk delete / restore / trash list | `status` + `deletedAt`, 45-day window | `delete_row` / `restore_row` patches over `_deleted_at`, 45 days; restore counts against quota like an insert | 3 |
| Per-row revision history (`/revisions/record/:id`, `restore-preview`, `/:version/changes`) | revision snapshots | built from `dataset_changes` for that `_rid` | 3 |
| Batch undo ("Save history") **Δ** | restores **whole-row pre-images** — later edits to other cells of those rows revert too | **per-cell** undo of the batch from `dataset_changes`, as a new small edit; later edits to the same cells are the `losses` the user consents to; same preview / result shape, `mutationId ≡ batchId` ([consistency §8.2](consistency.md#82-todays-contracts-on-the-change-log)) | 3 |
| Whole-dataset restore | — | separate, explicit, audited action | 3 |

## 5. Schema changes

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| Add / rename / delete field | form metadata | metadata + Delta schema evolution; applied to the form in the publish transaction (new `schemaRef` snapshot) | 3 |
| Column archive / restore | `collectFieldEntriesForArchive` → archived values; restore writes them back | old column kept through the undo window; restore copies it back as an operation | 3 |
| Change field type (migration) | pages of 200 + `bulkWrite` | cast job outside the lease → one revision; form type changes in the same publish | 3 |
| Unique-identifier reindex (toggle, type change, form restore) | `uniqueIdentifierService.reindexForm` | `reindex_unique` Delta job: recheck over the column (through the reader) before the commit; duplicates block the op | 3 |
| Option usage / "has rows?" guards | `distinct` / `existsByFormId` | `fields:stats` / `rows:count > 0` | 2 |

## 6. Transforms and derived data

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| Formula / score / conditional / field-pick derived fields (create, backfill) | keyset pages + `bulkWrite` / `updateMany` | spec compiled to SQL/Polars → one revision | 3 (recompute) / 6 (catalogue) |
| Derived recompute after source edits | per-row in write paths | in the same small edit (or Delta-job operation); set-based recompute for column-wide changes | 3 |
| Conversion (analyze / apply), range helpers | aggregate + pipeline update | same spec, executed by the Data Service | 6 |
| Cleaning rules + report | — (new) | rule catalogue → revision + report | 6 |

## 7. Analysis and outputs

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| All Python analyses + statistical tables + bulk DOCX | Python reads `records` from Mongo | `DataServiceRowSource`: rows of the published revision from the Data Service's one reader | 5 |
| Legacy survival, charts, field stats | Node reads rows / `Record.aggregate` | `streamRows` / `fields:stats` | 5 |
| Analysis staleness / recompute | from write paths | the changed fields, marked in the edit or publish transaction; recompute through the event outbox (today's debounced auto-recompute); compaction marks nothing | 3 |
| Filter → record ids for subsets | `findFilteredRecordIds` | `rows:ids` | 2 |
| Export to Excel | synchronous, in memory (`downloadAll` loads every row) | synchronous below a size threshold; above it an async export job (xlsx/csv/sav/parquet) | 7 |
| Screening export | `$lookup` on records | DataStore join | 7 |

## 8. AI features

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| AI records agent (query, count, get, submit, update, bulk edit/delete, restore) | core services | same core services over `DataStore`, same row scope; caps from config | 7 |
| AI doc-build (subset ids, field variance, subset counts) | aggregates + `findFilteredRecordIds` | `rows:ids`, `fields:stats` | 5 / 7 |
| AI field context, import type detection | `sampleEntriesByField` / Node helpers | `:sample` / detect job | 2 |

## 9. Platform

| Feature | Today | On the Data Service | Phase |
|---|---|---|---|
| Entry quota | `used_entries` / `Form.entryCount` deltas in the write transaction | small edits: guarded `$inc` in the edit transaction; Delta jobs: reserved **with the operation**, settled at publish ([consistency §7](consistency.md#7-quota-reservations)) | 3 |
| `Form.dataVersion` | `$inc` in the write transaction | = the latest **value-changing** revision, set in the edit or publish transaction (monotonic; a compaction leaves it) | 3 |
| Global counts (quota usage, admin analytics) | global `countByFilter` / `Form.entryCount` | Mongo counts + `datasets.rowCount` (set in the edit or publish transaction) | 7 |
| Storage quota | file uploads | + `datasets.storageBytes` | 7 |
| Audit trail, permissions | `auditScope()`, `checkFormPermission` | unchanged (Node); audit written in the edit or publish transaction | 2–7 |
| Soft delete form | flags the **form only** (`status: inactive`, `deletedAt`); records untouched; restore allowed within the 45-day trash window (`TRASH_RESTORE_WINDOW_MS`, same as rows) | same flag; dataset untouched; purge job after the window | 7 |
| Missing-data reminders (designed, not built) | planned on Mongo | build on `DataStore` (`isEmpty` filter) | after 3 |
