# Node Phase 1 audit — every place the backend touches row data

**As of:** 2026-10-25 · backend branch `feat/paired-missing-followup` · read-only audit (no
Node code changed) · input to [design/implementation-plan.md](design/implementation-plan.md)
Phase 1 (DataStore abstraction). Paths are relative to `EaseMyResearch_Backend_V1/`. Spot-checked
by hand: the unbound repo method in `core.helper.ts`, field variance matching `status` only.

## Headline

- **107 direct row-access call sites in 43 files** outside the three repositories.
- All six bypasses the plan names exist; the screening `$lookup` serves the attempts list,
  **not** the export (plan step 7.5 names the wrong path).
- The larger coupling: callers build Mongo shapes themselves — 9 raw `Q.aggregate` callers,
  2 pipeline pass-throughs (`findFilteredPaged` / `findFilteredIds`), 8 `M.bulkWrite`, 3
  `M.updateMany`, 8 raw-filter `countByFilter`.
- Recommended interface: **per-capability interfaces composed into `DataStore`** (§5).

## 1. The row model and the three repositories

**Model** `src/v2/modules/record/models/record.model.ts` (`'Record'`, collection `records`):
`userId`, `formId`, `formVersion`, `data[]` (`fieldId`, `type`, `value`, `optionIds`,
`rowIndex`, `parentFieldId`, `derivedFromFieldId`, `childrenIds`, `followUpNumber`,
`followUpStatus`), `uniqueIdentifierValue`, `status`, `deletedAt`, timestamps. Scopes live
inside `data[]`: follow-up = `followUpNumber`, table field = `parentFieldId` + `rowIndex`,
derived = `derivedFromFieldId`. Partial unique index `formId_uniqueIdentifierValue_active`.
Row data is also copied into revision snapshots, `RevisionArchive`, `ScreeningAttempt.recordId`,
`ExtractionJob.committedRecordId` (reference only).

| Repo | File | Size | Notes |
|---|---|---|---|
| Q | `record/record-query.repository.ts` | 854 lines, 34 methods | raw `aggregate` pass-through; `streamIdentifierEntriesByForm` includes inactive rows; `findByFormAndRecordId` not creator-scoped; revision reads keyed by `_id` only |
| M | `record/record-mutation.repository.ts` | 524 lines, 20 methods | raw `bulkWrite` / `updateMany`; `updateRecordData` has no user / `deletedAt` filter; `countOptionUsage` is a read sitting in M |
| D | `record/record-deleted-query.repository.ts` | 123 lines | trash list (`$facet`), restorable ids, bulk-delete ids |

Dead methods: `Q.findAllByFormId`, `Q.findActiveByForm`, `Q.findActiveByFormLean`,
`M.softDeleteRecord`, `M.restoreRecords`.

## 2. Sites per module

| Module | Direct sites | Highlights |
|---|---|---|
| record/core | 50 | submit, follow-up submit, edit, row lifecycle, bulk delete / edit, grid (fast path, cursor, filtered, column sort scan in memory), trash, schedule (raw aggregate ×2), JSON import, unique-id re-index, type migration (`bulkWrite` every row), column archive, revision adapters |
| record analysis / graphs / screening / reports | 7 | `analysis.repository.ts:44,83`; `graphs.repository.ts:34` (duplicate of `Q.aggregateRowIndexes`), `:91`, `:146`; `screening.repository.ts:266-272` `$lookup` (no status filter); `reports.service.ts:54` export loads every row |
| form | 9 (+3 indirect) | sample values, option-usage guards, CRF editor, form revision restore (drift, archive) |
| import | 4 | workbook inserts without a session; `Form.entryCount` set from a count |
| transform | 22 (+3 archive) in 18 files | backfill / sync `bulkWrite` by `_id` only; conversion analyze raw aggregate ×5; `$pull` in transform-update hits deleted rows too |
| analysis (doc-build) | 5 | field variance `status:'active'` only, no `deletedAt`; subset count imports the model directly |
| statistical-table / survival | 4 | survival `findRecordsByForm` loads the whole form |
| admin / user | 6 | global counts with raw filters |

Already behind services: AI record tools, bulk download, the unified analysis module (Python
reads `records` itself — `analysis-python.gateway.ts:101,144`).

## 3. What drives quota, `dataVersion`, staleness today

- `Form.entryCount` is the quota truth (`payment-quota.repository.ts:42-56` overwrites
  `used_entries` with the sum); JSON import instead checks `Q.countActiveByUser` — a second truth.
- submit / delete / restore / add row / bulk delete / JSON import: entryCount ± n, dataVersion +1,
  staleness mark-all, inside the transaction; edits and follow-up submit: dataVersion +1,
  staleness by column. Revision restores run their effects **after** commit, without a session.
- Transforms, conversions, type migration, identifier re-index: **no** dataVersion / staleness
  bump today (only structure events) — keep effects per caller in Phase 1 for parity.
- Storage quota comes from uploaded file sizes, not row bytes.

## 4. Tests

Unit tests exist for M `insertPreparedRecords`, bulk capture, grid order, unique-id re-index,
write guard, type-migration helper, revision adapters (they `vi.mock` Q/M by path), form
samples, import finalize / resume. **No unit tests** for submit, write, row lifecycle, read,
schedule, JSON import, column service, every transform service, `analysis.service`,
`graphs.service`, doc-build variance / subset / persist, survival, statistical table. e2e covers
most record HTTP routes and transforms (44); none for `record-ids`, `check-unique`, the
workbook worker, form revision restore, survival / stat-table row reads, admin counts.

## 5. Risks, order, interface

**Risks:** Mongo shapes leak into callers (turn them into typed intents, snapshot responses
first); inconsistent row filters must be kept as they are for parity (list in §2); edits are
read-modify-write without a version check (MongoStore keeps it, DataServiceStore must not);
whole-form loads into Node (export, column sort, survival, transform previews / backfills);
`core.helper.ts:276` takes `findFilteredActivePageByForm` off the repo **unbound** (a
`this`-delegating MongoStore would crash there); sessions are missing in places (OpContext
must allow "no session"); circular imports via barrels (`user.service.ts:7-9`); lint rule 1.7
needs exceptions for tests and migrations; two type-migration engines exist.

**Order** (snapshot → move → diff, smallest first): 0 scaffold a delegating MongoStore +
`dataStoreFor(form)` (always Mongo) + delete dead methods · 1 global counts · 2 small
form-scoped reads · 3 the five read-only bypasses · 4 analysis inputs · 5 grid family (move
`buildRecordGridPipeline` into MongoStore; fix the unbound method) · 6 follow-up schedule ·
7 single-row writes + OpContext + `WriteResult` · 8 bulk delete / edit (`applyBatch`) ·
9 unique identifier · 10 revision adapters, drift, archive · 11 type migration + CRF update ·
12 imports · 13 transforms last (write unit tests first) · 14 turn on the lint rule.

**Interface shapes:**

| Shape | For | Against |
|---|---|---|
| A. One wide `DataStore` (~35 methods, tech-design §4.2) | one resolver, one place | MongoStore far over the file-size limit; DataServiceStore must cover everything at once; heavy mocks |
| **B. Per-capability interfaces composed** — `RowReader`, `RowWriter`, `RowTrash`, `FieldStats`, `ColumnOps`, `RevisionRows`, `GlobalCounts` (recommended) | matches the codebase's existing narrow `*Like` dependency types; DataServiceStore ships capability by capability; the storage guard = "does this store implement X for this form"; small mocks | a little more type plumbing; boundaries agreed up front |
| C. Repository per module × 2 stores | closest to today | N×2 implementations; scattered scope rules and effects; N lint allowlists |

`GlobalCounts` stays separate in any shape: it spans forms that may live in different stores.

## 6. Plan assumptions that don't match the code

1. No hard delete of rows exists (`purgeDataset` has no counterpart); trashing a form leaves its rows untouched.
2. tech-design §4.3's row-scope table is incomplete: main / follow-up / file-access reads,
   `form-read:95` and follow-up submit are scoped by form access, not creator; revision restore
   allows the creator or the form owner.
3. Screening `$lookup` = attempts list, not export.
4. The proposed interface has no method for: schedule identifier search + paging, conversion
   metrics, numeric-range coverage, fill / distinct counts, removing a derived field's values,
   `countRecords(formIds)`, soft delete by form, many-row revision snapshots.
5. Python's analysis service reads `records` directly — Node's DataStore is not the only reader.
