# Data Service

> **Copy of the backend design (2026-10-24).** This folder and `EaseMyResearch_Backend_V1/docs/architecture/data-service/` must be kept identical — change both in the same change-set (this note is the only difference; copied from backend branch `docs/data-service-hybrid-edits`, commit `e966a6ed`).

**Status:** Proposed · design under review · Phase 0 prototype in progress · **Last updated:** 2026-10-24

A separate Python service that stores and computes over dataset rows (Delta Lake on S3,
DuckDB / Polars), so users can bring GB-scale CSV, SPSS, R and Parquet files, browse them in
a virtualised grid, edit, transform, clean and analyse them. Node remains the only public API,
orchestrates, and is the **sole publisher** of dataset **revisions** — each published in one
Mongo transaction with every effect (manifest, `dataVersion`, quota, staleness, history,
audit). Writes take one of two paths (**hybrid edits**, decided 2026-10-24): a **small edit**
(cell, row, bulk-edit part up to 500 rows) is a set of Mongo patches that Node writes in that
same transaction — no Delta commit; everything big (ingest, transforms, set-based edits,
compaction) is a durable operation that the Data Service commits to Delta under a fenced
writer lease and Node then publishes. **One reader** in the Data Service merges the Delta
tables and the patches for every read. Existing forms stay on MongoDB — no migration; a
per-form `storage` flag routes new forms to the Data Service.

| Document | What it covers |
|---|---|
| [prd.md](prd.md) | Problem, goals (targets until Phase 0), requirements, configuration, coexistence rule, decisions, open questions |
| [tech-design.md](tech-design.md) | Architecture, why Delta + DuckDB (and not Databricks yet), compute (Lambda / Fargate / always-on), DataStore abstraction, row scope as the code does it today, edits (Mongo patches + compaction), transforms, grid queries, security, failure handling |
| [data-model.md](data-model.md) | Mongo collections (metadata, `form.storage`, `dataset_patches`, `dataset_changes`, unique claims), Delta tables (base, child tables) |
| [api.md](api.md) | Public Node routes, internal Node ↔ Data Service API, callbacks, error codes |
| [feature-parity.md](feature-parity.md) | Every feature that exists today (verified against `src/v2`) and how it works on the Data Service, with its phase and deliberate changes |
| [consistency.md](consistency.md) | **Correctness protocol (v3.3, 2026-10-24)**: revision manifests (Delta versions + patch range), the small-edit Mongo transaction, the one reader and compaction, S3 conditional-write commits (C0 passed, no lock table), CAS writer lease with a fence commit in every table + attempt-aware reconciliation, durable operations + outbox for Delta jobs, schema snapshots per revision, post-publish event outbox, quota, multi-user editing (edits serialize on the revision, later commit wins with history), per-cell undo on the Mongo change log, nested cell addressing, retention, caches — and scenarios C0–C44 that must pass in Phase 0 |
| [implementation-plan.md](implementation-plan.md) | Phases 0–8 (prototype + benchmarks first, publish path before ingest) with steps, tests, acceptance, cost, risks |

**Decided (2026-10-21):** Delta Lake on S3 + DuckDB/Polars for rows (not MongoDB, not
Databricks); new repository; max file 10 GB; every current feature must work on the new
service, including editing on large data.

**Decided (2026-10-24):** no DynamoDB lock — S3 conditional writes coordinate Delta commits
(C0 passed); **hybrid edits** — small edits as Mongo patches in one Node transaction, big data
and big changes in Delta, one reader for every read; many users edit at once, and on the same
cell the later commit wins with the earlier value kept in history (PRD D8–D10).

**Still open:** typical dataset sizes + sample files (Q3) and the AWS budget (Q5) — the
design does not depend on them; Phase 0 infra sizing and the performance targets do.

The service's own repository (`easemyresearch-dataservice`) keeps an identical copy of these
documents in `docs/design/`; change both together.
