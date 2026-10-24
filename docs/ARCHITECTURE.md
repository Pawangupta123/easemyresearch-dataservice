# Architecture

**As of:** 2026-10-24 (`a14618e`) · Design (source of truth): [design/](design/README.md) ·
Progress: [STATUS.md](STATUS.md)

## 1. Where the service sits

The browser only talks to **Node** (the EaseMyResearch backend). Node authenticates, checks
permissions and quotas, writes the audit trail, keeps all metadata in MongoDB and is the **only
publisher** of dataset revisions. This service stores and computes over the **big data** (the
uploaded dataset, as Delta Lake on S3) and is the **one reader** of a revision.

| Lives in | What |
|---|---|
| **MongoDB** (Node owns it) | The app's brain: forms, fields, permissions, quotas, audit. Dataset bookkeeping: `datasets`, `dataset_revisions` (manifests), `dataset_leases`, `data_operations`, `data_jobs`, `dataset_schemas`. **Small edits**: `dataset_patches` (+ `dataset_changes`, `dataset_unique_claims`, `dataset_idempotency`) — [design/data-model.md](design/data-model.md) |
| **S3, Delta Lake** (this service owns it) | Big data: the base table and child tables per dataset, raw uploads, exports, reports, job change files. Private: only this service's role may read or write the Delta prefixes |

Two write paths ([design/consistency.md](design/consistency.md)):

- **Small edit** (≤ 500 rows): Node writes `dataset_patches` and the new revision in **one
  Mongo transaction** — this service is not involved in the write (§14 of the protocol).
- **Delta job** (ingest, transforms, set-based edits, compaction, dataset restore): this
  service's **writer** commits to Delta under a fenced lease; Node publishes (§4–§6).

**Mongo access rule:** this service reads exactly one collection, `dataset_patches`,
**read-only**, and never publishes. Everything else from Mongo comes through Node's internal
endpoints ([design/api.md §3](design/api.md#3-internal-api--data-service--node-writer-endpoints)).

## 2. Layers — the backend's, in Python

```txt
Backend: Route → DTO → Controller → Service → Repository → Model → MongoDB
Here:    router.py ─→ schemas.py ──→ service.py → repository.py → tables.py → S3 (Delta)
         (a FastAPI route function is the controller: one file)
```

| Backend | Here | Rule |
|---|---|---|
| Route + Controller | `modules/<name>/router.py` | Thin: validate, call the service, return `ApiResponse[T]` via `ok(...)` |
| DTO (Zod) | `modules/<name>/schemas.py` (Pydantic) | Request / response shapes |
| Service | `modules/<name>/service.py` | Business logic |
| Repository | `modules/<name>/repository.py` | Data access — reads through `storage/reader.py`, writes through `storage/fenced_commit.py` |
| Model | `modules/<name>/tables.py` | Delta table definitions |

Only `modules/health/` (`router.py`, `schemas.py`) exists so far; a module's files are created
when it is built — no empty placeholders.

## 3. Folder map

```txt
src/dataservice/
├── main.py                  app factory: settings, logging, error handlers, routers
├── config/                  settings.py (every tunable; refuses unsafe configs) · logging.py (structlog)
├── common/
│   ├── responses.py         the envelope: ApiResponse[T], ok(), ApiErrorResponse
│   ├── errors.py            AppError family + the ONE place errors become HTTP responses
│   ├── error_codes.py       ErrorCode — the one catalog (never rename a code)
│   ├── auth.py              require_service_token (on every route)
│   └── utils/service_token.py   sign / verify (pure)
├── storage/
│   ├── fenced_commit.py     the ONLY Delta write gate (append, fence, restore)
│   ├── reconcile.py         fence every table, then reconcile
│   ├── lease.py             Lease + LeaseAuthority (the contract Node serves)
│   ├── snapshot.py          Snapshot (base version + patch range), PatchSource, PATCH_SCHEMA
│   ├── mongo_patches.py     MongoPatchSource: reads dataset_patches (read-only)
│   └── reader.py            the ONE reader: base + patches → the revision's rows
└── modules/health/          GET /health
prototype/node_side/lease_store.py   REFERENCE Node logic: the Mongo writer lease
prototype/node_side/edit_store.py    REFERENCE Node logic: the small-edit transaction (hybrid edits)
probes/c0_real_s3.py                 C0 on the real AWS test bucket (on demand, env credentials)
probes/delta_api_probe.py            live facts about delta-rs / DuckDB
scripts/check_commit_msg.py          commit-msg hook
tests/unit/ · tests/integration/     see §8
docs/                                design copy, status, this file, onboarding
```

`types/` (shared types) is added when a second module needs one.

## 4. Import contracts (import-linter, `pyproject.toml`)

1. **Layers:** `dataservice.main → dataservice.modules → dataservice.storage →
   dataservice.common → dataservice.config` — never upward.
2. **Modules are independent:** no `dataservice.modules.*` imports another (the backend's no
   service → service across modules rule).

`tests/unit/test_architecture.py` adds three more:

- no delta-rs **write** call (`write_deltalake`, `.merge(`, `.restore(`, `.vacuum(`,
  `.optimize.`, `.alter.`, delta-rs `.update(` / `.delete(` …) outside `storage/fenced_commit.py`;
- no Delta **data read** (`QueryBuilder(`, `delta_scan`, `.to_pyarrow`, `.to_pandas(`,
  `.to_polars(`, …) outside `storage/reader.py`;
- `src/` never imports `prototype`, `tests` or `scripts`.

## 5. The single write gate and the single reader

- **Write gate** (`storage/fenced_commit.py`): every Delta commit carries
  `txn("emr-writer", token)` and metadata `token` plus either `ops = "opId:attempt,…"`,
  `opTables`, `part` or a `kind` (`fence`, `reconcile`); `max_commit_retries=0`; it refuses to
  commit when the table already carries a higher token (`FenceError`). A fence is a data-free
  `alter.set_table_properties({"emr.fence": token}, raise_if_not_exists=False)` commit; a
  rollback is a fenced `RESTORE` tagged `kind=reconcile`, `restoredTo`, `rolledBack`.
- **Reader** (`storage/reader.py`): a revision = the Delta base at its manifest version + the
  patches with `from_seq < seq ≤ to_seq` from a `PatchSource`. Rows = base ∪ `insert_row`
  patches; row state = latest `delete_row` / `restore_row` patch, else base `_deleted_at`;
  cell = latest `set` patch by `seq` (`t = "null"` clears), else base. Grid, analysis, export
  and compaction all read through it, so one revision always gives the same rows.

## 6. Request / response envelope and errors

Same as the backend's `sendSuccess` / `sendError`:

```txt
success: { "success": true,  "message": "Success", "data": … }
error:   { "success": false, "message": "…", "errors"?: [{ field, message }], "data": { "code": "<ErrorCode>" } }
```

- Routes return `ApiResponse[T]`; errors are raised as `AppError` subclasses and shaped only by
  the handlers in `common/errors.py`.
- Only a user-safe message is shown; anything else answers "Something went wrong. Please try
  again." and the cause is logged. Validation → 400 with `{ field, message }`; unknown route /
  method → 404 `ROUTE_NOT_FOUND` / 405 `METHOD_NOT_ALLOWED`.
- `ErrorCode` (`common/error_codes.py`): generic `INTERNAL_ERROR`, `VALIDATION_ERROR`,
  `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `ROUTE_NOT_FOUND`, `METHOD_NOT_ALLOWED`, `CONFLICT`;
  protocol `FENCED`; domain codes from [design/api.md §4](design/api.md#4-error-codes-user-safe-messages-via-the-friendly-messaging-layer)
  (`UNSUPPORTED_FORMAT` … `FORM_DELETED`). Node maps codes to user text, so a code is never
  renamed.

## 7. How Node and the Data Service talk

| Direction | Today (built) | Design |
|---|---|---|
| Node → Data Service | `x-service-token: <issuedAt>.<HMAC-SHA256(secret, issuedAt)>` on every route; shared `SERVICE_TOKEN_SECRET` | Short-lived token with `datasetId`, `scope`, `rowScope` ([design/api.md §2](design/api.md#2-internal-api-node--data-service)) |
| Data Service → Node | `LeaseAuthority` protocol (`acquire`, `renew`, `release`) — implemented only by the reference store in tests | HMAC-signed internal endpoints: lease, manifest, operations, publish, freeze, pins ([design/api.md §3](design/api.md#3-internal-api--data-service--node-writer-endpoints)) |
| Data Service ← Mongo | `MongoPatchSource` (`storage/mongo_patches.py`) reads `dataset_patches` with `from_seq < seq ≤ to_seq`, read-only | Same, with a read-only Mongo user limited to `dataset_patches` |

**`prototype/node_side/`** holds the **reference implementation of Node's side**: the
Mongo lease store (CAS on the expected previous token, `$$NOW`, a publish guard that writes the
lease row) and the small-edit transaction (idempotency, revision CAS, patches, change log, unique
claims, revision, `Form.dataVersion`, event outbox — all or nothing, in the documented shapes). It exists so the Phase 0 protocol tests can run end to end in Python; Node ports
exactly this logic to TypeScript in Phase 2. `src/` never imports it (test-enforced), and CI
type-checks and lints it.

## 8. Testing layers

| Layer | Where | Needs | Runs |
|---|---|---|---|
| Unit | `tests/unit/` | nothing (filesystem Delta, in-memory Arrow) | pre-commit, pre-push, CI |
| Integration | `tests/integration/` | in-process **moto** S3 (started by the fixture) + local Mongo replica set (`docker compose up -d`, port 27018; only localhost is accepted) | pre-push, CI (`REQUIRE_MONGO=1` turns a missing Mongo into a failure) |
| Probe | `probes/delta_api_probe.py` | the installed delta-rs / DuckDB | pre-push, CI |

moto cannot serve delta-rs Parquet data reads, so integration tests assert on the Delta log;
read-path tests use the local filesystem. Every change updates its tests; a new test must be
shown to fail on the old code (mutation-checked).

## 9. Commits and hooks

- Convention: `.github/COMMIT_CONVENTION.md` — `<emoji> <type>: <Subject>`; one type per
  commit; no `Co-Authored-By` lines.
- Hooks in `.githooks/` (enable once: `git config core.hooksPath .githooks`):
  - `prepare-commit-msg` — adds `<emoji> <type>: ` from the branch name (`feat/…`, `bug/…`,
    `docs/…`, …) when the message is a bare subject;
  - `commit-msg` — `scripts/check_commit_msg.py`: rejects a bare `type:`, a wrong emoji, an
    unknown type, a header over 100 characters;
  - `pre-commit` — ruff format + fix on staged files, mypy (strict), import-linter, unit tests;
  - `pre-push` — the whole test suite + the probe.
- CI (`.github/workflows/ci.yml`): ruff, ruff format check, mypy + ty, import-linter, pytest
  with a Mongo replica set, the probe.
