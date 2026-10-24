# EaseMyResearch Data Service (Python 3.12 / FastAPI / delta-rs / DuckDB)

Design lives in the backend repo: `../Desktop/EaseMyResearch_Backend_V1/docs/architecture/data-service/`,
with an identical copy in [docs/design/](docs/design/README.md) (change both together).
**consistency.md is the contract** — read it before touching any write or read path.
Start here: [docs/STATUS.md](docs/STATUS.md) (what is built, what is not, problems solved,
decisions) · [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · [docs/ONBOARDING.md](docs/ONBOARDING.md).

**Hybrid edits (decided 2026-10-24):** small edits (≤ 500 rows) are Mongo `dataset_patches`
that Node writes in one transaction — no Delta commit, lease or queue. Big data and big
changes (ingest, transforms, set-based edits, compaction, restore) stay Delta jobs under the
lease. Every read of a revision goes through the one reader (Delta at the manifest's versions +
the revision's patch range).

## Layout — the backend's layers, in Python

```txt
Backend: Route → DTO → Controller → Service → Repository → Model → MongoDB
Here:    router.py ─→ schemas.py ──→ service.py → repository.py → tables.py → S3 (Delta)
         (a FastAPI route function is the controller: one file)

src/dataservice/
├── main.py         app factory: settings, logging, error handlers, routers
├── config/         settings.py (every tunable; refuses unsafe configs) · logging.py
├── common/         errors.py (domain errors → HTTP) · auth.py (service token) · utils/ (pure)
├── storage/        fenced_commit.py — the ONLY gate for Delta writes · reader.py — the ONLY Delta reader ·
│                   reconcile.py · lease.py · operations.py (job contract) · compaction.py ·
│                   snapshot.py (manifest view, PatchSource) · mongo_patches.py (read-only)
├── types/          shared types (add when a second module needs one)
└── modules/<name>/ router.py · schemas.py · service.py · repository.py · tables.py
tests/unit/         no services, runs on commit · tests/integration/ in-process moto S3 (+ Mongo), runs on push
                    (moto cannot serve delta-rs Parquet reads: assert on the Delta log there)
probes/             live library probes (run in CI and on push)
prototype/node_side/ REFERENCE Node logic (lease, edits, undo, compaction, operations, publish) for the tests;
                    Node ports it to TypeScript in Phase 2. src/ never imports it (test-enforced)
scripts/            check_commit_msg.py (commit-msg hook)
```

Enforced by import-linter: `main → modules → storage → common → config` (never upward), and
modules never import each other (the backend's no service→service rule). Create a module's
files when it is built — no empty placeholders.

## Rules

- Every Delta write goes through `storage/fenced_commit.py` (txn token + `{opId, attempt,
  opTables, part}`); `tests/unit/test_architecture.py` fails on any write call elsewhere.
- Every read goes through `storage/reader.py`: the manifest's per-table versions + the patches
  with `from_seq < seq ≤ to_seq`; never "latest". `tests/unit/test_architecture.py` fails on any
  Delta data read elsewhere.
- The service reads only `dataset_patches` from MongoDB, read-only, and never publishes: Node
  does (consistency §6, §14).
- S3 commits are coordinated by S3 conditional writes (delta-rs ≥ 1.6 refuses to commit without them; C0 proves no commit is ever overwritten). No lock table. Startup refuses `AWS_S3_ALLOW_UNSAFE_RENAME`.
- No hardcoding: tunables in `config/settings.py`. Files ≤ 300–400 lines. Helpers in `utils/` pure.
- No `Any`, mypy strict, ruff clean. Every change updates its tests; prove a new test fails on
  the old code.
- Responses use the backend's envelope (`common/responses.py`): routes return `ApiResponse[T]` via
  `ok(data, message)`; errors are raised as `common/errors.py` subclasses with an `ErrorCode`
  (`common/error_codes.py`, never rename a code) and shaped only by the registered handlers —
  `{ success: false, message, errors?, data: { code } }`, user-safe messages only. FastAPI
  serialises the Pydantic return type in its Rust core (no ORJSONResponse — deprecated).
- Live-probe a library's behaviour before relying on it; add the fact to `probes/`.

## Commits — same as the backend

`.github/COMMIT_CONVENTION.md`. Hooks in `.githooks/` (`git config core.hooksPath .githooks`):
`prepare-commit-msg` adds `<emoji> <type>:` from the branch, `commit-msg` rejects anything else
(including a bare `type:`), `pre-commit` = ruff + mypy + layers + unit tests, `pre-push` = all
tests + probe. No Co-Authored-By lines. One type per commit.

## Gates

`uv run ruff check src tests probes scripts` · `uv run ruff format --check …` ·
`uv run python -m mypy src tests probes scripts prototype` · `uv run ty check …` · import-linter ·
`uv run python -m pytest`

Windows dev box: Application Control blocks `.venv\Scripts\*.exe`; use `uv run python -m …`.
