# EaseMyResearch Data Service (Python 3.12 / FastAPI / delta-rs / DuckDB)

Design lives in the backend repo: `../Desktop/EaseMyResearch_Backend_V1/docs/architecture/data-service/`.
**consistency.md is the contract** — read it before touching any write path.

## Layout — the backend's layers, in Python

```txt
Backend: Route → DTO → Controller → Service → Repository → Model → MongoDB
Here:    router.py ─→ schemas.py ──→ service.py → repository.py → tables.py → S3 (Delta)
         (a FastAPI route function is the controller: one file)

src/dataservice/
├── main.py         app factory: settings, logging, error handlers, routers
├── config/         settings.py (every tunable; refuses unsafe configs) · logging.py
├── common/         errors.py (domain errors → HTTP) · auth.py (service token) · utils/ (pure)
├── storage/        fenced_commit.py — the ONLY gate for Delta writes; manifest; Node client
├── types/          shared types (add when a second module needs one)
└── modules/<name>/ router.py · schemas.py · service.py · repository.py · tables.py
tests/unit/         no services, runs on commit · tests/integration/ in-process moto S3 (+ Mongo), runs on push
                    (moto cannot serve delta-rs Parquet reads: assert on the Delta log there)
probes/             live library probes (run in CI and on push)
scripts/            check_commit_msg.py (commit-msg hook)
```

Enforced by import-linter: `main → modules → storage → common → config` (never upward), and
modules never import each other (the backend's no service→service rule). Create a module's
files when it is built — no empty placeholders.

## Rules

- Every Delta write goes through `storage/fenced_commit.py` (txn token + `{opId, attempt,
  opTables, part}`); `tests/unit/test_architecture.py` fails on any write call elsewhere.
- Readers read only the manifest's per-table versions; never "latest".
- The service never reads MongoDB for product data and never publishes: Node does (consistency §6).
- S3 commits are coordinated by S3 conditional writes (delta-rs ≥ 1.6 refuses to commit without them; C0 proves no commit is ever overwritten). No lock table. Startup refuses `AWS_S3_ALLOW_UNSAFE_RENAME`.
- No hardcoding: tunables in `config/settings.py`. Files ≤ 300–400 lines. Helpers in `utils/` pure.
- No `Any`, mypy strict, ruff clean. Every change updates its tests; prove a new test fails on
  the old code.
- Routes return Pydantic models (FastAPI serialises in Pydantic's Rust core — no ORJSONResponse,
  it is deprecated).
- Live-probe a library's behaviour before relying on it; add the fact to `probes/`.

## Commits — same as the backend

`.github/COMMIT_CONVENTION.md`. Hooks in `.githooks/` (`git config core.hooksPath .githooks`):
`prepare-commit-msg` adds `<emoji> <type>:` from the branch, `commit-msg` rejects anything else
(including a bare `type:`), `pre-commit` = ruff + mypy + layers + unit tests, `pre-push` = all
tests + probe. No Co-Authored-By lines. One type per commit.

## Gates

`uv run ruff check src tests probes scripts` · `uv run ruff format --check …` ·
`uv run python -m mypy src tests probes scripts` · `uv run ty check …` · import-linter ·
`uv run python -m pytest`

Windows dev box: Application Control blocks `.venv\Scripts\*.exe`; use `uv run python -m …`.
