# EaseMyResearch Data Service

Stores and computes over large research datasets (CSV, SPSS `.sav`, R `.rds` / `.RData`,
up to 10 GB) as **Delta Lake on S3**, queried with **DuckDB**. Node (EaseMyResearch backend)
stays the only publisher and the only API the browser talks to. Small edits are Mongo patches
that Node writes itself (hybrid edits, 2026-10-24); this service reads them, read-only, through
its one reader and folds them into Delta by compaction.

**Status:** Phase 0 — scaffold + consistency prototype. Nothing is in production. What is
built and what is not: [docs/STATUS.md](docs/STATUS.md).

Design (source of truth): `EaseMyResearch_Backend_V1/docs/architecture/data-service/`, copied
identically to [docs/design/](docs/design/README.md) — `prd.md`, `tech-design.md`,
`consistency.md` (protocol + scenarios C0–C44), `data-model.md`, `api.md`, `feature-parity.md`,
`implementation-plan.md`. Code map and rules: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md);
first steps: [docs/ONBOARDING.md](docs/ONBOARDING.md).

## Why it is fast

Python only orchestrates; the heavy work runs in native engines that share Arrow memory
without copies: **DuckDB** (C++) for queries, **Polars** (Rust) for transforms and cleaning,
**delta-rs** (Rust) for storage, **obstore** (Rust) for S3 transfers, **Granian** (Rust) as the
HTTP server, **Pydantic** (Rust core) serialising responses straight to JSON bytes. Reads
project only the visible columns; counts and sort orders are cached per revision; a cell edit
is a small Mongo patch instead of a rewrite of Parquet files.

## Stack

| Layer | Tool | In use from |
|---|---|---|
| API | FastAPI 0.162 · Pydantic 2.14 · Granian 2.8 | now |
| Storage | delta-rs (deltalake) 1.6 · pyarrow 26 | now |
| Query | DuckDB 2.0 (delta extension) | now (probe) — query module |
| Transform / cleaning | Polars 2.0 | jobs module |
| S3 transfers | obstore | ingest module |
| SPSS / R | pyreadstat · pyreadr | ingest module |
| Logs · metrics · tracing | structlog · prometheus-client · OpenTelemetry | logs now, rest with the first module |
| Tooling | uv · ruff · mypy (gate) · ty · pytest · import-linter | now |

Python is **3.12**: DuckDB 2.0 ships wheels for 3.11 and 3.12 only.

## Run

```bash
uv sync
git config core.hooksPath .githooks   # once per clone: commit rules + quality gates
cp .env.example .env                  # set SERVICE_TOKEN_SECRET
uv run python -m granian --interface asgi dataservice.main:app --port 8100
uv run python -m pytest               # unit tests (filesystem Delta, no services)
docker compose up -d                  # Mongo replica set (protocol tests); S3 = in-process moto
uv run python probes/delta_api_probe.py
```

On the Windows dev box, Application Control blocks the `.venv\Scripts\*.exe` launchers — use
`uv run python -m <tool>`. CI runs on Linux.

## Commits

Same convention as the backend — `.github/COMMIT_CONVENTION.md`. Commit on a `feat/…`,
`bug/…`, `docs/…` branch with a bare subject; the hook adds the emoji and type.
