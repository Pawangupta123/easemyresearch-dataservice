# Onboarding

**As of:** 2026-10-24 · for people and for Claude sessions working in this repository

## Run it on the Windows dev box

Windows Application Control blocks the `.venv\Scripts\*.exe` launchers — always run tools as
`uv run python -m <tool>`.

```bash
uv sync                                  # Python 3.12 + all dependencies
git config core.hooksPath .githooks      # once per clone: commit rules + quality gates
cp .env.example .env                     # then set SERVICE_TOKEN_SECRET (≥ 32 characters)
docker compose up -d                     # local Mongo replica set on port 27018 (integration tests)
uv run python -m pytest tests/unit       # unit tests — no services needed
uv run python -m pytest                  # everything; integration tests skip without Mongo
uv run python probes/delta_api_probe.py  # library facts (DuckDB delta check runs on Linux only)
uv run python -m granian --interface asgi dataservice.main:app --port 8100   # the API
```

S3 needs nothing local: the integration tests start an in-process moto server. Never point
tests at a shared or production Mongo — the fixture accepts only localhost.

## How a Claude session works here

1. **Read first:** [CLAUDE.md](../CLAUDE.md), [STATUS.md](STATUS.md), and before touching any
   write or read path [design/consistency.md](design/consistency.md) — it is the contract.
   [ARCHITECTURE.md](ARCHITECTURE.md) maps the layers and rules to files.
2. **Explain before coding:** tell the user in simple Hinglish what will change and why
   (options + trade-offs when there is a real choice), and wait for a nod on design or
   infrastructure decisions.
3. **Follow the rules:** Delta writes only through `storage/fenced_commit.py`, Delta reads only
   through `storage/reader.py`, Mongo only `dataset_patches` read-only; no hardcoding (tunables
   in `config/settings.py`); no `Any`; mypy strict; files ≤ 300–400 lines; live-probe a
   library before relying on it.
4. **Tests with every change:** update or add tests in the same commit and show a new test
   fails on the old code.
5. **Commits:** print `git branch --show-current` before every commit; pass a bare subject and
   let the hook add `<emoji> <type>:` (a bare `type:` is rejected); subject < 50 characters;
   one type per commit; **never** a `Co-Authored-By` line; never `--no-verify` — fix what a
   hook reports.
6. **Keep the docs true:** update [STATUS.md](STATUS.md) when something is built or a problem
   is solved; a design change goes into both copies — `docs/design/` here and
   `EaseMyResearch_Backend_V1/docs/architecture/data-service/` in the backend.
7. **Never** read or print `.env` secrets, and never search the whole home directory — stay in
   this repository and the backend repository.
