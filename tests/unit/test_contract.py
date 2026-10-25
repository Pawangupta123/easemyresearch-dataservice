"""The committed contract (contracts/, copied into the backend's tests) is what the service
answers today. After a deliberate change: `uv run python -m scripts.export_contract`, then copy
contracts/ into the backend (tests/fixtures/dataservice/) — the backend's test checks the copy."""

from pathlib import Path

from scripts.export_contract import build, render

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


def test_the_committed_contract_is_what_the_service_answers() -> None:
    stale = [
        path for path, content in build().items() if (CONTRACTS / path).read_bytes().decode("utf-8") != render(content)
    ]
    assert stale == [], f"contract out of date: {stale} — run `python -m scripts.export_contract`"
