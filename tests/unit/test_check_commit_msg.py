import pytest

from scripts.check_commit_msg import problems


@pytest.mark.parametrize(
    "header",
    [
        "🔥 feat: Add dataset ingest",
        "🐛 bug: Keep the fence on retry",
        "📚 docs: Explain the layers",
        "Merge branch 'x'",
    ],
)
def test_accepts_the_convention(header: str) -> None:
    assert problems(header) == []


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("feat: Add dataset ingest", "header must be"),  # bare type — the backend's trap
        ("🐛 feat: Add dataset ingest", "takes the emoji 🔥"),
        ("🔥 feature: Add dataset ingest", "unknown type"),
        ("🔥 feat: " + "x" * 100, "limit is 100"),
    ],
)
def test_rejects_what_breaks_it(header: str, expected: str) -> None:
    assert any(expected in problem for problem in problems(header))
