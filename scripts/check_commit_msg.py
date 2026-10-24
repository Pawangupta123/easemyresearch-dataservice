"""commit-msg hook: the same rules as the backend's commitlint config, plus the emoji.

`<emoji> <type>: <subject>` — type from the list, emoji matching the type, header ≤ 100.
Unlike the backend, a bare `type:` is rejected: there it silently disarmed the emoji hook
(.github/COMMIT_CONVENTION.md, "The trap").
"""

import re
import sys
from pathlib import Path

EMOJI_FOR_TYPE = {
    "feat": "🔥",
    "bug": "🐛",
    "fix": "🐛",
    "perf": "⚡",
    "chore": "🧹",
    "docs": "📚",
    "refactor": "♻️",
    "hotfix": "🚨",
    "test": "✅",
    "config": "🔧",
    "remove": "🗑️",
    "deploy": "🚀",
    "style": "💄",
}
HEADER = re.compile(r"^(?P<emoji>\S+) (?P<type>[a-z]+): (?P<subject>\S.*)$")
MAX_HEADER = 100


def problems(header: str) -> list[str]:
    if header.startswith(("Merge ", "Revert ")):
        return []
    match = HEADER.match(header)
    if not match:
        return ["header must be '<emoji> <type>: <subject>', e.g. '🔥 feat: Add dataset ingest'"]
    found: list[str] = []
    commit_type, emoji = match["type"], match["emoji"]
    if commit_type not in EMOJI_FOR_TYPE:
        found.append(f"unknown type '{commit_type}' — use one of: {', '.join(EMOJI_FOR_TYPE)}")
    elif emoji != EMOJI_FOR_TYPE[commit_type]:
        found.append(f"type '{commit_type}' takes the emoji {EMOJI_FOR_TYPE[commit_type]}, not {emoji}")
    if len(header) > MAX_HEADER:
        found.append(f"header is {len(header)} characters; the limit is {MAX_HEADER}")
    return found


def main() -> int:
    lines = Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
    header = next((line for line in lines if line.strip() and not line.startswith("#")), "")
    found = problems(header)
    for problem in found:
        print(f"commit-msg: {problem}", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
