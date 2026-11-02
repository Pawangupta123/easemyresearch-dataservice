"""The background drain: one per dataset per process, a wake-up during a drain drains once more,
and a dataset without tables or with its lease held elsewhere is left alone."""

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from dataservice.common.errors import AppError
from dataservice.modules.writer.runner import DrainRunner
from dataservice.modules.writer.service import _rows

DATASET = "5eed000000000000000f4240"


class NoTablesRunner(DrainRunner):
    """Counts the drains instead of running them; `during` runs inside the first one."""

    drains: list[str]
    during: Callable[[], None] | None = None

    def drain_once(self, dataset_id: str) -> None:
        self.drains.append(dataset_id)
        during, self.during = self.during, None
        if during:
            during()


def _runner(tmp_path: Path, start: Callable[[Callable[[], None]], None]) -> NoTablesRunner:
    runner = NoTablesRunner(authority=None, lake_root=str(tmp_path), renew_seconds=10, owner="w", start=start)  # type: ignore[arg-type]
    runner.drains = []
    return runner


def test_a_wake_up_during_a_drain_drains_once_more(tmp_path: Path) -> None:
    pending: list[Callable[[], None]] = []
    runner = _runner(tmp_path, pending.append)
    answers: list[bool] = []
    runner.during = lambda: answers.extend([runner.request(DATASET), runner.request(DATASET)])
    assert runner.request(DATASET) is True
    assert runner.request(DATASET) is False  # before it began: that drain covers it
    pending.pop()()
    assert answers == [False, False]  # one is running: remembered, not started twice
    assert runner.drains == [DATASET, DATASET]  # the wake-ups during it drained once more, not twice
    assert not pending and runner.request(DATASET) is True  # finished: the next one starts anew


def test_a_dataset_without_tables_is_not_drained(tmp_path: Path) -> None:
    runner = DrainRunner(authority=None, lake_root=str(tmp_path), renew_seconds=10, owner="w")  # type: ignore[arg-type]
    runner.drain_once(DATASET)  # no table, and no call to Node (authority=None would raise)


def test_rows_arrive_as_json_and_are_read_by_the_column_types() -> None:
    schema = pa.schema(
        [
            ("_rid", pa.string()),
            ("_created_at", pa.timestamp("us", tz="UTC")),
            ("f_day", pa.date32()),
            ("f_age", pa.float64()),
        ]
    )
    table = _rows({"rows": [{"_rid": "r1", "_created_at": "2026-11-02T10:00:00+00:00", "f_day": "2026-01-31"}]}, schema)
    row = table.to_pylist()[0]
    assert row["_created_at"] == datetime.fromisoformat("2026-11-02T10:00:00+00:00")
    assert row["f_day"] == date(2026, 1, 31) and row["f_age"] is None


@pytest.mark.parametrize(
    "rows",
    [
        [{"_rid": "r1", "f_day": "31/01/2026"}],  # not a day
        [{"_rid": "r1", "f_age": "old"}],  # not a number
        [{"_rid": "r1", "f_nope": 1}],  # not a column
    ],
)
def test_rows_that_do_not_fit_the_columns_are_refused_not_retried(rows: list[dict[str, object]]) -> None:
    schema = pa.schema([("_rid", pa.string()), ("f_day", pa.date32()), ("f_age", pa.float64())])
    with pytest.raises(AppError):
        _rows({"rows": rows}, schema)
