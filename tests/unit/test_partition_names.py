from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from rez_ctx_consumer.partitions import add_months, month_start, partition_name


def test_month_start() -> None:
    assert month_start(datetime(2026, 11, 20, 15, 30, 1, 5, tzinfo=UTC)) == datetime(
        2026, 11, 1, tzinfo=UTC
    )


def test_month_start_converts_to_utc() -> None:
    # 1 Dec 02:00 in UTC+5 is still 30 Nov in UTC.
    plus5 = timezone(timedelta(hours=5))
    assert month_start(datetime(2026, 12, 1, 2, 0, tzinfo=plus5)) == datetime(
        2026, 11, 1, tzinfo=UTC
    )


@pytest.mark.parametrize(
    ("start", "count", "expected"),
    [
        ((2026, 11), 1, (2026, 12)),
        ((2026, 12), 1, (2027, 1)),
        ((2026, 1), -1, (2025, 12)),
        ((2026, 6), 18, (2027, 12)),
        ((2026, 6), 0, (2026, 6)),
    ],
)
def test_add_months(start: tuple[int, int], count: int, expected: tuple[int, int]) -> None:
    month = datetime(*start, 1, tzinfo=UTC)
    assert add_months(month, count) == datetime(*expected, 1, tzinfo=UTC)


def test_partition_name() -> None:
    assert partition_name(datetime(2026, 3, 1, tzinfo=UTC)) == "rez_context_event_y2026m03"
