"""Тесты для загрузчика курсов EUR/RUB."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from src.rates.fetch_usd_rub import (
    RateFetchError,
    RateRecord,
    build_date_range,
    build_table,
    fetch_eur_rub_rates,
    write_parquet,
)


class DummyResponse:
    """Упрощенный HTTP-ответ для имитации requests."""

    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        """Шаблонный метод для совместимости с requests."""
        return None


class DummySession:
    """Упрощенная HTTP-сессия с заранее определенным ответом."""

    def __init__(self, content: bytes) -> None:
        self._content = content
        self.last_params: dict[str, str] | None = None

    def get(self, url: str, params: dict[str, str], timeout: float) -> DummyResponse:
        """Возвращает заранее подготовленный ответ и сохраняет параметры."""
        self.last_params = params
        return DummyResponse(self._content)


def test_build_date_range_default_reference() -> None:
    """Проверяет вычисление диапазона дат с переданной конечной датой."""
    start, end = build_date_range(7, end_date=date(2025, 11, 18))
    assert start == date(2025, 11, 12)
    assert end == date(2025, 11, 18)


def test_fetch_eur_rub_rates_parses_xml() -> None:
    """Убеждается, что XML корректно переводится в список записей."""
    xml_payload = b"""
        <ValCurs>
            <Record Date="18.11.2025">
                <Nominal>2</Nominal>
                <Value>94,2000</Value>
            </Record>
            <Record Date="17.11.2025">
                <Nominal>1</Nominal>
                <Value>93,1000</Value>
            </Record>
        </ValCurs>
    """
    session = DummySession(xml_payload)
    records = fetch_eur_rub_rates(
        date(2025, 11, 12),
        date(2025, 11, 18),
        session=session,
    )
    assert session.last_params == {
        "date_req1": "12/11/2025",
        "date_req2": "18/11/2025",
        "VAL_NM_RQ": "R01239",
    }
    assert records == [
        RateRecord(as_of=date(2025, 11, 17), value=Decimal("93.1000")),
        RateRecord(as_of=date(2025, 11, 18), value=Decimal("47.1000")),
    ]


def test_fetch_eur_rub_rates_handles_invalid_xml() -> None:
    """Проверяет обработку некорректного XML."""
    session = DummySession(b"<broken>")
    with pytest.raises(RateFetchError):
        fetch_eur_rub_rates(date(2025, 11, 12), date(2025, 11, 18), session=session)


def test_build_table_and_write_roundtrip(tmp_path: Path) -> None:
    """Проверяет добавление метаданных и запись в Parquet."""
    records = [
        RateRecord(as_of=date(2025, 11, 17), value=Decimal("93.1000")),
        RateRecord(as_of=date(2025, 11, 18), value=Decimal("94.2000")),
    ]
    metadata = {
        "report_dt": "2025-11-18T10:00:00+00:00",
        "report_from_date": "2025-11-17",
        "report_to_date": "2025-11-18",
    }
    table = build_table(records, metadata)
    output = tmp_path / "rates.parquet"
    write_parquet(table, output)

    loaded = pq.read_table(output)
    assert loaded.num_rows == 2
    expected_metadata = {
        key.encode("utf-8"): value.encode("utf-8") for key, value in metadata.items()
    }
    assert loaded.schema.metadata == expected_metadata
