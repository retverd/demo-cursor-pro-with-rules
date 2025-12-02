"""Тесты для загрузчика котировок LQDT с МосБиржи."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.quotes.fetch_lqdt import (
    QuoteFetchError,
    build_dataframe,
    build_date_range,
    fetch_lqdt_candles,
    generate_output_filename,
    write_xlsx,
)


def test_build_date_range_default_reference() -> None:
    """Проверяет вычисление диапазона дат с переданной конечной датой."""
    start, end = build_date_range(2, end_date=date(2024, 1, 15))
    assert start == date(2022, 1, 15)
    assert end == date(2024, 1, 15)


def test_build_date_range_raises_on_invalid_years() -> None:
    """Проверяет, что функция выбрасывает ошибку при неположительном количестве лет."""
    with pytest.raises(ValueError, match="Число лет должно быть положительным"):
        build_date_range(0)


def test_generate_output_filename() -> None:
    """Проверяет генерацию имени файла с датами и временем создания."""
    from datetime import datetime

    created_at = datetime(2025, 1, 23, 14, 30)
    filename = generate_output_filename(date(2022, 1, 15), date(2024, 1, 15), created_at)
    assert filename == "lqdt_rates_2022-01-15_2024-01-15_2025-01-23_14-30.xlsx"


@patch("src.quotes.fetch_lqdt.apimoex.get_board_candles")
def test_fetch_lqdt_candles_success(mock_get_candles: MagicMock) -> None:
    """Проверяет успешное получение свечей."""
    mock_data = [
        {
            "begin": "2024-01-15T10:00:00",
            "open": 100.5,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1000,
            "value": 101000.0,
            "waprice": 101.0,
        },
        {
            "begin": "2024-01-16T10:00:00",
            "open": 101.0,
            "high": 103.0,
            "low": 100.0,
            "close": 102.0,
            "volume": 2000,
            "value": 204000.0,
            "waprice": 102.0,
        },
    ]
    mock_get_candles.return_value = mock_data

    session = MagicMock()
    result = fetch_lqdt_candles(
        date(2024, 1, 15),
        date(2024, 1, 16),
        session=session,
    )

    assert result == mock_data
    mock_get_candles.assert_called_once_with(
        session=session,
        security="LQDT",
        board="TQTF",
        start="2024-01-15",
        end="2024-01-16",
        interval=24,
        market="shares",
        engine="stock",
        columns=("begin", "open", "high", "low", "close", "volume", "value"),
    )


@patch("src.quotes.fetch_lqdt.apimoex.get_board_candles")
def test_fetch_lqdt_candles_empty_result(mock_get_candles: MagicMock) -> None:
    """Проверяет обработку пустого результата от API."""
    mock_get_candles.return_value = []

    session = MagicMock()
    with pytest.raises(QuoteFetchError, match="МосБиржа вернула пустой набор данных"):
        fetch_lqdt_candles(date(2024, 1, 15), date(2024, 1, 16), session=session)


@patch("src.quotes.fetch_lqdt.apimoex.get_board_candles")
def test_fetch_lqdt_candles_handles_request_exception(mock_get_candles: MagicMock) -> None:
    """Проверяет обработку ошибок запроса."""
    import requests

    mock_get_candles.side_effect = requests.RequestException("Network error")

    session = MagicMock()
    with pytest.raises(QuoteFetchError, match="Ошибка при запросе к API МосБиржи"):
        fetch_lqdt_candles(date(2024, 1, 15), date(2024, 1, 16), session=session)


def test_build_dataframe() -> None:
    """Проверяет создание DataFrame из данных свечей."""
    candles = [
        {
            "begin": "2024-01-15T10:00:00",
            "open": 100.5,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1000,
            "value": 101000.0,
            "waprice": 101.0,
        },
        {
            "begin": "2024-01-16T10:00:00",
            "open": 101.0,
            "high": 103.0,
            "low": 100.0,
            "close": 102.0,
            "volume": 2000,
            "value": 204000.0,
            "waprice": 102.0,
        },
    ]

    df = build_dataframe(candles)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "date" in df.columns
    assert "open" in df.columns
    assert "high" in df.columns
    assert "low" in df.columns
    assert "close" in df.columns
    assert "volume" in df.columns
    assert "value" in df.columns
    assert "vwap" in df.columns
    assert df["date"].dtype == "datetime64[ns]"
    assert df.iloc[0]["open"] == 100.5
    assert df.iloc[1]["vwap"] == 102.0


def test_build_dataframe_raises_on_empty() -> None:
    """Проверяет, что функция выбрасывает ошибку при пустых данных."""
    with pytest.raises(ValueError, match="Нельзя построить DataFrame без данных"):
        build_dataframe([])


def test_write_xlsx(tmp_path: Path) -> None:
    """Проверяет сохранение DataFrame в XLSX-файл."""
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
            "open": [100.5, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000, 2000],
            "value": [101000.0, 204000.0],
            "vwap": [101.0, 102.0],
        }
    )

    output = tmp_path / "test_output.xlsx"
    write_xlsx(df, output)

    assert output.exists()
    # Проверяем, что файл можно прочитать обратно
    loaded = pd.read_excel(output, engine="openpyxl")
    assert len(loaded) == 2
    assert "date" in loaded.columns
    assert "vwap" in loaded.columns
