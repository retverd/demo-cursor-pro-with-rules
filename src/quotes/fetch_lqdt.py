"""Сбор дневных свечей и средневзвешенной цены инструмента LQDT с МосБиржи и сохранение в XLSX-файл."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence

import apimoex
import pandas as pd
import requests

SECURITY = "LQDT"
BOARD = "TQTF"
MARKET = "shares"
ENGINE = "stock"
CANDLE_INTERVAL = 24  # Дневные свечи
COLUMNS = ("begin", "open", "high", "low", "close", "volume", "value")

class QuoteFetchError(RuntimeError):
    """Базовое исключение для ошибок получения котировок."""


def build_date_range(years: int, *, end_date: date | None = None) -> tuple[date, date]:
    """Возвращает диапазон дат за последние `years` лет, включая конечную дату."""
    if years < 1:
        msg = "Число лет должно быть положительным."
        raise ValueError(msg)
    effective_end = end_date or date.today()
    start = effective_end - timedelta(days=years * 365)
    return start, effective_end


def fetch_lqdt_candles(
    date_from: date,
    date_to: date,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, float | str]]:
    """Загружает дневные свечи инструмента LQDT с МосБиржи."""
    if session is None:
        with requests.Session() as http:
            return _execute_fetch(http, date_from, date_to)
    return _execute_fetch(session, date_from, date_to)


def _execute_fetch(
    session: requests.Session,
    date_from: date,
    date_to: date,
) -> list[dict[str, float | str]]:
    """Выполняет запрос к API МосБиржи."""
    try:
        data = apimoex.get_board_candles(
            session=session,
            security=SECURITY,
            board=BOARD,
            start=date_from.strftime("%Y-%m-%d"),
            end=date_to.strftime("%Y-%m-%d"),
            interval=CANDLE_INTERVAL,
            market=MARKET,
            engine=ENGINE,
            columns=COLUMNS,
        )
        if not data:
            msg = f"МосБиржа вернула пустой набор данных для {SECURITY}."
            raise QuoteFetchError(msg)
        return data
    except requests.RequestException as exc:
        msg = f"Ошибка при запросе к API МосБиржи: {exc}"
        raise QuoteFetchError(msg) from exc
    except Exception as exc:
        msg = f"Неожиданная ошибка при получении данных: {exc}"
        raise QuoteFetchError(msg) from exc


def build_dataframe(candles: list[dict[str, float | str]]) -> pd.DataFrame:
    """Создает pandas DataFrame из данных свечей."""
    if not candles:
        msg = "Нельзя построить DataFrame без данных."
        raise ValueError(msg)

    df = pd.DataFrame(candles)

    # Переименование колонок для удобства
    column_mapping = {
        "begin": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "value": "value",
        "waprice": "vwap",
    }

    # Выбираем только нужные колонки и переименовываем
    available_columns = [col for col in column_mapping.keys() if col in df.columns]
    df = df[available_columns].rename(columns=column_mapping)

    # Преобразуем дату в datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Сортируем по дате
    if "date" in df.columns:
        df = df.sort_values("date").reset_index(drop=True)

    return df


def generate_output_filename(date_from: date, date_to: date, created_at: datetime) -> str:
    """Генерирует имя файла с датами начала и окончания периода и датой создания с точностью до минут."""
    created_str = created_at.strftime("%Y-%m-%d_%H-%M")
    return f"lqdt_rates_{date_from.isoformat()}_{date_to.isoformat()}_{created_str}.xlsx"


def write_xlsx(df: pd.DataFrame, target: Path) -> None:
    """Сохраняет DataFrame в XLSX-файл, гарантируя существование каталога."""
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(target, index=False, engine="openpyxl")


def _positive_int(value: str) -> int:
    """Преобразует строку в положительный int."""
    try:
        parsed = int(value)
    except ValueError as exc:
        msg = f"Некорректное целое число: {value}"
        raise argparse.ArgumentTypeError(msg) from exc
    if parsed < 1:
        msg = "Значение должно быть больше нуля."
        raise argparse.ArgumentTypeError(msg)
    return parsed


def _parse_iso_date(value: str) -> date:
    """Преобразует строку ISO-формата в дату."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        msg = f"Некорректный формат даты (ожидается YYYY-MM-DD): {value}"
        raise argparse.ArgumentTypeError(msg) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Возвращает аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Загрузка дневных свечей и VWAP инструмента LQDT с МосБиржи и выгрузка в XLSX.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Путь до результирующего XLSX-файла (по умолчанию генерируется автоматически).",
    )
    parser.add_argument(
        "--years",
        type=_positive_int,
        default=2,
        help="Количество лет истории (по умолчанию 2).",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_iso_date,
        default=None,
        help="Последняя дата диапазона в формате YYYY-MM-DD (по умолчанию сегодня).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Точка входа командной строки."""
    args = parse_args(argv)
    start_date, end_date = build_date_range(args.years, end_date=args.end_date)
    candles = fetch_lqdt_candles(start_date, end_date)
    if not candles:
        msg = f"МосБиржа вернула пустой набор данных для {SECURITY}."
        raise QuoteFetchError(msg)

    df = build_dataframe(candles)

    # Генерируем имя файла, если не указано
    if args.output is None:
        created_at = datetime.now()
        output_filename = generate_output_filename(start_date, end_date, created_at)
        output_path = Path(output_filename)
    else:
        output_path = args.output

    write_xlsx(df, output_path)
    print(f"Данные сохранены в файл: {output_path}")


if __name__ == "__main__":
    main()

