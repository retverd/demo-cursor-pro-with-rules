"""Сбор курсов EUR/RUB с сайта ЦБ РФ и сохранение данных в Parquet-файл."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Sequence
from xml.etree import ElementTree

import pyarrow as pa
import pyarrow.parquet as pq
import requests

CBR_ENDPOINT = "https://www.cbr.ru/scripts/XML_dynamic.asp"
EUR_RUB_CODE = "R01239"
CBR_DATE_FORMAT = "%d/%m/%Y"
RECORD_DATE_FORMAT = "%d.%m.%Y"
DECIMAL_QUANT = Decimal("0.0001")


class RateFetchError(RuntimeError):
    """Базовое исключение для ошибок получения курсов."""


@dataclass(frozen=True, slots=True)
class RateRecord:
    """Содержит курс EUR/RUB на конкретную дату."""

    as_of: date
    value: Decimal


def build_date_range(days: int, *, end_date: date | None = None) -> tuple[date, date]:
    """Возвращает диапазон дат за последние `days` дней, включая конечную дату."""
    if days < 1:
        msg = "Число дней должно быть положительным."
        raise ValueError(msg)
    effective_end = end_date or date.today()
    start = effective_end - timedelta(days=days - 1)
    return start, effective_end


def fetch_eur_rub_rates(
    date_from: date,
    date_to: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 15.0,
) -> list[RateRecord]:
    """Загружает и парсит XML с курсами EUR/RUB от ЦБ РФ."""

    def _execute(http: requests.Session) -> list[RateRecord]:
        params = {
            "date_req1": date_from.strftime(CBR_DATE_FORMAT),
            "date_req2": date_to.strftime(CBR_DATE_FORMAT),
            "VAL_NM_RQ": EUR_RUB_CODE,
        }
        response = http.get(CBR_ENDPOINT, params=params, timeout=timeout)
        response.raise_for_status()
        return _parse_rates(response.content)

    if session is None:
        with requests.Session() as http:
            return _execute(http)
    return _execute(session)


def _parse_rates(payload: bytes) -> list[RateRecord]:
    """Преобразует XML в список записей курса."""
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        msg = "Не удалось распарсить ответ ЦБ РФ."
        raise RateFetchError(msg) from exc

    records: list[RateRecord] = []
    for record in root.findall("Record"):
        date_attr = record.attrib.get("Date")
        value_text = _text_or_none(record.find("Value"))
        nominal_text = _text_or_none(record.find("Nominal"))
        if not date_attr or value_text is None:
            msg = "В ответе отсутствуют обязательные поля."
            raise RateFetchError(msg)
        try:
            record_date = datetime.strptime(date_attr, RECORD_DATE_FORMAT).date()
        except ValueError as exc:
            msg = f"Некорректная дата в ответе ЦБ РФ: {date_attr}"
            raise RateFetchError(msg) from exc

        try:
            nominal = Decimal(nominal_text.replace(",", ".")) if nominal_text else Decimal(1)
            value = Decimal(value_text.replace(",", "."))
            if nominal == 0:
                raise RateFetchError("Номинал курса не может быть равен нулю.")
            per_unit = (value / nominal).quantize(DECIMAL_QUANT)
        except (InvalidOperation, ArithmeticError) as exc:
            msg = f"Некорректное числовое значение в ответе ЦБ РФ: {value_text}"
            raise RateFetchError(msg) from exc

        records.append(RateRecord(as_of=record_date, value=per_unit))

    records.sort(key=lambda item: item.as_of)
    return records


def _text_or_none(element: ElementTree.Element | None) -> str | None:
    """Возвращает текст узла XML либо None."""
    if element is None:
        return None
    return element.text


def build_table(records: list[RateRecord], metadata: dict[str, str]) -> pa.Table:
    """Создает pyarrow-таблицу с добавленными метаданными."""
    if not records:
        msg = "Нельзя построить таблицу без данных."
        raise ValueError(msg)

    table = pa.table(
        {
            "rate_date": pa.array([item.as_of for item in records], type=pa.date32()),
            "rate_value": pa.array([item.value for item in records], type=pa.decimal128(18, 4)),
        },
    )
    encoded_metadata = {key: value.encode("utf-8") for key, value in metadata.items()}
    return table.replace_schema_metadata(encoded_metadata)


def write_parquet(table: pa.Table, target: Path) -> None:
    """Сохраняет таблицу в Parquet, гарантируя существование каталога."""
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, target)


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
        description="Загрузка курсов EUR/RUB с сайта ЦБ РФ и выгрузка в Parquet.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("eur_rub_rates.parquet"),
        help="Путь до результирующего Parquet-файла.",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=7,
        help="Длина окна (в днях), включая конечную дату.",
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
    start_date, end_date = build_date_range(args.days, end_date=args.end_date)
    records = fetch_eur_rub_rates(start_date, end_date)
    if not records:
        msg = "ЦБ РФ вернул пустой набор данных."
        raise RateFetchError(msg)

    metadata = {
        "report_dt": datetime.now(timezone.utc).isoformat(),
        "report_from_date": records[0].as_of.isoformat(),
        "report_to_date": records[-1].as_of.isoformat(),
    }
        
    table = build_table(records, metadata)
    write_parquet(table, args.output)


if __name__ == "__main__":
    main()

