"""
Parsing helpers for SIROS schedule text files.

The API returns a SSIM-like CSV file whose header names can vary between
temporadas. We try to autodetect the delimiter, normalise column names and
return a tidy ``pandas.DataFrame`` with consistent fields expected by the
downstream pipeline.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, time as time_cls, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from pandas import DataFrame

logger = logging.getLogger(__name__)


STANDARD_COLUMNS = {
    "temporada",
    "cia",
    "numero_voo",
    "act_type",
    "origem",
    "destino",
    "aeroporto_operacao",
    "dt_partida_utc",
    "dt_chegada_utc",
    "natureza",
    "assentos_previstos",
}

COLUMN_ALIASES: Dict[str, Iterable[str]] = {
    "temporada": ("temporada", "ds_temporada", "season_code", "temporada_ref"),
    "cia": ("cia", "cia_aerea", "airline", "airline_code", "cia_operadora"),
    "numero_voo": ("numero_voo", "nr_voo", "flight_number", "numero"),
    "act_type": (
        "act_type",
        "aircraft_type",
        "icao_tipo_equipamento",
        "equipamento",
        "equipment",
    ),
    "origem": ("origem", "origin", "aerodromo_origem", "aeroporto_origem"),
    "destino": ("destino", "destination", "aerodromo_destino", "aeroporto_destino"),
    "aeroporto_operacao": (
        "aeroporto",
        "aeroporto_operacao",
        "airport",
        "icao_aeroporto",
    ),
    "dt_partida_utc": (
        "dt_partida_utc",
        "partida_utc",
        "horario_partida_utc",
        "departure_utc",
    ),
    "dt_chegada_utc": (
        "dt_chegada_utc",
        "chegada_utc",
        "horario_chegada_utc",
        "arrival_utc",
    ),
    "natureza": ("natureza", "tipo_voo", "dom_int", "flight_nature"),
    "assentos_previstos": (
        "assentos_previstos",
        "assentos",
        "capacity",
    "payload_assentos",
    ),
}

CARGO_SERVICE_TYPES = {"F", "M"}


def _looks_like_ssim(text: str) -> bool:
    header = text.lstrip()[:40].upper()
    return header.startswith("1AIRLINE STANDARD SCHEDULE DATA SET")


def _parse_hhmm(value: str) -> Optional[time_cls]:
    value = value.strip()
    if not value or len(value) != 4 or not value.isdigit():
        return None
    hours = int(value[:2])
    minutes = int(value[2:])
    if hours >= 24 or minutes >= 60:
        return None
    return time_cls(hour=hours, minute=minutes)


def _parse_offset(value: str) -> Optional[int]:
    value = value.strip()
    if not value:
        return None
    try:
        sign = -1 if value.startswith("-") else 1
        value = value.lstrip("+-")
        if len(value) != 4 or not value.isdigit():
            return None
        hours = int(value[:2])
        minutes = int(value[2:])
        return sign * (hours * 60 + minutes)
    except Exception:
        return None


def _ssim_days(days_field: str) -> Sequence[int]:
    digits = [int(ch) for ch in days_field if ch.isdigit()]
    return digits if digits else tuple()


def _parse_ssim_line(line: str) -> Optional[Dict[str, object]]:
    if len(line) < 75:
        return None

    airline = line[2:4].strip().upper()
    flight_number = line[5:9].strip().upper()
    suffix = line[9:11].strip().upper()
    leg_sequence = line[11:13].strip().upper()
    service_type = line[13].strip().upper()

    start_date_raw = line[14:21].strip().upper()
    end_date_raw = line[21:28].strip().upper()
    days_raw = line[28:35]

    departure_airport = line[36:39].strip().upper()
    departure_utc_time = _parse_hhmm(line[43:47])
    arrival_airport = line[54:57].strip().upper()
    arrival_utc_time = _parse_hhmm(line[61:65])

    equipment = line[72:75].strip().upper()

    if not (airline and flight_number and departure_airport and arrival_airport):
        return None
    if departure_utc_time is None or arrival_utc_time is None:
        return None

    try:
        start_date = datetime.strptime(start_date_raw, "%d%b%y").date()
        end_date = datetime.strptime(end_date_raw, "%d%b%y").date()
    except Exception:
        return None

    days = tuple(_ssim_days(days_raw))
    departure_offset = _parse_offset(line[47:52])
    arrival_offset = _parse_offset(line[65:70])
    return {
        "cia": airline,
        "numero_voo": flight_number,
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "departure_airport": departure_airport,
        "arrival_airport": arrival_airport,
        "departure_utc_time": departure_utc_time,
        "arrival_utc_time": arrival_utc_time,
        "departure_offset": departure_offset,
        "arrival_offset": arrival_offset,
        "equipment": equipment or None,
        "suffix": suffix or None,
        "leg_sequence": leg_sequence or None,
        "service_type": service_type or None,
    }


def _expand_ssim_record(record: Dict[str, object]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    start_date = record["start_date"]
    end_date = record["end_date"]
    days: Sequence[int] = record["days"]  # type: ignore[assignment]
    cia = record["cia"]
    numero_voo = record["numero_voo"]
    origem = record["departure_airport"]
    destino = record["arrival_airport"]
    dep_time: time_cls = record["departure_utc_time"]
    arr_time: time_cls = record["arrival_utc_time"]
    equipamento = record.get("equipment")
    service_type = record.get("service_type")

    current = start_date
    while current <= end_date:
        weekday = current.weekday() + 1  # Monday=1
        if not days or weekday in days:
            dep_dt = datetime.combine(current, dep_time, tzinfo=timezone.utc)
            arr_date = current
            arr_dt = datetime.combine(arr_date, arr_time, tzinfo=timezone.utc)
            if arr_dt < dep_dt:
                arr_dt += timedelta(days=1)

            rows.append(
                {
                    "cia": cia,
                    "numero_voo": numero_voo,
                    "act_type": equipamento,
                    "origem": origem,
                    "destino": destino,
                    "dt_partida_utc": pd.Timestamp(dep_dt),
                    "dt_chegada_utc": pd.Timestamp(arr_dt),
                    "service_type": service_type,
                }
            )
        current += timedelta(days=1)
    return rows


def _parse_ssim_dataset(text: str) -> DataFrame:
    records: List[Dict[str, object]] = []
    for line in text.splitlines():
        if not line.startswith("3 "):
            continue
        parsed = _parse_ssim_line(line)
        if not parsed:
            continue
        records.extend(_expand_ssim_record(parsed))

    frame = pd.DataFrame(records)
    if frame.empty:
        return frame

    frame["temporada"] = pd.NA
    frame["aeroporto_operacao"] = pd.NA
    frame["natureza"] = pd.NA
    frame["assentos_previstos"] = pd.NA

    if "service_type" in frame.columns:
        svc_series = frame["service_type"].astype("string").str.strip().str.upper()
        svc_series = svc_series.fillna("")
        frame["service_type"] = svc_series
        cargo_mask = svc_series.isin(CARGO_SERVICE_TYPES)
        frame.loc[cargo_mask, "natureza"] = "CARGO"
        pax_mask = (~cargo_mask) & (frame["natureza"].isna())
        frame.loc[pax_mask, "natureza"] = "PAX"

    for col in ("cia", "numero_voo", "act_type", "origem", "destino"):
        frame[col] = frame[col].astype(str).str.strip().str.upper()
    return frame


def _normalise_headers(frame: DataFrame) -> DataFrame:
    """Rename dataframe columns using the alias mapping."""

    rename_map: Dict[str, str] = {}
    lower_map = {col.lower(): col for col in frame.columns}

    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            col = lower_map.get(alias.lower())
            if col:
                rename_map[col] = canonical
                break

    logger.debug("Mapeamento de colunas detectado: %s", rename_map)
    frame = frame.rename(columns=rename_map)

    missing = {"cia", "numero_voo", "act_type", "origem", "destino"}
    if missing - set(frame.columns):
        raise ValueError(
            f"Arquivo SIROS não possui colunas obrigatórias: {sorted(missing)}. "
            f"Detectamos {list(frame.columns)}."
        )

    # Derivar aeroporto_operacao quando ausente.
    if "aeroporto_operacao" not in frame.columns:
        logger.debug("Coluna 'aeroporto_operacao' ausente. Será atribuída na etapa de pipeline.")
        frame["aeroporto_operacao"] = pd.NA

    for field in ("temporada", "natureza"):
        if field not in frame.columns:
            frame[field] = pd.NA

    if "assentos_previstos" not in frame.columns:
        frame["assentos_previstos"] = pd.NA

    return frame


def _detect_delimiter(sample: str) -> str:
    """Try to automatically detect the CSV delimiter."""
    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except Exception:  # pragma: no cover - fallback when sniffer fails
        for delimiter in (";", ",", "|", "\t"):
            if delimiter in sample:
                return delimiter
        return ","  # default


def parse_schedule_text(text: str) -> DataFrame:
    """
    Parse the schedule text into a DataFrame with standard columns.

    Raises
    ------
    ValueError
        Se o conteúdo não puder ser interpretado.
    """

    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Arquivo SIROS vazio.")

    if _looks_like_ssim(cleaned):
        frame = _parse_ssim_dataset(cleaned)
        if frame.empty:
            raise ValueError("Arquivo SIROS (SSIM) sem voos processáveis.")
        return frame

    sample = "\n".join(cleaned.splitlines()[:5])
    delimiter = _detect_delimiter(sample)

    buffer = io.StringIO(cleaned)

    try:
        frame = pd.read_csv(buffer, delimiter=delimiter)
    except Exception as exc:
        logger.warning("Falha ao usar pandas.read_csv (%s). Tentando fallback FWF.", exc)
        buffer.seek(0)
        try:
            frame = pd.read_fwf(buffer)
        except Exception as fwf_exc:
            raise ValueError("Não foi possível parsear o arquivo SIROS.") from fwf_exc

    frame = _normalise_headers(frame)

    # Conversões de tipo
    datetime_fields = ("dt_partida_utc", "dt_chegada_utc")
    for field in datetime_fields:
        if field in frame.columns:
            frame[field] = pd.to_datetime(frame[field], errors="coerce", utc=True)

    int_fields = ("assentos_previstos",)
    for field in int_fields:
        if field in frame.columns:
            frame[field] = pd.to_numeric(frame[field], errors="coerce").astype("Int64")

    # Normalizar strings críticas
    for col in ("cia", "numero_voo", "act_type", "origem", "destino", "aeroporto_operacao"):
        frame[col] = frame[col].astype(str).str.strip().str.upper()

    if "temporada" in frame.columns:
        frame["temporada"] = frame["temporada"].astype(str).str.strip().str.upper()

    return frame


__all__ = ["parse_schedule_text", "STANDARD_COLUMNS"]
