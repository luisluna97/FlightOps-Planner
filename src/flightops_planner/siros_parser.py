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
from typing import Dict, Iterable, Optional

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
