"""
Helpers to persist processed dataframes into Supabase.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Iterator, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
from pandas import DataFrame
from postgrest import APIError

from .pipeline import PipelineResult
from .supabase_client import get_supabase_client, table

logger = logging.getLogger("flightops.supabase")


def _chunk(records: List[Mapping[str, object]], size: int) -> Iterator[List[Mapping[str, object]]]:
    for idx in range(0, len(records), size):
        yield records[idx : idx + size]


def _serialise_value(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
        return ts.isoformat()
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if isinstance(value, (pd.Series, pd.DataFrame)):
        return value.to_dict()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, str):
        return value
    if pd.isna(value):  # handles numpy.nan
        return None
    return value


def _to_records(df: DataFrame, columns: Optional[Iterable[str]] = None) -> List[dict]:
    if df.empty:
        return []
    frame = df.copy()
    if columns is not None:
        frame = frame.loc[:, list(columns)]
    records: List[dict] = []
    for record in frame.to_dict(orient="records"):
        serialised = {key: _serialise_value(value) for key, value in record.items()}
        records.append(serialised)
    return records


def upsert_dataframe(
    table_name: str,
    df: DataFrame,
    *,
    conflict_cols: Iterable[str],
    columns: Optional[Iterable[str]] = None,
    chunk_size: int = 500,
) -> None:
    if df.empty:
        logger.info("Tabela %s: nenhum registro para inserir.", table_name)
        return

    data = _to_records(df, columns=columns)
    total_rows = len(data)
    logger.info("Tabela %s: preparando upsert de %s registros.", table_name, total_rows)

    tbl = table(table_name)
    on_conflict = ",".join(conflict_cols)

    for batch in _chunk(data, chunk_size):
        try:
            tbl.upsert(
                batch,
                on_conflict=on_conflict,
                returning="minimal",
            ).execute()
        except APIError as exc:  # pragma: no cover - depends on API behaviour
            code = getattr(exc, "code", None)
            message = getattr(exc, "message", None)
            details = getattr(exc, "details", None)
            hint = getattr(exc, "hint", None)

            def _shorten(value: object, cap: int = 200) -> object:
                if isinstance(value, str) and len(value) > cap:
                    return value[:cap] + "... (truncated)"
                return value

            logger.error(
                "Falha ao upsert em %s | code=%s | message=%s | details=%s | hint=%s",
                table_name,
                code,
                _shorten(message),
                _shorten(details),
                _shorten(hint),
            )
            raise

    logger.info("Tabela %s: upsert concluído (%s registros).", table_name, total_rows)


def delete_airport_data(
    airports: Sequence[str],
    *,
    season: Optional[str] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> None:
    """
    Remove dados existentes para a lista de aeroportos nas tabelas principais.
    """

    if not airports:
        return

    client = get_supabase_client()

    if season is not None and window_start is None and window_end is None:
        for airport in airports:
            params = {"p_aeroporto": airport, "p_temporada": season, "p_batch": 5000}
            while True:
                response = client.rpc("delete_airport_temporada_step", params=params).execute()
                deleted = response.data
                if isinstance(deleted, list):
                    if not deleted:
                        deleted_value = None
                    else:
                        value = deleted[0]
                        if isinstance(value, dict):
                            deleted_value = next(iter(value.values()))
                        else:
                            deleted_value = value
                else:
                    deleted_value = deleted

                if not deleted_value:
                    break

                logger.debug(
                    "Removidos %s (aeroporto=%s, temporada=%s).",
                    deleted_value,
                    airport,
                    season,
                )

            logger.info(
                "Limpeza concluída para aeroporto %s temporada %s.",
                airport,
                season,
            )
        return

    targets = (
        ("slots_atendimento", "aeroporto", "temporada", "slot_ts"),
        ("slots_solo", "aeroporto", "temporada", "slot_ts"),
        ("voos_tratados", "aeroporto", "temporada", "chegada_slot"),
        ("voos_raw", "aeroporto_operacao", "temporada", "timestamp_evento"),
    )

    start_iso = window_start.isoformat() if window_start else None
    end_iso = window_end.isoformat() if window_end else None

    for table_name, column, season_column, time_column in targets:
        for airport in airports:
            query = table(table_name).delete().eq(column, airport)
            if season:
                query = query.eq(season_column, season)
            if start_iso:
                query = query.gte(time_column, start_iso)
            if end_iso:
                query = query.lt(time_column, end_iso)
            logger.info(
                "Removendo registros de %s para aeroporto %s%s.%s",
                table_name,
                airport,
                f" temporada {season}" if season else "",
                " Janela aplicada" if start_iso or end_iso else "",
            )
            query.execute()


def load_pipeline_result(
    result: PipelineResult,
    *,
    chunk_size: int = 500,
    replace_existing: bool = False,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> None:
    if replace_existing and result.aeroportos_processados:
        delete_airport_data(
            result.aeroportos_processados,
            season=result.temporada,
            window_start=window_start,
            window_end=window_end,
        )

    upsert_dataframe(
        "voos_raw",
        result.voos_raw,
        conflict_cols=["event_id"],
        columns=[
            "event_id",
            "flight_id",
            "temporada",
            "cia",
            "numero_voo",
            "act_type",
            "origem",
            "destino",
            "aeroporto_operacao",
            "evento",
            "timestamp_evento",
            "dt_partida_utc",
            "dt_chegada_utc",
            "natureza",
            "assentos_previstos",
        ],
        chunk_size=chunk_size,
    )

    upsert_dataframe(
        "voos_tratados",
        result.voos_tratados,
        conflict_cols=["voo_id"],
        columns=[
            "voo_id",
            "temporada",
            "aeroporto",
            "cia",
            "act_type",
            "classe_aeronave",
            "chegada_utc",
            "chegada_slot",
            "partida_utc",
            "partida_slot",
            "solo_min",
            "pnt_tst",
            "dom_int",
            "link_status",
            "numero_voo_in",
            "numero_voo_out",
            "origem",
            "destino",
            "arrival_event_id",
            "arrival_flight_id",
            "departure_event_id",
            "departure_flight_id",
        ],
        chunk_size=chunk_size,
    )

    upsert_dataframe(
        "slots_atendimento",
        result.slots_atendimento,
        conflict_cols=["voo_id", "slot_ts", "fase"],
        columns=[
            "voo_id",
            "slot_ts",
            "fase",
            "temporada",
            "aeroporto",
            "cia",
            "classe_aeronave",
            "dom_int",
            "pnt_tst",
            "numero_voo",
        ],
        chunk_size=chunk_size,
    )

    upsert_dataframe(
        "slots_solo",
        result.slots_solo,
        conflict_cols=["voo_id", "slot_ts"],
        columns=[
            "voo_id",
            "slot_ts",
            "temporada",
            "aeroporto",
            "cia",
            "classe_aeronave",
            "dom_int",
            "pnt_tst",
            "atendimento_embarque_desembarque",
            "atendimento_limpeza",
        ],
        chunk_size=chunk_size,
    )


__all__ = ["upsert_dataframe", "load_pipeline_result", "delete_airport_data"]
