"""
Helpers to persist processed dataframes into Supabase.
"""

from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping, Optional

import pandas as pd
from pandas import DataFrame
import numpy as np

from .pipeline import PipelineResult
from .supabase_client import table


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
        return
    data = _to_records(df, columns=columns)
    on_conflict = ",".join(conflict_cols)
    tbl = table(table_name)
    for batch in _chunk(data, chunk_size):
        tbl.upsert(
            batch,
            on_conflict=on_conflict,
            returning="minimal",
        ).execute()


def load_pipeline_result(result: PipelineResult, *, chunk_size: int = 500) -> None:
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
        ],
        chunk_size=chunk_size,
    )


__all__ = ["upsert_dataframe", "load_pipeline_result"]
