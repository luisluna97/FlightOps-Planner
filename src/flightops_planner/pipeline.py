"""
High-level orchestration for Phase 1 of the FlightOps Planner ETL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from pandas import DataFrame

from .config import get_settings
from .linker import AirportLinkResult, link_airport
from .siros_client import fetch_schedule
from .siros_parser import parse_schedule_text


@dataclass
class PipelineResult:
    temporada: str
    voos_raw: DataFrame
    voos_tratados: DataFrame
    slots_atendimento: DataFrame
    slots_solo: DataFrame
    aeroportos_processados: Sequence[str]


def _assign_raw_ids(frame: DataFrame, season: str) -> DataFrame:
    frame = frame.copy()
    frame = frame.reset_index(drop=True)
    flight_ids = []
    for idx, row in frame.iterrows():
        key = ":".join(
            [
                season,
                row.get("cia", ""),
                row.get("numero_voo", ""),
                row.get("origem", ""),
                row.get("destino", ""),
                str(idx),
            ]
        )
        flight_ids.append(str(uuid5(NAMESPACE_URL, key)))
    frame["flight_id"] = flight_ids
    return frame


def run_pipeline(
    *,
    temporada: Optional[str],
    aeroportos: Optional[Iterable[str]],
    airport_country_map: Optional[Dict[str, str]] = None,
    schedule_text_override: Optional[str] = None,
    window_start: Optional[pd.Timestamp] = None,
    window_end: Optional[pd.Timestamp] = None,
) -> PipelineResult:
    """
    Execute the Phase 1 processing: download SIROS, parse, link flights,
    expand slots and return tidy dataframes.
    """

    settings = get_settings()
    season = (temporada or settings.default_season or "").strip().upper()
    if not season:
        raise ValueError("Informe uma temporada (ex.: S25).")

    if schedule_text_override is not None:
        schedule_text = schedule_text_override
    else:
        schedule_text = fetch_schedule(season)

    if window_start is not None:
        window_start = pd.Timestamp(window_start)
        if window_start.tz is None:
            window_start = window_start.tz_localize('UTC')
        else:
            window_start = window_start.tz_convert('UTC')
    if window_end is not None:
        window_end = pd.Timestamp(window_end)
        if window_end.tz is None:
            window_end = window_end.tz_localize('UTC')
        else:
            window_end = window_end.tz_convert('UTC')

    raw_frame = parse_schedule_text(schedule_text)
    if "temporada" not in raw_frame.columns or raw_frame["temporada"].isna().all():
        raw_frame["temporada"] = season

    if window_start or window_end:
        arrival_ts = raw_frame.get("dt_chegada_utc")
        departure_ts = raw_frame.get("dt_partida_utc")

        def _in_window(series: pd.Series) -> pd.Series:
            cond = pd.Series(True, index=series.index)
            if window_start is not None:
                cond &= series >= window_start
            if window_end is not None:
                cond &= series < window_end
            cond &= ~series.isna()
            return cond

        mask = pd.Series(False, index=raw_frame.index)
        if arrival_ts is not None:
            mask |= _in_window(arrival_ts)
        if departure_ts is not None:
            mask |= _in_window(departure_ts)

        # Include flights overlapping the start boundary (arrival before start but departure inside window)
        if window_start is not None and departure_ts is not None and arrival_ts is not None:
            overlap = (
                arrival_ts.notna()
                & departure_ts.notna()
                & (arrival_ts < window_start)
                & (departure_ts >= window_start)
            )
            if window_end is not None:
                overlap &= departure_ts < window_end
            mask |= overlap

        raw_frame = raw_frame.loc[mask].reset_index(drop=True)

    raw_frame = _assign_raw_ids(raw_frame, season)

    aeroportos_list: List[str] = []
    if aeroportos:
        aeroportos_list = [ap.strip().upper() for ap in aeroportos if ap and ap.strip()]

    if not aeroportos_list:
        origens = raw_frame["origem"].dropna().astype(str).str.strip().str.upper()
        destinos = raw_frame["destino"].dropna().astype(str).str.strip().str.upper()
        aeroportos_list = sorted(set(origens).union(set(destinos)))

    if not aeroportos_list:
        raise ValueError("Nenhum aeroporto disponível para processamento após analisar o arquivo SIROS.")

    treated_frames = []
    atendimento_frames = []
    solo_frames = []
    raw_frames = []

    for aeroporto in aeroportos_list:
        link_result: AirportLinkResult = link_airport(
            raw_frame,
            airport=aeroporto,
            season=season,
            settings=settings,
            airport_country_map=airport_country_map,
        )

        treated_frames.append(link_result.voos_tratados.assign(aeroporto=aeroporto))
        atendimento_frames.append(link_result.slots_atendimento.assign(aeroporto=aeroporto))
        solo_frames.append(link_result.slots_solo.assign(aeroporto=aeroporto))
        raw_frames.append(link_result.raw_subset.assign(aeroporto=aeroporto))

    voos_tratados = pd.concat(treated_frames, ignore_index=True) if treated_frames else pd.DataFrame()
    slots_atendimento = pd.concat(atendimento_frames, ignore_index=True) if atendimento_frames else pd.DataFrame()
    slots_solo = pd.concat(solo_frames, ignore_index=True) if solo_frames else pd.DataFrame()
    voos_raw = pd.concat(raw_frames, ignore_index=True) if raw_frames else pd.DataFrame()

    return PipelineResult(
        temporada=season,
        voos_raw=voos_raw,
        voos_tratados=voos_tratados,
        slots_atendimento=slots_atendimento,
        slots_solo=slots_solo,
        aeroportos_processados=aeroportos_list,
    )


__all__ = ["PipelineResult", "run_pipeline"]
