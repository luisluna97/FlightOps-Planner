"""
Flight linking and slot expansion logic.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from pandas import DataFrame

from .classifiers import classify_aircraft, classify_domestic, classify_operation
from .config import Settings
from .slot_utils import expand_slots, round_to_slot, slot_range

logger = logging.getLogger(__name__)


@dataclass
class AirportLinkResult:
    aeroporto: str
    voos_tratados: DataFrame
    slots_atendimento: DataFrame
    slots_solo: DataFrame
    raw_subset: DataFrame


def _generate_voo_id(season: str, airport: str, arrival_id: str, departure_id: Optional[str]) -> str:
    key = f"{season}:{airport}:{arrival_id}:{departure_id or 'na'}"
    return str(uuid5(NAMESPACE_URL, key))


def _prepare_events(frame: DataFrame, airport: str) -> Tuple[DataFrame, DataFrame]:
    arrivals = frame.loc[frame["destino"] == airport].copy()
    departures = frame.loc[frame["origem"] == airport].copy()

    arrivals["evento"] = "ARR"
    arrivals["timestamp_evento"] = arrivals["dt_chegada_utc"]
    arrivals["numero_voo_evento"] = arrivals["numero_voo"]
    arrivals["event_id"] = arrivals.apply(
        lambda row: str(uuid5(NAMESPACE_URL, f"{row['flight_id']}:ARR:{airport}")),
        axis=1,
    )

    departures["evento"] = "DEP"
    departures["timestamp_evento"] = departures["dt_partida_utc"]
    departures["numero_voo_evento"] = departures["numero_voo"]
    departures["event_id"] = departures.apply(
        lambda row: str(uuid5(NAMESPACE_URL, f"{row['flight_id']}:DEP:{airport}")),
        axis=1,
    )

    arrivals = arrivals.dropna(subset=["timestamp_evento"])
    departures = departures.dropna(subset=["timestamp_evento"])

    arrivals = arrivals.sort_values("timestamp_evento").reset_index(drop=True)
    departures = departures.sort_values("timestamp_evento").reset_index(drop=True)

    return arrivals, departures


def _build_departure_lookup(departures: DataFrame) -> Dict[Tuple[str, str], List[Dict[str, object]]]:
    bucket: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
    for _, row in departures.iterrows():
        key = (row["cia"], row["act_type"])
        bucket[key].append(
            {
                "timestamp": row["timestamp_evento"],
                "numero_voo": row["numero_voo_evento"],
                "flight_id": row["flight_id"],
                "event_id": row["event_id"],
                "destino": row["destino"],
                "origem": row["origem"],
                "row": row,
            }
        )

    for key in bucket:
        bucket[key].sort(key=lambda item: item["timestamp"])

    return bucket


def link_airport(
    frame: DataFrame,
    *,
    airport: str,
    season: str,
    settings: Settings,
    airport_country_map: Optional[Dict[str, str]] = None,
) -> AirportLinkResult:
    """
    Link arrivals and departures for a given airport.
    """

    airport = airport.upper()
    arrivals, departures = _prepare_events(frame, airport)
    departures_lookup = _build_departure_lookup(departures)

    min_turnaround = timedelta(minutes=settings.min_turnaround_minutes)
    max_turnaround = timedelta(hours=36)
    slot_minutes = settings.rounding_granularity_minutes

    treated_rows: List[Dict[str, object]] = []
    atendimento_rows: List[Dict[str, object]] = []
    solo_rows: List[Dict[str, object]] = []

    used_departures: Dict[str, bool] = {}

    for _, arrival in arrivals.iterrows():
        key = (arrival["cia"], arrival["act_type"])
        candidates = departures_lookup.get(key, [])

        arrival_time: pd.Timestamp = arrival["timestamp_evento"]
        earliest = arrival_time + min_turnaround
        latest = arrival_time + max_turnaround

        best_candidate_idx: Optional[int] = None
        best_diff: Optional[timedelta] = None
        best_number_delta: Optional[int] = None

        for idx, candidate in enumerate(candidates):
            if used_departures.get(candidate["event_id"]):  # skip already used
                continue
            dep_time: pd.Timestamp = candidate["timestamp"]  # type: ignore[assignment]
            if dep_time < earliest:
                continue
            if dep_time > latest:
                break
            diff = dep_time - arrival_time
            number_diff = None
            try:
                number_diff = abs(int(candidate["numero_voo"]) - int(arrival["numero_voo"]))
            except Exception:
                number_diff = None

            better = False
            if best_diff is None or diff < best_diff:
                better = True
            elif best_diff and diff == best_diff and number_diff is not None:
                if best_number_delta is None or number_diff < best_number_delta:
                    better = True

            if better:
                best_candidate_idx = idx
                best_diff = diff
                best_number_delta = number_diff

        if best_candidate_idx is not None:
            candidate = candidates[best_candidate_idx]
            used_departures[candidate["event_id"]] = True
            departure_time: pd.Timestamp = candidate["timestamp"]  # type: ignore[assignment]
            link_status = "linked"
            numero_voo_out = candidate["numero_voo"]
            destino = candidate["destino"]
            departure_event_id = candidate["event_id"]
            departure_flight_id = candidate["flight_id"]
        else:
            departure_time = pd.NaT
            link_status = "no_departure"
            numero_voo_out = None
            destino = arrival["destino"]
            departure_event_id = None
            departure_flight_id = None

        chegada_slot = round_to_slot(arrival_time, minutes=slot_minutes)
        if isinstance(departure_time, pd.Timestamp) and not pd.isna(departure_time):
            partida_slot = round_to_slot(departure_time, minutes=slot_minutes)
            solo_minutes = (departure_time - arrival_time).total_seconds() / 60.0
        else:
            partida_slot = pd.NaT
            solo_minutes = settings.solo_open_minutes

        classe = classify_aircraft(arrival["act_type"], is_cargo=False)
        dom_int = classify_domestic(
            arrival["origem"],
            destino,
            airport_country_map=airport_country_map,
        )
        pnt_tst = classify_operation(solo_minutes)

        voo_id = _generate_voo_id(season, airport, arrival["event_id"], departure_event_id)

        treated_rows.append(
            {
                "voo_id": voo_id,
                "temporada": season,
                "aeroporto": airport,
                "cia": arrival["cia"],
                "act_type": arrival["act_type"],
                "classe_aeronave": classe,
                "chegada_utc": arrival_time,
                "chegada_slot": chegada_slot,
                "partida_utc": departure_time,
                "partida_slot": partida_slot,
                "solo_min": solo_minutes,
                "pnt_tst": pnt_tst,
                "dom_int": dom_int,
                "link_status": link_status,
                "numero_voo_in": arrival["numero_voo"],
                "numero_voo_out": numero_voo_out,
                "origem": arrival["origem"],
                "destino": destino,
                "arrival_event_id": arrival["event_id"],
                "arrival_flight_id": arrival["flight_id"],
                "departure_event_id": departure_event_id,
                "departure_flight_id": departure_flight_id,
            }
        )

        for slot in expand_slots(
            chegada_slot,
            before=timedelta(minutes=10),
            after=timedelta(minutes=30),
            minutes=slot_minutes,
        ):
            atendimento_rows.append(
                {
                    "voo_id": voo_id,
                    "slot_ts": slot,
                    "fase": "ARR",
                    "temporada": season,
                    "aeroporto": airport,
                    "cia": arrival["cia"],
                    "classe_aeronave": classe,
                    "dom_int": dom_int,
                    "pnt_tst": pnt_tst,
                    "numero_voo": arrival["numero_voo"],
                }
            )

        if not pd.isna(partida_slot):
            for slot in expand_slots(
                partida_slot,
                before=timedelta(minutes=30),
                after=timedelta(minutes=10),
                minutes=slot_minutes,
            ):
                atendimento_rows.append(
                    {
                        "voo_id": voo_id,
                        "slot_ts": slot,
                        "fase": "DEP",
                        "temporada": season,
                        "aeroporto": airport,
                        "cia": arrival["cia"],
                        "classe_aeronave": classe,
                        "dom_int": dom_int,
                        "pnt_tst": pnt_tst,
                        "numero_voo": numero_voo_out,
                    }
                )

        solo_end = partida_slot
        if pd.isna(solo_end):
            solo_end = round_to_slot(
                arrival_time + timedelta(minutes=settings.solo_open_minutes),
                minutes=slot_minutes,
            )
        for slot in slot_range(chegada_slot, solo_end, minutes=slot_minutes):
            solo_rows.append(
                {
                    "voo_id": voo_id,
                    "slot_ts": slot,
                    "temporada": season,
                    "aeroporto": airport,
                    "cia": arrival["cia"],
                    "classe_aeronave": classe,
                    "dom_int": dom_int,
                    "pnt_tst": pnt_tst,
                }
            )

    treated_df = pd.DataFrame(treated_rows)
    atendimento_df = pd.DataFrame(atendimento_rows)
    solo_df = pd.DataFrame(solo_rows)

    raw_subset = pd.concat([arrivals, departures], ignore_index=True, axis=0)
    raw_subset["aeroporto_operacao"] = airport

    return AirportLinkResult(
        aeroporto=airport,
        voos_tratados=treated_df,
        slots_atendimento=atendimento_df,
        slots_solo=solo_df,
        raw_subset=raw_subset,
    )


__all__ = ["AirportLinkResult", "link_airport"]
