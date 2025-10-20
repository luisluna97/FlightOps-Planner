"""
Domain-specific classification helpers.
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

WIDE_BODY_TYPES = {
    "772",
    "773",
    "77W",
    "77L",
    "787",
    "788",
    "789",
    "78X",
    "330",
    "332",
    "333",
    "339",
    "359",
    "764",
    "763",
    "A35K",
    "A359",
}
NARROW_BODY_PREFIXES = ("73", "32", "E19", "E17", "E18", "E14", "B73")
ATR_PREFIXES = ("ATR", "AT4", "AT7")
LIGHT_PREFIXES = ("C", "P", "BE", "SR", "TB", "PA")


def classify_aircraft(act_type: str, *, is_cargo: bool = False) -> str:
    """
    Classify an aircraft type into buckets (WIDE, NARROW, ATR, CARGO, MELI, CESNNA).
    """

    code = (act_type or "").strip().upper()
    if not code:
        return "UNKNOWN"

    if is_cargo or code.endswith("F"):
        return "CARGO"

    if code in {"73C", "73M", "MELI"}:
        return "MELI"

    if any(code.startswith(prefix) for prefix in ATR_PREFIXES):
        return "ATR"

    if code in WIDE_BODY_TYPES:
        return "WIDE"

    if any(code.startswith(prefix) for prefix in NARROW_BODY_PREFIXES):
        return "NARROW"

    if any(code.startswith(prefix) for prefix in LIGHT_PREFIXES):
        return "CESNNA"

    return "NARROW"


def classify_domestic(
    origem: str,
    destino: str,
    *,
    airport_country_map: Optional[Dict[str, str]] = None,
) -> str:
    """
    Classify flight nature as DOM (domestic) or INT (international).

    When no metadata is provided we fall back to a simple heuristic: Brazilian
    IATA codes are assumed domestic (length 3 and all alphabetic).
    """

    origin = (origem or "").upper()
    destination = (destino or "").upper()

    if airport_country_map:
        origin_country = airport_country_map.get(origin)
        destination_country = airport_country_map.get(destination)
        if origin_country and destination_country:
            return "DOM" if origin_country == destination_country else "INT"

    # Heuristic fallback: assume domestic if both look like Brazilian IATA/ICAO.
    if len(origin) == 3 and len(destination) == 3 and origin.isalpha() and destination.isalpha():
        return "DOM"

    return "INT"


def classify_operation(solo_minutes: Optional[float]) -> Optional[str]:
    """
    Classify operation as PNT (>4h) or TST (<=4h). Returns None if time unknown.
    """

    if solo_minutes is None or pd.isna(solo_minutes):
        return None
    return "PNT" if solo_minutes > 240 else "TST"


__all__ = ["classify_aircraft", "classify_domestic", "classify_operation"]
