"""
Helpers for retrieving reference data stored in Supabase.
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable

from supabase import Client

from .supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


def _safe_select(client: Client, table_name: str, columns: Iterable[str]) -> list:
    try:
        response = client.table(table_name).select(",".join(columns)).execute()
    except Exception as exc:  # pragma: no cover - network/supabase failure
        logger.warning("Não foi possível carregar %s: %s", table_name, exc)
        return []
    return response.data or []


def load_airport_country_map() -> Dict[str, str]:
    """
    Produce a mapping IATA/ICAO -> país a partir da tabela `aeroportos_ref`.
    """

    client = get_supabase_client()
    rows = _safe_select(client, "aeroportos_ref", ("iata", "icao", "pais"))

    mapping: Dict[str, str] = {}
    for row in rows:
        country = row.get("pais")
        if not country:
            continue
        for key in ("iata", "icao"):
            code = (row.get(key) or "").strip().upper()
            if code:
                mapping[code] = country
    return mapping


__all__ = ["load_airport_country_map"]
