"""
Utilities to load reference datasets (airports, etc.) into Supabase.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from .logging_utils import configure_logging
from .supabase_loader import upsert_dataframe

logger = logging.getLogger(__name__)


AIRPORT_COLUMNS = {
    "Airport": "nome",
    "City": "cidade",
    "Country": "pais",
    "IATA": "iata",
    "ICAO": "icao",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Altitude": "altitude",
    "Timezone": "timezone",
    "Tz": "tz",
}


def _build_codigo(row: pd.Series) -> Optional[str]:
    for key in ("IATA", "ICAO"):
        value = str(row.get(key, "")).strip().upper()
        if value and value != "nan" and value != "NONE":
            return value
    return None


def load_airports_csv(path: Path) -> None:
    df = pd.read_csv(path)
    df["codigo"] = df.apply(_build_codigo, axis=1)
    df = df.dropna(subset=["codigo"])
    df["codigo"] = df["codigo"].str.upper()
    df["IATA"] = df["IATA"].astype(str).str.strip().str.upper().replace({"": None, "nan": None})
    df["ICAO"] = df["ICAO"].astype(str).str.strip().str.upper().replace({"": None, "nan": None})

    df = df.drop_duplicates(subset=["codigo"])

    columns = ["codigo"] + list(AIRPORT_COLUMNS.values())
    for original, renamed in AIRPORT_COLUMNS.items():
        if original not in df.columns:
            df[original] = None
    payload = df[["codigo"] + list(AIRPORT_COLUMNS.keys())]
    payload = payload.rename(columns=AIRPORT_COLUMNS)
    payload["iata"] = payload["iata"].replace({"": None})
    payload["icao"] = payload["icao"].replace({"": None})

    numeric_cols = ("latitude", "longitude")
    for col in numeric_cols:
        payload[col] = pd.to_numeric(payload[col], errors="coerce")

    payload["altitude"] = (
        pd.to_numeric(payload["altitude"], errors="coerce")
        .round()
        .astype("Int64")
    )

    logger.info("Preparando %s aeroportos para upsert.", len(payload))
    upsert_dataframe(
        "aeroportos_ref",
        payload,
        conflict_cols=["codigo"],
        columns=columns,
        chunk_size=1000,
    )
    logger.info("Carga de aeroportos concluída.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Carrega CSV de aeroportos no Supabase.")
    parser.add_argument("csv_path", type=Path, help="Caminho para airports.csv (OurAirports).")
    parser.add_argument("--log-level", default="INFO", help="Nível de log (ex.: DEBUG).")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    log_level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    configure_logging(level=log_level, module="flightops.ref_loader")

    if not args.csv_path.exists():
        parser.error(f"Arquivo não encontrado: {args.csv_path}")

    load_airports_csv(args.csv_path)


if __name__ == "__main__":  # pragma: no cover
    main()
