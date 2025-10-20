"""
Command-line entrypoint for running the FlightOps Planner ETL.
"""

from __future__ import annotations

import argparse
import logging
from typing import List

from .logging_utils import configure_logging
from .metadata import load_airport_country_map
from .pipeline import run_pipeline
from .supabase_loader import load_pipeline_result


def _parse_airports(values: List[str]) -> List[str]:
    airports: List[str] = []
    for value in values:
        for token in value.replace(" ", "").split(","):
            if token:
                airports.append(token.upper())
    return airports


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FlightOps Planner ETL")
    parser.add_argument(
        "-a",
        "--airport",
        dest="airports",
        action="append",
        required=True,
        help="Código(s) IATA/ICAO do(s) aeroporto(s) alvo (pode repetir ou usar vírgula).",
    )
    parser.add_argument(
        "-s",
        "--season",
        dest="season",
        help="Código da temporada (ex.: S25, W26). Se omitido usa DEFAULT_SEASON.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa todo o processamento mas não faz upsert no Supabase.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nível de log (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser


def main(argv: List[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    configure_logging(level=log_level, module="flightops.etl")

    airports = _parse_airports(args.airports)
    if not airports:
        parser.error("Informe ao menos um aeroporto via --airport.")

    airport_country_map = load_airport_country_map()

    result = run_pipeline(
        temporada=args.season,
        aeroportos=airports,
        airport_country_map=airport_country_map,
    )

    logging.getLogger("flightops.etl").info(
        "Processamento concluído: voos_raw=%s, voos_tratados=%s, slots_atendimento=%s, slots_solo=%s",
        len(result.voos_raw),
        len(result.voos_tratados),
        len(result.slots_atendimento),
        len(result.slots_solo),
    )

    if args.dry_run:
        logging.getLogger("flightops.etl").info("Execução em modo dry-run: não enviando dados para o Supabase.")
        return

    load_pipeline_result(result)


if __name__ == "__main__":  # pragma: no cover
    main()
