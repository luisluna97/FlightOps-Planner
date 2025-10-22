"""
Exportador de dados SIROS para CSV.

Uso básico:

    python export_to_csv.py --airport MCZ --season S25 --output-dir exports/mcz

Para exportar todos os aeroportos da temporada, use `--airport ALL`.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# garante que "src/" esteja disponível como pacote
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from flightops_planner.logging_utils import configure_logging  # noqa: E402
from flightops_planner.siros_client import fetch_schedule  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Exporta dados tratáveis da ANAC (SIROS) em CSV simples."
    )
    parser.add_argument(
        "--airport",
        dest="airports",
        action="append",
        required=True,
        help="Código IATA/ICAO alvo. Use `ALL` para exportar todos os aeroportos da temporada.",
    )
    parser.add_argument(
        "--season",
        dest="season",
        help="Código da temporada SIROS (ex.: S25). Se omitido, usa DEFAULT_SEASON do .env.",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default="exports",
        help="Diretório de saída (será criado se não existir).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nível de log (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser


def parse_record(line: str) -> dict[str, str]:
    airline = line[2:4].strip()
    flight_number = line[5:9].strip()
    variation = line[9:11].strip()
    leg_sequence = line[11:13].strip()
    service_type = line[13].strip()
    start_date = line[14:21].strip()
    end_date = line[21:28].strip()
    days_of_week = line[28:35].strip()

    departure_terminal = line[35].strip()
    departure_airport = line[36:39].strip()
    departure_local_time = line[39:43].strip()
    departure_utc_time = line[43:47].strip()
    departure_offset = line[47:52].strip()

    arrival_terminal = line[52:54].strip()
    arrival_airport = line[54:57].strip()
    arrival_local_time = line[57:61].strip()
    arrival_utc_time = line[61:65].strip()
    arrival_offset = line[65:70].strip()

    equipment = line[72:75].strip()

    return {
        "airline": airline,
        "flight_number": flight_number,
        "variation": variation,
        "leg_sequence": leg_sequence,
        "service_type": service_type,
        "start_date": start_date,
        "end_date": end_date,
        "days_of_week": days_of_week,
        "departure_terminal": departure_terminal,
        "departure_airport": departure_airport,
        "departure_local_time": departure_local_time,
        "departure_utc_time": departure_utc_time,
        "departure_offset": departure_offset,
        "arrival_terminal": arrival_terminal,
        "arrival_airport": arrival_airport,
        "arrival_local_time": arrival_local_time,
        "arrival_utc_time": arrival_utc_time,
        "arrival_offset": arrival_offset,
        "equipment": equipment,
    }


def parse_ssim(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        if not line.startswith("3 "):
            continue
        if len(line) < 74:
            continue
        rows.append(parse_record(line))
    return rows


def main(argv: list[str] | None = None) -> None:
    env_path = ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    configure_logging(level=log_level, module="flightops.export")
    logger = logging.getLogger("flightops.export")

    airports_raw = [code.strip().upper() for code in args.airports if code.strip()]
    if not airports_raw:
        parser.error("Informe ao menos um aeroporto via --airport (use ALL para todos).")

    use_all_airports = any(code in {"ALL", "*"} for code in airports_raw)
    season = args.season or os.getenv("DEFAULT_SEASON")

    logger.info(
        "Exportando %s (temporada=%s)",
        "todos os aeroportos" if use_all_airports else ", ".join(airports_raw),
        season or "<default>",
    )

    text = fetch_schedule(season or "")
    rows = parse_ssim(text)

    import pandas as pd

    dataframe = pd.DataFrame(rows)

    if not use_all_airports:
        airports_set = {code.strip().upper() for code in airports_raw}
        dataframe = dataframe[
            dataframe["departure_airport"].isin(airports_set)
            | dataframe["arrival_airport"].isin(airports_set)
        ]

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "voos.csv"
    dataframe.to_csv(output_path, index=False)
    logger.info("Arquivo gerado: %s (%s linhas)", output_path, len(dataframe))


if __name__ == "__main__":  # pragma: no cover
    main()
