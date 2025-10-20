"""
Orquestra a Fase 1 do FlightOps Planner em um único comando.

Uso básico:

    python run_phase1.py --airport GRU --airport GIG --season S25

Parâmetros adicionais permitem atualizar a tabela de aeroportos a partir do
`airports.csv` antes da execução do ETL.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Garante que o pacote dentro de src/ esteja no sys.path quando o script é chamado
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from flightops_planner.logging_utils import configure_logging  # noqa: E402
from flightops_planner.metadata import load_airport_country_map  # noqa: E402
from flightops_planner.pipeline import run_pipeline  # noqa: E402
from flightops_planner.reference_loader import load_airports_csv  # noqa: E402
from flightops_planner.supabase_loader import load_pipeline_result  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Executa a Fase 1 do FlightOps Planner.")
    parser.add_argument(
        "--airport",
        dest="airports",
        action="append",
        required=True,
        help="Código IATA/ICAO do aeroporto alvo (use múltiplas flags para vários aeroportos).",
    )
    parser.add_argument(
        "--season",
        dest="season",
        help="Código da temporada SIROS (ex.: S25). Se omitido, usa DEFAULT_SEASON do .env.",
    )
    parser.add_argument(
        "--airports-csv",
        dest="airports_csv",
        type=Path,
        help="Atualiza a tabela aeroportos_ref usando o CSV antes do ETL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa todo o pipeline, mas não grava no Supabase.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nível de log (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    env_path = ROOT / ".env"
    load_dotenv(dotenv_path=env_path, override=False)

    required = ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(
            f"Variáveis ausentes: {', '.join(missing)}. "
            f"Verifique seu arquivo .env em {env_path}."
        )

    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    configure_logging(level=log_level, module="flightops.runner")
    logger = logging.getLogger("flightops.runner")

    airports_raw = [code.strip().upper() for code in args.airports if code.strip()]
    if not airports_raw:
        parser.error("Informe ao menos um aeroporto via --airport (use ALL para todos).")

    use_all_airports = any(code in {"ALL", "*"} for code in airports_raw)
    airports = None if use_all_airports else airports_raw

    if args.airports_csv:
        if not args.airports_csv.exists():
            parser.error(f"CSV de aeroportos não encontrado: {args.airports_csv}")
        logger.info("Carregando CSV de aeroportos: %s", args.airports_csv)
        load_airports_csv(args.airports_csv)

    airport_country_map = load_airport_country_map()
    if not airport_country_map:
        logger.warning(
            "Mapa de aeroportos vazio. Classificação DOM/INT pode ficar imprecisa."
        )

    if use_all_airports:
        logger.info(
            "Iniciando ETL para todos os aeroportos da malha SIROS (temporada=%s)",
            args.season or "<default>",
        )
    else:
        logger.info(
            "Iniciando ETL para aeroportos %s (temporada=%s)",
            airports,
            args.season or "<default>",
        )
    result = run_pipeline(
        temporada=args.season,
        aeroportos=airports,
        airport_country_map=airport_country_map,
    )

    logger.info(
        "ETL concluído: voos_raw=%s, voos_tratados=%s, slots_atendimento=%s, slots_solo=%s",
        len(result.voos_raw),
        len(result.voos_tratados),
        len(result.slots_atendimento),
        len(result.slots_solo),
    )

    if args.dry_run:
        logger.info("Dry-run habilitado. Nada foi gravado no Supabase.")
        return

    load_pipeline_result(result)
    logger.info("Dados gravados no Supabase com sucesso.")


if __name__ == "__main__":  # pragma: no cover
    main()
