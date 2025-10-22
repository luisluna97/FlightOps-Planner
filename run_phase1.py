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
from datetime import datetime, timezone

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


from typing import Optional


def _add_months(dt: datetime, months: int) -> datetime:
    month_index = dt.month - 1 + months
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    return dt.replace(year=year, month=month, day=1)


def _infer_season(year: int, month: int) -> str:
    if month >= 10:
        return f"W{year % 100:02d}"
    if 4 <= month <= 9:
        return f"S{year % 100:02d}"
    return f"W{(year - 1) % 100:02d}"


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
        "--month",
        dest="month",
        help="Filtra a execução para o mês informado (YYYY-MM).",
    )
    parser.add_argument(
        "--include-prev-month",
        dest="include_prev_month",
        action="store_true",
        help="Com --month, inclui o mês anterior para comparação.",
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
        "--replace",
        action="store_true",
        help="Remove dados existentes dos aeroportos processados antes de gravar.",
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
            "Mapa de aeroportos vazio. Classifica??o DOM/INT pode ficar imprecisa."
        )

    season_arg: Optional[str] = args.season
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None

    if args.month:
        try:
            month_dt = datetime.strptime(args.month, '%Y-%m')
        except ValueError:
            parser.error('Informe --month no formato YYYY-MM (ex.: 2025-12).')
        month_dt = month_dt.replace(day=1, tzinfo=timezone.utc)
        window_start = _add_months(month_dt, -1) if args.include_prev_month else month_dt
        window_end = _add_months(month_dt, 1)
        if season_arg is None:
            season_arg = _infer_season(month_dt.year, month_dt.month)
        logger.info(
            'Janela temporal aplicada: %s -> %s',
            window_start.isoformat(),
            window_end.isoformat(),
        )

    season_display = season_arg or '<default>'

    if use_all_airports:
        logger.info(
            'Iniciando ETL para todos os aeroportos da malha SIROS (temporada=%s)',
            season_display,
        )
    else:
        logger.info(
            'Iniciando ETL para aeroportos %s (temporada=%s)',
            airports,
            season_display,
        )
    result = run_pipeline(
        temporada=season_arg,
        aeroportos=airports,
        airport_country_map=airport_country_map,
        window_start=window_start,
        window_end=window_end,
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

    load_pipeline_result(
        result,
        replace_existing=args.replace,
        window_start=window_start,
        window_end=window_end,
    )
    logger.info("Dados gravados no Supabase com sucesso.")


if __name__ == "__main__":  # pragma: no cover
    main()
