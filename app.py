"""
FastAPI wrapper to trigger the FlightOps ETL on-demand (Render/Railway deployment).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from run_phase1 import main as run_etl

LOGGER = logging.getLogger("flightops.api")

app = FastAPI(title="FlightOps Planner ETL", version="1.0.0")


class EtlRequest(BaseModel):
    airport: Optional[str] = None
    season: Optional[str] = None
    month: Optional[str] = None  # YYYY-MM
    include_prev_month: bool = False
    replace: bool = True
    airports_csv: Optional[str] = None
    dry_run: bool = False


@app.post("/etl")
def trigger_etl(body: EtlRequest):
    """
    Trigger an ETL run. When `airport` is omitted, ALL airports will be processed.
    """

    args = []

    if body.airports_csv:
        csv_path = Path(body.airports_csv)
        if not csv_path.exists():
            raise HTTPException(status_code=400, detail=f"CSV not found: {csv_path}")
        args.extend(["--airports-csv", body.airports_csv])

    if body.airport:
        args.extend(["--airport", body.airport])
    else:
        args.extend(["--airport", "ALL"])

    if body.season:
        args.extend(["--season", body.season])
    if body.month:
        try:
            datetime.strptime(body.month, "%Y-%m")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="month must be YYYY-MM") from exc
        args.extend(["--month", body.month])
        if body.include_prev_month:
            args.append("--include-prev-month")

    if body.replace:
        args.append("--replace")
    if body.dry_run:
        args.append("--dry-run")

    LOGGER.info("Starting ETL with args: %s", args)
    try:
        run_etl(args)
    except Exception as exc:  # pragma: no cover - depends on runtime
        LOGGER.exception("ETL failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"status": "ok"}
