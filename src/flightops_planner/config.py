"""
Configuration helpers for the FlightOps Planner ETL pipeline.

This module centralises all environment-bound settings so the rest of the
codebase can remain declarative. We use Pydantic to validate and document the
expected variables. Values are lazily cached because they are immutable for the
lifetime of a pipeline run.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration driven by environment variables."""

    supabase_url: str = Field(
        alias="SUPABASE_URL",
        description="Base URL for the target Supabase project.",
    )
    supabase_service_role_key: str = Field(
        alias="SUPABASE_SERVICE_ROLE_KEY",
        description="Service role key with insert/update permissions.",
    )
    supabase_schema: str = Field(
        alias="SUPABASE_SCHEMA",
        default="public",
        description="Target database schema for all tables.",
    )
    supabase_verify_ssl: bool = Field(
        alias="SUPABASE_VERIFY_SSL",
        default=True,
        description="When false, disables SSL verification (use only in trusted networks).",
    )
    default_season: Optional[str] = Field(
        alias="DEFAULT_SEASON",
        default=None,
        description="Optional default SIROS season code (e.g. S25).",
    )
    http_timeout_seconds: float = Field(
        alias="HTTP_TIMEOUT_SECONDS",
        default=30.0,
        ge=1.0,
        description="Timeout for outbound HTTP calls to external APIs.",
    )
    http_concurrency: int = Field(
        alias="HTTP_CONCURRENCY",
        default=4,
        ge=1,
        le=16,
        description="Maximum number of concurrent API requests.",
    )
    siros_base_url: str = Field(
        alias="SIROS_BASE_URL",
        default="https://sas.anac.gov.br/sas/siros_api",
        description="Base URL for the ANAC SIROS API.",
    )
    siros_verify_ssl: bool = Field(
        alias="SIROS_VERIFY_SSL",
        default=True,
        description="When false, disables SSL verification for SIROS requests.",
    )
    min_turnaround_minutes: int = Field(
        alias="MIN_TURNAROUND_MINUTES",
        default=30,
        ge=0,
        description=(
            "Minimum ground time (arrival -> departure) when matching flights."
        ),
    )
    solo_open_minutes: int = Field(
        alias="SOLO_OPEN_MINUTES",
        default=180,
        ge=0,
        description=(
            "Tempo máximo considerado para solo quando não existe partida linkada."
        ),
    )
    rounding_granularity_minutes: Literal[5, 10, 15] = Field(
        alias="ROUNDING_GRANULARITY_MINUTES",
        default=10,
        description="Slot duration used for rounding timestamps.",
    )

    model_config = SettingsConfigDict(
        populate_by_name=True,
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("rounding_granularity_minutes", mode="before")
    @classmethod
    def _coerce_rounding(cls, value: object) -> int:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("ROUNDING_GRANULARITY_MINUTES não pode ser vazio.")
            value = int(value)
        if value not in (5, 10, 15):
            raise ValueError("ROUNDING_GRANULARITY_MINUTES deve ser 5, 10 ou 15.")
        return int(value)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return validated settings or raise a helpful error."""
    try:
        return Settings()  # type: ignore[arg-type]
    except ValidationError as exc:  # pragma: no cover - surfacing as RuntimeError
        raise RuntimeError(f"Invalid or missing configuration: {exc}") from exc


__all__ = ["Settings", "get_settings"]
