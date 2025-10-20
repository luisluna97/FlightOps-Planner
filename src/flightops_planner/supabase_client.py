"""
Supabase client helpers.

We initialise the Supabase Python client lazily and reuse a single instance for
the duration of a pipeline run. The service role key is required because the
ETL performs upserts into managed tables.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import httpx
from supabase import Client, create_client
from supabase.lib.client_options import ClientOptions

from .config import get_settings


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """Create or reuse a Supabase client configured from environment vars."""
    settings = get_settings()
    options = ClientOptions()
    if not settings.supabase_verify_ssl:
        options.http_client = httpx.Client(verify=False)
    return create_client(
        settings.supabase_url,
        settings.supabase_service_role_key,
        options,
    )


def table(table_name: str) -> Any:
    """
    Short-hand for accessing a Supabase table within the configured schema.

    Parameters
    ----------
    table_name:
        Name of the table (without schema).
    """

    client = get_supabase_client()
    schema = get_settings().supabase_schema
    query = client.table(table_name)
    if schema and schema != "public":
        query = query.schema(schema)
    return query


__all__ = ["get_supabase_client", "table"]
