"""
Client utilities for downloading the SIROS schedule files.

The endpoint returns a SSIM-like payload that may be plain text, gzip, or ZIP
depending on the season. We attempt to normalise the bytes into a decoded
string ready for downstream parsing.
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from typing import Optional

import httpx

from .config import get_settings

logger = logging.getLogger(__name__)


class SirosDownloadError(RuntimeError):
    """Raised when we fail to retrieve or decode a SIROS schedule."""


def _decode_payload(raw: bytes) -> str:
    """
    Decode raw bytes returned by the SIROS endpoint.

    We support:
    - Plain text (latin-1 / utf-8)
    - Gzip streams
    - ZIP archives (first file wins)
    """

    if not raw:
        raise SirosDownloadError("Resposta vazia do SIROS.")

    # ZIP signatures start with PK
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            # pick the first regular file
            for info in archive.infolist():
                if not info.is_dir():
                    with archive.open(info) as zf:
                        data = zf.read()
                        logger.debug("Arquivo ZIP extraído: %s (%s bytes)", info.filename, len(data))
                        return _decode_payload(data)
        raise SirosDownloadError("Arquivo ZIP não contém conteúdo legível.")

    # Gzip streams
    if raw[:2] == b"\x1f\x8b":
        import gzip

        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            data = gz.read()
            logger.debug("Payload gzip descomprimido (%s bytes).", len(data))
            return _decode_payload(data)

    # Plain text fallback
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue

    raise SirosDownloadError("Não foi possível decodificar o payload do SIROS.")


def fetch_schedule(season: str, *, timeout: Optional[float] = None) -> str:
    """
    Download the SIROS schedule for a given season.

    Parameters
    ----------
    season:
        Código da temporada (ex.: S25, W26).
    timeout:
        Timeout opcional a ser usado (segundos). Quando não informado, usamos o
        valor configurado no `.env`.
    """

    settings = get_settings()
    url = f"{settings.siros_base_url.rstrip('/')}/ssimfile"
    query = {"ds_temporada": season}

    logger.info("Baixando SIROS temporada %s ...", season)

    with httpx.Client(
        timeout=timeout or settings.http_timeout_seconds,
        verify=settings.siros_verify_ssl,
    ) as client:
        response = client.get(url, params=query)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:  # pragma: no cover - depends on network
        raise SirosDownloadError(
            f"Falha ao consultar SIROS ({exc.response.status_code}): {exc}"
        ) from exc

    logger.debug("Download concluído (%s bytes).", len(response.content))
    text = _decode_payload(response.content)

    stripped = text.strip()
    if stripped.startswith('"') and stripped.endswith('"'):
        stripped = stripped[1:-1]

    try:
        stripped = stripped.encode('utf-8').decode('unicode_escape')
    except Exception:
        pass

    if stripped.startswith('[') and 'ssimfile' in stripped:
        try:
            data = json.loads(stripped)
            if isinstance(data, list):
                parts = []
                for item in data:
                    if isinstance(item, dict):
                        part = item.get('ssimfile')
                    else:
                        part = None
                    if part:
                        parts.append(part)
                if parts:
                    text = "\n".join(parts)
                    logger.debug(
                        "SSIM recebido em formato JSON; convertido para texto (%s linhas).",
                        len(parts),
                    )
        except Exception as exc:  # pragma: no cover
            logger.warning("Falha ao interpretar payload JSON: %s", exc)

    return text



__all__ = ["fetch_schedule", "SirosDownloadError"]
