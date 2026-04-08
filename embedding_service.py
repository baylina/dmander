"""
embedding_service.py — utilidades de embeddings y búsqueda semántica.
"""

from __future__ import annotations

import json
import logging
import math
import os
from functools import lru_cache
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)

DEFAULT_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
DEFAULT_RERANK_MODEL = os.getenv("OPENAI_RERANK_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))

_client: OpenAI | None = None
_client_failed = False


def _openai_client() -> OpenAI | None:
    global _client, _client_failed
    if _client_failed:
        return None
    if _client is not None:
        return _client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        _client_failed = True
        return None
    try:
        _client = OpenAI(api_key=api_key)
    except Exception as exc:  # pragma: no cover - depende de entorno
        logger.warning("No he podido inicializar cliente de embeddings: %s", exc)
        _client_failed = True
        return None
    return _client


def embedding_model_name() -> str:
    return DEFAULT_EMBEDDING_MODEL


def build_demand_search_text(
    raw_text: str,
    *,
    summary: str = "",
    location_label: str = "",
    budget_max: Any = None,
    budget_unit: str = "total",
    suggested_missing_details: list[str] | None = None,
) -> str:
    parts: list[str] = []
    if summary:
        parts.append(f"Resumen: {summary.strip()}")
    if raw_text:
        parts.append(f"Demanda: {str(raw_text).strip()}")
    if location_label:
        parts.append(f"Ubicación: {str(location_label).strip()}")
    if budget_max not in (None, ""):
        unit = str(budget_unit or "total").strip()
        parts.append(f"Precio máximo: {budget_max} euros {unit}")
    if suggested_missing_details:
        joined = ", ".join(str(item).strip() for item in suggested_missing_details if str(item).strip())
        if joined:
            parts.append(f"Detalles sugeridos: {joined}")
    return ". ".join(part for part in parts if part).strip()


def _embed_text_uncached(text: str) -> list[float]:
    client = _openai_client()
    if not client or not text.strip():
        return []
    try:
        response = client.embeddings.create(
            model=embedding_model_name(),
            input=text,
        )
        vector = response.data[0].embedding if response.data else []
        return [float(value) for value in vector]
    except Exception as exc:  # pragma: no cover - depende de red/proveedor
        logger.warning("No he podido generar embedding: %s", exc)
        return []


@lru_cache(maxsize=256)
def _cached_query_embedding(text: str) -> tuple[float, ...]:
    return tuple(_embed_text_uncached(text))


def embed_query_text(text: str) -> list[float]:
    normalized = str(text or "").strip()
    if not normalized:
        return []
    return list(_cached_query_embedding(normalized))


def embed_document_text(text: str) -> list[float]:
    return _embed_text_uncached(str(text or ""))


def cosine_similarity(left: list[float] | tuple[float, ...], right: list[float] | tuple[float, ...]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for l_value, r_value in zip(left, right):
        dot += float(l_value) * float(r_value)
        left_norm += float(l_value) * float(l_value)
        right_norm += float(r_value) * float(r_value)
    if left_norm <= 0 or right_norm <= 0:
        return 0.0
    return dot / (math.sqrt(left_norm) * math.sqrt(right_norm))


def rerank_demand_candidates(query_text: str, candidates: list[dict[str, Any]]) -> dict[int, float]:
    client = _openai_client()
    if not client or not query_text.strip() or not candidates:
        return {}
    payload_candidates = [
        {
            "id": item.get("id"),
            "summary": str(item.get("summary") or "").strip(),
            "text": str(item.get("text") or "").strip(),
        }
        for item in candidates
        if item.get("id") is not None
    ]
    if not payload_candidates:
        return {}
    try:
        response = client.chat.completions.create(
            model=DEFAULT_RERANK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Evalua la relevancia semantica entre una consulta de ofertante y una lista de demandas. "
                        "Prioriza el tipo real de producto o servicio buscado. "
                        "No sobrevalores coincidencias superficiales de ubicacion, precio o lenguaje generico si el servicio/producto no encaja. "
                        "Devuelve SOLO JSON con una clave 'scores' que sea una lista de objetos {id, score}. "
                        "score debe ser un numero entre 0 y 1."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "query": query_text,
                            "candidates": payload_candidates,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        scores: dict[int, float] = {}
        for item in parsed.get("scores", []) or []:
            try:
                candidate_id = int(item.get("id"))
                score = max(0.0, min(1.0, float(item.get("score", 0.0))))
            except (TypeError, ValueError):
                continue
            scores[candidate_id] = score
        return scores
    except Exception as exc:  # pragma: no cover - depende de red/proveedor
        logger.warning("No he podido rerankear candidatos: %s", exc)
        return {}
