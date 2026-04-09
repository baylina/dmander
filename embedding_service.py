"""
embedding_service.py — utilidades de embeddings y búsqueda semántica configurables.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from openai import OpenAI

from llm_client import get_llm_runtime_config
from utils import parse_json_response

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderConfig:
    provider: str
    model: str
    base_url: str | None
    api_key: str
    enabled: bool = True


def _normalize_provider(value: str | None, default: str) -> str:
    provider = str(value or default).strip().lower()
    aliases = {
        "inherit": default,
        "same": default,
        "openai-compatible": "lmstudio",
        "lm_studio": "lmstudio",
        "local": "ollama",
        "off": "disabled",
        "none": "disabled",
    }
    return aliases.get(provider, provider)


def _default_local_embedding_model(provider: str) -> str:
    if provider == "ollama":
        return "nomic-embed-text"
    return "text-embedding-nomic-embed-text-v1.5"


def _default_local_rerank_model(provider: str) -> str:
    if provider == "ollama":
        return "qwen2.5:7b-instruct"
    return "local-model"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _provider_client(config: ProviderConfig) -> OpenAI | None:
    if not config.enabled:
        return None
    try:
        kwargs = {"api_key": config.api_key}
        if config.base_url:
            kwargs["base_url"] = config.base_url
        return OpenAI(**kwargs)
    except Exception as exc:  # pragma: no cover
        logger.warning("No he podido inicializar cliente %s: %s", config.provider, exc)
        return None


def _provider_health(config: ProviderConfig) -> dict[str, Any]:
    if not config.enabled:
        return {"configured": False, "available": False, "error": "disabled"}
    try:
        kwargs = {"api_key": config.api_key, "timeout": 2.0}
        if config.base_url:
            kwargs["base_url"] = config.base_url
        client = OpenAI(**kwargs)
        client.models.list()
        return {"configured": True, "available": True, "error": ""}
    except Exception as exc:  # pragma: no cover
        return {"configured": True, "available": False, "error": str(exc)}


def _embedding_provider_config() -> ProviderConfig:
    llm_runtime = get_llm_runtime_config()
    provider = _normalize_provider(os.getenv("EMBEDDING_PROVIDER"), llm_runtime.provider)
    if provider == "disabled":
        return ProviderConfig(provider=provider, model="", base_url=None, api_key="", enabled=False)
    if provider == "openai":
        api_key = os.getenv("EMBEDDING_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            return ProviderConfig(provider=provider, model="", base_url=None, api_key="", enabled=False)
        return ProviderConfig(
            provider="openai",
            model=os.getenv("EMBEDDING_MODEL") or os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
            base_url=os.getenv("EMBEDDING_BASE_URL") or None,
            api_key=api_key,
            enabled=True,
        )
    if provider == "ollama":
        return ProviderConfig(
            provider="ollama",
            model=os.getenv("EMBEDDING_MODEL", _default_local_embedding_model("ollama")),
            base_url=(os.getenv("EMBEDDING_BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/"),
            api_key=os.getenv("EMBEDDING_API_KEY", "ollama"),
            enabled=True,
        )
    if provider == "lmstudio":
        return ProviderConfig(
            provider="lmstudio",
            model=os.getenv("EMBEDDING_MODEL", _default_local_embedding_model("lmstudio")),
            base_url=(os.getenv("EMBEDDING_BASE_URL") or "http://127.0.0.1:1234/v1").rstrip("/"),
            api_key=os.getenv("EMBEDDING_API_KEY", "lm-studio"),
            enabled=True,
        )
    return ProviderConfig(provider=provider, model="", base_url=None, api_key="", enabled=False)


def _rerank_provider_config() -> ProviderConfig:
    llm_runtime = get_llm_runtime_config()
    provider = _normalize_provider(os.getenv("RERANK_PROVIDER"), llm_runtime.provider)
    if provider == "disabled":
        return ProviderConfig(provider=provider, model="", base_url=None, api_key="", enabled=False)
    if provider == "ollama" and not _env_flag("RERANK_FORCE_LOCAL", default=False):
        return ProviderConfig(provider="disabled", model="", base_url=None, api_key="", enabled=False)
    if provider == "openai":
        api_key = os.getenv("RERANK_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            return ProviderConfig(provider=provider, model="", base_url=None, api_key="", enabled=False)
        return ProviderConfig(
            provider="openai",
            model=os.getenv("RERANK_MODEL") or os.getenv("OPENAI_RERANK_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            base_url=os.getenv("RERANK_BASE_URL") or None,
            api_key=api_key,
            enabled=True,
        )
    if provider == "ollama":
        return ProviderConfig(
            provider="ollama",
            model=os.getenv("RERANK_MODEL", _default_local_rerank_model("ollama")),
            base_url=(os.getenv("RERANK_BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/"),
            api_key=os.getenv("RERANK_API_KEY", "ollama"),
            enabled=True,
        )
    if provider == "lmstudio":
        return ProviderConfig(
            provider="lmstudio",
            model=os.getenv("RERANK_MODEL", _default_local_rerank_model("lmstudio")),
            base_url=(os.getenv("RERANK_BASE_URL") or "http://127.0.0.1:1234/v1").rstrip("/"),
            api_key=os.getenv("RERANK_API_KEY", "lm-studio"),
            enabled=True,
        )
    return ProviderConfig(provider=provider, model="", base_url=None, api_key="", enabled=False)


_embedding_client: OpenAI | None = None
_embedding_client_failed = False
_rerank_client: OpenAI | None = None
_rerank_client_failed = False
_semantic_runtime_cache: tuple[float, dict[str, Any]] | None = None


def _embedding_client_instance() -> OpenAI | None:
    global _embedding_client, _embedding_client_failed
    if _embedding_client_failed:
        return None
    if _embedding_client is not None:
        return _embedding_client
    config = _embedding_provider_config()
    client = _provider_client(config)
    if not client:
        _embedding_client_failed = True
        return None
    _embedding_client = client
    return _embedding_client


def _rerank_client_instance() -> OpenAI | None:
    global _rerank_client, _rerank_client_failed
    if _rerank_client_failed:
        return None
    if _rerank_client is not None:
        return _rerank_client
    config = _rerank_provider_config()
    client = _provider_client(config)
    if not client:
        _rerank_client_failed = True
        return None
    _rerank_client = client
    return _rerank_client


def embedding_model_name() -> str:
    return _embedding_provider_config().model


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
    client = _embedding_client_instance()
    config = _embedding_provider_config()
    if not client or not config.enabled or not text.strip():
        return []
    try:
        response = client.embeddings.create(
            model=config.model,
            input=text,
        )
        vector = response.data[0].embedding if response.data else []
        return [float(value) for value in vector]
    except Exception as exc:  # pragma: no cover
        logger.warning("No he podido generar embedding con %s: %s", config.provider, exc)
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
    client = _rerank_client_instance()
    config = _rerank_provider_config()
    if not client or not config.enabled or not query_text.strip() or not candidates:
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
        payload = {
            "model": config.model,
            "messages": [
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
                        {"query": query_text, "candidates": payload_candidates},
                        ensure_ascii=False,
                    ),
                },
            ],
            "temperature": 0,
        }
        if config.provider != "ollama":
            payload["response_format"] = {"type": "json_object"}
        response = client.chat.completions.create(**payload)
        raw = response.choices[0].message.content or "{}"
        parsed = parse_json_response(raw)
        scores: dict[int, float] = {}
        for item in parsed.get("scores", []) or []:
            try:
                candidate_id = int(item.get("id"))
                score = max(0.0, min(1.0, float(item.get("score", 0.0))))
            except (TypeError, ValueError):
                continue
            scores[candidate_id] = score
        return scores
    except Exception as exc:  # pragma: no cover
        logger.warning("No he podido rerankear candidatos con %s: %s", config.provider, exc)
        return {}


def describe_semantic_runtime() -> dict[str, Any]:
    global _semantic_runtime_cache
    now = time.time()
    if _semantic_runtime_cache and _semantic_runtime_cache[0] > now:
        return _semantic_runtime_cache[1]

    llm_runtime = get_llm_runtime_config()
    embedding_config = _embedding_provider_config()
    rerank_config = _rerank_provider_config()
    embedding_health = _provider_health(embedding_config)

    rerank_same_backend = (
        rerank_config.provider == embedding_config.provider
        and rerank_config.base_url == embedding_config.base_url
        and bool(rerank_config.enabled) == bool(embedding_config.enabled)
    )
    rerank_health = embedding_health if rerank_same_backend else _provider_health(rerank_config)

    provider_label = {
        "ollama": "Ollama",
        "openai": "OpenAI",
        "lmstudio": "LM Studio",
    }.get(llm_runtime.provider, llm_runtime.provider.title())
    semantic_mode = "normal" if embedding_health.get("available") else "degraded"

    snapshot = {
        "provider": llm_runtime.provider,
        "provider_label": provider_label,
        "model": llm_runtime.model,
        "base_url": llm_runtime.base_url or "",
        "embedding_provider": embedding_config.provider,
        "embedding_model": embedding_config.model,
        "embedding_available": bool(embedding_health.get("available")),
        "embedding_error": str(embedding_health.get("error") or ""),
        "rerank_provider": rerank_config.provider,
        "rerank_model": rerank_config.model,
        "rerank_available": bool(rerank_health.get("available")),
        "rerank_error": str(rerank_health.get("error") or ""),
        "semantic_mode": semantic_mode,
        "status_label": "Búsqueda semántica activa" if semantic_mode == "normal" else "Búsqueda degradada",
    }
    _semantic_runtime_cache = (now + 20, snapshot)
    return snapshot
