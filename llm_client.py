"""
llm_client.py — Abstracción configurable del cliente LLM para DMANDER.

Permite cambiar fácilmente entre:
- OpenAI
- Ollama
- LM Studio

Todos se consumen mediante interfaz OpenAI-compatible para simplificar el código.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


class LLMClient(ABC):
    """Interfaz base para cualquier proveedor de LLM."""

    @abstractmethod
    def analyze(self, system_prompt: str, user_prompt: str) -> str:
        """Envía un prompt al modelo y devuelve la respuesta cruda."""
        ...


@dataclass(frozen=True)
class LLMRuntimeConfig:
    provider: str
    model: str
    base_url: str | None
    api_key: str
    supports_response_format: bool


def _normalize_provider(value: str | None) -> str:
    provider = str(value or "openai").strip().lower()
    aliases = {
        "openai-compatible": "lmstudio",
        "lm_studio": "lmstudio",
        "local": "ollama",
    }
    return aliases.get(provider, provider)


def _load_project_dotenv() -> None:
    dotenv_path = Path(__file__).resolve().with_name(".env")
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def get_llm_runtime_config() -> LLMRuntimeConfig:
    _load_project_dotenv()
    provider = _normalize_provider(os.getenv("LLM_PROVIDER", "openai"))
    if provider == "openai":
        api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            raise ValueError(
                "❌ No se encontró OPENAI_API_KEY o LLM_API_KEY.\n"
                "   Configura credenciales de OpenAI o cambia LLM_PROVIDER a ollama/lmstudio."
            )
        return LLMRuntimeConfig(
            provider="openai",
            model=os.getenv("LLM_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            base_url=os.getenv("LLM_BASE_URL") or None,
            api_key=api_key,
            supports_response_format=True,
        )
    if provider == "ollama":
        return LLMRuntimeConfig(
            provider="ollama",
            model=os.getenv("LLM_MODEL", "qwen2.5:7b-instruct"),
            base_url=(os.getenv("LLM_BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/"),
            api_key=os.getenv("LLM_API_KEY", "ollama"),
            supports_response_format=False,
        )
    if provider == "lmstudio":
        return LLMRuntimeConfig(
            provider="lmstudio",
            model=os.getenv("LLM_MODEL", "local-model"),
            base_url=(os.getenv("LLM_BASE_URL") or "http://127.0.0.1:1234/v1").rstrip("/"),
            api_key=os.getenv("LLM_API_KEY", "lm-studio"),
            supports_response_format=True,
        )
    raise ValueError(
        "❌ LLM_PROVIDER no soportado. Usa openai, ollama o lmstudio."
    )


class OpenAIClient(LLMClient):
    """
    Cliente LLM configurable.

    Conserva el nombre OpenAIClient por compatibilidad con el resto del código,
    pero internamente puede apuntar a OpenAI, Ollama o LM Studio.
    """

    def __init__(self) -> None:
        self.config = get_llm_runtime_config()
        kwargs = {"api_key": self.config.api_key}
        if self.config.base_url:
            kwargs["base_url"] = self.config.base_url
        self.client = OpenAI(**kwargs)
        self.model = self.config.model

    def analyze(self, system_prompt: str, user_prompt: str) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.3,
        }
        if self.config.supports_response_format:
            payload["response_format"] = {"type": "json_object"}

        response = self.client.chat.completions.create(**payload)
        return response.choices[0].message.content or ""


def describe_llm_runtime() -> dict[str, str]:
    config = get_llm_runtime_config()
    return {
        "provider": config.provider,
        "model": config.model,
        "base_url": config.base_url or "",
    }
