"""
llm_client.py — Abstracción del cliente LLM para DMANDER.

Provee una clase base abstracta y una implementación para OpenAI.
Diseñado para poder añadir fácilmente otros proveedores (Anthropic, Ollama, etc.).
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod

from openai import OpenAI


# ---------------------------------------------------------------------------
# Clase base abstracta
# ---------------------------------------------------------------------------

class LLMClient(ABC):
    """Interfaz base para cualquier proveedor de LLM."""

    @abstractmethod
    def analyze(self, system_prompt: str, user_prompt: str) -> str:
        """
        Envía un prompt al LLM y devuelve la respuesta como string.

        Args:
            system_prompt: Instrucciones del sistema para el modelo.
            user_prompt: Prompt del usuario con contexto acumulado.

        Returns:
            Respuesta cruda del modelo como string.
        """
        ...


# ---------------------------------------------------------------------------
# Implementación OpenAI
# ---------------------------------------------------------------------------

class OpenAIClient(LLMClient):
    """Cliente LLM usando la API de OpenAI."""

    def __init__(self) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "❌ No se encontró OPENAI_API_KEY.\n"
                "   Copia .env.example a .env y añade tu API key."
            )

        self.client = OpenAI(api_key=api_key)
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    def analyze(self, system_prompt: str, user_prompt: str) -> str:
        """Envía el prompt al modelo de OpenAI y devuelve la respuesta."""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,  # Baja temperatura para respuestas más consistentes
            response_format={"type": "json_object"},  # Forzar output JSON
        )

        return response.choices[0].message.content or ""
