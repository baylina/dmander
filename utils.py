"""
utils.py — Utilidades para DMANDER POC.

Funciones de parseo de JSON y formateo de salida en consola.
"""

from __future__ import annotations

import json
import re
from typing import Any


# ---------------------------------------------------------------------------
# Colores ANSI para la consola
# ---------------------------------------------------------------------------

class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Parseo de JSON
# ---------------------------------------------------------------------------

def parse_json_response(raw: str) -> dict[str, Any]:
    """
    Extrae y parsea JSON de la respuesta del LLM.

    Maneja varios formatos:
    - JSON directo
    - JSON envuelto en bloques ```json ... ```
    - JSON envuelto en ``` ... ```

    Args:
        raw: Respuesta cruda del LLM.

    Returns:
        Diccionario parseado.

    Raises:
        ValueError: Si no se puede extraer JSON válido.
    """
    text = raw.strip()

    # Intentar extraer de bloques markdown ```json ... ``` o ``` ... ```
    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if match:
        text = match.group(1).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"No se pudo parsear JSON: {e}\nRespuesta: {raw[:200]}") from e


# ---------------------------------------------------------------------------
# Formateo de consola
# ---------------------------------------------------------------------------

def print_header() -> None:
    """Imprime el encabezado de la aplicación."""
    print()
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 60}")
    print(f"   🔍  DMANDER — POC Local")
    print(f"   Conectando demanda con oferta")
    print(f"{'=' * 60}{Colors.RESET}")
    print()


def print_agent_thinking() -> None:
    """Muestra indicador de que el agente está procesando."""
    print(f"\n{Colors.DIM}⏳ Analizando tu demanda...{Colors.RESET}")


def print_agent_analysis(
    summary: str,
    known_fields: dict[str, Any],
    missing_fields: list[str],
    intent_type: str,
    confidence: float,
) -> None:
    """Imprime el análisis del agente de forma amigable."""
    print(f"\n{Colors.BOLD}{Colors.GREEN}🤖 Agente:{Colors.RESET}")
    print(f"   {summary}")

    print(f"\n   {Colors.BOLD}Tipo detectado:{Colors.RESET} {intent_type} "
          f"{Colors.DIM}(confianza: {confidence:.0%}){Colors.RESET}")

    if known_fields:
        print(f"\n   {Colors.BOLD}Ya tengo:{Colors.RESET}")
        for k, v in known_fields.items():
            print(f"   • {k}: {Colors.CYAN}{v}{Colors.RESET}")

    if missing_fields:
        print(f"\n   {Colors.BOLD}Me falta concretar:{Colors.RESET}")
        for f in missing_fields:
            print(f"   • {Colors.YELLOW}{f}{Colors.RESET}")


def print_question(question: str) -> None:
    """Imprime la pregunta del agente."""
    print(f"\n   {Colors.BOLD}Pregunta:{Colors.RESET}")
    print(f"   {Colors.BLUE}{question}{Colors.RESET}")


def print_final_demand(demand_dict: dict[str, Any]) -> None:
    """Imprime la demanda final estructurada."""
    print(f"\n{Colors.BOLD}{Colors.GREEN}{'=' * 60}")
    print(f"   ✅  DEMANDA FINAL ESTRUCTURADA")
    print(f"{'=' * 60}{Colors.RESET}")

    # Resumen legible
    if "summary" in demand_dict:
        print(f"\n   {Colors.BOLD}Resumen:{Colors.RESET} {demand_dict['summary']}")
    if "intent_type" in demand_dict:
        print(f"   {Colors.BOLD}Tipo:{Colors.RESET} {demand_dict['intent_type']}")

    # JSON completo
    print(f"\n{Colors.BOLD}   JSON completo:{Colors.RESET}")
    formatted = json.dumps(demand_dict, indent=2, ensure_ascii=False)
    for line in formatted.split("\n"):
        print(f"   {Colors.CYAN}{line}{Colors.RESET}")
    print()


def print_error(message: str) -> None:
    """Imprime un mensaje de error."""
    print(f"\n{Colors.RED}❌ Error: {message}{Colors.RESET}")


def print_warning(message: str) -> None:
    """Imprime un mensaje de advertencia."""
    print(f"\n{Colors.YELLOW}⚠️  {message}{Colors.RESET}")


def print_iteration_info(iteration: int, max_iterations: int) -> None:
    """Muestra el progreso de las iteraciones."""
    print(f"\n{Colors.DIM}── Iteración {iteration}/{max_iterations} ──{Colors.RESET}")
