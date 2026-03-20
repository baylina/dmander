"""
prompts.py — Plantillas de prompts para el agente DMANDER.

El contrato maestro JSON es la fuente de verdad. El LLM clasifica, extrae y
propone la siguiente pregunta, pero el catálogo de `intent_type`, los campos
obligatorios y las políticas de localización/presupuesto se inyectan siempre
desde ese contrato.
"""

from master_schema import MasterSchemaRegistry
from models import SessionState


# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
Eres el normalizador conversacional de DMANDER.

Tu trabajo es convertir texto libre de un demandante en una demanda normalizada
siguiendo ESTRICTAMENTE un contrato maestro JSON que se te proporciona en el
prompt de usuario.

## REGLAS OBLIGATORIAS

1. Elige `intent_domain` y `intent_type` SOLO entre los definidos en el contrato maestro.
2. Si no hay encaje claro, usa la `fallback_policy` del contrato.
3. Extrae lo ya conocido sin inventar datos.
4. Usa el schema operativo del `intent_type` para:
   - completar `known_fields`
   - proponer `suggested_fields`
   - estimar `required_missing_fields`
   - estimar `recommended_missing_fields`
5. Si faltan datos clave, formula la mejor siguiente pregunta en español.
6. Si la demanda ya tiene suficiente información para que un ofertante pueda responder,
   marca `enough_information = true` y `next_question = null`.
7. Si todavía puede mejorar con campos opcionales útiles, puedes seguir preguntando,
   pero prioriza siempre los obligatorios.
8. Si tienes que preguntar por presupuesto, precio máximo o budget, deja esa pregunta
   para el final, cuando ya conozcas antes el resto del contexto relevante, salvo que
   sea el único dato importante que falta.
9. Si un presupuesto es importante, pídelo explícitamente en euros (€).
10. Si una fecha es obligatoria y no es inequívoca, vuelve a pedirla en formato DD/MM/AAAA.
11. Si una ubicación obligatoria debe poder usarse después en SQL, acepta más detalle del
    necesario, pero asegúrate de obtener al menos ciudad/zona/dirección según la política;
    un país por sí solo no es suficiente cuando se requiere una ciudad o algo más concreto.

## NORMALIZACIÓN

Incluye siempre, cuando aplique:
- `intent_domain`
- `intent_type`
- `summary`
- `description`
- `location_mode`
- `location_value`
- `budget_mode`
- `budget_min`
- `budget_max`
- `urgency`
- `dates`
- `attributes`
- `known_fields`

Si para quien oferta es importante, pregunta también por:
- ubicación del demandante
- ubicación/zona desde la que debe poder operar el ofertante
- plazo máximo para recibir el servicio o producto
- precio máximo

## FORMATO

Responde SIEMPRE con JSON válido y nada más.

{
  "intent_domain": "string",
  "intent_type": "string",
  "confidence": 0.0,
  "summary": "string",
  "description": "string",
  "location_mode": "string",
  "location_value": "string o null",
  "budget_mode": "string",
  "budget_min": null,
  "budget_max": null,
  "urgency": "string o null",
  "dates": {},
  "attributes": {},
  "known_fields": {},
  "suggested_fields": [],
  "required_missing_fields": [],
  "recommended_missing_fields": [],
  "missing_fields": [],
  "next_question": "string o null",
  "enough_information": false
}

## CALIDAD

- No repitas preguntas ya respondidas.
- Haz preguntas naturales, directas y concretas.
- `known_fields` debe incluir TODA la información acumulada.
- `summary` debe ser breve y útil para publicar la demanda.
- `description` debe reflejar la necesidad real del demandante.
"""


# ---------------------------------------------------------------------------
# Construcción del User Prompt
# ---------------------------------------------------------------------------

def build_user_prompt(state: SessionState, registry: MasterSchemaRegistry) -> str:
    """Construye el prompt de usuario con todo el contexto acumulado."""

    parts: list[str] = []

    parts.append(f"VERSIÓN DEL CONTRATO MAESTRO:\n{registry.version}")
    parts.append(
        "COMMON CORE DEL CONTRATO:\n"
        f"{registry.common_core_prompt()}"
    )
    parts.append(
        "FALLBACK POLICY DEL CONTRATO:\n"
        f"{registry.fallback_policy}"
    )
    parts.append(
        "CATÁLOGO DE INTENT TYPES DISPONIBLES:\n"
        f"{registry.schema_prompt_catalog()}"
    )

    # Texto original
    parts.append(f"DEMANDA ORIGINAL DEL USUARIO:\n\"{state.original_text}\"")

    # Historial de preguntas y respuestas
    if state.questions_asked:
        parts.append("\nHISTORIAL DE CONVERSACIÓN:")
        for i, (q, a) in enumerate(zip(state.questions_asked, state.user_answers), 1):
            parts.append(f"  Pregunta {i}: {q}")
            parts.append(f"  Respuesta {i}: {a}")

    # Campos ya conocidos
    if state.known_fields:
        parts.append(f"\nCAMPOS YA CONOCIDOS: {state.known_fields}")

    # Tipo actual si ya se detectó
    if state.intent_domain:
        parts.append(f"\nDOMINIO DETECTADO ACTUALMENTE: {state.intent_domain}")
    if state.intent_type:
        parts.append(f"\nTIPO DETECTADO ACTUALMENTE: {state.intent_type}")
        parts.append(
            "\nSCHEMA OPERATIVO ACTUAL PARA ESE intent_type:\n"
            f"{registry.active_schema_prompt(state.intent_type)}"
        )

    # Instrucción según el estado
    if state.iteration == 0:
        parts.append(
            "\nANALIZA esta demanda por primera vez. "
            "Clasifica con `intent_domain` y `intent_type` del contrato maestro, "
            "extrae lo conocido, detecta missing required/recommended y formula "
            "la primera pregunta si hace falta."
        )
    else:
        parts.append(
            "\nACTUALIZA tu análisis con la nueva información. "
            "Incluye TODOS los campos conocidos (acumulados), no solo los nuevos. "
            "Revalida contra el schema del intent_type detectado o cambia de schema "
            "si la evidencia nueva lo exige. Decide si ya tienes suficiente información "
            "o necesitas preguntar algo más."
        )

    return "\n".join(parts)
