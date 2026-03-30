"""
agent.py — Agente principal de DMANDER.

Orquesta el flujo iterativo de análisis de demandas:
1. Recibe texto inicial del demandante
2. Llama al LLM para analizar
3. Parsea y valida la respuesta JSON
4. Actualiza el estado de sesión
5. Repite hasta tener suficiente información
6. Genera la demanda final estructurada
"""

from __future__ import annotations

import os
from typing import Any

from demand_normalizer import build_normalized_demand, merge_known_fields
from field_specs import is_budget_field
from llm_client import LLMClient
from master_schema import get_master_schema_registry
from models import DemandResult, LLMResponse, SessionState
from normalization_rules import get_field_prompt
from prompts import SYSTEM_PROMPT, build_user_prompt
from utils import parse_json_response


# Máximo de reintentos si el LLM devuelve JSON malformado
MAX_PARSE_RETRIES = 3


class DemandAgent:
    """Agente que analiza demandas de forma iterativa mediante un LLM."""

    def __init__(self, llm_client: LLMClient) -> None:
        self.llm = llm_client
        self.max_iterations = int(os.getenv("MAX_ITERATIONS", "8"))
        self.registry = get_master_schema_registry()

    # ------------------------------------------------------------------
    # Análisis: llama al LLM y parsea la respuesta
    # ------------------------------------------------------------------

    def analyze(self, state: SessionState) -> LLMResponse:
        """
        Envía el estado actual al LLM y devuelve un LLMResponse validado.

        Reintenta hasta MAX_PARSE_RETRIES veces si la respuesta no es JSON válido.

        Raises:
            RuntimeError: Si no se puede obtener una respuesta válida después de los reintentos.
        """
        user_prompt = build_user_prompt(state, self.registry)
        last_error = ""

        for attempt in range(1, MAX_PARSE_RETRIES + 1):
            try:
                raw = self.llm.analyze(SYSTEM_PROMPT, user_prompt)
                data = parse_json_response(raw)
                response = LLMResponse.model_validate(data)
                response = self._validate_and_complete_response(state, response)
                return response

            except (ValueError, Exception) as e:
                last_error = str(e)
                if attempt < MAX_PARSE_RETRIES:
                    continue  # Reintentar

        raise RuntimeError(
            f"No se pudo obtener una respuesta válida del LLM "
            f"después de {MAX_PARSE_RETRIES} intentos.\n"
            f"Último error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Actualización del estado de sesión
    # ------------------------------------------------------------------

    def update_state(
        self,
        state: SessionState,
        response: LLMResponse,
        user_answer: str | None = None,
    ) -> None:
        """
        Actualiza el estado de sesión con la respuesta del LLM y la respuesta del usuario.

        - Acumula campos conocidos
        - Registra preguntas y respuestas
        - Actualiza intent_type y summary
        """
        # Actualizar campos conocidos (acumular, no reemplazar)
        state.known_fields = merge_known_fields(state.known_fields, response.known_fields, response.attributes)

        # Actualizar tipo y resumen
        state.intent_domain = response.intent_domain
        state.intent_type = response.intent_type
        state.summary = response.summary

        if response.next_question and user_answer is not None:
            state.questions_asked.append(response.next_question)
            state.user_answers.append(user_answer)

        state.iteration += 1

    # ------------------------------------------------------------------
    # Construcción de la demanda final
    # ------------------------------------------------------------------

    def build_final_demand(
        self,
        state: SessionState,
        response: LLMResponse,
    ) -> DemandResult:
        """
        Construye la demanda final estructurada a partir del estado y la última respuesta.

        Separa los campos del núcleo común de los atributos dinámicos.
        """
        return build_normalized_demand(
            raw_text=state.original_text,
            known_fields=state.known_fields,
            response=response,
            registry=self.registry,
        )

    # ------------------------------------------------------------------
    # Propiedades de control
    # ------------------------------------------------------------------

    def has_reached_max_iterations(self, state: SessionState) -> bool:
        """Comprueba si se ha alcanzado el máximo de iteraciones."""
        return state.iteration >= self.max_iterations

    def _validate_and_complete_response(self, state: SessionState, response: LLMResponse) -> LLMResponse:
        draft = build_normalized_demand(
            raw_text=state.original_text,
            known_fields=state.known_fields,
            response=response,
            registry=self.registry,
        )
        response.intent_domain = draft.intent_domain
        response.intent_type = draft.intent_type
        response.description = draft.description
        response.location_mode = draft.location_mode
        response.location_value = draft.location_value
        response.budget_mode = draft.budget_mode
        response.budget_min = draft.budget_min
        response.budget_max = draft.budget_max
        response.urgency = draft.urgency
        response.dates = draft.dates
        response.attributes = draft.attributes
        response.known_fields = draft.known_fields
        response.required_missing_fields = self._order_missing_fields(draft.required_missing_fields)
        response.recommended_missing_fields = self._order_missing_fields(draft.recommended_missing_fields)
        response.validation_issues = draft.validation_issues
        response.missing_fields = response.required_missing_fields or response.recommended_missing_fields
        response.enough_information = draft.enough_information and response.enough_information

        issue_question = self._select_issue_question(
            draft.validation_issues,
            response.required_missing_fields,
            response.recommended_missing_fields,
        )
        if issue_question:
            response.next_question_field = issue_question["field_name"]
            response.next_question = issue_question["question"]
            response.enough_information = False
        elif draft.required_missing_fields:
            response.enough_information = False
            first_missing = self._select_next_missing_target(
                response.required_missing_fields,
                response.recommended_missing_fields,
            )
            schema = self.registry.resolve_intent_schema(response.intent_type)
            if (
                not response.next_question
                or self._looks_like_budget_question(response.next_question)
                or not self._question_targets_any(response.next_question, response.required_missing_fields)
            ):
                target = first_missing
                response.next_question_field = target
                response.next_question = get_field_prompt(
                    target,
                    state.original_text,
                    response.intent_type,
                    response.intent_domain,
                    field_description=schema.field_spec(target).description,
                )["question"]
        elif response.recommended_missing_fields and self._looks_like_budget_question(response.next_question or ""):
            first_recommended = self._select_next_missing_target([], response.recommended_missing_fields)
            if first_recommended:
                schema = self.registry.resolve_intent_schema(response.intent_type)
                response.next_question_field = first_recommended
                response.next_question = get_field_prompt(
                    first_recommended,
                    state.original_text,
                    response.intent_type,
                    response.intent_domain,
                    field_description=schema.field_spec(first_recommended).description,
                )["question"]

        technical_field = self._detect_technical_field_in_question(
            response.next_question or "",
            response.required_missing_fields,
            response.recommended_missing_fields,
        )
        if technical_field:
            schema = self.registry.resolve_intent_schema(response.intent_type)
            response.next_question_field = technical_field
            response.next_question = get_field_prompt(
                technical_field,
                state.original_text,
                response.intent_type,
                response.intent_domain,
                field_description=schema.field_spec(technical_field).description,
            )["question"]

        return response

    def _order_missing_fields(self, fields: list[str]) -> list[str]:
        unique: list[str] = []
        for field in fields:
            if field not in unique:
                unique.append(field)
        return sorted(unique, key=lambda field: (self._is_budget_field(field), unique.index(field)))

    def _is_budget_field(self, field_name: str) -> bool:
        return is_budget_field(field_name)

    def _looks_like_budget_question(self, question: str) -> bool:
        normalized = question.lower()
        return any(token in normalized for token in ("presupuesto", "precio", "budget", "cuánto", "cuanto"))

    def _select_issue_question(
        self,
        issues: list[dict[str, Any]],
        required_missing_fields: list[str],
        recommended_missing_fields: list[str],
    ) -> dict[str, str] | None:
        if not issues:
            return None

        required_set = set(required_missing_fields)
        recommended_set = set(recommended_missing_fields)

        def issue_sort_key(issue: dict[str, Any]) -> tuple[int, int]:
            field_name = issue.get("field_name", "")
            budget_group = 1 if self._is_budget_field(field_name) else 0
            if field_name in required_set:
                priority_group = 0
            elif field_name in recommended_set:
                priority_group = 1
            else:
                priority_group = 2
            return (budget_group, priority_group)

        chosen = sorted(issues, key=issue_sort_key)[0]
        return {
            "field_name": chosen.get("field_name", ""),
            "question": chosen.get("question", ""),
        }

    def _select_next_missing_target(self, required_missing_fields: list[str], recommended_missing_fields: list[str]) -> str | None:
        combined = [*required_missing_fields, *recommended_missing_fields]
        for field_name in combined:
            if not self._is_budget_field(field_name):
                return field_name
        return combined[0] if combined else None

    def _detect_technical_field_in_question(
        self,
        question: str,
        required_missing_fields: list[str],
        recommended_missing_fields: list[str],
    ) -> str | None:
        lowered = question.lower()
        for field_name in [*required_missing_fields, *recommended_missing_fields]:
            if field_name and field_name.lower() in lowered:
                return field_name
        return None

    def _question_targets_any(self, question: str, field_names: list[str]) -> bool:
        lowered = question.lower()
        return any(field_name.lower() in lowered for field_name in field_names if field_name)
