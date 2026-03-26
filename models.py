"""
models.py — Modelos de datos para DMANDER POC.

Define las estructuras de datos principales:
- LLMResponse: respuesta esperada del LLM en cada iteración
- DemandResult: demanda final estructurada con núcleo común + attributes dinámicos
- SessionState: estado de sesión en memoria durante la conversación
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Respuesta del LLM en cada iteración
# ---------------------------------------------------------------------------

class LLMResponse(BaseModel):
    """Estructura que el LLM debe devolver en cada análisis."""

    intent_domain: str = Field(
        default="",
        description="Dominio macro estimado según el contrato maestro"
    )
    intent_type: str = Field(
        description="Tipo estimado de demanda (ej: math_tutoring, hotel_booking, car_purchase...)"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Confianza aproximada del modelo en su análisis (0-1)"
    )
    known_fields: dict[str, Any] = Field(
        default_factory=dict,
        description="Campos que el modelo cree que ya conoce"
    )
    description: str = Field(
        default="",
        description="Descripción libre de la demanda, cercana al texto original"
    )
    location_mode: str = Field(
        default="unspecified",
        description="Modo de localización normalizado"
    )
    location_value: Optional[str] = Field(
        default=None,
        description="Valor de localización normalizado"
    )
    budget_mode: str = Field(
        default="optional_range",
        description="Política o modo de presupuesto aplicable"
    )
    budget_min: Optional[float] = Field(default=None)
    budget_max: Optional[float] = Field(default=None)
    urgency: Optional[str] = Field(default=None)
    dates: dict[str, Any] = Field(
        default_factory=dict,
        description="Fechas relevantes extraídas de la demanda"
    )
    attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Atributos dinámicos no cubiertos por el núcleo común"
    )
    suggested_fields: list[str] = Field(
        default_factory=list,
        description="Campos típicos y útiles que el modelo sugiere para este tipo de demanda"
    )
    required_missing_fields: list[str] = Field(
        default_factory=list,
        description="Campos obligatorios faltantes según el contrato maestro"
    )
    recommended_missing_fields: list[str] = Field(
        default_factory=list,
        description="Campos opcionales recomendables aún no presentes"
    )
    validation_issues: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Problemas de normalización detectados en campos presentes pero aún no utilizables"
    )
    next_question_field: Optional[str] = Field(
        default=None,
        description="Campo concreto que se está intentando completar o corregir"
    )
    missing_fields: list[str] = Field(
        default_factory=list,
        description="Campos importantes que aún faltan por completar"
    )
    next_question: Optional[str] = Field(
        default=None,
        description="Mejor siguiente pregunta para el demandante, o null si ya hay suficiente info"
    )
    enough_information: bool = Field(
        default=False,
        description="Indica si ya hay suficiente contexto para cerrar la demanda"
    )
    summary: str = Field(
        default="",
        description="Resumen breve y legible de la demanda actual"
    )


# ---------------------------------------------------------------------------
# Demanda final estructurada
# ---------------------------------------------------------------------------

class DemandResult(BaseModel):
    """Demanda normalizada final compatible con el contrato maestro."""

    id: Optional[int] = None
    entity_type: str = "d"
    raw_text: str = ""
    intent_domain: str = ""
    intent_type: str
    summary: str
    description: str = ""
    location_mode: str = "unspecified"
    location_value: Optional[str] = None
    location_label: Optional[str] = None
    location_admin_level: Optional[str] = None
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None
    location_radius_km: Optional[int] = None
    location_radius_bucket: Optional[str] = None
    location_source: Optional[str] = None
    location_raw_query: Optional[str] = None
    location_bbox: list[float] = Field(default_factory=list)
    location_geojson: dict[str, Any] = Field(default_factory=dict)
    location_json: dict[str, Any] = Field(default_factory=dict)
    budget_mode: str = "optional_range"
    location: Optional[str] = None
    budget_min: Optional[float] = None
    budget_max: Optional[float] = None
    urgency: Optional[str] = None
    dates: dict[str, Any] = Field(default_factory=dict)
    expires_at: Optional[datetime] = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    known_fields: dict[str, Any] = Field(default_factory=dict)
    required_missing_fields: list[str] = Field(default_factory=list)
    recommended_missing_fields: list[str] = Field(default_factory=list)
    validation_issues: list[dict[str, Any]] = Field(default_factory=list)
    next_question: Optional[str] = None
    enough_information: bool = False
    confidence: float = 0.0
    schema_version: str = ""
    needs_review: bool = False
    created_at: Optional[datetime] = None


class UserProfile(BaseModel):
    """Usuario autenticado en la web."""

    id: int
    email: str
    full_name: str
    password_hash: Optional[str] = None
    avatar_url: Optional[str] = None
    auth_source: str = "local"
    created_at: Optional[datetime] = None


class PublicDemand(BaseModel):
    """Demanda pública mostrada en la web."""

    id: int
    user_id: Optional[int] = None
    intent_domain: str = ""
    summary: str
    intent_type: str
    location: Optional[str] = None
    location_label: Optional[str] = None
    location_display: Optional[str] = None
    location_mode: Optional[str] = None
    location_admin_level: Optional[str] = None
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None
    location_radius_km: Optional[int] = None
    location_radius_bucket: Optional[str] = None
    location_source: Optional[str] = None
    location_raw_query: Optional[str] = None
    location_bbox: list[float] = Field(default_factory=list)
    location_geojson: dict[str, Any] = Field(default_factory=dict)
    location_json: dict[str, Any] = Field(default_factory=dict)
    budget_min: Optional[float] = None
    budget_max: Optional[float] = None
    urgency: Optional[str] = None
    status: str = "open"
    effective_status: str = "open"
    is_pinned: bool = False
    can_pause: bool = False
    can_reactivate: bool = False
    can_delete: bool = False
    can_pin: bool = False
    offer_count: int = 0
    owner_name: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    normalized_payload: dict[str, Any] = Field(default_factory=dict)
    viewer_has_offer: bool = False
    viewer_offer_id: Optional[int] = None


class OfferResult(BaseModel):
    """Oferta enviada por un ofertante para una demanda."""

    id: Optional[int] = None
    demand_id: int
    supplier_user_id: int
    supplier_name: Optional[str] = None
    supplier_email: Optional[str] = None
    message: str
    created_at: Optional[datetime] = None


class OfferMessageResult(BaseModel):
    """Mensaje simple en el hilo entre demandante y ofertante."""

    id: Optional[int] = None
    offer_id: int
    sender_user_id: int
    sender_name: Optional[str] = None
    body: str
    created_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Estado de sesión en memoria
# ---------------------------------------------------------------------------

@dataclass
class SessionState:
    """Estado acumulado durante la conversación con el demandante."""

    original_text: str = ""
    questions_asked: list[str] = field(default_factory=list)
    user_answers: list[str] = field(default_factory=list)
    known_fields: dict[str, Any] = field(default_factory=dict)
    intent_domain: str = ""
    intent_type: str = ""
    summary: str = ""
    iteration: int = 0
    telegram_user_id: int | None = None
