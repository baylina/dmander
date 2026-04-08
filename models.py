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
    """Respuesta ligera del LLM para demandas en texto libre."""

    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    summary: str = Field(default="")
    known_fields: dict[str, Any] = Field(default_factory=dict)
    description: str = Field(default="")
    location_mode: str = Field(default="unspecified")
    location_value: Optional[str] = Field(default=None)
    budget_max: Optional[float] = Field(default=None)
    budget_unit: str = Field(default="total")
    attributes: dict[str, Any] = Field(default_factory=dict)
    suggested_missing_details: list[str] = Field(default_factory=list)
    next_question: Optional[str] = Field(default=None)
    enough_information: bool = Field(default=True)
    intent_domain: str = Field(default="")
    intent_type: str = Field(default="")
    budget_mode: str = Field(default="optional_fixed")
    budget_min: Optional[float] = Field(default=None)
    urgency: Optional[str] = Field(default=None)
    dates: dict[str, Any] = Field(default_factory=dict)
    suggested_fields: list[str] = Field(default_factory=list)
    required_missing_fields: list[str] = Field(default_factory=list)
    recommended_missing_fields: list[str] = Field(default_factory=list)
    validation_issues: list[dict[str, Any]] = Field(default_factory=list)
    next_question_field: Optional[str] = Field(default=None)
    missing_fields: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Demanda final estructurada
# ---------------------------------------------------------------------------

class DemandResult(BaseModel):
    """Demanda simplificada: texto libre + ubicación y presupuesto opcionales."""

    id: Optional[int] = None
    entity_type: str = "d"
    raw_text: str = ""
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
    location_json: dict[str, Any] = Field(default_factory=dict)
    budget_mode: str = "optional_fixed"
    location: Optional[str] = None
    budget_max: Optional[float] = None
    budget_unit: str = "total"
    expires_at: Optional[datetime] = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    known_fields: dict[str, Any] = Field(default_factory=dict)
    suggested_missing_details: list[str] = Field(default_factory=list)
    next_question: Optional[str] = None
    enough_information: bool = True
    confidence: float = 0.0
    llm_metadata: dict[str, Any] = Field(default_factory=dict)
    intent_domain: str = ""
    intent_type: str = "free_text"
    budget_min: Optional[float] = None
    urgency: Optional[str] = None
    dates: dict[str, Any] = Field(default_factory=dict)
    required_missing_fields: list[str] = Field(default_factory=list)
    recommended_missing_fields: list[str] = Field(default_factory=list)
    validation_issues: list[dict[str, Any]] = Field(default_factory=list)
    schema_version: str = ""
    needs_review: bool = False
    location_geojson: dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class UserProfile(BaseModel):
    """Usuario autenticado en la web."""

    id: int
    email: str
    full_name: str
    password_hash: Optional[str] = None
    avatar_url: Optional[str] = None
    auth_source: str = "local"
    role: str = "user"
    is_active: bool = True
    last_login_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class APITokenInfo(BaseModel):
    """Token de API asociado a un usuario."""

    id: int
    user_id: int
    name: str
    token_prefix: str
    last_used_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class PublicDemand(BaseModel):
    """Demanda pública mostrada en la web."""

    id: int
    public_id: str = ""
    user_id: Optional[int] = None
    summary: str
    original_text: str = ""
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
    location_json: dict[str, Any] = Field(default_factory=dict)
    budget_max: Optional[float] = None
    budget_unit: str = "total"
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
    llm_metadata: dict[str, Any] = Field(default_factory=dict)
    suggested_missing_details: list[str] = Field(default_factory=list)
    viewer_has_offer: bool = False
    viewer_offer_id: Optional[int] = None
    intent_domain: str = ""
    intent_type: str = "free_text"
    budget_min: Optional[float] = None
    urgency: Optional[str] = None
    location_geojson: dict[str, Any] = Field(default_factory=dict)


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
