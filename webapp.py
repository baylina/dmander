"""
webapp.py — Aplicación web de DMANDER.

Incluye:
- Home pública con demandas activas
- Registro, login y logout
- Inicio de sesión social vía OAuth
- Dashboard privado
- Creación y borrado de demandas
- Envío de ofertas con validez obligatoria
"""

from __future__ import annotations

import os
import secrets
import json
import re
import smtplib
import logging
import asyncio
from pathlib import Path
from email.message import EmailMessage
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen
from typing import Any, Optional
from datetime import datetime, date

from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from psycopg2 import IntegrityError
from starlette.middleware.sessions import SessionMiddleware

from agent import DemandAgent
from database import (
    admin_delete_demand,
    authenticate_api_token,
    authenticate_user,
    clear_demand_wizard as clear_demand_wizard_record,
    change_user_password,
    consume_magic_login_token,
    create_offer_message,
    create_offer,
    create_api_token,
    create_password_reset_token,
    create_user,
    delete_user_permanently,
    delete_filter,
    delete_web_demand,
    get_editable_demand,
    get_demand_id_by_public_id,
    get_admin_user_detail,
    get_dashboard_data,
    get_demand_detail,
    get_demand_wizard as get_demand_wizard_record,
    get_user_by_email,
    get_notification_summary,
    get_offer_thread,
    get_offers_for_owner,
    get_or_create_oauth_user,
    get_public_demands,
    get_password_reset_user,
    get_saved_filter,
    get_user_by_id,
    list_admin_users,
    list_api_tokens,
    init_db,
    list_admin_demands,
    list_saved_filters,
    record_user_login,
    reset_password_with_token,
    revoke_api_token,
    save_web_demand_from_agent,
    save_demand_wizard as save_demand_wizard_record,
    save_filter,
    set_user_role,
    set_user_active_status,
    update_filter,
    update_supplier_offer_workspace,
    update_web_demand_lifecycle,
    update_web_demand_from_agent,
)
from demand_normalizer import build_normalized_demand, merge_known_fields
from llm_client import OpenAIClient
from master_schema import get_master_schema_registry
from models import DemandResult, LLMResponse, SessionState
from normalization_rules import dynamic_required_fields, get_field_prompt
from field_normalizers import parse_date_value
from field_specs import SPANISH_MONTHS, is_budget_field, is_date_field, is_location_field
from location_geometry import zone_has_geometry, zones_intersect
from schema_editor import (
    SchemaEditorError,
    create_domain,
    create_intent_type,
    reorder_domains,
    reorder_intent_types,
    get_field_definition,
    schema_editor_context,
    update_domain,
    update_intent_type,
    delete_domain,
    delete_intent_type,
)
from zone_selector import (
    RADIUS_OPTIONS,
    compact_zone_for_transport,
    compact_zone_label,
    default_zone_payload,
    normalize_zone_payload,
    zone_display_value,
)
from utils import parse_json_response

try:
    from authlib.integrations.starlette_client import OAuth
except ImportError:  # pragma: no cover - depende de requirements
    OAuth = None


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
RUNTIME_DIR = Path(BASE_DIR) / ".runtime"
GUEST_WIZARDS_DIR = RUNTIME_DIR / "guest_wizards"
logger = logging.getLogger(__name__)

DEMAND_TEXT_MIN_LENGTH = 8
DEMAND_TEXT_MAX_LENGTH = 200
MESSAGE_TEXT_MAX_LENGTH = 200

SELECT_FIELD_OPTIONS: dict[str, list[dict[str, str]]] = {
    "modality": [
        {"value": "presencial", "label": "Presenciales"},
        {"value": "online", "label": "Online"},
        {"value": "me da igual", "label": "Me da igual"},
    ],
    "service_mode": [
        {"value": "presencial", "label": "Presencial"},
        {"value": "online", "label": "Online"},
        {"value": "a domicilio", "label": "A domicilio"},
        {"value": "me da igual", "label": "Me da igual"},
    ],
    "urgency": [
        {"value": "lo necesito ya", "label": "Lo necesito ya"},
        {"value": "esta semana", "label": "Esta semana"},
        {"value": "en los proximos dias", "label": "En los próximos días"},
        {"value": "sin prisa", "label": "Sin prisa"},
    ],
}

SOCIAL_PROVIDERS: dict[str, dict[str, Any]] = {
    "google": {
        "label": "Google",
        "icon": "G",
        "kind": "oidc",
        "server_metadata_url": "https://accounts.google.com/.well-known/openid-configuration",
        "scope": "openid email profile",
    },
    "github": {
        "label": "GitHub",
        "icon": "GH",
        "kind": "oauth2",
        "authorize_url": "https://github.com/login/oauth/authorize",
        "access_token_url": "https://github.com/login/oauth/access_token",
        "api_base_url": "https://api.github.com/",
        "scope": "read:user user:email",
    },
    "meta": {
        "label": "Meta",
        "icon": "M",
        "kind": "oauth2",
        "authorize_url": "https://www.facebook.com/v19.0/dialog/oauth",
        "access_token_url": "https://graph.facebook.com/v19.0/oauth/access_token",
        "api_base_url": "https://graph.facebook.com/v19.0/",
        "scope": "email public_profile",
    },
    "x": {
        "label": "X",
        "icon": "X",
        "kind": "oauth2",
        "authorize_url": "https://twitter.com/i/oauth2/authorize",
        "access_token_url": "https://api.x.com/2/oauth2/token",
        "api_base_url": "https://api.x.com/2/",
        "scope": "users.read tweet.read offline.access",
    },
    "apple": {
        "label": "Apple",
        "icon": "A",
        "kind": "oauth2",
        "authorize_url": "https://appleid.apple.com/auth/authorize",
        "access_token_url": "https://appleid.apple.com/auth/token",
        "scope": "name email",
        "client_kwargs": {"response_mode": "form_post"},
    },
}


class AgentDemandAnalyzeRequest(BaseModel):
    text: str
    known_fields: dict[str, Any] = Field(default_factory=dict)


class AgentDemandPublishRequest(BaseModel):
    text: str
    known_fields: dict[str, Any] = Field(default_factory=dict)


class LightweightDemandAnalysis(BaseModel):
    summary: str = ""
    location_hint: Optional[str] = None
    budget_max: Optional[float] = None
    budget_unit: str = "total"
    suggested_missing_details: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


BUDGET_UNIT_OPTIONS: list[dict[str, str]] = [
    {"value": "total", "label": "En total"},
    {"value": "hour", "label": "Por hora"},
    {"value": "night", "label": "Por noche"},
    {"value": "day", "label": "Por día"},
    {"value": "month", "label": "Al mes"},
    {"value": "item", "label": "Por producto"},
    {"value": "service", "label": "Por servicio"},
]

BUDGET_UNIT_LABELS = {item["value"]: item["label"] for item in BUDGET_UNIT_OPTIONS}

LIGHTWEIGHT_DEMAND_SYSTEM_PROMPT = """
Analiza una demanda escrita en lenguaje natural para un marketplace.

Devuelve SOLO un JSON con estas claves:
- summary: resumen breve y claro en el mismo idioma del texto
- location_hint: texto corto con la ubicación si está clara, o null
- budget_max: número máximo en euros si aparece con claridad, o null
- budget_unit: una de estas opciones exactas: total, hour, night, day, month, item, service
- suggested_missing_details: lista corta de detalles que sería útil añadir al texto original, pero nunca obligatorios
- confidence: número entre 0 y 1

Reglas:
- No clasifiques por categorías ni intent_types.
- Si el precio no está claro, usa budget_max = null y budget_unit = total.
- No incluyas nunca en suggested_missing_details sugerencias sobre ubicación, ciudad, zona, provincia, mapa, presupuesto, precio o importe, porque esos atributos ya se revisan aparte en la aplicación.
- suggested_missing_details debe contener como máximo 4 elementos, redactados en el idioma original del texto.
- Si el texto ya está suficientemente claro, suggested_missing_details puede ser [].
""".strip()


def build_app() -> FastAPI:
    init_db()

    app = FastAPI(title="DMANDER", version="1.0.0")
    session_secret = os.getenv("SESSION_SECRET", "change-this-session-secret")
    app.add_middleware(
        SessionMiddleware,
        secret_key=session_secret,
        same_site="lax",
        https_only=False,
        max_age=60 * 60 * 24 * 14,
    )
    app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

    oauth = _build_oauth_registry()
    agent = DemandAgent(OpenAIClient())
    debug_normalization = _normalization_debug_enabled()

    @app.get("/", response_class=HTMLResponse)
    async def home(
        request: Request,
        q: str = "",
        location: str = "",
        location_zone_json: str = "",
        location_label: str = "",
        location_lat: float | None = None,
        location_lon: float | None = None,
        location_radius_km: int | None = None,
        location_radius_bucket: str = "",
        location_source: str = "",
        location_raw_query: str = "",
        saved_filter_id: str = "",
        page: int = 1,
    ) -> HTMLResponse:
        current_user = _get_display_user(request)
        current_saved_filter = None
        selected_saved_filter_id = int(saved_filter_id) if str(saved_filter_id).strip().isdigit() else None
        has_explicit_search_inputs = bool(
            q.strip()
            or location.strip()
            or location_zone_json.strip()
            or location_label.strip()
            or location_lat is not None
            or location_lon is not None
            or location_radius_km is not None
            or location_radius_bucket.strip()
            or location_source.strip()
            or location_raw_query.strip()
        )
        if current_user and selected_saved_filter_id:
            current_saved_filter = get_saved_filter(current_user.id, selected_saved_filter_id)
            if current_saved_filter and not has_explicit_search_inputs:
                q = current_saved_filter.get("query_text", "")
                location = current_saved_filter.get("location", "")
                location_zone_json = json.dumps(current_saved_filter.get("location_json") or {})
                location_label = current_saved_filter.get("location_label", "")
                location_lat = current_saved_filter.get("location_lat")
                location_lon = current_saved_filter.get("location_lon")
                location_radius_km = current_saved_filter.get("location_radius_km")
                location_radius_bucket = current_saved_filter.get("location_radius_bucket", "")
                location_source = current_saved_filter.get("location_source", "")
                location_raw_query = current_saved_filter.get("location_raw_query", "")
        zone_filter = _parse_zone_json(location_zone_json)
        if not zone_filter and (location_lat is not None and location_lon is not None):
            zone_filter = normalize_zone_payload(
                {
                    "label": location_label or location,
                    "center": {"lat": location_lat, "lon": location_lon},
                    "radius_km": location_radius_km,
                    "radius_bucket": location_radius_bucket,
                    "source": location_source or "autocomplete",
                    "raw_query": location_raw_query or location,
                }
            )
        has_zone_filter = bool(
            zone_filter
            and zone_filter.get("center", {}).get("lat") is not None
            and zone_filter.get("center", {}).get("lon") is not None
        )
        current_search_matches_saved_filter = _search_matches_saved_filter(
            current_saved_filter,
            q=q,
            location=location,
            zone_filter=zone_filter,
        )
        has_active_search = bool(
            q.strip()
            or location.strip()
            or has_zone_filter
            or selected_saved_filter_id
        )
        effective_location_text = "" if has_zone_filter else location
        all_demands = get_public_demands(
            q,
            effective_location_text,
            current_user.id if current_user else None,
            zone_filter=zone_filter,
        )
        if zone_filter:
            all_demands = [
                demand for demand in all_demands
                if zone_has_geometry(demand.location_json) and zones_intersect(demand.location_json, zone_filter)
            ]
        page_size = 50
        total_count = len(all_demands)
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        page = min(max(page, 1), total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        demands = all_demands[start:end]
        return _render(
            request,
            "home.html",
            {
                "demands": demands,
                "title": "Explorar demandas",
                "active_nav": "explore",
                "page_kind": "public",
                "search_filters": {
                    "q": q,
                    "location": location,
                    "location_zone": _compact_zone_for_query(zone_filter) or default_zone_payload(),
                    "location_zone_json": json.dumps(_compact_zone_for_query(zone_filter) or default_zone_payload(), ensure_ascii=False),
                },
                "saved_filters": list_saved_filters(current_user.id) if current_user else [],
                "current_saved_filter": current_saved_filter,
                "has_active_search": has_active_search,
                "search_matches_current_saved_filter": current_search_matches_saved_filter,
                "show_save_search_actions": bool(
                    current_user and has_active_search and (not current_saved_filter or not current_search_matches_saved_filter)
                ),
                "keyword_suggestions": _keyword_suggestions(),
                "pagination": _home_pagination(
                    page,
                    total_pages,
                    q,
                    location,
                    zone_filter,
                    selected_saved_filter_id,
                ),
            },
        )

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request) -> HTMLResponse:
        return _render(request, "login.html", {"title": "Acceder", "active_nav": "login", "page_kind": "auth"})

    @app.post("/login")
    async def login(
        request: Request,
        email: str = Form(...),
        password: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = authenticate_user(email, password)
        if not user:
            known_user = get_user_by_email(email)
            if known_user and not known_user.is_active:
                _flash(request, "Tu cuenta está desactivada. Contacta con administración si necesitas recuperarla.", "error")
                return _redirect("/login")
            _flash(request, "Email o contraseña incorrectos.", "error")
            return _redirect("/login")

        request.session["user_id"] = user.id
        redirect_target = _complete_pending_guest_publication(request, user) or "/app/chats"
        _flash(request, f"Bienvenido de nuevo, {user.full_name}.", "success")
        return _redirect(redirect_target)

    @app.get("/auth/telegram/{token}")
    async def telegram_magic_login(request: Request, token: str, next: str = "/app/chats") -> RedirectResponse:
        user = consume_magic_login_token(token)
        if not user:
            _flash(request, "El enlace de acceso desde Telegram ya no es válido. Pide uno nuevo desde el bot.", "error")
            return _redirect("/login")
        request.session["user_id"] = user.id
        request.session.pop("admin_view_user_id", None)
        _flash(request, f"Has accedido a dmander con tu cuenta de Telegram, {user.full_name}.", "success")
        return _redirect(_safe_internal_redirect_path(next))

    @app.get("/signup", response_class=HTMLResponse)
    async def signup_page(request: Request) -> HTMLResponse:
        return _render(request, "signup.html", {"title": "Crear cuenta", "active_nav": "signup", "page_kind": "auth"})

    @app.get("/forgot-password", response_class=HTMLResponse)
    async def forgot_password_page(request: Request) -> HTMLResponse:
        current_user = _get_current_user(request)
        return _render(
            request,
            "forgot_password.html",
            {
                "title": "Recuperar contraseña",
                "active_nav": "login",
                "page_kind": "auth",
                "email_hint": _mask_email_hint(current_user.email) if current_user else "",
            },
        )

    @app.post("/forgot-password")
    async def forgot_password(
        request: Request,
        email: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        normalized_email = email.strip().lower()
        if "@" not in normalized_email or "." not in normalized_email:
            _flash(request, "Introduce el email de la cuenta para enviarte el enlace.", "error")
            return _redirect("/forgot-password")
        user, token = create_password_reset_token(normalized_email)
        if user and token:
            base_url = _app_base_url(request)
            reset_url = f"{base_url}/reset-password/{token}"
            body = (
                f"Hola {user.full_name},\n\n"
                "Hemos recibido una solicitud para restablecer tu contraseña en dmander.\n\n"
                f"Abre este enlace para elegir una nueva contraseña:\n{reset_url}\n\n"
                "Si tú no has solicitado este cambio, puedes ignorar este email.\n"
                "El enlace caduca en 2 horas.\n"
            )
            try:
                _send_email_message(user.email, "Restablecer contraseña en dmander", body)
            except Exception:
                logger.exception("No se pudo enviar el email de reset a %s", user.email)
        _flash(
            request,
            "Si existe una cuenta activa para ese email, te acabamos de enviar un enlace para restablecer la contraseña.",
            "success",
        )
        return _redirect("/login")

    @app.post("/signup")
    async def signup(
        request: Request,
        full_name: str = Form(...),
        email: str = Form(...),
        password: str = Form(...),
        password_confirm: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        full_name = full_name.strip()
        email = email.strip().lower()

        if len(full_name) < 2:
            _flash(request, "Introduce tu nombre completo.", "error")
            return _redirect("/signup")
        if "@" not in email or "." not in email:
            _flash(request, "Introduce un email válido.", "error")
            return _redirect("/signup")
        if len(password) < 8:
            _flash(request, "La contraseña debe tener al menos 8 caracteres.", "error")
            return _redirect("/signup")
        if password != password_confirm:
            _flash(request, "Las contraseñas no coinciden.", "error")
            return _redirect("/signup")

        try:
            user = create_user(email=email, password=password, full_name=full_name)
        except IntegrityError:
            _flash(request, "Ya existe una cuenta con ese email.", "error")
            return _redirect("/signup")

        try:
            base_url = _app_base_url(request)
            body = (
                f"Hola {user.full_name},\n\n"
                "Tu cuenta en dmander ya está creada correctamente.\n\n"
                f"Ya puedes acceder aquí:\n{base_url}/login\n\n"
                "Si no has sido tú, ignora este mensaje.\n"
            )
            _send_email_message(user.email, "Tu cuenta de dmander ya está activa", body)
        except Exception:
            logger.exception("No se pudo enviar el email de bienvenida a %s", user.email)

        request.session["user_id"] = user.id
        redirect_target = _complete_pending_guest_publication(request, user) or "/app/chats"
        _flash(request, f"Cuenta creada. Hola, {user.full_name}.", "success")
        return _redirect(redirect_target)

    @app.get("/account/security", response_class=HTMLResponse)
    async def account_security_page(request: Request) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a la seguridad de tu cuenta.", "error")
            return _redirect("/login")
        return _render(
            request,
            "account_security.html",
            {
                "title": "Seguridad de la cuenta",
                "active_nav": "account-security",
                "page_kind": "app",
                "email_hint": _mask_email_hint(user.email),
                "api_tokens": list_api_tokens(user.id),
                "new_api_token": _pop_created_api_token(request),
                "api_base_url": _app_base_url(request),
            },
        )

    @app.post("/account/password/change")
    async def account_change_password(
        request: Request,
        current_password: str = Form(...),
        new_password: str = Form(...),
        new_password_confirm: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para cambiar la contraseña.", "error")
            return _redirect("/login")
        if not user.password_hash:
            _flash(request, "Tu cuenta accede actualmente con Google. Usa el envío por email para definir una contraseña local.", "error")
            return _redirect("/account/security")
        if len(new_password) < 8:
            _flash(request, "La nueva contraseña debe tener al menos 8 caracteres.", "error")
            return _redirect("/account/security")
        if new_password != new_password_confirm:
            _flash(request, "La confirmación de la nueva contraseña no coincide.", "error")
            return _redirect("/account/security")
        if not change_user_password(user.id, current_password, new_password):
            _flash(request, "No he podido cambiar la contraseña. Revisa la contraseña actual.", "error")
            return _redirect("/account/security")
        _flash(request, "Tu contraseña se ha actualizado correctamente.", "success")
        return _redirect("/account/security")

    @app.post("/account/password/reset-email")
    async def account_send_password_reset(
        request: Request,
        email: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para solicitar un reset por email.", "error")
            return _redirect("/login")
        normalized_email = email.strip().lower()
        if normalized_email != user.email.lower():
            _flash(request, "Para enviar el enlace debes escribir exactamente el email de tu cuenta.", "error")
            return _redirect("/account/security")
        reset_user, token = create_password_reset_token(normalized_email)
        if reset_user and token:
            base_url = _app_base_url(request)
            reset_url = f"{base_url}/reset-password/{token}"
            body = (
                f"Hola {reset_user.full_name},\n\n"
                "Has solicitado restablecer tu contraseña en dmander.\n\n"
                f"Puedes hacerlo desde este enlace:\n{reset_url}\n\n"
                "El enlace caduca en 2 horas.\n"
            )
            try:
                _send_email_message(reset_user.email, "Restablecer contraseña en dmander", body)
            except Exception:
                logger.exception("No se pudo enviar el email de reset autenticado a %s", reset_user.email)
        _flash(request, "Si el email corresponde a tu cuenta activa, te hemos enviado el enlace de restablecimiento.", "success")
        return _redirect("/account/security")

    @app.post("/account/api-tokens")
    async def account_create_api_token(
        request: Request,
        name: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para crear un token de API.", "error")
            return _redirect("/login")
        token_name = (name or "").strip()
        if len(token_name) < 2:
            _flash(request, "Ponle un nombre reconocible al token.", "error")
            return _redirect("/account/security")
        try:
            _, plain_token = create_api_token(user.id, token_name)
        except ValueError:
            _flash(request, "No he podido crear el token.", "error")
            return _redirect("/account/security")
        _store_created_api_token(request, token_name, plain_token)
        _flash(request, "Token creado. Cópialo ahora: por seguridad solo se muestra una vez.", "success")
        return _redirect("/account/security")

    @app.post("/account/api-tokens/{token_id}/revoke")
    async def account_revoke_api_token(
        request: Request,
        token_id: int,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para revocar un token de API.", "error")
            return _redirect("/login")
        if revoke_api_token(user.id, token_id):
            _flash(request, "Token revocado.", "success")
        else:
            _flash(request, "No he podido revocar ese token.", "error")
        return _redirect("/account/security")

    @app.get("/reset-password/{token}", response_class=HTMLResponse)
    async def reset_password_page(request: Request, token: str) -> HTMLResponse:
        token_user = get_password_reset_user(token)
        return _render(
            request,
            "reset_password.html",
            {
                "title": "Nueva contraseña",
                "active_nav": "login",
                "page_kind": "auth",
                "reset_token": token,
                "token_valid": bool(token_user),
                "email_hint": _mask_email_hint(token_user.email) if token_user else "",
            },
        )

    @app.post("/reset-password/{token}")
    async def reset_password_submit(
        request: Request,
        token: str,
        password: str = Form(...),
        password_confirm: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        if len(password) < 8:
            _flash(request, "La nueva contraseña debe tener al menos 8 caracteres.", "error")
            return _redirect(f"/reset-password/{token}")
        if password != password_confirm:
            _flash(request, "La confirmación de la nueva contraseña no coincide.", "error")
            return _redirect(f"/reset-password/{token}")
        if not reset_password_with_token(token, password):
            _flash(request, "El enlace ya no es válido o ha caducado.", "error")
            return _redirect(f"/reset-password/{token}")
        _flash(request, "Contraseña restablecida. Ya puedes iniciar sesión.", "success")
        return _redirect("/login")

    @app.post("/logout")
    async def logout(request: Request, csrf_token: str = Form(...)) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        request.session.clear()
        return _redirect("/")

    @app.get("/dashboard")
    async def dashboard_redirect() -> RedirectResponse:
        return _redirect("/app/chats")

    @app.get("/how-it-works", response_class=HTMLResponse)
    async def how_it_works_page(request: Request) -> HTMLResponse:
        return _render(
            request,
            "how_it_works.html",
            {
                "title": "Cómo funciona",
                "active_nav": "how-it-works",
                "page_kind": "public",
            },
        )

    @app.get("/app/chats", response_class=HTMLResponse)
    async def app_chats_page(
        request: Request,
        offer_id: int | None = None,
        demand_id: int | None = None,
        role: str = "owner",
        demand_status: str = "active",
        offer_filter: str = "visible",
        admin_view_user_id: int | None = None,
    ) -> HTMLResponse:
        actor_user = _get_current_user(request)
        if not actor_user:
            _flash(request, "Necesitas iniciar sesión para acceder a tus chats.", "error")
            return _redirect("/login")
        workspace_user = _get_display_user(request, actor_user) or actor_user
        admin_read_only = _is_admin_readonly_view(request, actor_user)
        if admin_view_user_id and _is_superadmin(actor_user):
            target_user = get_user_by_id(admin_view_user_id)
            if target_user:
                workspace_user = target_user
                admin_read_only = workspace_user.id != actor_user.id
        data = get_dashboard_data(workspace_user.id, demand_status_filter=demand_status, offer_filter=offer_filter)
        return _render(
            request,
            "app_chats.html",
            {
                "title": "Mis chats",
                "active_nav": "app-chats",
                "page_kind": "app",
                "workspace_summary": {
                    "owner_threads": sum(len(item.get("conversations", [])) for item in data["my_demands_active"]),
                    "supplier_threads": len(data["my_offers_active"]),
                    "open_demands": len(data["my_demands_active"]),
                },
                "initial_offer_id": offer_id,
                "initial_demand_id": demand_id,
                "initial_chat_role": role if role in {"owner", "supplier"} else "owner",
                "initial_demand_status_filter": data.get("demand_status_filter", "active"),
                "initial_offer_filter": data.get("offer_filter", "visible"),
                "demand_status_counts": data.get("my_demands_status_counts", {}),
                "offer_status_counts": data.get("my_offers_status_counts", {}),
                "workspace_user": workspace_user,
                "admin_workspace_user_id": workspace_user.id if admin_read_only else None,
                "admin_workspace_read_only": admin_read_only,
            },
        )

    @app.get("/app/filters", response_class=HTMLResponse)
    async def app_filters_page(request: Request) -> HTMLResponse:
        user = _get_display_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a tus filtros.", "error")
            return _redirect("/login")
        saved_filters = list_saved_filters(user.id)
        return _render(
            request,
            "app_filters.html",
            {
                "title": "Mis filtros",
                "active_nav": "app-filters",
                "page_kind": "app",
                "saved_filters": saved_filters,
            },
        )

    @app.get("/app/activity", response_class=HTMLResponse)
    async def app_activity_page(request: Request) -> RedirectResponse:
        return _redirect("/app/chats")

    @app.get("/my-demands", response_class=HTMLResponse)
    async def my_demands_page(request: Request, selected_demand_id: int | None = None, selected_offer_id: int | None = None) -> RedirectResponse:
        target = "/app/chats"
        if selected_offer_id:
            target = f"/app/chats?offer_id={selected_offer_id}"
        return _redirect(target)

    @app.get("/admin/demands", response_class=HTMLResponse)
    async def admin_demands_page(request: Request) -> HTMLResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        return _render(
            request,
            "admin_demands.html",
            {
                "title": "Admin Demandas",
                "demands": list_admin_demands(),
            },
        )

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_index_page(request: Request) -> HTMLResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        return _render(
            request,
            "admin_index.html",
            {
                "title": "Utilidades internas",
                "admin_tools": [
                    {
                        "title": "Usuarios",
                        "href": "/admin/users",
                        "description": "Gestiona usuarios, fechas de alta, último acceso, activación y borrado definitivo.",
                    },
                    {
                        "title": "Administración de Demandas",
                        "href": "/admin/demands",
                        "description": "Revisa demandas publicadas y elimina las que necesites junto con sus ofertas y conversaciones.",
                    },
                    {
                        "title": "Administración de Schema",
                        "href": "/admin/schema",
                        "description": "Edita dominios, intent_type, campos requeridos y reglas de validación del master schema.",
                    },
                    {
                        "title": "Tipos de Campos de Demandas",
                        "href": "/admin/schema/types",
                        "description": "Consulta el catálogo técnico de tipos disponibles para definir campos en las demandas.",
                    },
                ],
            },
        )

    @app.get("/admin/users", response_class=HTMLResponse)
    async def admin_users_page(request: Request, q: str = "") -> HTMLResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        return _render(
            request,
            "admin_users.html",
            {
                "title": "Usuarios",
                "users": list_admin_users(q),
                "search_query": q,
            },
        )

    @app.get("/admin/view-as")
    async def admin_view_as(
        request: Request,
        user_id: int | None = None,
        next: str = "/",
    ) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        actor = _get_current_user(request)
        if not actor:
            return _redirect("/login")
        if user_id and user_id != actor.id:
            target_user = get_user_by_id(user_id)
            if target_user:
                request.session["admin_view_user_id"] = target_user.id
            else:
                request.session.pop("admin_view_user_id", None)
        else:
            request.session.pop("admin_view_user_id", None)
        next_target = (next or "/").strip() or "/"
        if not next_target.startswith("/"):
            next_target = "/"
        return _redirect(next_target)

    @app.get("/admin/users/{user_id}", response_class=HTMLResponse)
    async def admin_user_detail_page(request: Request, user_id: int) -> HTMLResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        user_detail = get_admin_user_detail(user_id)
        if not user_detail:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return _render(
            request,
            "admin_user_detail.html",
            {
                "title": f"Usuario · {user_detail['full_name']}",
                "managed_user": user_detail,
            },
        )

    @app.post("/admin/users/{user_id}/toggle-active")
    async def admin_user_toggle_active(
        request: Request,
        user_id: int,
        csrf_token: str = Form(...),
        action: str = Form(...),
        q: str = Form(""),
    ) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        _validate_csrf(request, csrf_token)
        target_state = action.strip().lower() == "activate"
        if set_user_active_status(user_id, target_state):
            _flash(request, "Estado del usuario actualizado.", "success")
        else:
            _flash(request, "No he podido actualizar el estado de ese usuario.", "error")
        return _redirect(f"/admin/users?q={quote(q)}" if q else "/admin/users")

    @app.post("/admin/users/{user_id}/toggle-role")
    async def admin_user_toggle_role(
        request: Request,
        user_id: int,
        csrf_token: str = Form(...),
        role: str = Form(...),
        q: str = Form(""),
    ) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        _validate_csrf(request, csrf_token)
        if set_user_role(user_id, role):
            _flash(request, "Rol del usuario actualizado.", "success")
        else:
            _flash(request, "No he podido actualizar el rol de ese usuario.", "error")
        return _redirect(f"/admin/users?q={quote(q)}" if q else "/admin/users")

    @app.post("/admin/users/{user_id}/delete")
    async def admin_user_delete(
        request: Request,
        user_id: int,
        csrf_token: str = Form(...),
        q: str = Form(""),
    ) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        _validate_csrf(request, csrf_token)
        if delete_user_permanently(user_id):
            _flash(request, "Usuario eliminado definitivamente.", "success")
            return _redirect(f"/admin/users?q={quote(q)}" if q else "/admin/users")
        _flash(request, "No he podido eliminar ese usuario.", "error")
        return _redirect(f"/admin/users?q={quote(q)}" if q else "/admin/users")

    @app.post("/admin/demands/{demand_id}/delete")
    async def admin_delete_demand_route(
        request: Request,
        demand_id: int,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        _validate_csrf(request, csrf_token)
        if admin_delete_demand(demand_id):
            _flash(request, "Demanda eliminada junto con sus ofertas y conversaciones.", "success")
        else:
            _flash(request, "No he podido eliminar esa demanda.", "error")
        return _redirect("/admin/demands")

    @app.get("/admin/schema", response_class=HTMLResponse)
    async def admin_schema_page(request: Request) -> HTMLResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        return _render(
            request,
            "admin_schema.html",
            {
                "title": "Admin Schema",
                **schema_editor_context(),
            },
        )

    @app.get("/admin/schema/types", response_class=HTMLResponse)
    async def admin_schema_types_page(request: Request) -> HTMLResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        return _render(
            request,
            "admin_schema_fields.html",
            {
                "title": "Tipos de campo",
                **schema_editor_context(),
            },
        )

    @app.get("/admin/schema/fields")
    async def admin_schema_fields_legacy_redirect(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        return _redirect("/admin/schema/types")

    @app.post("/admin/schema/domains/create")
    async def admin_schema_create_domain(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            create_domain(str(form.get("code", "")).strip(), str(form.get("name", "")).strip())
            _flash(request, "Dominio creado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/domains/update")
    async def admin_schema_update_domain(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            update_domain(
                str(form.get("original_code", "")).strip(),
                str(form.get("code", "")).strip(),
                str(form.get("name", "")).strip(),
            )
            _flash(request, "Dominio actualizado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/domains/delete")
    async def admin_schema_delete_domain(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            delete_domain(str(form.get("code", "")).strip())
            _flash(request, "Dominio eliminado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/domains/reorder")
    async def admin_schema_reorder_domains(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            reorder_domains(_form_text_list(form, "domain_order"))
            _flash(request, "Orden de dominios actualizado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/create")
    async def admin_schema_create_intent(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            create_intent_type(_intent_payload_from_form(form))
            _flash(request, "intent_type creado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/reorder")
    async def admin_schema_reorder_intents(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            reorder_intent_types(
                str(form.get("domain_code", "")).strip(),
                _form_text_list(form, "intent_order"),
            )
            _flash(request, "Orden de intent_type actualizado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/update")
    async def admin_schema_update_intent(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            update_intent_type(str(form.get("original_intent_type", "")).strip(), _intent_payload_from_form(form))
            _flash(request, "intent_type actualizado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/delete")
    async def admin_schema_delete_intent(request: Request) -> RedirectResponse:
        redirect = _require_superadmin_redirect(request)
        if redirect:
            return redirect
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        try:
            delete_intent_type(str(form.get("intent_type", "")).strip())
            _flash(request, "intent_type eliminado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.get("/my-offers", response_class=HTMLResponse)
    async def my_offers_page(request: Request, selected_offer_id: int | None = None) -> RedirectResponse:
        target = "/app/chats?role=supplier"
        if selected_offer_id:
            target = f"/app/chats?role=supplier&offer_id={selected_offer_id}"
        return _redirect(target)

    @app.get("/api/offers/{offer_id}/thread")
    async def offer_thread_api(request: Request, offer_id: int, admin_view_user_id: int | None = None) -> dict[str, Any]:
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        participant_user_id = None
        mark_read = True
        if admin_view_user_id and _is_superadmin(user):
            target_user = get_user_by_id(admin_view_user_id)
            if target_user:
                participant_user_id = target_user.id
                mark_read = False
        thread = get_offer_thread(offer_id, user.id, participant_user_id=participant_user_id, mark_read=mark_read)
        if not thread:
            raise HTTPException(status_code=404, detail="Conversación no encontrada")
        return _to_json_ready(thread)

    @app.post("/filters")
    async def save_filter_route(
        request: Request,
        name: str = Form(...),
        query_text: str = Form(""),
        location: str = Form(""),
        location_zone_json: str = Form(""),
        filter_id: int | None = Form(None),
        save_mode: str = Form("create"),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para guardar filtros.", "error")
            return _redirect("/login")
        if not name.strip():
            _flash(request, "Ponle un nombre al filtro.", "error")
            return _redirect("/")
        zone_filter = _parse_zone_json(location_zone_json)
        should_update = filter_id is not None and save_mode == "update"
        saved_filter_target_id: int | None = None
        if should_update:
            if update_filter(
                user.id,
                filter_id,
                name,
                query_text,
                location,
                "",
                zone_filter=zone_filter,
                intent_domains=[],
                intent_types=[],
            ):
                saved_filter_target_id = filter_id
                _flash(request, "Filtro actualizado.", "success")
            else:
                _flash(request, "No he podido actualizar ese filtro.", "error")
        else:
            saved_filter_target_id = save_filter(
                user.id,
                name,
                query_text,
                location,
                "",
                zone_filter=zone_filter,
                intent_domains=[],
                intent_types=[],
            )
            _flash(request, "Filtro guardado.", "success")
        return _redirect(f"/?saved_filter_id={saved_filter_target_id}" if saved_filter_target_id else "/")

    @app.post("/filters/{filter_id}/delete")
    async def delete_filter_route(
        request: Request,
        filter_id: int,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para borrar filtros.", "error")
            return _redirect("/login")
        if delete_filter(user.id, filter_id):
            _flash(request, "Filtro eliminado.", "success")
        else:
            _flash(request, "No he podido eliminar ese filtro.", "error")
        return _redirect("/")

    @app.get("/api/geocode/search")
    async def geocode_search_api(q: str = "", countrycodes: str = "") -> dict[str, Any]:
        query = q.strip()
        if len(query) < 2:
            return {"items": []}
        return {"items": _nominatim_search(query, countrycodes=countrycodes.strip())}

    @app.get("/api/geocode/reverse")
    async def geocode_reverse_api(lat: float, lon: float) -> dict[str, Any]:
        return _nominatim_reverse(lat, lon)

    @app.get("/api/notifications")
    async def notifications_api(request: Request) -> dict[str, Any]:
        user = _get_current_user(request)
        if not user:
            return {"my_demands_unread": 0, "my_offers_unread": 0, "items": []}
        return get_notification_summary(user.id)

    @app.get("/favicon.ico")
    async def favicon() -> RedirectResponse:
        return RedirectResponse(url="/static/img/logo.svg", status_code=307)

    @app.get("/api/notifications/stream")
    async def notifications_stream(request: Request) -> StreamingResponse:
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")

        async def event_stream():
            last_payload = None
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    payload = _to_json_ready(get_notification_summary(user.id))
                    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                    if serialized != last_payload:
                        yield _sse_event("notifications", serialized)
                        last_payload = serialized
                    else:
                        yield ": keep-alive\n\n"
                    await asyncio.sleep(8)
            except asyncio.CancelledError:
                return

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/api/workspace")
    async def workspace_api(
        request: Request,
        demand_status: str = "active",
        offer_filter: str = "visible",
        admin_view_user_id: int | None = None,
    ) -> dict[str, Any]:
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        workspace_user_id = user.id
        if admin_view_user_id and _is_superadmin(user):
            target_user = get_user_by_id(admin_view_user_id)
            if target_user:
                workspace_user_id = target_user.id
        data = get_dashboard_data(workspace_user_id, demand_status_filter=demand_status, offer_filter=offer_filter)
        return _to_json_ready(data)

    @app.get("/api/workspace/stream")
    async def workspace_stream(
        request: Request,
        demand_status: str = "active",
        offer_filter: str = "visible",
        admin_view_user_id: int | None = None,
    ) -> StreamingResponse:
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")

        workspace_user_id = user.id
        if admin_view_user_id and _is_superadmin(user):
            target_user = get_user_by_id(admin_view_user_id)
            if target_user:
                workspace_user_id = target_user.id

        async def event_stream():
            last_payload = None
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    payload = _to_json_ready(
                        get_dashboard_data(
                            workspace_user_id,
                            demand_status_filter=demand_status,
                            offer_filter=offer_filter,
                        )
                    )
                    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                    if serialized != last_payload:
                        yield _sse_event("workspace", serialized)
                        last_payload = serialized
                    else:
                        yield ": keep-alive\n\n"
                    await asyncio.sleep(8)
            except asyncio.CancelledError:
                return

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/api/agent/demands/analyze")
    async def agent_demands_analyze(
        request: Request,
        payload: AgentDemandAnalyzeRequest,
        verbose: bool = False,
    ) -> dict[str, Any]:
        _require_api_user(request)
        context = _build_lightweight_publication_context(agent.llm, payload.text, payload.known_fields, verbose=verbose)
        return _to_json_ready(_api_analysis_payload(context["payload"], verbose=verbose))

    @app.post("/api/agent/demands/publish", status_code=status.HTTP_201_CREATED)
    async def agent_demands_publish(
        request: Request,
        payload: AgentDemandPublishRequest,
        verbose: bool = False,
    ) -> JSONResponse:
        api_user = _require_api_user(request)
        context = _build_lightweight_publication_context(agent.llm, payload.text, payload.known_fields, verbose=verbose)
        analysis_payload = _api_analysis_payload(context["payload"], verbose=verbose)
        if not analysis_payload.get("publish_ready"):
            return JSONResponse(status_code=422, content=_to_json_ready(analysis_payload))

        persisted = save_web_demand_from_agent(api_user.id, context["draft"], context["state"])
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=_to_json_ready(
                {
                    "status": "published",
                    "demand": _api_published_demand_payload(persisted),
                    "detail_url": _demand_public_path(persisted),
                    "chats_url": f"/app/chats?demand_id={persisted.id}",
                    "analysis": analysis_payload,
                }
            ),
        )

    @app.post("/api/demands/{demand_id}/lifecycle")
    async def demand_lifecycle_api(
        request: Request,
        demand_id: int,
        action: str = Form(...),
        demand_status: str = Form("active"),
        csrf_token: str = Form(...),
    ) -> dict[str, Any]:
        _validate_csrf(request, csrf_token)
        if _is_admin_readonly_view(request):
            raise HTTPException(status_code=403, detail="Vista solo lectura")
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        if not update_web_demand_lifecycle(user.id, demand_id, action):
            raise HTTPException(status_code=400, detail="No se ha podido actualizar la demanda")
        data = get_dashboard_data(user.id, demand_status_filter=demand_status)
        return _to_json_ready(data)

    @app.post("/api/offers/{offer_id}/workspace-action")
    async def offer_workspace_action_api(
        request: Request,
        offer_id: int,
        action: str = Form(...),
        demand_status: str = Form("active"),
        offer_filter: str = Form("visible"),
        csrf_token: str = Form(...),
    ) -> dict[str, Any]:
        _validate_csrf(request, csrf_token)
        if _is_admin_readonly_view(request):
            raise HTTPException(status_code=403, detail="Vista solo lectura")
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        if not update_supplier_offer_workspace(user.id, offer_id, action):
            raise HTTPException(status_code=400, detail="No se ha podido actualizar la oferta")
        data = get_dashboard_data(user.id, demand_status_filter=demand_status, offer_filter=offer_filter)
        return _to_json_ready(data)

    @app.get("/demands/new", response_class=HTMLResponse)
    async def new_demand_page(request: Request) -> HTMLResponse:
        user = _get_current_user(request)
        if request.query_params.get("fresh") in {"1", "true", "yes"}:
            _clear_demand_wizard(request, user.id if user else None)
        wizard = _load_demand_wizard(request, user.id if user else None)
        active_wizard = _inflate_wizard_for_view(wizard) if wizard and wizard.get("mode") == "create" else None
        return _render(
            request,
            "new_demand.html",
            {
                "title": "Nueva demanda",
                "active_nav": "publish",
                "page_kind": "public",
                "wizard_mode": "create",
                "active_wizard": active_wizard,
                "submit_action": "/demands",
                "submit_label": "Analizar y continuar",
                "page_title": "Publica lo que necesitas",
                "page_note": "Escribe tu necesidad con libertad. Después podrás revisar el texto, la ubicación y el precio máximo si aplica.",
                "initial_text": active_wizard["state"].original_text if active_wizard else "",
                "edit_demand": None,
                "auth_stage": "guest" if not user else "authenticated",
            },
        )

    @app.get("/demands/{demand_id}/edit", response_class=HTMLResponse)
    async def edit_demand_page(request: Request, demand_id: int) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para editar una demanda.", "error")
            return _redirect("/login")
        demand = get_editable_demand(demand_id, user.id)
        if not demand:
            _flash(request, "Solo puedes editar tus demandas abiertas.", "error")
            return _redirect("/my-demands")
        wizard = _load_demand_wizard(request, user.id)
        active_wizard = _inflate_wizard_for_view(wizard) if wizard and wizard.get("mode") == "edit" and wizard.get("target_demand_id") == demand_id else None
        return _render(
            request,
            "new_demand.html",
            {
                "title": "Editar demanda",
                "active_nav": "publish",
                "page_kind": "app",
                "wizard_mode": "edit",
                "active_wizard": active_wizard,
                "submit_action": f"/demands/{demand_id}/edit",
                "submit_label": "Analizar y actualizar",
                "page_title": "Edita tu demanda",
                "page_note": "Revisa el texto, la ubicación opcional y el precio máximo opcional antes de guardar.",
                "initial_text": (active_wizard["state"].original_text if active_wizard else "") or demand.get("original_text") or demand.get("normalized_payload", {}).get("description", ""),
                "edit_demand": demand,
            },
        )

    @app.post("/demands")
    async def create_demand_route(
        request: Request,
        demand_text: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        original_text = str(demand_text or "").strip()
        demand_text = _normalize_text_limit(demand_text, max_length=DEMAND_TEXT_MAX_LENGTH)
        if len(demand_text) < DEMAND_TEXT_MIN_LENGTH:
            _flash(request, "Escribe tu demanda con un poco más de detalle.", "error")
            return _redirect("/demands/new")
        if len(original_text) > DEMAND_TEXT_MAX_LENGTH:
            _flash(request, f"La demanda no puede superar {DEMAND_TEXT_MAX_LENGTH} caracteres.", "error")
            return _redirect("/demands/new")
        draft = _analyze_lightweight_demand(agent.llm, demand_text)
        _save_demand_wizard(
            request,
            user.id if user else None,
            _simple_wizard_session(draft, wizard_mode="create", step="questions"),
        )
        return _redirect("/demands/new")

    @app.post("/demands/{demand_id}/edit")
    async def edit_demand_route(
        request: Request,
        demand_id: int,
        demand_text: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para editar una demanda.", "error")
            return _redirect("/login")
        if not get_editable_demand(demand_id, user.id):
            _flash(request, "Solo puedes editar tus demandas abiertas.", "error")
            return _redirect("/my-demands")
        original_text = str(demand_text or "").strip()
        demand_text = _normalize_text_limit(demand_text, max_length=DEMAND_TEXT_MAX_LENGTH)
        if len(demand_text) < DEMAND_TEXT_MIN_LENGTH:
            _flash(request, "Escribe tu demanda con un poco más de detalle.", "error")
            return _redirect(f"/demands/{demand_id}/edit")
        if len(original_text) > DEMAND_TEXT_MAX_LENGTH:
            _flash(request, f"La demanda no puede superar {DEMAND_TEXT_MAX_LENGTH} caracteres.", "error")
            return _redirect(f"/demands/{demand_id}/edit")
        draft = _analyze_lightweight_demand(agent.llm, demand_text)
        _save_demand_wizard(
            request,
            user.id,
            _simple_wizard_session(draft, wizard_mode="edit", step="questions", target_demand_id=demand_id),
        )
        return _redirect(f"/demands/{demand_id}/edit")

    @app.post("/demands/review")
    async def review_demand_route(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        user = _get_current_user(request)
        wizard = _load_demand_wizard(request, user.id if user else None)
        if not wizard:
            _flash(request, "No hay ninguna demanda pendiente de completar.", "error")
            return _redirect("/demands/new")

        if wizard.get("flow_version") == "simple_free_text":
            original_text = str(form.get("demand_text", "")).strip()
            demand_text = _normalize_text_limit(original_text, max_length=DEMAND_TEXT_MAX_LENGTH)
            if len(demand_text) < DEMAND_TEXT_MIN_LENGTH:
                _flash(request, "Escribe tu demanda con un poco más de detalle.", "error")
                return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))
            if len(original_text) > DEMAND_TEXT_MAX_LENGTH:
                _flash(request, f"La demanda no puede superar {DEMAND_TEXT_MAX_LENGTH} caracteres.", "error")
                return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))

            zone_payload = _parse_zone_json(str(form.get("location_zone_json", "")))
            budget_max_amount = _coerce_budget_amount(form.get("budget_max_amount"))
            budget_unit = _normalize_budget_unit(form.get("budget_unit"))
            overrides = {
                "location_json": zone_payload,
                "budget_max_amount": budget_max_amount,
                "budget_unit": budget_unit,
            }
            draft = _analyze_lightweight_demand(agent.llm, demand_text, overrides)
            state = SessionState(original_text=draft.raw_text, known_fields=draft.known_fields, summary=draft.summary)
            wizard_mode = wizard.get("mode", "create")
            target_demand_id = wizard.get("target_demand_id")

            if user is None:
                wizard_data = _simple_wizard_session(
                    draft,
                    wizard_mode=wizard_mode,
                    step="review",
                    target_demand_id=target_demand_id,
                    awaiting_auth=True,
                )
                _save_demand_wizard(request, None, wizard_data)
                active_wizard = _inflate_wizard_for_view(wizard_data)
                return _render(
                    request,
                    "new_demand.html",
                    {
                        "title": "Completa tu publicación",
                        "active_nav": "publish",
                        "page_kind": "public",
                        "wizard_mode": wizard_mode,
                        "active_wizard": active_wizard,
                        "submit_action": "/demands",
                        "submit_label": "Analizar y continuar",
                        "page_title": "Tu demanda está lista para publicarse",
                        "page_note": "",
                        "initial_text": demand_text,
                        "edit_demand": None,
                        "auth_stage": "guest",
                    },
                )

            if wizard_mode == "edit" and target_demand_id:
                persisted_demand = update_web_demand_from_agent(target_demand_id, user.id, draft, state)
                if not persisted_demand:
                    _flash(request, "No he podido actualizar la demanda. Comprueba que sigue activa.", "error")
                    return _redirect("/app/chats")
            else:
                persisted_demand = save_web_demand_from_agent(user.id, draft, state)

            _clear_demand_wizard(request, user.id)
            active_wizard = _inflate_wizard_for_view(
                _simple_wizard_session(
                    draft,
                    wizard_mode=wizard_mode,
                    step="review",
                    target_demand_id=target_demand_id,
                    published_message="Tu demanda ya está visible y lista para recibir respuestas.",
                )
            )
            return _render(
                request,
                "new_demand.html",
                {
                    "title": "Nueva demanda" if wizard_mode == "create" else "Editar demanda",
                    "active_nav": "publish",
                    "page_kind": "app",
                    "wizard_mode": wizard_mode,
                    "active_wizard": active_wizard,
                    "submit_action": "/demands",
                    "submit_label": "Analizar y continuar",
                    "page_title": "Publica lo que necesitas",
                    "page_note": "",
                    "initial_text": demand_text,
                    "edit_demand": persisted_demand if wizard_mode == "edit" else None,
                },
            )

        _clear_demand_wizard(request, user.id if user else None)
        _flash(request, "He descartado un borrador antiguo del asistente para mantener el nuevo flujo simple de publicación.", "error")
        return _redirect("/demands/new?fresh=1")

    @app.post("/demands/wizard/edit-text")
    async def demand_wizard_edit_text(
        request: Request,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        wizard = _load_demand_wizard(request, user.id if user else None)
        if not wizard:
            return _redirect("/demands/new")
        wizard["step"] = "text"
        _save_demand_wizard(request, user.id if user else None, wizard)
        return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))

    @app.post("/demands/wizard/back-to-questions")
    async def demand_wizard_back_to_questions(
        request: Request,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        wizard = _load_demand_wizard(request, user.id if user else None)
        if not wizard:
            return _redirect("/demands/new")
        wizard["step"] = "questions"
        _save_demand_wizard(request, user.id if user else None, wizard)
        return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))

    @app.get("/demands/{public_id}", response_class=HTMLResponse)
    async def demand_detail_page(request: Request, public_id: str, offer_id: int | None = None) -> HTMLResponse:
        actor_user = _get_current_user(request)
        current_user = _get_display_user(request, actor_user)
        demand = get_demand_detail(public_id, current_user.id if current_user else None)
        if not demand:
            raise HTTPException(status_code=404, detail="Demanda no encontrada")
        if demand.get("effective_status") != "open" and not (demand.get("is_owner") or demand.get("viewer_has_offer")):
            raise HTTPException(status_code=404, detail="Demanda no disponible")
        offers = get_offers_for_owner(demand["id"], current_user.id) if current_user and demand.get("is_owner") else []
        owner_selected_offer_id = None
        if offers:
            valid_offer_ids = {item.id for item in offers}
            if offer_id in valid_offer_ids:
                owner_selected_offer_id = offer_id
            else:
                owner_selected_offer_id = offers[0].id
        existing_thread = None
        if current_user and demand.get("viewer_has_offer") and demand.get("viewer_offer_id"):
            existing_thread = get_offer_thread(
                demand["viewer_offer_id"],
                actor_user.id if actor_user else current_user.id,
                participant_user_id=current_user.id,
                mark_read=not _is_admin_readonly_view(request, actor_user),
            )
        detail_entries = _demand_detail_answered_fields(demand)
        return _render(
            request,
            "demand_detail.html",
            {
                "title": demand["summary"],
                "active_nav": "explore",
                "page_kind": "public",
                "demand": demand,
                "offers": offers,
                "owner_selected_offer_id": owner_selected_offer_id,
                "detail_entries": detail_entries,
                "existing_thread": existing_thread,
            },
        )

    @app.post("/demands/{public_id}/offers")
    async def create_offer_route(
        request: Request,
        public_id: str,
        message: str = Form(...),
        redirect_to: str = Form(""),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        readonly_redirect = _reject_admin_readonly(request)
        if readonly_redirect:
            return readonly_redirect
        user = _get_current_user(request)
        demand_id = get_demand_id_by_public_id(public_id)
        target = redirect_to.strip() or (f"/demands/{public_id}" if public_id else "/")
        if not demand_id:
            _flash(request, "La demanda ya no existe.", "error")
            return _redirect("/")
        if not user:
            _flash(request, "Necesitas iniciar sesión para enviar una oferta.", "error")
            return _redirect("/login")

        message_raw = str(message or "").strip()
        message = _normalize_text_limit(message_raw, max_length=MESSAGE_TEXT_MAX_LENGTH)
        if len(message) < 12:
            _flash(request, "La oferta debe explicar qué ofreces con algo más de detalle.", "error")
            return _redirect(target)
        if len(message_raw) > MESSAGE_TEXT_MAX_LENGTH:
            _flash(request, f"El mensaje no puede superar {MESSAGE_TEXT_MAX_LENGTH} caracteres.", "error")
            return _redirect(target)

        try:
            offer = create_offer(
                demand_id=demand_id,
                supplier_user_id=user.id,
                message=message,
            )
        except ValueError as exc:
            _flash(request, str(exc), "error")
            return _redirect(target)

        _flash(request, "Tu oferta ha quedado registrada.", "success")
        return _redirect(f"/app/chats?role=supplier&offer_id={offer.id}")

    @app.get("/offers/{offer_id}", response_class=HTMLResponse)
    async def offer_thread_page(request: Request, offer_id: int) -> RedirectResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para ver esta conversación.", "error")
            return _redirect("/login")
        return _redirect(f"/app/chats?role=supplier&offer_id={offer_id}")

    @app.post("/offers/{offer_id}/messages")
    async def offer_message_route(
        request: Request,
        offer_id: int,
        body: str = Form(...),
        redirect_to: str = Form(""),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        readonly_redirect = _reject_admin_readonly(request)
        if readonly_redirect:
            return readonly_redirect
        user = _get_current_user(request)
        target = redirect_to.strip() or request.headers.get("referer") or f"/offers/{offer_id}"
        if not user:
            _flash(request, "Necesitas iniciar sesión para responder en la conversación.", "error")
            return _redirect("/login")
        body_raw = str(body or "").strip()
        body = _normalize_text_limit(body_raw, max_length=MESSAGE_TEXT_MAX_LENGTH)
        if not body:
            _flash(request, "El mensaje no puede estar vacío.", "error")
            return _redirect(target)
        if len(body_raw) > MESSAGE_TEXT_MAX_LENGTH:
            _flash(request, f"El mensaje no puede superar {MESSAGE_TEXT_MAX_LENGTH} caracteres.", "error")
            return _redirect(target)
        if not create_offer_message(offer_id, user.id, body):
            _flash(request, "No se ha podido enviar el mensaje.", "error")
            return _redirect("/app/chats")
        return _redirect(target)

    @app.post("/api/offers/{offer_id}/messages")
    async def offer_message_api(
        request: Request,
        offer_id: int,
        body: str = Form(...),
        csrf_token: str = Form(...),
    ) -> dict[str, Any]:
        _validate_csrf(request, csrf_token)
        if _is_admin_readonly_view(request):
            raise HTTPException(status_code=403, detail="Vista solo lectura")
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        body_raw = str(body or "").strip()
        body = _normalize_text_limit(body_raw, max_length=MESSAGE_TEXT_MAX_LENGTH)
        if not body:
            raise HTTPException(status_code=400, detail="Mensaje vacío")
        if len(body_raw) > MESSAGE_TEXT_MAX_LENGTH:
            raise HTTPException(status_code=400, detail=f"El mensaje no puede superar {MESSAGE_TEXT_MAX_LENGTH} caracteres")
        if not create_offer_message(offer_id, user.id, body):
            raise HTTPException(status_code=400, detail="No se ha podido enviar el mensaje")
        thread = get_offer_thread(offer_id, user.id)
        if not thread:
            raise HTTPException(status_code=404, detail="Conversación no encontrada")
        return {"thread": _to_json_ready(thread), "notifications": get_notification_summary(user.id)}

    @app.post("/demands/{public_id}/delete")
    async def delete_demand_route(
        request: Request,
        public_id: str,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        readonly_redirect = _reject_admin_readonly(request)
        if readonly_redirect:
            return readonly_redirect
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para borrar una demanda.", "error")
            return _redirect("/login")
        demand_id = get_demand_id_by_public_id(public_id)
        if not demand_id:
            _flash(request, "No he encontrado esa demanda.", "error")
            return _redirect("/app/chats")

        if delete_web_demand(demand_id, user.id):
            _flash(request, "La demanda se ha eliminado.", "success")
        else:
            _flash(request, "No he podido eliminar esa demanda.", "error")
        return _redirect("/my-demands")

    @app.post("/demands/{public_id}/lifecycle")
    async def demand_lifecycle_page_route(
        request: Request,
        public_id: str,
        action: str = Form(...),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        readonly_redirect = _reject_admin_readonly(request)
        if readonly_redirect:
            return readonly_redirect
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para actualizar una demanda.", "error")
            return _redirect("/login")
        demand_id = get_demand_id_by_public_id(public_id)
        if not demand_id:
            _flash(request, "No he encontrado esa demanda.", "error")
            return _redirect("/app/chats")

        normalized_action = (action or "").strip().lower()
        if not update_web_demand_lifecycle(user.id, demand_id, normalized_action):
            _flash(request, "No he podido actualizar esa demanda.", "error")
            return _redirect(f"/demands/{public_id}")

        if normalized_action == "delete":
            _flash(request, "La demanda se ha eliminado.", "success")
            return _redirect("/app/chats")

        if normalized_action == "pause":
            _flash(request, "La demanda ha quedado pausada.", "success")
        elif normalized_action == "reactivate":
            _flash(request, "La demanda ha vuelto a estar activa.", "success")
        elif normalized_action == "pin":
            _flash(request, "La demanda ha quedado fijada.", "success")
        elif normalized_action == "unpin":
            _flash(request, "La demanda ya no está fijada.", "success")

        return _redirect(f"/demands/{public_id}")

    @app.get("/auth/{provider}")
    async def auth_start(request: Request, provider: str) -> RedirectResponse:
        if not oauth or provider not in oauth:
            _flash(request, "Ese proveedor no está disponible todavía.", "error")
            return _redirect("/login")

        redirect_uri = request.url_for("auth_callback", provider=provider)
        client = oauth[provider]
        return await client.authorize_redirect(request, redirect_uri)

    @app.api_route("/auth/{provider}/callback", methods=["GET", "POST"], name="auth_callback")
    async def auth_callback(request: Request, provider: str) -> RedirectResponse:
        if not oauth or provider not in oauth:
            _flash(request, "Ese proveedor no está disponible todavía.", "error")
            return _redirect("/login")

        client = oauth[provider]
        token = await client.authorize_access_token(request)
        profile = await _fetch_social_profile(client, provider, token)
        email = profile["email"]
        full_name = profile["full_name"]

        user = get_or_create_oauth_user(
            provider=provider,
            provider_user_id=profile["provider_user_id"],
            email=email,
            full_name=full_name,
            avatar_url=profile.get("avatar_url"),
        )
        if not user.is_active:
            _flash(request, "Tu cuenta está desactivada. Contacta con administración si necesitas recuperarla.", "error")
            return _redirect("/login")
        request.session["user_id"] = user.id
        redirect_target = _complete_pending_guest_publication(request, user) or "/app/chats"
        return _redirect(redirect_target)

    return app


def _build_oauth_registry():
    if OAuth is None:
        return None

    oauth = OAuth()
    enabled = {}
    for provider, config in SOCIAL_PROVIDERS.items():
        if provider != "google":
            continue
        client_id = os.getenv(f"{provider.upper()}_CLIENT_ID")
        client_secret = os.getenv(f"{provider.upper()}_CLIENT_SECRET")
        if not client_id or not client_secret:
            continue

        kwargs = {
            "client_id": client_id,
            "client_secret": client_secret,
            "client_kwargs": {"scope": config["scope"]},
        }

        if config["kind"] == "oidc":
            kwargs["server_metadata_url"] = config["server_metadata_url"]
        else:
            kwargs["authorize_url"] = config["authorize_url"]
            kwargs["access_token_url"] = config["access_token_url"]
            kwargs["api_base_url"] = config["api_base_url"]

        if "client_kwargs" in config:
            kwargs["client_kwargs"] = {**kwargs["client_kwargs"], **config["client_kwargs"]}

        oauth.register(provider, **kwargs)
        enabled[provider] = oauth.create_client(provider)

    return enabled


async def _fetch_social_profile(client, provider: str, token: dict[str, Any]) -> dict[str, str]:
    if provider == "google":
        user_info = token.get("userinfo")
        if not user_info:
            user_info = await client.userinfo(token=token)
        return {
            "provider_user_id": str(user_info["sub"]),
            "email": user_info["email"],
            "full_name": user_info.get("name") or user_info["email"].split("@")[0],
            "avatar_url": user_info.get("picture"),
        }

    if provider == "github":
        profile = (await client.get("user", token=token)).json()
        emails = (await client.get("user/emails", token=token)).json()
        primary_email = next((item["email"] for item in emails if item.get("primary")), None)
        fallback_email = primary_email or f"github-{profile['id']}@users.dmander.local"
        return {
            "provider_user_id": str(profile["id"]),
            "email": fallback_email,
            "full_name": profile.get("name") or profile.get("login") or fallback_email.split("@")[0],
            "avatar_url": profile.get("avatar_url"),
        }

    if provider == "meta":
        profile = (
            await client.get(
                "me?fields=id,name,email,picture.type(large)",
                token=token,
            )
        ).json()
        fallback_email = profile.get("email") or f"meta-{profile['id']}@users.dmander.local"
        picture = profile.get("picture", {}).get("data", {}).get("url")
        return {
            "provider_user_id": str(profile["id"]),
            "email": fallback_email,
            "full_name": profile.get("name") or fallback_email.split("@")[0],
            "avatar_url": picture,
        }

    if provider == "x":
        profile = (await client.get("users/me", token=token)).json().get("data", {})
        synthetic_email = f"x-{profile['id']}@users.dmander.local"
        return {
            "provider_user_id": str(profile["id"]),
            "email": synthetic_email,
            "full_name": profile.get("name") or profile.get("username") or synthetic_email.split("@")[0],
            "avatar_url": None,
        }

    if provider == "apple":
        user_info = token.get("userinfo", {})
        provider_user_id = str(user_info.get("sub") or token.get("id_token", "apple-user"))
        email = user_info.get("email") or f"apple-{provider_user_id[:18]}@users.dmander.local"
        full_name = user_info.get("name") or email.split("@")[0]
        return {
            "provider_user_id": provider_user_id,
            "email": email,
            "full_name": full_name,
            "avatar_url": None,
        }

    raise HTTPException(status_code=400, detail="Proveedor OAuth no soportado")


def _require_api_user(request: Request):
    authorization = str(request.headers.get("authorization") or "").strip()
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Debes enviar Authorization: Bearer <api_token>",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Token de API vacío",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user = authenticate_api_token(token)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Token de API inválido o revocado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def _store_created_api_token(request: Request, name: str, plain_token: str) -> None:
    request.session["_created_api_token"] = {"name": name, "token": plain_token}


def _pop_created_api_token(request: Request) -> dict[str, str] | None:
    payload = request.session.pop("_created_api_token", None)
    if not isinstance(payload, dict):
        return None
    token = str(payload.get("token") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not token:
        return None
    return {"name": name, "token": token}


def _get_current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    user = get_user_by_id(user_id)
    if not user or not user.is_active:
        request.session.pop("user_id", None)
        return None
    return user


def _get_admin_view_user(request: Request, actor_user: Any | None = None):
    actor = actor_user or _get_current_user(request)
    if not _is_superadmin(actor):
        request.session.pop("admin_view_user_id", None)
        return None
    view_user_id = request.session.get("admin_view_user_id")
    if not view_user_id:
        return None
    if actor and int(view_user_id) == int(actor.id):
        request.session.pop("admin_view_user_id", None)
        return None
    return get_user_by_id(int(view_user_id))


def _get_display_user(request: Request, actor_user: Any | None = None):
    actor = actor_user or _get_current_user(request)
    return _get_admin_view_user(request, actor) or actor


def _is_admin_readonly_view(request: Request, actor_user: Any | None = None) -> bool:
    actor = actor_user or _get_current_user(request)
    view_user = _get_admin_view_user(request, actor)
    return bool(actor and view_user and int(view_user.id) != int(actor.id))


def _reject_admin_readonly(request: Request) -> RedirectResponse | None:
    if _is_admin_readonly_view(request):
        _flash(request, "Estás navegando como otro usuario en modo solo lectura.", "error")
        return _redirect(str(request.headers.get("referer") or "/"))
    return None


def _flash(request: Request, message: str, category: str = "info") -> None:
    flashes = request.session.get("_flashes", [])
    flashes.append({"message": message, "category": category})
    request.session["_flashes"] = flashes


def _pop_flashes(request: Request) -> list[dict[str, str]]:
    return request.session.pop("_flashes", [])


def _get_csrf_token(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(24)
        request.session["csrf_token"] = token
    return token


def _validate_csrf(request: Request, csrf_token: str) -> None:
    stored = request.session.get("csrf_token")
    if not stored or stored != csrf_token:
        raise HTTPException(status_code=400, detail="CSRF token inválido")


def _parse_zone_json(raw: str) -> Optional[dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return normalize_zone_payload(payload)


def _zone_signature(zone: Optional[dict[str, Any]]) -> str:
    parsed = compact_zone_for_transport(zone or {}) or {}
    if not (
        (
            parsed.get("center", {}).get("lat") is not None
            and parsed.get("center", {}).get("lon") is not None
        )
        or parsed.get("label")
    ):
        return ""
    return json.dumps(parsed, sort_keys=True, ensure_ascii=False)


def _search_matches_saved_filter(
    saved_filter: Optional[dict[str, Any]],
    *,
    q: str,
    location: str,
    zone_filter: Optional[dict[str, Any]],
) -> bool:
    if not saved_filter:
        return False
    return (
        (q or "").strip() == (saved_filter.get("query_text") or "").strip()
        and (location or "").strip() == (saved_filter.get("location") or "").strip()
        and _zone_signature(zone_filter) == _zone_signature(saved_filter.get("location_json"))
    )


def _nominatim_headers() -> dict[str, str]:
    return {
        "User-Agent": os.getenv("GEOCODER_USER_AGENT", "DMANDER/1.0 (local prototype)"),
        "Accept": "application/json",
    }


def _nominatim_search(query: str, countrycodes: str = "") -> list[dict[str, Any]]:
    payload = {"q": query, "format": "jsonv2", "limit": 6, "addressdetails": 1, "polygon_geojson": 1}
    if countrycodes:
        payload["countrycodes"] = countrycodes
    params = urlencode(payload)
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    request = UrlRequest(url, headers=_nominatim_headers())
    try:
        with urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    items: list[dict[str, Any]] = []
    for item in payload:
        try:
            display_name = item.get("display_name") or query
            lat = float(item["lat"])
            lon = float(item["lon"])
            bbox = _extract_nominatim_bbox(item.get("boundingbox"))
            mode = _nominatim_result_mode(item)
            items.append(
                {
                    "label": _nominatim_result_label(item, query),
                    "full_label": display_name,
                    "lat": lat,
                    "lon": lon,
                    "raw_query": query,
                    "source": "autocomplete",
                    "mode": mode,
                    "admin_level": _nominatim_admin_level(item),
                    "bbox": bbox,
                    "geojson": item.get("geojson") if mode == "area" and isinstance(item.get("geojson"), dict) else None,
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return items


def _nominatim_reverse(lat: float, lon: float) -> dict[str, Any]:
    params = urlencode({"lat": lat, "lon": lon, "format": "jsonv2"})
    url = f"https://nominatim.openstreetmap.org/reverse?{params}"
    request = UrlRequest(url, headers=_nominatim_headers())
    try:
        with urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        payload = {}
    return {
        "label": _nominatim_result_label(payload, "Punto seleccionado en el mapa") if payload else "Punto seleccionado en el mapa",
        "lat": lat,
        "lon": lon,
        "source": "map_click",
        "mode": "radius_from_point",
    }


def _guess_zone_from_text(raw_value: Any, original_text: str = "") -> dict[str, Any] | None:
    query = str(raw_value or "").strip()
    if not query:
        return None
    if any(char.isdigit() for char in query) and "," not in query and len(query) < 5:
        return None

    preferred_country = "es" if _prefer_spain_for_location(query, original_text) else ""
    candidates: list[dict[str, Any]] = []
    if preferred_country:
        candidates.extend(_nominatim_search(query, countrycodes=preferred_country))
    candidates.extend(_nominatim_search(query))
    best = _best_geocode_candidate(query, candidates, preferred_country)
    if not best:
        return None
    return normalize_zone_payload(best)


def _prefer_spain_for_location(query: str, original_text: str) -> bool:
    haystack = f"{query} {original_text}".lower()
    return any(token in haystack for token in ("españa", "espanya", "spain", "barcelona", "madrid", "catalunya", "cataluña"))


def _best_geocode_candidate(query: str, candidates: list[dict[str, Any]], preferred_country: str = "") -> dict[str, Any] | None:
    if not candidates:
        return None
    query_lower = query.strip().lower()
    normalized_query = _normalize_geo_name(query)
    admin_query = _is_spanish_admin_query(query)

    def score(item: dict[str, Any]) -> tuple[int, int, int, int, int, int]:
        label = str(item.get("label") or "").lower()
        full_label = str(item.get("full_label") or item.get("label") or "").lower()
        first_part = label.split(",")[0].strip()
        normalized_first = _normalize_geo_name(first_part)
        exact_first = 1 if first_part == query_lower else 0
        starts = 1 if first_part.startswith(query_lower) else 0
        preferred = 1 if preferred_country == "es" and ("españa" in full_label or "spain" in full_label) else 0
        area_with_geo = 1 if item.get("mode") == "area" and isinstance(item.get("geojson"), dict) else 0
        area_mode = 1 if item.get("mode") == "area" else 0
        admin_name_match = 1 if admin_query and normalized_first == normalized_query else 0
        return (admin_name_match, area_with_geo, area_mode, preferred, exact_first, starts)

    return sorted(candidates, key=score, reverse=True)[0]


def _normalize_geo_name(value: str) -> str:
    replacements = str(value).strip().lower().translate(
        str.maketrans(
            {
                "á": "a",
                "é": "e",
                "í": "i",
                "ó": "o",
                "ú": "u",
                "à": "a",
                "è": "e",
                "ì": "i",
                "ò": "o",
                "ù": "u",
                "ï": "i",
                "ü": "u",
            }
        )
    )
    return " ".join(replacements.replace("-", " ").split())


def _is_spanish_admin_query(query: str) -> bool:
    normalized = _normalize_geo_name(query)
    if not normalized:
        return False
    if normalized.startswith(("provincia de ", "comunidad de ", "comunitat ", "comunidad foral de ")):
        return True
    if normalized in _spain_admin_area_aliases():
        return True
    return False


def _spain_admin_area_aliases() -> set[str]:
    return {
        "espana",
        "spain",
        "catalunya",
        "cataluna",
        "catalonia",
        "andalucia",
        "aragon",
        "asturias",
        "principado de asturias",
        "illes balears",
        "islas baleares",
        "canarias",
        "cantabria",
        "castilla la mancha",
        "castilla y leon",
        "cataluna",
        "comunidad de madrid",
        "madrid",
        "comunidad valenciana",
        "comunitat valenciana",
        "extremadura",
        "galicia",
        "la rioja",
        "navarra",
        "comunidad foral de navarra",
        "murcia",
        "region de murcia",
        "pais vasco",
        "euskadi",
        "ceuta",
        "melilla",
        "a coruna",
        "alava",
        "araba",
        "albacete",
        "alicante",
        "almeria",
        "asturias",
        "avila",
        "badajoz",
        "barcelona",
        "burgos",
        "caceres",
        "cadiz",
        "castellon",
        "ciudad real",
        "cordoba",
        "cuenca",
        "girona",
        "gerona",
        "granada",
        "guadalajara",
        "gipuzkoa",
        "guipuzcoa",
        "huelva",
        "huesca",
        "jaen",
        "leon",
        "lleida",
        "lerida",
        "lugo",
        "malaga",
        "murcia",
        "ourense",
        "orense",
        "palencia",
        "pontevedra",
        "la rioja",
        "salamanca",
        "segovia",
        "sevilla",
        "soria",
        "tarragona",
        "teruel",
        "toledo",
        "valencia",
        "valladolid",
        "bizkaia",
        "vizcaya",
        "zamora",
        "zaragoza",
    }


def _extract_nominatim_bbox(raw_bbox: Any) -> list[float] | None:
    if not isinstance(raw_bbox, list) or len(raw_bbox) != 4:
        return None
    try:
        south = float(raw_bbox[0])
        north = float(raw_bbox[1])
        west = float(raw_bbox[2])
        east = float(raw_bbox[3])
        return [west, south, east, north]
    except (TypeError, ValueError):
        return None


def _nominatim_result_mode(item: dict[str, Any]) -> str:
    geometry = item.get("geojson")
    item_class = str(item.get("class") or "").lower()
    item_type = str(item.get("type") or "").lower()
    addresstype = str(item.get("addresstype") or "").lower()
    area_types = {
        "country",
        "state",
        "province",
        "county",
        "municipality",
        "city",
        "town",
        "village",
        "suburb",
        "hamlet",
        "region",
        "neighbourhood",
        "boundary",
    }
    if isinstance(geometry, dict) and (item_class == "boundary" or item_type in area_types or addresstype in area_types):
        return "area"
    return "radius_from_point"


def _nominatim_admin_level(item: dict[str, Any]) -> str:
    item_type = str(item.get("type") or item.get("addresstype") or "").strip().lower()
    mapping = {
        "country": "country",
        "state": "autonomous_community",
        "province": "province",
        "county": "county",
        "municipality": "municipality",
        "city": "city",
        "town": "city",
        "village": "village",
        "hamlet": "hamlet",
        "suburb": "district",
        "neighbourhood": "district",
    }
    return mapping.get(item_type, item_type)


def _nominatim_result_label(item: dict[str, Any], fallback_query: str) -> str:
    address = item.get("address") or {}
    locality_parts = [
        address.get("suburb") or address.get("neighbourhood") or address.get("quarter") or address.get("village"),
        address.get("town") or address.get("city") or address.get("municipality"),
        address.get("county"),
        address.get("state"),
    ]
    parts = [str(part).strip() for part in locality_parts if part]
    if parts:
        deduped: list[str] = []
        for part in parts:
            if part not in deduped:
                deduped.append(part)
        return ", ".join(deduped[:3])
    return compact_zone_label(item.get("display_name"), fallback_query) or fallback_query


def _enabled_social_providers() -> list[dict[str, str]]:
    providers = []
    for provider, config in SOCIAL_PROVIDERS.items():
        if provider != "google":
            continue
        if os.getenv(f"{provider.upper()}_CLIENT_ID") and os.getenv(f"{provider.upper()}_CLIENT_SECRET"):
            providers.append({"key": provider, "label": config["label"], "icon": config["icon"]})
    return providers


def _app_base_url(request: Request | None = None) -> str:
    configured = os.getenv("APP_BASE_URL", "").strip()
    if configured:
        return configured.rstrip("/")
    if request is not None:
        return str(request.base_url).rstrip("/")
    return "http://127.0.0.1:8000"


def _safe_internal_redirect_path(value: str, default: str = "/app/chats") -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return default
    if not candidate.startswith("/"):
        return default
    if candidate.startswith("//"):
        return default
    return candidate


def _demand_public_path(demand: Any | None) -> str:
    if not demand:
        return "/"
    public_id = str(getattr(demand, "public_id", None) or (demand.get("public_id") if isinstance(demand, dict) else "") or "").strip()
    if public_id:
        return f"/demands/{public_id}"
    demand_id = getattr(demand, "id", None) if not isinstance(demand, dict) else demand.get("id")
    return f"/demands/{demand_id}" if demand_id else "/"


def _send_email_message(to_email: str, subject: str, body_text: str) -> bool:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    smtp_from = os.getenv("SMTP_FROM", smtp_user or "doselevadoatres@gmail.com").strip()

    if not smtp_user or not smtp_password:
        logger.warning("SMTP no configurado; no se ha podido enviar email a %s", to_email)
        return False

    message = EmailMessage()
    message["From"] = smtp_from
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body_text)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as smtp:
        smtp.starttls()
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(message)
    return True


def _mask_email_hint(email: str) -> str:
    local, _, domain = (email or "").partition("@")
    if not local or not domain:
        return email
    visible_local = local[:2]
    masked_local = visible_local + "•" * max(1, len(local) - len(visible_local))
    domain_name, dot, domain_tld = domain.partition(".")
    visible_domain = (domain_name[:1] + "•" * max(1, len(domain_name) - 1)) if domain_name else domain
    return f"{masked_local}@{visible_domain}{dot}{domain_tld}"


def _is_superadmin(user: Any | None) -> bool:
    return bool(user and getattr(user, "role", "") == "superadmin" and getattr(user, "is_active", False))


def _require_superadmin(request: Request) -> Any | None:
    user = _get_current_user(request)
    if not _is_superadmin(user):
        return None
    return user


def _require_superadmin_redirect(request: Request) -> RedirectResponse | None:
    user = _get_current_user(request)
    if not user:
        _flash(request, "Necesitas iniciar sesión para acceder al área de administración.", "error")
        return _redirect("/login")
    if not _is_superadmin(user):
        _flash(request, "Solo un superadministrador puede acceder a /admin.", "error")
        return _redirect("/")
    return None


def _render(request: Request, template_name: str, context: dict[str, Any]) -> HTMLResponse:
    actor_user = _get_current_user(request)
    current_user = _get_display_user(request, actor_user)
    admin_view_target = _get_admin_view_user(request, actor_user)
    admin_view_active = bool(actor_user and admin_view_target and actor_user.id != admin_view_target.id)
    wizard = _load_demand_wizard(request, current_user.id) if current_user and not admin_view_active else None
    if wizard:
        wizard = _inflate_wizard_for_view(wizard)
    notification_summary = (
        get_notification_summary(current_user.id) if current_user and not admin_view_active else {"my_demands_unread": 0, "my_offers_unread": 0, "items": []}
    )
    response = templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "current_user": current_user,
            "actor_user": actor_user,
            "flash_messages": _pop_flashes(request),
            "csrf_token": _get_csrf_token(request),
            "oauth_providers": _enabled_social_providers(),
            "demand_wizard": wizard,
            "debug_normalization": _normalization_debug_enabled(),
            "notification_summary": notification_summary,
            "admin_view_active": admin_view_active,
            "admin_view_target": admin_view_target,
            "admin_view_users": list_admin_users() if _is_superadmin(actor_user) else [],
            "demand_budget_display": _demand_budget_display,
            "page_kind": context.get("page_kind", "public"),
            "active_nav": context.get("active_nav", ""),
            **context,
        },
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _format_eur_amount(value: Any) -> str:
    if value in {None, ""}:
        return ""
    try:
        amount = round(float(value), 2)
    except (TypeError, ValueError):
        return ""
    if amount.is_integer():
        whole = f"{int(amount):,}".replace(",", ".")
        return f"{whole} €"
    formatted = f"{amount:,.2f}"
    formatted = formatted.replace(",", "_").replace(".", ",").replace("_", ".")
    return f"{formatted} €"


def _item_value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _budget_unit_marketplace_label(intent_type: str) -> str:
    return BUDGET_UNIT_LABELS.get(_normalize_budget_unit(intent_type), "")


def _demand_budget_display(demand: Any) -> dict[str, Any]:
    min_value = _item_value(demand, "budget_min")
    max_value = _item_value(demand, "budget_max")
    has_min = min_value is not None
    has_max = max_value is not None
    unit_label = BUDGET_UNIT_LABELS.get(_normalize_budget_unit(_item_value(demand, "budget_unit", "")), "")
    unit_suffix = f" · {unit_label}" if unit_label else ""
    if has_min and has_max:
        try:
            min_amount = float(min_value)
            max_amount = float(max_value)
        except (TypeError, ValueError):
            min_amount = None
            max_amount = None
        if min_amount is not None and max_amount is not None and max_amount < min_amount:
            min_amount, max_amount = max_amount, min_amount
        if min_amount is not None and max_amount is not None and abs(min_amount - max_amount) < 0.005:
            return {
                "main": _format_eur_amount(max_amount),
                "meta": f"Precio orientativo{unit_suffix}",
                "has_price": True,
            }
        return {
            "main": f"{_format_eur_amount(min_amount)} - {_format_eur_amount(max_amount)}",
            "meta": f"Rango de precios{unit_suffix}",
            "has_price": True,
        }
    if has_max:
        return {
            "main": f"Hasta {_format_eur_amount(max_value)}",
            "meta": f"Precio máximo{unit_suffix}",
            "has_price": True,
        }
    if has_min:
        return {
            "main": f"Desde {_format_eur_amount(min_value)}",
            "meta": f"Presupuesto orientativo{unit_suffix}",
            "has_price": True,
        }
    return {"main": "Precio a concretar", "meta": "", "has_price": False}


def _keyword_suggestions() -> list[str]:
    return [
        "clases particulares",
        "cuidado de niños",
        "hotel",
        "fontanero",
        "electricista",
        "coche usado",
        "seguro",
        "mudanza",
    ]


def _normalize_string_list(values: Optional[list[str]]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def _parse_json_string_list(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return _normalize_string_list([str(item) for item in payload if isinstance(item, str | int | float)])


def _form_multilist(form: Any, key: str) -> list[str]:
    try:
        values = form.getlist(key)
    except AttributeError:
        value = form.get(key)
        values = value if isinstance(value, list) else [value]
    return _normalize_string_list([str(item) for item in values if item is not None])


def _form_text_list(form: Any, key: str) -> list[str]:
    try:
        values = form.getlist(key)
    except AttributeError:
        value = form.get(key)
        values = value if isinstance(value, list) else [value]
    return [str(item or "") for item in values]


def _field_specs_from_form(form: Any, prefix: str) -> list[dict[str, Any]]:
    names = _form_text_list(form, f"{prefix}_field_name")
    labels = _form_text_list(form, f"{prefix}_field_label_es")
    value_types = _form_text_list(form, f"{prefix}_field_value_type")
    required_flags = _form_text_list(form, f"{prefix}_field_required")
    when_fields = _form_text_list(form, f"{prefix}_field_when_field")
    when_operators = _form_text_list(form, f"{prefix}_field_when_operator")
    when_values = _form_text_list(form, f"{prefix}_field_when_value")
    choices_texts = _form_text_list(form, f"{prefix}_field_choices_text")
    min_values = _form_text_list(form, f"{prefix}_field_min_value")
    max_values = _form_text_list(form, f"{prefix}_field_max_value")
    min_lengths = _form_text_list(form, f"{prefix}_field_min_length")
    max_lengths = _form_text_list(form, f"{prefix}_field_max_length")
    min_dates = _form_text_list(form, f"{prefix}_field_min_date")
    max_dates = _form_text_list(form, f"{prefix}_field_max_date")
    min_times = _form_text_list(form, f"{prefix}_field_min_time")
    max_times = _form_text_list(form, f"{prefix}_field_max_time")
    min_datetimes = _form_text_list(form, f"{prefix}_field_min_datetime")
    max_datetimes = _form_text_list(form, f"{prefix}_field_max_datetime")
    budget_fix_or_ranges = _form_text_list(form, f"{prefix}_field_budget_fix_or_range")
    budget_units = _form_text_list(form, f"{prefix}_field_budget_unit")
    removable_flags = _form_text_list(form, f"{prefix}_field_removable")
    system_flags = _form_text_list(form, f"{prefix}_field_system")
    total = max(
        [
            len(names),
            len(labels),
            len(value_types),
            len(choices_texts),
            len(min_values),
            len(max_values),
            len(min_lengths),
            len(max_lengths),
            len(min_dates),
            len(max_dates),
            len(min_times),
            len(max_times),
            len(min_datetimes),
            len(max_datetimes),
            len(when_fields),
            len(when_operators),
            len(when_values),
            len(budget_fix_or_ranges),
            len(budget_units),
            len(removable_flags),
            len(system_flags),
        ],
        default=0,
    )
    specs: list[dict[str, Any]] = []
    for index in range(total):
        field_name = (names[index] if index < len(names) else "").strip()
        if not field_name:
            continue
        specs.append(
            {
                "name": field_name,
                "label_es": (labels[index] if index < len(labels) else "").strip(),
                "value_type": (value_types[index] if index < len(value_types) else "").strip(),
                "required": str(required_flags[index] if index < len(required_flags) else "").strip() or "never",
                "when_field": str(when_fields[index] if index < len(when_fields) else "").strip(),
                "when_operator": str(when_operators[index] if index < len(when_operators) else "").strip() or "equals",
                "when_value": str(when_values[index] if index < len(when_values) else "").strip(),
                "choices_text": choices_texts[index] if index < len(choices_texts) else "",
                "min_value": min_values[index] if index < len(min_values) else "",
                "max_value": max_values[index] if index < len(max_values) else "",
                "min_length": min_lengths[index] if index < len(min_lengths) else "",
                "max_length": max_lengths[index] if index < len(max_lengths) else "",
                "min_date": min_dates[index] if index < len(min_dates) else "",
                "max_date": max_dates[index] if index < len(max_dates) else "",
                "min_time": min_times[index] if index < len(min_times) else "",
                "max_time": max_times[index] if index < len(max_times) else "",
                "min_datetime": min_datetimes[index] if index < len(min_datetimes) else "",
                "max_datetime": max_datetimes[index] if index < len(max_datetimes) else "",
                "budget_fix_or_range": budget_fix_or_ranges[index] if index < len(budget_fix_or_ranges) else "",
                "budget_unit": budget_units[index] if index < len(budget_units) else "",
                "removable": str(removable_flags[index] if index < len(removable_flags) else "").strip() in {"1", "true", "on", "yes"},
                "system": str(system_flags[index] if index < len(system_flags) else "").strip() in {"1", "true", "on", "yes"},
            }
        )
    return specs


def _intent_payload_from_form(form: Any) -> dict[str, Any]:
    examples = [line.strip() for line in str(form.get("examples_text", "")).splitlines() if line.strip()]
    field_specs = _field_specs_from_form(form, "field")

    def _spec_to_field(item: dict[str, Any]) -> dict[str, Any]:
        required_mode = str(item.get("required") or "never").strip()
        validation: dict[str, Any] = {}
        choices = [line.strip() for line in str(item["choices_text"] or "").splitlines() if line.strip()]
        options: list[str] = []
        for choice in choices:
            if "|" in choice:
                _, label = choice.split("|", 1)
                options.append(label.strip() or choice.split("|", 1)[0].strip())
            else:
                options.append(choice)
        if options:
            validation["options"] = options
            validation["allow_custom"] = True
        for source, target in (
            ("min_value", "min"),
            ("max_value", "max"),
            ("min_length", "min_length"),
            ("max_length", "max_length"),
            ("min_date", "min_date"),
            ("max_date", "max_date"),
            ("min_time", "min_time"),
            ("max_time", "max_time"),
            ("min_datetime", "min_datetime"),
            ("max_datetime", "max_datetime"),
        ):
            raw = str(item.get(source, "") or "").strip()
            if raw:
                validation[target] = raw
        if item["value_type"] == "Texto":
            validation.setdefault("min_length", "1" if required_mode == "always" else "0")
            validation.setdefault("max_length", "300")
        payload = {
            "name": item["name"],
            "type": item["value_type"] or "Texto",
            "description": item["label_es"] or item["name"],
            "required": required_mode,
            "validation": validation,
        }
        if required_mode == "conditional" and item.get("when_field") and item.get("when_value"):
            when_value = str(item.get("when_value") or "").strip()
            when_operator = str(item.get("when_operator") or "equals").strip() or "equals"
            payload["when"] = {
                "field": str(item.get("when_field") or "").strip(),
                "operator": when_operator,
                "value": [part.strip() for part in when_value.split(",") if part.strip()] if when_operator == "in" else when_value,
            }
        return payload

    fields: list[dict[str, Any]] = []
    for item in field_specs:
        field_name = item.get("name")
        if field_name == "_location":
            fields.append(
                {
                    "name": "_location",
                    "type": "System Location",
                    "description": item.get("label_es") or "Ubicación de la demanda",
                    "required": str(item.get("required") or "never").strip() or "never",
                    **(
                        {
                            "when": {
                                "field": str(item.get("when_field") or "").strip(),
                                "operator": str(item.get("when_operator") or "equals").strip() or "equals",
                                "value": [part.strip() for part in str(item.get("when_value") or "").split(",") if part.strip()]
                                if str(item.get("when_operator") or "equals").strip() == "in"
                                else str(item.get("when_value") or "").strip(),
                            }
                        }
                        if str(item.get("required") or "never").strip() == "conditional"
                        and str(item.get("when_field") or "").strip()
                        and str(item.get("when_value") or "").strip()
                        else {}
                    ),
                    "validation": {},
                }
            )
            continue
        if field_name == "_budget":
            fields.append(
                {
                    "name": "_budget",
                    "type": "System Budget",
                    "description": item.get("label_es") or "Presupuesto de la demanda",
                    "required": str(item.get("required") or "never").strip() or "never",
                    **(
                        {
                            "when": {
                                "field": str(item.get("when_field") or "").strip(),
                                "operator": str(item.get("when_operator") or "equals").strip() or "equals",
                                "value": [part.strip() for part in str(item.get("when_value") or "").split(",") if part.strip()]
                                if str(item.get("when_operator") or "equals").strip() == "in"
                                else str(item.get("when_value") or "").strip(),
                            }
                        }
                        if str(item.get("required") or "never").strip() == "conditional"
                        and str(item.get("when_field") or "").strip()
                        and str(item.get("when_value") or "").strip()
                        else {}
                    ),
                    "fix_or_range": str(item.get("budget_fix_or_range") or "").strip(),
                    "unit": str(item.get("budget_unit") or "").strip(),
                    "validation": {
                        "min": str(item.get("min_value") or "").strip(),
                        "max": str(item.get("max_value") or "").strip(),
                    },
                }
            )
            continue
        fields.append(_spec_to_field(item))
    return {
        "intent_domain": str(form.get("intent_domain", "")).strip(),
        "intent_type": str(form.get("intent_type", "")).strip(),
        "display_name": str(form.get("display_name", "")).strip(),
        "fields": fields,
        "examples": examples,
    }


def _redirect(path: str) -> RedirectResponse:
    return RedirectResponse(path, status_code=status.HTTP_303_SEE_OTHER)


def _to_float(raw: str) -> Optional[float]:
    raw = raw.strip()
    if not raw:
        return None
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _to_json_ready(value.model_dump(mode="json"))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_to_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_json_ready(item) for key, item in value.items()}
    return value


def _normalize_budget_unit(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "": "total",
        "total": "total",
        "one-time": "total",
        "one_time": "total",
        "overall": "total",
        "hour": "hour",
        "per hour": "hour",
        "hora": "hour",
        "por hora": "hour",
        "night": "night",
        "per night": "night",
        "noche": "night",
        "por noche": "night",
        "day": "day",
        "per day": "day",
        "dia": "day",
        "día": "day",
        "por dia": "day",
        "por día": "day",
        "month": "month",
        "per month": "month",
        "mes": "month",
        "al mes": "month",
        "item": "item",
        "product": "item",
        "producto": "item",
        "por producto": "item",
        "service": "service",
        "servicio": "service",
        "por servicio": "service",
    }
    return aliases.get(normalized, "total")


def _coerce_budget_amount(value: Any) -> float | None:
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        return None
    try:
        amount = round(float(raw), 2)
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def _dedupe_text_items(values: list[Any], limit: int = 4) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        items.append(text)
        seen.add(key)
        if len(items) >= limit:
            break
    return items


def _is_structured_optional_suggestion(text: str) -> bool:
    normalized = (
        str(text or "")
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    blocked_tokens = (
        "ubicacion",
        "ciudad",
        "zona",
        "provincia",
        "municipio",
        "localizacion",
        "direccion",
        "presupuesto",
        "precio",
        "importe",
        "coste",
        "costo",
        "budget",
        "amount",
        "location",
        "price",
    )
    return any(token in normalized for token in blocked_tokens)


def _filter_suggested_missing_details(values: list[Any], limit: int = 4) -> list[str]:
    filtered = [value for value in values if not _is_structured_optional_suggestion(str(value or ""))]
    return _dedupe_text_items(filtered, limit=limit)


def _fallback_demand_summary(raw_text: str) -> str:
    text = " ".join(str(raw_text or "").strip().split())
    if not text:
        return ""
    compact = text.rstrip(".")
    if len(compact) <= 96:
        return compact[:1].upper() + compact[1:] + "."
    shortened = compact[:93].rstrip(" ,;:")
    return shortened[:1].upper() + shortened[1:] + "..."


def _normalize_text_limit(value: Any, *, max_length: int) -> str:
    text = str(value or "").strip()
    return text[:max_length].strip()


def _normalize_lightweight_known_fields(raw_text: str, known_fields: dict[str, Any] | None = None) -> dict[str, Any]:
    data = dict(known_fields or {})
    zone_payload = None
    for key in ("location_json", "location_zone"):
        current = data.get(key)
        if isinstance(current, dict):
            zone_payload = normalize_zone_payload(current)
            if zone_payload:
                break
    if not zone_payload:
        location_text = str(
            data.get("location")
            or data.get("location_value")
            or ""
        ).strip()
        if location_text:
            zone_payload = _guess_zone_from_text(location_text, raw_text)
    if zone_payload:
        data["location_json"] = zone_payload
        data["location"] = zone_payload.get("label", "")
        data["location_value"] = zone_payload.get("label", "")
    budget_amount = _coerce_budget_amount(
        data.get("budget_max_amount", data.get("budget_max"))
    )
    if budget_amount is not None:
        data["budget_max"] = budget_amount
        data["budget_max_amount"] = budget_amount
    if str(data.get("budget_unit") or "").strip():
        data["budget_unit"] = _normalize_budget_unit(data.get("budget_unit"))
    return data


def _analyze_lightweight_demand(llm_client: Any, raw_text: str, known_fields: dict[str, Any] | None = None) -> DemandResult:
    original_text = " ".join(str(raw_text or "").strip().split())
    normalized_known = _normalize_lightweight_known_fields(original_text, known_fields)
    analysis_data: dict[str, Any] = {}
    if original_text:
        try:
            analysis_data = parse_json_response(
                llm_client.analyze(
                    LIGHTWEIGHT_DEMAND_SYSTEM_PROMPT,
                    json.dumps(
                        {
                            "text": original_text,
                            "known_fields": {
                                "location": normalized_known.get("location") or "",
                                "budget_max_amount": normalized_known.get("budget_max_amount"),
                                "budget_unit": normalized_known.get("budget_unit"),
                            },
                        },
                        ensure_ascii=False,
                    ),
                )
            )
        except Exception as exc:
            logger.warning("No he podido obtener análisis ligero del LLM: %s", exc)
            analysis_data = {}

    analysis = LightweightDemandAnalysis.model_validate(analysis_data or {})
    zone_payload = normalized_known.get("location_json")
    if not isinstance(zone_payload, dict) or not zone_payload.get("center"):
        hinted_location = str(analysis.location_hint or "").strip()
        if hinted_location:
            zone_payload = _guess_zone_from_text(hinted_location, original_text)
    zone_payload = normalize_zone_payload(zone_payload) if isinstance(zone_payload, dict) else None

    budget_amount = _coerce_budget_amount(
        normalized_known.get("budget_max_amount", normalized_known.get("budget_max"))
    )
    if budget_amount is None:
        budget_amount = _coerce_budget_amount(analysis.budget_max)
    budget_unit_source = normalized_known.get("budget_unit") or analysis.budget_unit
    budget_unit = _normalize_budget_unit(budget_unit_source)
    suggestions = _filter_suggested_missing_details(
        list(normalized_known.get("suggested_missing_details") or []) + list(analysis.suggested_missing_details or [])
    )
    summary = str(analysis.summary or "").strip() or _fallback_demand_summary(original_text)

    known = {
        "location_json": _compact_zone_for_session(zone_payload) if zone_payload else default_zone_payload(),
        "location": zone_payload.get("label", "") if zone_payload else "",
        "location_value": zone_payload.get("label", "") if zone_payload else "",
        "budget_max": budget_amount,
        "budget_max_amount": budget_amount,
        "budget_unit": budget_unit,
        "suggested_missing_details": suggestions,
    }
    llm_metadata = {
        "suggested_missing_details": suggestions,
        "confidence": analysis.confidence,
        "location_hint": analysis.location_hint,
    }
    return DemandResult(
        raw_text=original_text,
        summary=summary,
        description=original_text,
        location_mode=zone_payload.get("mode", "unspecified") if zone_payload else "unspecified",
        location_value=zone_payload.get("label") if zone_payload else None,
        location_label=zone_payload.get("label") if zone_payload else None,
        location_admin_level=zone_payload.get("admin_level") if zone_payload else None,
        location_lat=(zone_payload.get("center") or {}).get("lat") if zone_payload else None,
        location_lon=(zone_payload.get("center") or {}).get("lon") if zone_payload else None,
        location_radius_km=zone_payload.get("radius_km") if zone_payload else None,
        location_radius_bucket=zone_payload.get("radius_bucket") if zone_payload else None,
        location_source=zone_payload.get("source") if zone_payload else None,
        location_raw_query=zone_payload.get("raw_query") if zone_payload else None,
        location_json=zone_payload or {},
        location=zone_payload.get("label") if zone_payload else None,
        budget_max=budget_amount,
        budget_unit=budget_unit,
        attributes={},
        known_fields=known,
        suggested_missing_details=suggestions,
        next_question=None,
        enough_information=len(original_text) >= 8,
        confidence=analysis.confidence,
        llm_metadata=llm_metadata,
    )


def _simple_fields_to_complete(draft: DemandResult) -> list[dict[str, Any]]:
    return [
        {
            "field_name": "location",
            "field_label": "Ubicación",
            "question": "Ubicación",
            "current_value": draft.location_value or "",
            "control": {
                "kind": "zone_selector",
                "value": draft.location_json or default_zone_payload(),
            },
        },
        {
            "field_name": "budget_max_amount",
            "field_label": "Precio máximo",
            "question": "Precio máximo",
            "current_value": draft.budget_max,
            "control": {"kind": "number", "value": draft.budget_max, "min": "0.01", "step": "0.01"},
        },
        {
            "field_name": "budget_unit",
            "field_label": "Unidad del precio",
            "question": "Unidad del precio",
            "current_value": draft.budget_unit,
            "control": {"kind": "select", "value": draft.budget_unit, "options": BUDGET_UNIT_OPTIONS},
        },
    ]


def _api_location_payload(zone: Any) -> Optional[dict[str, Any]]:
    payload = normalize_zone_payload(zone if isinstance(zone, dict) else {})
    if not payload:
        return None
    center = payload.get("center") or {}
    lat = center.get("lat")
    lon = center.get("lon")
    label = str(payload.get("label") or "").strip()
    display = compact_zone_label(label, payload.get("raw_query"))
    if not label and lat is None and lon is None:
        return None
    return {
        "label": label or display or None,
        "display": display or label or None,
        "center": {"lat": lat, "lon": lon} if lat is not None and lon is not None else None,
        "radius_km": payload.get("radius_km"),
    }


def _api_fields_to_complete_payload(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for entry in fields:
        item = dict(entry)
        control = dict(item.get("control") or {})
        if control.get("kind") == "zone_selector":
            control["value"] = _api_location_payload(control.get("value"))
        item["control"] = control
        serialized.append(item)
    return serialized


def _api_analysis_payload(payload: dict[str, Any], *, verbose: bool = False) -> dict[str, Any]:
    known_fields = dict(payload.get("known_fields") or {})
    compact = {
        "status": payload.get("status"),
        "publish_ready": bool(payload.get("publish_ready")),
        "original_text": payload.get("original_text"),
        "summary": payload.get("summary"),
        "suggested_missing_details": list(payload.get("suggested_missing_details") or []),
        "known_fields": {
            "location": _api_location_payload(known_fields.get("location_json"))
            or _api_location_payload(known_fields.get("location")),
            "budget_max_amount": known_fields.get("budget_max_amount"),
            "budget_unit": known_fields.get("budget_unit") or "total",
        },
        "fields_to_complete": _api_fields_to_complete_payload(list(payload.get("fields_to_complete") or [])),
        "publish_endpoint": payload.get("publish_endpoint") or "/api/agent/demands/publish",
    }
    if verbose:
        draft = dict(payload.get("draft") or {})
        compact["draft"] = {
            "raw_text": draft.get("raw_text"),
            "summary": draft.get("summary"),
            "location": _api_location_payload(draft.get("location_json")),
            "budget_max_amount": draft.get("budget_max"),
            "budget_unit": draft.get("budget_unit") or "total",
            "suggested_missing_details": list(draft.get("suggested_missing_details") or []),
            "confidence": draft.get("confidence"),
        }
    return compact


def _api_published_demand_payload(demand: Any) -> dict[str, Any]:
    if hasattr(demand, "model_dump"):
        data = demand.model_dump(mode="json")
    elif isinstance(demand, dict):
        data = dict(demand)
    else:
        data = {}
    return {
        "id": data.get("id"),
        "summary": data.get("summary"),
        "original_text": data.get("original_text") or "",
        "location": _api_location_payload(data.get("location_json"))
        or (
            {
                "label": data.get("location_label") or data.get("location"),
                "display": data.get("location_display") or data.get("location_label") or data.get("location"),
                "center": (
                    {"lat": data.get("location_lat"), "lon": data.get("location_lon")}
                    if data.get("location_lat") is not None and data.get("location_lon") is not None
                    else None
                ),
                "radius_km": data.get("location_radius_km"),
            }
            if any(
                value is not None and value != ""
                for value in (
                    data.get("location"),
                    data.get("location_label"),
                    data.get("location_lat"),
                    data.get("location_lon"),
                    data.get("location_radius_km"),
                )
            )
            else None
        ),
        "budget_max_amount": data.get("budget_max"),
        "budget_unit": data.get("budget_unit") or "total",
        "status": data.get("effective_status") or data.get("status") or "open",
        "created_at": data.get("created_at"),
        "expires_at": data.get("expires_at"),
    }


def _build_lightweight_publication_context(llm_client: Any, raw_text: str, known_fields: dict[str, Any] | None = None, *, verbose: bool = False) -> dict[str, Any]:
    original_text = _normalize_text_limit(raw_text, max_length=DEMAND_TEXT_MAX_LENGTH)
    if len(original_text) < 4:
        raise HTTPException(status_code=400, detail="El campo text debe contener la demanda original")
    if len(str(raw_text or "").strip()) > DEMAND_TEXT_MAX_LENGTH:
        raise HTTPException(status_code=400, detail=f"El campo text no puede superar {DEMAND_TEXT_MAX_LENGTH} caracteres")
    if known_fields is not None and not isinstance(known_fields, dict):
        raise HTTPException(status_code=400, detail="known_fields debe ser un objeto JSON")

    draft = _analyze_lightweight_demand(llm_client, original_text, known_fields)
    state = SessionState(
        original_text=draft.raw_text,
        known_fields=draft.known_fields,
        summary=draft.summary,
    )
    fields_to_complete = _simple_fields_to_complete(draft)
    payload = {
        "status": "ready_to_publish" if draft.enough_information else "needs_input",
        "publish_ready": draft.enough_information,
        "original_text": draft.raw_text,
        "summary": draft.summary,
        "suggested_missing_details": list(draft.suggested_missing_details),
        "known_fields": {
            "location": _api_location_payload(draft.location_json),
            "budget_max_amount": draft.budget_max,
            "budget_unit": draft.budget_unit,
        },
        "fields_to_complete": fields_to_complete,
        "publish_endpoint": "/api/agent/demands/publish",
    }
    if verbose:
        payload["draft"] = draft.model_dump(mode="json")
    return {"state": state, "draft": draft, "payload": payload}


def _sse_event(event_name: str, payload: str) -> str:
    normalized_payload = str(payload or "").replace("\r\n", "\n").replace("\r", "\n")
    body = "\n".join(f"data: {line}" for line in normalized_payload.split("\n"))
    return f"event: {event_name}\n{body}\n\n"


def _build_demands_workspace(demands: list[dict[str, Any]], selected_demand_id: int | None, selected_offer_id: int | None) -> dict[str, Any]:
    chosen_demand = None
    chosen_offer = None

    if selected_offer_id and not selected_demand_id:
        for demand in demands:
            match = next((c for c in demand.get("conversations", []) if c["offer_id"] == selected_offer_id), None)
            if match:
                chosen_demand = demand
                chosen_offer = match
                break

    if selected_demand_id and not chosen_demand:
        chosen_demand = next((d for d in demands if d["id"] == selected_demand_id), None)

    if chosen_demand and selected_offer_id and not chosen_offer:
        chosen_offer = next((c for c in chosen_demand.get("conversations", []) if c["offer_id"] == selected_offer_id), None)

    return {"selected_demand": chosen_demand, "selected_offer": chosen_offer}


def _simple_wizard_session(
    draft: DemandResult,
    *,
    wizard_mode: str,
    step: str,
    target_demand_id: int | None = None,
    awaiting_auth: bool = False,
    published_message: str = "",
) -> dict[str, Any]:
    state = SessionState(
        original_text=draft.raw_text,
        known_fields=draft.known_fields,
        summary=draft.summary,
    )
    return {
        "flow_version": "simple_free_text",
        "mode": wizard_mode,
        "step": step,
        "target_demand_id": target_demand_id,
        "return_path": _wizard_return_path(wizard_mode, target_demand_id),
        "awaiting_auth": awaiting_auth,
        "published_message": published_message,
        "state": _state_to_session(state),
        "draft": draft.model_dump(mode="json"),
    }


def _build_offers_workspace(offers: list[dict[str, Any]], selected_offer_id: int | None) -> dict[str, Any]:
    chosen_offer = None
    if selected_offer_id:
        chosen_offer = next((o for o in offers if o["offer_id"] == selected_offer_id), None)
    return {"selected_offer": chosen_offer}


def _prepare_demand_questionnaire(
    request: Request,
    user_id: int | None,
    agent: DemandAgent,
    state: SessionState,
    wizard_mode: str = "create",
    target_demand_id: int | None = None,
) -> RedirectResponse:
    try:
        response = agent.analyze(state)
    except RuntimeError as exc:
        _flash(request, f"No he podido analizar la demanda: {exc}", "error")
        return _redirect(_wizard_return_path(wizard_mode, target_demand_id))

    _apply_wizard_inference(state, response)
    agent.update_state(state, response)
    wizard_data = _build_wizard_session(
        state=state,
        response=response,
        wizard_mode=wizard_mode,
        target_demand_id=target_demand_id,
        step="questions",
    )
    if not _inflate_wizard_for_view(wizard_data).get("field_entries"):
        return _finalize_published_demand(
            request,
            request,
            user_id,
            agent,
            state,
            response,
            wizard_mode=wizard_mode,
            target_demand_id=target_demand_id,
        )
    _save_demand_wizard(request, user_id, wizard_data)
    return _redirect(_wizard_return_path(wizard_mode, target_demand_id))


def _prepare_demand_review(
    request: Request,
    user_id: int | None,
    agent: DemandAgent,
    state: SessionState,
    wizard_mode: str = "create",
    target_demand_id: int | None = None,
    asked_entries: list[dict[str, Any]] | None = None,
) -> RedirectResponse:
    response, draft_demand = _build_local_review_response(state)
    state.intent_domain = response.intent_domain
    state.intent_type = response.intent_type
    state.summary = response.summary
    state.known_fields = merge_known_fields(
        state.known_fields,
        response.known_fields,
        response.attributes,
        {"dates": response.dates},
    )

    has_blocking_required = bool(response.required_missing_fields)
    has_blocking_issues = bool(response.validation_issues)
    if has_blocking_required or has_blocking_issues:
        if has_blocking_issues:
            field_list = ", ".join(_field_display_label(name) for name in response.required_missing_fields or [issue.get("field_name", "") for issue in response.validation_issues if issue.get("field_name")])
            if field_list:
                _flash(request, f"Hay campos con errores de validación. Revisa y corrige: {field_list}.", "error")
            else:
                _flash(request, "Hay campos con errores de validación. Revisa los mensajes marcados en rojo y corrígelos antes de publicar.", "error")
        else:
            field_list = ", ".join(_field_display_label(name) for name in response.required_missing_fields)
            if field_list:
                _flash(request, f"Faltan o no he podido validar estos campos obligatorios: {field_list}.", "error")
            else:
                _flash(request, "Faltan algunos campos obligatorios antes de publicar la demanda.", "error")
        _save_demand_wizard(
            request,
            user_id,
            _build_wizard_session(
            state=state,
            response=response,
            wizard_mode=wizard_mode,
            target_demand_id=target_demand_id,
            step="questions",
            field_errors=_field_errors_from_response(response, state),
            ),
        )
        return _redirect(_wizard_return_path(wizard_mode, target_demand_id))

    return _finalize_published_demand(
        request,
        user_id,
        agent,
        state,
        response,
        wizard_mode=wizard_mode,
        target_demand_id=target_demand_id,
        asked_entries=asked_entries,
        prebuilt_demand=draft_demand,
    )


def _build_local_review_response(state: SessionState) -> tuple[LLMResponse, DemandResult]:
    registry = get_master_schema_registry()
    seed_response = LLMResponse(
        intent_domain=state.intent_domain or "",
        intent_type=state.intent_type or "general_request",
        confidence=1.0,
        summary=state.summary or state.original_text,
        description=state.original_text,
        known_fields=state.known_fields,
        suggested_fields=[],
        required_missing_fields=[],
        recommended_missing_fields=[],
        validation_issues=[],
        missing_fields=[],
        next_question=None,
        enough_information=True,
        dates=dict(state.known_fields.get("dates") or {}),
        attributes={},
    )
    draft = build_normalized_demand(
        raw_text=state.original_text,
        known_fields=state.known_fields,
        response=seed_response,
        registry=registry,
    )
    response = LLMResponse(
        intent_domain=draft.intent_domain,
        intent_type=draft.intent_type,
        confidence=draft.confidence or 1.0,
        summary=draft.summary,
        description=draft.description,
        known_fields=draft.known_fields,
        location_mode=draft.location_mode,
        location_value=draft.location_value,
        budget_mode=draft.budget_mode,
        budget_min=draft.budget_min,
        budget_max=draft.budget_max,
        urgency=draft.urgency,
        dates=draft.dates,
        attributes=draft.attributes,
        suggested_fields=[],
        required_missing_fields=list(draft.required_missing_fields),
        recommended_missing_fields=list(draft.recommended_missing_fields),
        validation_issues=list(draft.validation_issues),
        missing_fields=list(draft.required_missing_fields or draft.recommended_missing_fields),
        next_question=None,
        enough_information=draft.enough_information,
    )
    if draft.validation_issues:
        first_issue = next((item for item in draft.validation_issues if item.get("field_name")), draft.validation_issues[0])
        response.next_question_field = first_issue.get("field_name")
        response.next_question = first_issue.get("question")
        response.enough_information = False
    elif draft.required_missing_fields:
        target = draft.required_missing_fields[0]
        draft_schema = get_master_schema_registry().resolve_intent_schema(draft.intent_type)
        response.next_question_field = target
        response.next_question = get_field_prompt(
            target,
            state.original_text,
            draft.intent_type,
            draft.intent_domain,
            field_description=draft_schema.field_spec(target).description,
        )["question"]
        response.enough_information = False
    return response, draft


def _finalize_published_demand(
    request: Request,
    user_id: int | None,
    agent: DemandAgent,
    state: SessionState,
    response: LLMResponse,
    wizard_mode: str = "create",
    target_demand_id: int | None = None,
    asked_entries: list[dict[str, Any]] | None = None,
    prebuilt_demand: DemandResult | None = None,
) -> HTMLResponse | RedirectResponse:
    demand = prebuilt_demand or agent.build_final_demand(state, response)
    if user_id is None:
        wizard_data = _build_wizard_session(
            state=_compact_review_state(state),
            response=response,
            wizard_mode=wizard_mode,
            target_demand_id=target_demand_id,
            step="review",
            review_demand=demand,
            answered_fields=_collect_answered_fields(state, asked_entries or []),
        )
        wizard_data["awaiting_auth"] = True
        _save_demand_wizard(request, None, wizard_data)
        active_wizard = _inflate_wizard_for_view(wizard_data)
        return _render(
            request,
            "new_demand.html",
            {
                "title": "Completa tu publicación",
                "active_nav": "publish",
                "page_kind": "public",
                "wizard_mode": wizard_mode,
                "active_wizard": active_wizard,
                "submit_action": "/demands",
                "submit_label": "Analizar y continuar",
                "page_title": "Tu demanda está lista para publicarse",
                "page_note": "",
                "initial_text": state.original_text,
                "edit_demand": None,
                "auth_stage": "guest",
            },
        )
    if wizard_mode == "edit" and target_demand_id:
        persisted_demand = update_web_demand_from_agent(target_demand_id, user_id, demand, state)
        if not persisted_demand:
            _flash(request, "No he podido actualizar la demanda. Comprueba que sigue abierta.", "error")
            return _redirect("/my-demands")
    else:
        persisted_demand = save_web_demand_from_agent(user_id, demand, state)

    wizard_data = _build_wizard_session(
        state=_compact_review_state(state),
        response=response,
        wizard_mode=wizard_mode,
        target_demand_id=target_demand_id,
        step="review",
        review_demand=demand,
        answered_fields=_collect_answered_fields(state, asked_entries or []),
    )
    active_wizard = _inflate_wizard_for_view(wizard_data)
    _clear_demand_wizard(request, user_id)
    return _render(
        request,
        "new_demand.html",
        {
            "title": "Nueva demanda" if wizard_mode == "create" else "Editar demanda",
            "active_nav": "publish",
            "page_kind": "app" if user_id else "public",
            "wizard_mode": wizard_mode,
            "active_wizard": active_wizard,
            "submit_action": "/demands" if wizard_mode == "create" else f"/demands/{target_demand_id}/edit",
            "submit_label": "Analizar y continuar" if wizard_mode == "create" else "Analizar y actualizar",
            "page_title": "Qué necesitas?" if wizard_mode == "create" else "Edita tu demanda",
            "page_note": "",
            "initial_text": state.original_text,
            "edit_demand": persisted_demand if wizard_mode == "edit" else None,
        },
    )


def _build_wizard_session(
    state: SessionState,
    response: LLMResponse,
    wizard_mode: str,
    target_demand_id: int | None,
    step: str,
    review_demand: DemandResult | None = None,
    field_errors: dict[str, str] | None = None,
    answered_fields: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "step": step,
        "state": _state_to_session(state),
        "missing_fields": response.missing_fields,
        "required_missing_fields": response.required_missing_fields,
        "recommended_missing_fields": response.recommended_missing_fields,
        "validation_issues": _compact_validation_issues_for_session(getattr(response, "validation_issues", [])),
        "summary": response.summary,
        "intent_domain": response.intent_domain,
        "intent_type": response.intent_type,
        "mode": wizard_mode,
        "target_demand_id": target_demand_id,
        "return_path": _wizard_return_path(wizard_mode, target_demand_id),
        "review_demand": _compact_review_demand(review_demand) if review_demand else None,
        "field_errors": field_errors or {},
        "answered_fields": answered_fields or [],
    }


def _inflate_wizard_for_view(wizard: dict[str, Any]) -> dict[str, Any]:
    view = dict(wizard)
    if view.get("flow_version") == "simple_free_text":
        draft = DemandResult.model_validate(view.get("draft") or {})
        state = _session_to_state(view.get("state", {}))
        zone_payload = normalize_zone_payload(draft.location_json) if isinstance(draft.location_json, dict) else None
        view["draft"] = draft
        view["draft_debug_json"] = draft.model_dump(mode="json")
        view["state"] = state
        view["field_entries"] = []
        view["answered_fields"] = []
        view["location_zone"] = _compact_zone_for_session(zone_payload or {})
        view["budget_unit_options"] = BUDGET_UNIT_OPTIONS
        view["suggested_missing_details"] = list(draft.suggested_missing_details)
        return view
    view["state"] = _session_to_state(view.get("state", {}))
    view["field_entries"] = []
    view["answered_fields"] = list(view.get("answered_fields", []))
    view["budget_unit_options"] = BUDGET_UNIT_OPTIONS
    view["suggested_missing_details"] = []
    return view


def _schema_debug_outline(schema) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []

    def append_item(name: str, mode: str) -> None:
        marker = "*" if mode == "always" else "o" if mode == "conditional" else ""
        items.append({"name": name, "marker": marker})

    if getattr(schema, "has_location_field", False):
        append_item("location", schema.location_policy.required_mode)
    if getattr(schema, "has_budget_field", False):
        append_item("budget", schema.budget_policy.required_mode)
    for field in schema.fields:
        append_item(field.name, field.required)
    return items


def _raw_intent_schema_payload(intent_type: str | None) -> dict[str, Any]:
    if not intent_type:
        return {}
    registry = get_master_schema_registry()
    for domain in registry.raw_schema.get("domains", []) or []:
        domain_code = str(domain.get("code") or "").strip()
        for item in domain.get("intent_types", []) or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("intent_type") or "").strip() != str(intent_type).strip():
                continue
            return {
                **item,
                "intent_domain": str(item.get("intent_domain") or domain_code).strip(),
            }
    return {}


def _response_from_wizard(wizard: dict[str, Any]) -> LLMResponse:
    state = _session_to_state(wizard.get("state", {}))
    return LLMResponse(
        intent_domain=wizard.get("intent_domain", state.intent_domain),
        intent_type=wizard.get("intent_type", state.intent_type or "general_request"),
        confidence=0.0,
        summary=wizard.get("summary", state.summary or state.original_text),
        description=state.original_text,
        known_fields=state.known_fields,
        suggested_fields=[],
        required_missing_fields=list(wizard.get("required_missing_fields", [])),
        recommended_missing_fields=list(wizard.get("recommended_missing_fields", [])),
        validation_issues=list(wizard.get("validation_issues", [])),
        missing_fields=list(wizard.get("missing_fields", [])),
        next_question=None,
        enough_information=wizard.get("step") == "review",
    )


def _compact_review_demand(demand: DemandResult) -> dict[str, Any]:
    return {
        "entity_type": demand.entity_type,
        "raw_text": demand.raw_text,
        "intent_domain": demand.intent_domain,
        "intent_type": demand.intent_type,
        "summary": demand.summary,
        "description": demand.description,
        "location_mode": demand.location_mode,
        "location_value": demand.location_value,
        "location_label": demand.location_label,
        "location_admin_level": demand.location_admin_level,
        "location_lat": demand.location_lat,
        "location_lon": demand.location_lon,
        "location_radius_km": demand.location_radius_km,
        "location_radius_bucket": demand.location_radius_bucket,
        "location_source": demand.location_source,
        "location_raw_query": demand.location_raw_query,
        "location_bbox": demand.location_bbox,
        "location_geojson": demand.location_geojson,
        "location_json": demand.location_json,
        "budget_mode": demand.budget_mode,
        "budget_min": demand.budget_min,
        "budget_max": demand.budget_max,
        "urgency": demand.urgency,
        "dates": demand.dates,
        "attributes": demand.attributes,
        "known_fields": demand.known_fields,
        "enough_information": demand.enough_information,
        "confidence": demand.confidence,
        "schema_version": demand.schema_version,
    }


def _apply_wizard_inference(state: SessionState, response: LLMResponse) -> None:
    schema = get_master_schema_registry().resolve_intent_schema(response.intent_type)
    _apply_range_date_inference(state, schema)
    for field_name in schema.active_required_fields(state.known_fields):
        if _has_content(state.known_fields.get(field_name)) or _has_content(response.known_fields.get(field_name)):
            continue
        field_spec = schema.field_spec(field_name)
        if field_spec.value_type in {"date", "checkin_date", "checkout_date"} or is_date_field(field_name):
            parsed, issue = parse_date_value(state.original_text, field_name)
            if parsed and not issue:
                state.known_fields[field_name] = parsed
                continue
        if field_name == "people":
            people = _infer_people_from_text(state.original_text)
            if people is not None:
                state.known_fields[field_name] = people


def _apply_range_date_inference(state: SessionState, schema) -> None:
    checkin_field = ""
    checkout_field = ""
    for field in schema.fields:
        if field.value_type == "checkin_date" and not checkin_field:
            checkin_field = field.name
        elif field.value_type == "checkout_date" and not checkout_field:
            checkout_field = field.name
    if not checkin_field or not checkout_field:
        return
    if _has_content(state.known_fields.get(checkin_field)) and _has_content(state.known_fields.get(checkout_field)):
        return
    inferred = _infer_stay_date_range(state.original_text)
    if not inferred:
        return
    if not _has_content(state.known_fields.get(checkin_field)):
        state.known_fields[checkin_field] = inferred["checkin"]
    if not _has_content(state.known_fields.get(checkout_field)):
        state.known_fields[checkout_field] = inferred["checkout"]


def _infer_stay_date_range(raw_text: str, today: date | None = None) -> dict[str, str] | None:
    today = today or date.today()
    lowered = str(raw_text or "").strip().lower()
    numeric_result = _infer_numeric_stay_date_range(lowered, today)
    if numeric_result:
        return numeric_result
    pattern = re.compile(
        r"\b(?:del?\s+)?(?P<start_day>\d{1,2})"
        r"(?:\s+de\s+(?P<start_month>[a-záéíóú]+))?"
        r"\s+(?:al|hasta)\s+"
        r"(?P<end_day>\d{1,2})"
        r"(?:\s+de\s+(?P<end_month>[a-záéíóú]+))?"
        r"(?:\s+de\s+(?P<year>\d{4}))?\b"
    )
    match = pattern.search(lowered)
    if not match:
        return None

    start_day = int(match.group("start_day"))
    end_day = int(match.group("end_day"))
    start_month_name = (match.group("start_month") or "").strip()
    end_month_name = (match.group("end_month") or "").strip()
    month_name = end_month_name or start_month_name
    if month_name not in SPANISH_MONTHS:
        return None

    start_month = SPANISH_MONTHS[start_month_name] if start_month_name in SPANISH_MONTHS else SPANISH_MONTHS[month_name]
    end_month = SPANISH_MONTHS[end_month_name] if end_month_name in SPANISH_MONTHS else SPANISH_MONTHS[month_name]

    base_year = int(match.group("year") or today.year)
    try:
        checkin = date(base_year, start_month, start_day)
        checkout = date(base_year, end_month, end_day)
    except ValueError:
        return None

    if match.group("year") is None and checkout < today:
        try:
            checkin = date(base_year + 1, start_month, start_day)
            checkout = date(base_year + 1, end_month, end_day)
        except ValueError:
            return None

    if checkout <= checkin:
        return None

    return {
        "checkin": checkin.isoformat(),
        "checkout": checkout.isoformat(),
    }


def _infer_numeric_stay_date_range(lowered: str, today: date) -> dict[str, str] | None:
    patterns = [
        re.compile(
            r"\b(?:del?\s+)?(?P<start>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(?:al|hasta)\s+(?P<end>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b"
        ),
        re.compile(
            r"\b(?:del?\s+)?(?P<start>\d{1,2}[/-]\d{1,2})\s+(?:al|hasta)\s+(?P<end>\d{1,2}[/-]\d{1,2})(?:[/-](?P<year>\d{2,4}))?\b"
        ),
    ]
    for pattern in patterns:
        match = pattern.search(lowered)
        if not match:
            continue
        if match.groupdict().get("start") and match.groupdict().get("end"):
            start_raw = match.group("start")
            end_raw = match.group("end")
            trailing_year = match.groupdict().get("year")
            parsed = _parse_numeric_date_range_values(start_raw, end_raw, trailing_year, today)
            if parsed:
                return parsed
    return None


def _parse_numeric_date_range_values(
    start_raw: str,
    end_raw: str,
    trailing_year: str | None,
    today: date,
) -> dict[str, str] | None:
    start_parts = [part for part in re.split(r"[/-]", start_raw) if part]
    end_parts = [part for part in re.split(r"[/-]", end_raw) if part]
    if len(start_parts) < 2 or len(end_parts) < 2:
        return None

    start_day = int(start_parts[0])
    start_month = int(start_parts[1])
    end_day = int(end_parts[0])
    end_month = int(end_parts[1])
    start_year = _coerce_year_component(start_parts[2], today.year) if len(start_parts) >= 3 else None
    end_year = _coerce_year_component(end_parts[2], today.year) if len(end_parts) >= 3 else None
    trailing = _coerce_year_component(trailing_year, today.year) if trailing_year else None

    if start_year is None and end_year is not None:
        start_year = end_year
    if end_year is None and start_year is not None:
        end_year = start_year
    if start_year is None and end_year is None:
        start_year = trailing or today.year
        end_year = trailing or start_year

    try:
        checkin = date(start_year, start_month, start_day)
        checkout = date(end_year, end_month, end_day)
    except ValueError:
        return None

    if trailing_year is None and len(start_parts) < 3 and len(end_parts) < 3 and checkout < today:
        try:
            checkin = date(checkin.year + 1, checkin.month, checkin.day)
            checkout = date(checkout.year + 1, checkout.month, checkout.day)
        except ValueError:
            return None

    if checkout <= checkin:
        corrected_checkout = _try_fix_checkout_year_typo(checkin, checkout, start_year, end_year, end_month, end_day)
        if corrected_checkout is not None:
            checkout = corrected_checkout
        else:
            return None

    return {
        "checkin": checkin.isoformat(),
        "checkout": checkout.isoformat(),
    }


def _coerce_year_component(raw_year: str | None, default_year: int) -> int | None:
    if raw_year is None:
        return None
    text = str(raw_year).strip()
    if not text:
        return None
    value = int(text)
    if len(text) == 2:
        century = (default_year // 100) * 100
        return century + value
    return value


def _try_fix_checkout_year_typo(
    checkin: date,
    checkout: date,
    start_year: int,
    end_year: int,
    end_month: int,
    end_day: int,
) -> date | None:
    if end_year != start_year - 1:
        return None
    try:
        candidate = date(start_year, end_month, end_day)
    except ValueError:
        return None
    if candidate > checkin:
        return candidate
    return None


def _infer_people_from_text(raw_text: str) -> int | None:
    lowered = (
        raw_text.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    if any(token in lowered for token in ("pareja", "dos personas", "2 personas", "para dos", "para 2")):
        return 2
    if any(token in lowered for token in ("una persona", "1 persona", "para uno", "para 1", "yo solo", "yo sola")):
        return 1
    text_numbers = {
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7,
        "ocho": 8,
        "nueve": 9,
        "diez": 10,
    }
    for token, value in text_numbers.items():
        if f"para {token}" in lowered or f"{token} personas" in lowered:
            return value
    import re

    match = re.search(r"\bpara\s+(\d{1,2})\b", lowered) or re.search(r"\b(\d{1,2})\s+personas?\b", lowered)
    if match:
        try:
            parsed = int(match.group(1))
            return parsed if parsed > 0 else None
        except ValueError:
            return None
    return None


def _build_field_entries(state: SessionState, response: LLMResponse, field_errors: dict[str, str] | None = None) -> list[dict[str, Any]]:
    registry = get_master_schema_registry()
    schema = registry.resolve_intent_schema(response.intent_type)
    field_errors = field_errors or {}
    issue_map = {
        issue.get("field_name", ""): issue
        for issue in getattr(response, "validation_issues", [])
        if issue.get("field_name")
    }

    merged_known = merge_known_fields(state.known_fields, response.known_fields, {"dates": response.dates})
    required_universe: list[str] = list(schema.active_required_fields(merged_known))
    for field_name in schema.conditional_dependency_fields(merged_known):
        if field_name not in required_universe:
            required_universe.append(field_name)
    for field_name in dynamic_required_fields(schema.intent_domain, schema.intent_type, merged_known):
        if field_name not in required_universe:
            required_universe.append(field_name)

    ordered_required: list[str] = []
    for field_name in required_universe:
        if field_name not in ordered_required:
            ordered_required.append(field_name)

    optional_universe: list[str] = []
    for field_name in schema.visible_optional_fields(merged_known):
        if field_name not in required_universe and field_name not in optional_universe:
            optional_universe.append(field_name)

    entries: list[dict[str, Any]] = []
    required_missing_set = set(response.required_missing_fields)

    if getattr(schema, "has_location_field", False) and schema.location_required_for(merged_known) and "location_value" not in ordered_required:
        ordered_required.insert(0, "location_value")

    budget_entry_name = _budget_entry_name(schema)
    if getattr(schema, "has_location_field", False) and schema.location_policy.required_mode == "never" and "location_value" not in required_universe and "location_value" not in optional_universe:
        optional_universe.insert(0, "location_value")

    if getattr(schema, "has_budget_field", False) and budget_entry_name and budget_entry_name not in ordered_required and budget_entry_name not in optional_universe:
        if schema.budget_required_for(merged_known):
            insert_at = 1 if schema.location_required_for(merged_known) else 0
            ordered_required.insert(insert_at, budget_entry_name)
        else:
            insert_at = 1 if optional_universe and optional_universe[0] == "location_value" else 0
            optional_universe.insert(insert_at, budget_entry_name)

    latent_conditional_required: list[str] = []
    for field in schema.fields:
        if field.required != "conditional":
            continue
        if field.name not in ordered_required and field.name not in latent_conditional_required:
            latent_conditional_required.append(field.name)
    if getattr(schema, "has_location_field", False) and schema.location_policy.required_mode == "conditional" and "location_value" not in ordered_required and "location_value" not in latent_conditional_required:
        latent_conditional_required.insert(0, "location_value")
    if getattr(schema, "has_budget_field", False) and schema.budget_policy.required_mode == "conditional" and budget_entry_name and budget_entry_name not in ordered_required and budget_entry_name not in latent_conditional_required:
        latent_conditional_required.append(budget_entry_name)

    ordered_required = _reorder_conditional_required_fields(schema, ordered_required, merged_known)
    latent_conditional_required = _reorder_conditional_required_fields(schema, latent_conditional_required, merged_known)

    for field_name in [*ordered_required, *latent_conditional_required]:
        prompt = _prompt_for_entry(field_name, state.original_text, schema)
        issue = issue_map.get(field_name, {})
        current_value = _current_value_for_entry(field_name, state, response, issue)
        issue_message = field_errors.get(field_name)
        conditional_rule = _conditional_rule_for_entry(schema, field_name)
        condition_active = field_name in ordered_required
        additional_required = condition_active and (field_name in required_missing_set or not _has_content(current_value) or field_name in issue_map) and not issue_message
        control = _field_control(field_name, current_value, response.intent_type, response.intent_domain, state.original_text)
        entries.append(
            {
                "field_name": field_name,
                "category": "required",
                "field_label": _field_display_label(field_name),
                "question": prompt["question"],
                "placeholder": prompt.get("placeholder", ""),
                "examples": prompt.get("examples", []),
                "current_value": current_value,
                "issue_message": issue_message,
                "is_additional_required": additional_required,
                "conditional_rule": conditional_rule,
                "client_visible": condition_active,
                "control": control,
            }
        )

    for field_name in optional_universe:
        prompt = _prompt_for_entry(field_name, state.original_text, schema, optional=True)
        issue = issue_map.get(field_name, {})
        current_value = _current_value_for_entry(field_name, state, response, issue)
        control = _field_control(field_name, current_value, response.intent_type, response.intent_domain, state.original_text)
        entries.append(
            {
                "field_name": field_name,
                "category": "optional",
                "field_label": _field_display_label(field_name),
                "question": prompt["question"],
                "placeholder": prompt.get("placeholder", ""),
                "examples": prompt.get("examples", []),
                "current_value": current_value,
                "issue_message": field_errors.get(field_name),
                "is_additional_required": False,
                "conditional_rule": None,
                "client_visible": True,
                "control": control,
            }
        )
    return entries


def _reorder_conditional_required_fields(schema, field_names: list[str], known_fields: dict[str, Any]) -> list[str]:
    ordered = [name for name in field_names if name]
    if len(ordered) < 2:
        return ordered

    dependency_map: dict[str, str] = {}
    for item in schema.fields:
        if item.required == "conditional" and item.when_field:
            dependency_map[item.name] = item.when_field
    if getattr(schema, "has_location_field", False) and schema.location_policy.required_mode == "conditional" and schema.location_policy.when_field:
        dependency_map["location_value"] = schema.location_policy.when_field
    budget_entry_name = _budget_entry_name(schema)
    if getattr(schema, "has_budget_field", False) and schema.budget_policy.required_mode == "conditional" and budget_entry_name and schema.budget_policy.when_field:
        dependency_map[budget_entry_name] = schema.budget_policy.when_field

    moved = True
    while moved:
        moved = False
        for field_name in list(ordered):
            dependency = dependency_map.get(field_name)
            if not dependency or dependency not in ordered:
                continue
            current_index = ordered.index(field_name)
            dependency_index = ordered.index(dependency)
            target_index = dependency_index + 1
            if current_index != target_index:
                ordered.pop(current_index)
                if current_index < target_index:
                    target_index -= 1
                ordered.insert(target_index, field_name)
                moved = True
    return ordered


def _conditional_rule_for_entry(schema, field_name: str) -> dict[str, Any] | None:
    if field_name == "location_value" and schema.location_policy.required_mode == "conditional":
        return {
            "field": schema.location_policy.when_field,
            "operator": schema.location_policy.when_operator,
            "values": list(schema.location_policy.when_values),
        }
    budget_entry_name = _budget_entry_name(schema)
    if field_name == budget_entry_name and schema.budget_policy.required_mode == "conditional":
        return {
            "field": schema.budget_policy.when_field,
            "operator": schema.budget_policy.when_operator,
            "values": list(schema.budget_policy.when_values),
        }
    for item in schema.fields:
        if item.name != field_name or item.required != "conditional":
            continue
        return {
            "field": item.when_field,
            "operator": item.when_operator,
            "values": list(item.when_values),
        }
    return None


def _budget_entry_name(schema) -> str:
    if not getattr(schema, "has_budget_field", False):
        return ""
    return "budget_range" if schema.budget_fix_or_range == "range" else "budget_max"


def _current_value_for_entry(field_name: str, state: SessionState, response: LLMResponse, issue: dict[str, Any]) -> Any:
    if field_name == "budget_range":
        min_value = (
            issue.get("raw_value", {}).get("min")
            if isinstance(issue.get("raw_value"), dict)
            else None
        ) or state.known_fields.get("budget_min") or response.known_fields.get("budget_min") or ""
        max_value = (
            issue.get("raw_value", {}).get("max")
            if isinstance(issue.get("raw_value"), dict)
            else None
        ) or state.known_fields.get("budget_max") or response.known_fields.get("budget_max") or ""
        return {"min": min_value, "max": max_value}
    if field_name == "budget_max":
        return issue.get("raw_value") or state.known_fields.get("budget_max") or response.known_fields.get("budget_max") or response.budget_max or ""
    if is_location_field(field_name):
        return (
            state.known_fields.get("location_json")
            or issue.get("raw_value")
            or state.known_fields.get(field_name)
            or response.known_fields.get(field_name)
            or ""
        )
    return (
        issue.get("raw_value")
        or state.known_fields.get(field_name)
        or response.known_fields.get(field_name)
        or response.dates.get(field_name)
        or ""
    )


def _prompt_for_entry(field_name: str, raw_text: str, schema, optional: bool = False) -> dict[str, Any]:
    if field_name in {"budget_max", "budget_range"}:
        return _budget_prompt_for_schema(raw_text, schema, optional=optional)
    if field_name == "location_value":
        return _location_prompt_for_schema(raw_text, schema, optional=optional)
    prompt = get_field_prompt(
        field_name,
        raw_text,
        schema.intent_type,
        schema.intent_domain,
        field_description=schema.field_spec(field_name).description,
    )
    if optional:
        prompt["question"] = f"{prompt['question']} (Opcional)"
    return prompt


def _budget_prompt_for_schema(raw_text: str, schema, optional: bool = False) -> dict[str, Any]:
    unit_map = {
        "one-time": "en total",
        "per hour": "por hora",
        "per day": "por día",
        "per night": "por noche",
        "per season": "por temporada",
        "weekly": "por semana",
        "monthly": "al mes",
        "anual": "al año",
    }
    unit_text = unit_map.get(schema.budget_unit or "", "en total")
    if schema.budget_fix_or_range == "range":
        question = f"¿Qué rango de precio en euros (€) quieres pagar {unit_text}?"
        placeholder = "Ej.: mínimo 20.00 € y máximo 35.50 €"
        examples = ["20.00 y 35.50", "50 y 80.25"]
    else:
        question = f"¿Cuál es tu presupuesto máximo en euros (€) {unit_text}?"
        placeholder = "Ej.: 120.50 €"
        examples = ["50", "120.50"]
    if optional:
        question = f"{question} (Opcional)"
    return {"question": question, "placeholder": placeholder, "examples": examples}


def _location_prompt_for_schema(raw_text: str, schema, optional: bool = False) -> dict[str, Any]:
    base = get_field_prompt(
        "location_value",
        raw_text,
        schema.intent_type,
        schema.intent_domain,
        field_description="ubicación",
    )
    question = base["question"]
    if optional:
        question = f"{question} (Opcional)"
    return {**base, "question": question}


def _field_control(field_name: str, current_value: Any, intent_type: str, intent_domain: str, original_text: str = "") -> dict[str, Any]:
    field_definition = get_field_definition(field_name, intent_type)
    schema = get_master_schema_registry().resolve_intent_schema(intent_type)
    field_value_type = field_definition.get("value_type")
    field_choices = field_definition.get("choices") or []
    min_value = field_definition.get("min_value") or ""
    max_value = field_definition.get("max_value") or ""
    if field_name == "budget_range":
        range_value = current_value if isinstance(current_value, dict) else {}
        return {
            "kind": "budget_range",
            "min_value": str(range_value.get("min") or ""),
            "max_value": str(range_value.get("max") or ""),
            "min": schema.budget_policy.min_value or min_value or "0.01",
            "max": schema.budget_policy.max_value or max_value or "",
        }
    if field_name == "budget_max":
        return {
            "kind": "number",
            "value": current_value,
            "step": "0.01",
            "min": schema.budget_policy.min_value or min_value or "0.01",
            "max": schema.budget_policy.max_value or max_value,
        }
    if is_location_field(field_name):
        zone_payload = normalize_zone_payload(current_value if isinstance(current_value, dict) else None)
        if not zone_payload:
            zone_payload = default_zone_payload()
            if isinstance(current_value, str) and current_value.strip():
                guessed_zone = _guess_zone_from_text(current_value, original_text)
                if guessed_zone:
                    zone_payload = guessed_zone
                else:
                    zone_payload["label"] = current_value.strip()
                    zone_payload["raw_query"] = current_value.strip()
        return {
            "kind": "zone_selector",
            "value": zone_payload or default_zone_payload(),
            "radius_options": RADIUS_OPTIONS,
        }
    if field_value_type == "location":
        zone_payload = normalize_zone_payload(current_value if isinstance(current_value, dict) else None) or default_zone_payload()
        if isinstance(current_value, str) and current_value.strip() and not zone_payload.get("label"):
            zone_payload["label"] = current_value.strip()
            zone_payload["raw_query"] = current_value.strip()
        return {
            "kind": "zone_selector",
            "value": zone_payload,
            "radius_options": RADIUS_OPTIONS,
        }
    if field_name == "dates":
        start_value = ""
        end_value = ""
        if isinstance(current_value, dict):
            start_value = _coerce_date_input_value(current_value.get("start_date") or current_value.get("checkin") or current_value.get("date_from"))
            end_value = _coerce_date_input_value(current_value.get("end_date") or current_value.get("checkout") or current_value.get("date_to"))
        return {
            "kind": "date_range",
            "min": field_definition.get("min_date") or date.today().isoformat(),
            "max": field_definition.get("max_date") or "",
            "start_value": start_value,
            "end_value": end_value,
        }
    if field_value_type == "date_range":
        start_value = ""
        end_value = ""
        if isinstance(current_value, dict):
            start_value = _coerce_date_input_value(current_value.get("start_date") or current_value.get("checkin") or current_value.get("date_from"))
            end_value = _coerce_date_input_value(current_value.get("end_date") or current_value.get("checkout") or current_value.get("date_to"))
        return {
            "kind": "date_range",
            "min": field_definition.get("min_date") or date.today().isoformat(),
            "max": field_definition.get("max_date") or "",
            "start_value": start_value,
            "end_value": end_value,
        }
    if is_date_field(field_name):
        return {
            "kind": "date",
            "min": _resolve_date_control_min(field_name, field_value_type, field_definition.get("min_date")),
            "max": field_definition.get("max_date") or "",
            "after_field": _extract_after_field(field_definition.get("min_date")),
            "value": _coerce_date_input_value(current_value),
        }
    if field_value_type in {"date", "checkin_date", "checkout_date"}:
        return {
            "kind": "date",
            "min": _resolve_date_control_min(field_name, field_value_type, field_definition.get("min_date")),
            "max": field_definition.get("max_date") or "",
            "after_field": _extract_after_field(field_definition.get("min_date")),
            "value": _coerce_date_input_value(current_value),
        }
    if field_value_type == "time":
        return {
            "kind": "time",
            "value": str(current_value or "").strip(),
            "min": field_definition.get("min_time") or "",
            "max": field_definition.get("max_time") or "",
        }
    if field_value_type == "datetime":
        return {
            "kind": "datetime",
            "value": str(current_value or "").strip(),
            "min": field_definition.get("min_datetime") or "",
            "max": field_definition.get("max_datetime") or "",
        }
    if field_value_type == "enum" and field_choices:
        return {
            "kind": "select",
            "value": _normalize_select_value(current_value),
            "options": field_choices,
        }
    if field_value_type == "boolean":
        return {
            "kind": "select",
            "value": _normalize_select_value(current_value),
            "options": [
                {"value": "si", "label": "Sí"},
                {"value": "no", "label": "No"},
            ],
        }
    options = _select_options_for_field(field_name, intent_type, intent_domain)
    if options:
        return {
            "kind": "select",
            "value": _normalize_select_value(current_value),
            "options": options,
        }
    if field_value_type == "integer":
        return {"kind": "integer", "value": current_value, "min": min_value, "max": max_value}
    if field_value_type in {"float", "money_eur", "money_eur_range"}:
        return {"kind": "number", "value": current_value, "step": "0.01", "min": min_value, "max": max_value}
    return {
        "kind": "textarea",
        "min_length": field_definition.get("min_length") or "",
        "max_length": field_definition.get("max_length") or "",
    }


def _resolve_date_control_min(field_name: str, field_value_type: str, raw_min: str) -> str:
    min_value = str(raw_min or "").strip()
    if min_value == "today":
        return date.today().isoformat()
    if min_value.startswith("after:"):
        return date.today().isoformat()
    if field_value_type in {"date", "checkin_date", "checkout_date"} or is_date_field(field_name):
        return min_value or date.today().isoformat()
    return min_value


def _extract_after_field(raw_min: Any) -> str:
    text = str(raw_min or "").strip()
    if not text.startswith("after:"):
        return ""
    return text.split(":", 1)[1].strip()


def _select_options_for_field(field_name: str, intent_type: str, intent_domain: str) -> list[dict[str, str]]:
    options = list(SELECT_FIELD_OPTIONS.get(field_name, []))
    if field_name == "modality" and intent_type in {"employee_hiring", "freelance_project", "recruiter_search"}:
        return [
            {"value": "presencial", "label": "Presencial"},
            {"value": "hibrido", "label": "Híbrido"},
            {"value": "remoto", "label": "Remoto"},
        ]
    if field_name == "modality" and intent_domain == "education":
        return [
            {"value": "presencial", "label": "Presenciales"},
            {"value": "online", "label": "Online"},
            {"value": "me da igual", "label": "Me da igual"},
        ]
    return options


def _coerce_date_input_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value).strip()
    if not raw:
        return ""
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _normalize_select_value(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip().lower()
    replacements = {
        "presenciales": "presencial",
        "presencial": "presencial",
        "online": "online",
        "remotas": "online",
        "remota": "online",
        "remoto": "remoto",
        "hibrido": "hibrido",
        "híbrido": "hibrido",
        "me da igual": "me da igual",
        "indiferente": "me da igual",
        "a domicilio": "a domicilio",
        "lo necesito ya": "lo necesito ya",
        "esta semana": "esta semana",
        "en los próximos días": "en los proximos dias",
        "en los proximos dias": "en los proximos dias",
        "sin prisa": "sin prisa",
    }
    return replacements.get(raw, raw)


def _safe_int(value: Any) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _safe_float(value: Any) -> Optional[float]:
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _field_has_value(field_name: str, state: SessionState, response: LLMResponse) -> bool:
    candidates = [state.known_fields.get(field_name), response.known_fields.get(field_name)]
    if field_name in {"dates"}:
        return bool(state.known_fields.get("dates") or response.dates)
    if is_date_field(field_name):
        candidates.extend([(state.known_fields.get("dates") or {}).get(field_name), response.dates.get(field_name)])
    if is_location_field(field_name):
        candidates.extend(
            [
                state.known_fields.get("location"),
                state.known_fields.get("location_value"),
                state.known_fields.get("search_location"),
                response.known_fields.get("location"),
                response.known_fields.get("location_value"),
                response.known_fields.get("search_location"),
            ]
        )
    if is_budget_field(field_name):
        candidates.extend(
            [
                state.known_fields.get("budget_max"),
                state.known_fields.get("budget_total"),
                response.known_fields.get("budget_max"),
                response.known_fields.get("budget_total"),
            ]
        )
    return any(_has_content(value) for value in candidates)


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(_has_content(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_content(item) for item in value)
    return True


def _collect_answered_fields(state: SessionState, field_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    answered: list[dict[str, Any]] = []
    for entry in field_entries:
        field_name = entry.get("field_name", "")
        if field_name == "budget_range":
            min_value = state.known_fields.get("budget_min")
            max_value = state.known_fields.get("budget_max")
            value = {"min": min_value, "max": max_value} if _has_content(min_value) or _has_content(max_value) else None
        else:
            value = state.known_fields.get(field_name)
        if entry.get("control", {}).get("kind") == "zone_selector":
            value = state.known_fields.get("location_json") or value
        if value in (None, ""):
            continue
        answered.append(
            {
                "field_name": field_name,
                "field_label": entry.get("field_label", _field_display_label(field_name)),
                "category": entry.get("category", "recommended"),
                "question": entry.get("question", field_name),
                "answer": _format_answer_for_review(value, entry.get("control", {})),
            }
        )
    return answered


def _demand_detail_answered_fields(demand: dict[str, Any]) -> list[dict[str, str]]:
    return []


def _extract_field_answer(form, field_name: str, control: dict[str, Any]) -> Any:
    kind = control.get("kind")
    if kind == "zone_selector":
        payload = _parse_zone_json(str(form.get(f"field__{field_name}__zone_json", "")))
        return payload
    if kind == "budget_range":
        min_value = str(form.get(f"field__{field_name}__min", "")).strip()
        max_value = str(form.get(f"field__{field_name}__max", "")).strip()
        if min_value or max_value:
            return {"min": min_value, "max": max_value}
        return None
    if kind == "date_range":
        start = str(form.get(f"field__{field_name}__start", "")).strip()
        end = str(form.get(f"field__{field_name}__end", "")).strip()
        if start or end:
            return {"start_date": start, "end_date": end}
        return None
    return str(form.get(f"field__{field_name}", "")).strip()


def _validate_control_answer(
    answer: Any,
    control: dict[str, Any],
    field_name: str,
    known_fields: Optional[dict[str, Any]] = None,
    submitted_answers: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    kind = control.get("kind")
    if kind == "zone_selector":
        if not answer:
            return "Selecciona una zona válida en el mapa o desde el buscador."
        if not isinstance(answer, dict):
            return "Selecciona una zona válida en el mapa o desde el buscador."
        center = answer.get("center") or {}
        if center.get("lat") is None or center.get("lon") is None:
            return "Selecciona una zona válida en el mapa o desde el buscador."
        if not answer.get("label"):
            return "Selecciona una zona válida en el mapa o desde el buscador."
    if kind == "select":
        if not answer:
            return None
        allowed = {option["value"] for option in control.get("options", [])}
        if str(answer) not in allowed:
            return "Selecciona una opción válida."
    if kind == "integer" and answer:
        try:
            parsed = int(str(answer))
            if parsed <= 0:
                return "Introduce un número entero mayor que 0."
            min_value = _safe_int(control.get("min"))
            max_value = _safe_int(control.get("max"))
            if min_value is not None and parsed < min_value:
                return f"Introduce un número entero mayor o igual que {min_value}."
            if max_value is not None and parsed > max_value:
                return f"Introduce un número entero menor o igual que {max_value}."
        except ValueError:
            return "Introduce un número entero válido."
    if kind == "number" and answer:
        try:
            parsed = float(str(answer).replace(",", "."))
            if parsed <= 0:
                return "Introduce un número mayor que 0."
            min_value = _safe_float(control.get("min"))
            max_value = _safe_float(control.get("max"))
            if min_value is not None and parsed < min_value:
                return f"Introduce un número mayor o igual que {min_value:g}."
            if max_value is not None and parsed > max_value:
                return f"Introduce un número menor o igual que {max_value:g}."
        except ValueError:
            return "Introduce un número válido."
    if kind == "budget_range" and answer:
        if not isinstance(answer, dict):
            return "Introduce un rango válido en euros."
        min_raw = str(answer.get("min") or "").strip()
        max_raw = str(answer.get("max") or "").strip()
        if not min_raw or not max_raw:
            return "Introduce precio mínimo y precio máximo en euros."
        try:
            min_parsed = float(min_raw.replace(",", "."))
            max_parsed = float(max_raw.replace(",", "."))
        except ValueError:
            return "Introduce importes válidos en euros."
        if min_parsed <= 0 or max_parsed <= 0:
            return "Introduce importes mayores que 0."
        if max_parsed < min_parsed:
            return "El precio máximo debe ser mayor o igual que el mínimo."
    if kind == "date_range":
        if not answer:
            return None
        if not isinstance(answer, dict):
            return "Selecciona un rango de fechas válido."
        start = str(answer.get("start_date", "")).strip()
        end = str(answer.get("end_date", "")).strip()
        if not start or not end:
            return "Selecciona fecha de inicio y fecha de fin."
        try:
            start_date = date.fromisoformat(start)
            end_date = date.fromisoformat(end)
        except ValueError:
            return "Selecciona un rango de fechas válido."
        min_value = control.get("min") or ""
        max_value = control.get("max") or ""
        if min_value and (start_date < date.fromisoformat(min_value) or end_date < date.fromisoformat(min_value)):
            return "Selecciona un rango igual o posterior al mínimo permitido."
        if max_value and (start_date > date.fromisoformat(max_value) or end_date > date.fromisoformat(max_value)):
            return "Selecciona un rango igual o anterior al máximo permitido."
        if end_date < start_date:
            return "La fecha final debe ser igual o posterior a la fecha inicial."
    if kind == "date" and answer:
        try:
            parsed = date.fromisoformat(str(answer))
            min_value = control.get("min") or ""
            max_value = control.get("max") or ""
            if min_value and parsed < date.fromisoformat(min_value):
                return f"Selecciona una fecha igual o posterior a {datetime.strptime(min_value, '%Y-%m-%d').strftime('%d/%m/%Y')}."
            if max_value and parsed > date.fromisoformat(max_value):
                return f"Selecciona una fecha igual o anterior a {datetime.strptime(max_value, '%Y-%m-%d').strftime('%d/%m/%Y')}."
            compare_field = str(control.get("after_field") or "").strip()
            if compare_field:
                compare_raw = ""
                if submitted_answers:
                    compare_raw = str(submitted_answers.get(compare_field) or "").strip()
                if not compare_raw and known_fields:
                    compare_raw = str(known_fields.get(compare_field) or "").strip()
                compare_date = _coerce_date_input_value(compare_raw)
                if compare_date:
                    reference_date = date.fromisoformat(compare_date)
                    if parsed <= reference_date:
                        return "Esta fecha debe ser posterior a la fecha de entrada."
        except ValueError:
            return "Selecciona una fecha válida."
    if kind == "time" and answer:
        raw = str(answer).strip()
        if len(raw) != 5 or raw[2] != ":":
            return "Selecciona una hora válida."
        min_value = control.get("min") or ""
        max_value = control.get("max") or ""
        if min_value and raw < min_value:
            return f"Selecciona una hora igual o posterior a {min_value}."
        if max_value and raw > max_value:
            return f"Selecciona una hora igual o anterior a {max_value}."
    if kind == "datetime" and answer:
        raw = str(answer).strip()
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return "Selecciona una fecha y hora válidas."
        min_value = control.get("min") or ""
        max_value = control.get("max") or ""
        if min_value:
            if parsed < datetime.fromisoformat(min_value):
                return "Selecciona una fecha y hora posterior al mínimo permitido."
        if max_value:
            if parsed > datetime.fromisoformat(max_value):
                return "Selecciona una fecha y hora anterior al máximo permitido."
    if kind == "textarea" and answer:
        raw = str(answer).strip()
        min_length = _safe_int(control.get("min_length"))
        max_length = _safe_int(control.get("max_length"))
        if min_length is not None and len(raw) < min_length:
            return f"Escribe al menos {min_length} caracteres."
        if max_length is not None and len(raw) > max_length:
            return f"Escribe como máximo {max_length} caracteres."
    return None


def _format_answer_for_review(value: Any, control: dict[str, Any]) -> str:
    kind = control.get("kind")
    if kind == "budget_range" and isinstance(value, dict):
        min_value = str(value.get("min") or "").strip()
        max_value = str(value.get("max") or "").strip()
        if min_value and max_value:
            return f"Entre {min_value} € y {max_value} €"
    if kind == "zone_selector" and isinstance(value, dict):
        return zone_display_value(value)
    if kind == "date" and value:
        formatted = _format_review_date(value)
        return formatted or str(value)
    if kind == "time" and value:
        return str(value)
    if kind == "datetime" and value:
        raw = str(value).strip()
        try:
            return datetime.fromisoformat(raw).strftime("%d/%m/%Y %H:%M")
        except ValueError:
            return raw
    if kind == "date_range" and isinstance(value, dict):
        start = value.get("start_date") or ""
        end = value.get("end_date") or ""
        if start and end:
            start_label = _format_review_date(start) or str(start)
            end_label = _format_review_date(end) or str(end)
            return f"Del {start_label} al {end_label}"
    if isinstance(value, dict):
        return ", ".join(f"{key}: {item}" for key, item in value.items() if item)
    return str(value)


def _format_review_date(value: Any) -> str:
    raw = _coerce_date_input_value(value)
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return ""


def _compact_zone_for_session(zone: Any) -> dict[str, Any]:
    payload = normalize_zone_payload(zone if isinstance(zone, dict) else {})
    if not payload:
        return default_zone_payload()
    if not (
        payload.get("label")
        or (
            payload.get("center", {}).get("lat") is not None
            and payload.get("center", {}).get("lon") is not None
        )
    ):
        return default_zone_payload()
    payload["bbox"] = None
    payload["geojson"] = None
    return payload


def _compact_zone_for_query(zone: Optional[dict[str, Any]]) -> dict[str, Any]:
    payload = compact_zone_for_transport(zone or {})
    if not payload:
        return default_zone_payload()
    return payload


def _compact_known_fields_for_session(known_fields: dict[str, Any]) -> dict[str, Any]:
    compacted = dict(known_fields or {})
    for key in ("location_json", "location_zone"):
        if isinstance(compacted.get(key), dict):
            compacted[key] = _compact_zone_for_session(compacted[key])
    if isinstance(compacted.get("location_structured"), dict):
        structured = dict(compacted["location_structured"])
        if "geojson" in structured:
            structured["geojson"] = {}
        compacted["location_structured"] = structured
    if "location_geojson" in compacted:
        compacted["location_geojson"] = {}
    return compacted


def _compact_validation_issues_for_session(issues: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for issue in issues or []:
        item = dict(issue)
        raw_value = item.get("raw_value")
        if isinstance(raw_value, dict):
            item["raw_value"] = _compact_zone_for_session(raw_value)
        elif isinstance(raw_value, list) and len(raw_value) > 8:
            item["raw_value"] = raw_value[:8]
        compacted.append(item)
    return compacted


def _state_to_session(state: SessionState) -> dict[str, Any]:
    return {
        "original_text": state.original_text,
        "questions_asked": state.questions_asked,
        "user_answers": state.user_answers,
        "known_fields": _compact_known_fields_for_session(state.known_fields),
        "intent_domain": state.intent_domain,
        "intent_type": state.intent_type,
        "summary": state.summary,
        "iteration": state.iteration,
        "telegram_user_id": state.telegram_user_id,
    }


def _session_to_state(data: dict[str, Any]) -> SessionState:
    return SessionState(
        original_text=data.get("original_text", ""),
        questions_asked=list(data.get("questions_asked", [])),
        user_answers=list(data.get("user_answers", [])),
        known_fields=dict(data.get("known_fields", {})),
        intent_domain=data.get("intent_domain", ""),
        intent_type=data.get("intent_type", ""),
        summary=data.get("summary", ""),
        iteration=int(data.get("iteration", 0)),
        telegram_user_id=data.get("telegram_user_id"),
    )


def _guest_wizard_session_key() -> str:
    return "guest_demand_wizard"


def _guest_wizard_storage_key() -> str:
    return "guest_demand_wizard_id"


def _guest_wizard_file_path(request: Request, create: bool = False) -> Path | None:
    storage_id = request.session.get(_guest_wizard_storage_key())
    if not storage_id and create:
        storage_id = secrets.token_urlsafe(18)
        request.session[_guest_wizard_storage_key()] = storage_id
    if not storage_id:
        return None
    GUEST_WIZARDS_DIR.mkdir(parents=True, exist_ok=True)
    return GUEST_WIZARDS_DIR / f"{storage_id}.json"


def _load_demand_wizard(request: Request, user_id: int | None = None) -> Optional[dict[str, Any]]:
    if user_id is not None:
        return get_demand_wizard_record(user_id)
    path = _guest_wizard_file_path(request)
    if path and path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    legacy_payload = request.session.get(_guest_wizard_session_key())
    if isinstance(legacy_payload, dict):
        _save_demand_wizard(request, None, dict(legacy_payload))
        request.session.pop(_guest_wizard_session_key(), None)
        return dict(legacy_payload)
    return None


def _save_demand_wizard(request: Request, user_id: int | None, wizard: dict[str, Any]) -> None:
    if user_id is not None:
        save_demand_wizard_record(user_id, wizard)
        return
    path = _guest_wizard_file_path(request, create=True)
    if not path:
        return
    path.write_text(json.dumps(wizard, ensure_ascii=False), encoding="utf-8")
    request.session.pop(_guest_wizard_session_key(), None)


def _clear_demand_wizard(request: Request, user_id: int | None) -> None:
    if user_id is not None:
        clear_demand_wizard_record(user_id)
        return
    path = _guest_wizard_file_path(request)
    if path and path.exists():
        try:
            path.unlink()
        except OSError:
            pass
    request.session.pop(_guest_wizard_storage_key(), None)
    request.session.pop(_guest_wizard_session_key(), None)


def _complete_pending_guest_publication(request: Request, user: Any) -> str | None:
    wizard = _load_demand_wizard(request, None)
    if not wizard or wizard.get("step") != "review" or not wizard.get("awaiting_auth"):
        return None
    try:
        state = _session_to_state(wizard.get("state", {}))
        if wizard.get("flow_version") == "simple_free_text":
            draft = DemandResult.model_validate(wizard.get("draft") or {})
        else:
            draft = DemandResult.model_validate(wizard.get("review_demand") or {})
        save_web_demand_from_agent(user.id, draft, state)
    except Exception:
        return None
    _clear_demand_wizard(request, None)
    return "/app/chats"


def _field_display_label(field_name: str, field_description: str = "") -> str:
    question = get_field_prompt(field_name, field_description=field_description).get("question", "")
    normalized = question.strip().strip("¿").strip("?").strip()
    if normalized:
        return normalized[:1].lower() + normalized[1:]
    return field_name.replace("_", " ")


def _detail_question_label(question: str) -> str:
    text = str(question or "").strip()
    if not text:
        return ""
    text = text.replace("(Opcional)", "").strip()
    text = text.lstrip("¿").rstrip("?").strip()
    if not text:
        return ""
    return text[:1].upper() + text[1:]


def _field_errors_from_response(response: LLMResponse, state: SessionState) -> dict[str, str]:
    errors: dict[str, str] = {}
    for issue in response.validation_issues:
        field_name = issue.get("field_name", "")
        message = issue.get("message", "")
        if field_name and message:
            errors[field_name] = message
    for field_name in response.required_missing_fields:
        if field_name in errors:
            continue
        current_value = state.known_fields.get(field_name)
        if current_value:
            errors[field_name] = "He recibido un valor para este campo, pero todavía no he podido validarlo correctamente. Revísalo o concreta un poco más."
    return errors


def _compact_review_state(state: SessionState) -> SessionState:
    return SessionState(
        original_text=state.original_text,
        intent_domain=state.intent_domain,
        intent_type=state.intent_type,
        summary=state.summary,
        iteration=state.iteration,
        telegram_user_id=state.telegram_user_id,
    )


def _normalization_debug_enabled() -> bool:
    flags = [
        os.getenv("DEBUG_NORMALIZATION", ""),
        os.getenv("SHOW_NORMALIZED_JSON", ""),
    ]
    return any(value.strip().lower() in {"1", "true", "yes", "on"} for value in flags)


def _published_confirmation_message(state: SessionState, review_demand: dict[str, Any] | None = None) -> str:
    raw_text = (state.original_text or "").strip()
    lowered = raw_text.lower()
    intent_domain = (state.intent_domain or (review_demand or {}).get("intent_domain") or "").strip()
    intent_type = (state.intent_type or (review_demand or {}).get("intent_type") or "").strip()

    if any(token in lowered for token in ("pizza", "pizzeria", "restaurante", "cenar")):
        if any(token in lowered for token in ("esta noche", "esta tarde", "hoy")):
            return "Prepárate para recibir propuestas de restaurantes para esta noche."
        return "Prepárate para recibir propuestas de restaurantes."
    if intent_domain == "education":
        return "Prepárate para recibir propuestas de profesores y academias."
    if intent_domain in {"technical_repairs", "home_services", "construction"}:
        return "Prepárate para recibir propuestas de profesionales para ayudarte con esto."
    if intent_domain in {"travel", "vacation_real_estate"} or intent_type in {"hotel_booking", "tourist_apartment", "rural_house"}:
        return "Prepárate para recibir propuestas de alojamiento y viaje."
    if intent_domain in {"automotive", "used_products", "new_products"}:
        return "Prepárate para recibir propuestas relacionadas con tu búsqueda."
    return "Prepárate para recibir propuestas de personas y empresas interesadas."


def _home_pagination(
    page: int,
    total_pages: int,
    q: str,
    location: str,
    zone_filter: Optional[dict[str, Any]],
    saved_filter_id: int | None,
) -> dict[str, Any]:
    def page_url(target_page: int) -> str:
        params: dict[str, Any] = {"page": target_page}
        if q:
            params["q"] = q
        if location:
            params["location"] = location
        if zone_filter:
            params["location_zone_json"] = json.dumps(_compact_zone_for_query(zone_filter), ensure_ascii=False)
        if saved_filter_id:
            params["saved_filter_id"] = saved_filter_id
        return f"/?{urlencode(params)}"

    pages = []
    start = max(1, page - 2)
    end = min(total_pages, page + 2)
    for item in range(start, end + 1):
        pages.append({"number": item, "url": page_url(item), "current": item == page})

    return {
        "page": page,
        "total_pages": total_pages,
        "prev_url": page_url(page - 1) if page > 1 else None,
        "next_url": page_url(page + 1) if page < total_pages else None,
        "pages": pages,
    }


def _wizard_return_path(wizard_mode: str, target_demand_id: int | None) -> str:
    if wizard_mode == "edit" and target_demand_id:
        return f"/demands/{target_demand_id}/edit"
    return "/demands/new"


def post_no_trailing_slash(app: FastAPI, path: str):
    def decorator(func):
        return app.post(path)(func)

    return decorator
