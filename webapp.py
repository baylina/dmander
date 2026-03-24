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
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen
from typing import Any, Optional
from datetime import datetime, date

from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from psycopg2 import IntegrityError
from starlette.middleware.sessions import SessionMiddleware

from agent import DemandAgent
from database import (
    admin_delete_demand,
    authenticate_user,
    clear_demand_wizard as clear_demand_wizard_record,
    create_offer_message,
    create_offer,
    create_user,
    delete_filter,
    delete_web_demand,
    get_editable_demand,
    get_dashboard_data,
    get_demand_detail,
    get_demand_wizard as get_demand_wizard_record,
    get_notification_summary,
    get_offer_thread,
    get_offers_for_owner,
    get_or_create_oauth_user,
    get_public_demands,
    get_saved_filter,
    get_user_by_id,
    init_db,
    list_admin_demands,
    list_saved_filters,
    save_web_demand_from_agent,
    save_demand_wizard as save_demand_wizard_record,
    save_filter,
    update_filter,
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

try:
    from authlib.integrations.starlette_client import OAuth
except ImportError:  # pragma: no cover - depende de requirements
    OAuth = None


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

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
        intent_type: str = "",
        intent_domains_json: str = "",
        intent_types_json: str = "",
        saved_filter_id: str = "",
        page: int = 1,
    ) -> HTMLResponse:
        current_user = _get_current_user(request)
        current_saved_filter = None
        selected_saved_filter_id = int(saved_filter_id) if str(saved_filter_id).strip().isdigit() else None
        selected_domains = _parse_json_string_list(intent_domains_json)
        selected_types = _parse_json_string_list(intent_types_json)
        has_explicit_search_inputs = bool(
            q.strip()
            or location.strip()
            or location_zone_json.strip()
            or intent_type.strip()
            or selected_domains
            or selected_types
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
                intent_type = current_saved_filter.get("intent_type", "")
                selected_domains, selected_types = _saved_filter_categories(current_saved_filter)
        zone_filter = _parse_zone_json(location_zone_json)
        effective_intent_type_filter = "" if selected_types else (intent_type or "").strip()
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
            intent_type=effective_intent_type_filter,
            zone_filter=zone_filter,
            intent_domains=selected_domains,
            intent_types=selected_types,
        )
        has_active_search = bool(
            q.strip()
            or location.strip()
            or effective_intent_type_filter
            or has_zone_filter
            or selected_saved_filter_id
            or selected_domains
            or selected_types
        )
        effective_location_text = "" if has_zone_filter else location
        all_demands = get_public_demands(
            q,
            effective_location_text,
            effective_intent_type_filter,
            current_user.id if current_user else None,
            zone_filter=zone_filter,
            intent_domains=selected_domains,
            intent_types=selected_types,
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
                "title": "Ver todas",
                "search_filters": {
                    "q": q,
                    "location": location,
                    "intent_type": effective_intent_type_filter,
                    "intent_domains": selected_domains,
                    "intent_types": selected_types,
                    "intent_domains_json": json.dumps(selected_domains, ensure_ascii=False),
                    "intent_types_json": json.dumps(selected_types, ensure_ascii=False),
                    "category_summary": _category_summary(selected_domains, selected_types),
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
                "category_catalog": _category_catalog(),
                "pagination": _home_pagination(
                    page,
                    total_pages,
                    q,
                    location,
                    effective_intent_type_filter,
                    zone_filter,
                    selected_saved_filter_id,
                    selected_domains,
                    selected_types,
                ),
            },
        )

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request) -> HTMLResponse:
        return _render(request, "login.html", {"title": "Acceder"})

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
            _flash(request, "Email o contraseña incorrectos.", "error")
            return _redirect("/login")

        request.session["user_id"] = user.id
        _flash(request, f"Bienvenido de nuevo, {user.full_name}.", "success")
        return _redirect("/my-demands")

    @app.get("/signup", response_class=HTMLResponse)
    async def signup_page(request: Request) -> HTMLResponse:
        return _render(request, "signup.html", {"title": "Crear cuenta"})

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

        request.session["user_id"] = user.id
        _flash(request, f"Cuenta creada. Hola, {user.full_name}.", "success")
        return _redirect("/my-demands")

    @app.post("/logout")
    async def logout(request: Request, csrf_token: str = Form(...)) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        request.session.clear()
        return _redirect("/")

    @app.get("/dashboard")
    async def dashboard_redirect() -> RedirectResponse:
        return _redirect("/my-demands")

    @app.get("/my-demands", response_class=HTMLResponse)
    async def my_demands_page(request: Request, selected_demand_id: int | None = None, selected_offer_id: int | None = None) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a tus demandas.", "error")
            return _redirect("/login")
        data = get_dashboard_data(user.id)
        demand_view = _build_demands_workspace(data["my_demands_active"], selected_demand_id, selected_offer_id)
        archived_view = _build_demands_workspace(data["my_demands_archived"], None, None)
        return _render(
            request,
            "my_demands.html",
            {
                "title": "Mis Demandas",
                "my_demands_active": data["my_demands_active"],
                "my_demands_archived": data["my_demands_archived"],
                "demand_view": demand_view,
                "archived_demand_view": archived_view,
                "my_demands_active_json": _to_json_ready(data["my_demands_active"]),
            },
        )

    @app.get("/admin/demands", response_class=HTMLResponse)
    async def admin_demands_page(request: Request) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a esta vista.", "error")
            return _redirect("/login")
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
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
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a esta vista.", "error")
            return _redirect("/login")
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        return _render(
            request,
            "admin_index.html",
            {
                "title": "Utilidades internas",
                "admin_tools": [
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

    @app.post("/admin/demands/{demand_id}/delete")
    async def admin_delete_demand_route(
        request: Request,
        demand_id: int,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a esta vista.", "error")
            return _redirect("/login")
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        if admin_delete_demand(demand_id):
            _flash(request, "Demanda eliminada junto con sus ofertas y conversaciones.", "success")
        else:
            _flash(request, "No he podido eliminar esa demanda.", "error")
        return _redirect("/admin/demands")

    @app.get("/admin/schema", response_class=HTMLResponse)
    async def admin_schema_page(request: Request) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a esta vista.", "error")
            return _redirect("/login")
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
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
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a esta vista.", "error")
            return _redirect("/login")
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        return _render(
            request,
            "admin_schema_fields.html",
            {
                "title": "Tipos de campo",
                **schema_editor_context(),
            },
        )

    @app.get("/admin/schema/fields")
    async def admin_schema_fields_legacy_redirect() -> RedirectResponse:
        return _redirect("/admin/schema/types")

    @app.post("/admin/schema/domains/create")
    async def admin_schema_create_domain(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        try:
            create_domain(str(form.get("code", "")).strip(), str(form.get("name", "")).strip())
            _flash(request, "Dominio creado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/domains/update")
    async def admin_schema_update_domain(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
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
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        try:
            delete_domain(str(form.get("code", "")).strip())
            _flash(request, "Dominio eliminado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/domains/reorder")
    async def admin_schema_reorder_domains(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        try:
            reorder_domains(_form_text_list(form, "domain_order"))
            _flash(request, "Orden de dominios actualizado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/create")
    async def admin_schema_create_intent(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        try:
            create_intent_type(_intent_payload_from_form(form))
            _flash(request, "intent_type creado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/reorder")
    async def admin_schema_reorder_intents(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
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
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        try:
            update_intent_type(str(form.get("original_intent_type", "")).strip(), _intent_payload_from_form(form))
            _flash(request, "intent_type actualizado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.post("/admin/schema/intents/delete")
    async def admin_schema_delete_intent(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        if not _normalization_debug_enabled():
            raise HTTPException(status_code=404, detail="Vista de depuración no disponible")
        try:
            delete_intent_type(str(form.get("intent_type", "")).strip())
            _flash(request, "intent_type eliminado.", "success")
        except SchemaEditorError as exc:
            _flash(request, str(exc), "error")
        return _redirect("/admin/schema")

    @app.get("/my-offers", response_class=HTMLResponse)
    async def my_offers_page(request: Request, selected_offer_id: int | None = None) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para acceder a tus ofertas.", "error")
            return _redirect("/login")
        data = get_dashboard_data(user.id)
        offers_view = _build_offers_workspace(data["my_offers_active"], selected_offer_id)
        archived_offers_view = _build_offers_workspace(data["my_offers_archived"], None)
        return _render(
            request,
            "my_offers.html",
            {
                "title": "Mis Ofertas",
                "my_offers_active": data["my_offers_active"],
                "my_offers_archived": data["my_offers_archived"],
                "offers_view": offers_view,
                "archived_offers_view": archived_offers_view,
                "my_offers_active_json": _to_json_ready(data["my_offers_active"]),
            },
        )

    @app.get("/api/offers/{offer_id}/thread")
    async def offer_thread_api(request: Request, offer_id: int) -> dict[str, Any]:
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        thread = get_offer_thread(offer_id, user.id)
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
        intent_type: str = Form(""),
        intent_domains_json: str = Form(""),
        intent_types_json: str = Form(""),
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
        selected_domains = _parse_json_string_list(intent_domains_json)
        selected_types = _parse_json_string_list(intent_types_json)
        effective_intent_type = (selected_types[0] if selected_types else intent_type).strip()
        should_update = filter_id is not None and save_mode == "update"
        saved_filter_target_id: int | None = None
        if should_update:
            if update_filter(
                user.id,
                filter_id,
                name,
                query_text,
                location,
                effective_intent_type,
                zone_filter=zone_filter,
                intent_domains=selected_domains,
                intent_types=selected_types,
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
                effective_intent_type,
                zone_filter=zone_filter,
                intent_domains=selected_domains,
                intent_types=selected_types,
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
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        return get_notification_summary(user.id)

    @app.get("/api/workspace")
    async def workspace_api(request: Request) -> dict[str, Any]:
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        data = get_dashboard_data(user.id)
        return _to_json_ready(data)

    @app.get("/demands/new", response_class=HTMLResponse)
    async def new_demand_page(request: Request) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para publicar una demanda.", "error")
            return _redirect("/login")
        if request.query_params.get("fresh") in {"1", "true", "yes"}:
            _clear_demand_wizard(user.id)
        wizard = _load_demand_wizard(user.id)
        active_wizard = _inflate_wizard_for_view(wizard) if wizard and wizard.get("mode") == "create" else None
        return _render(
            request,
            "new_demand.html",
            {
                "title": "Nueva demanda",
                "wizard_mode": "create",
                "active_wizard": active_wizard,
                "submit_action": "/demands",
                "submit_label": "Analizar y continuar",
                "page_title": "Qué necesitas?",
                "page_note": "",
                "initial_text": (active_wizard or {}).get("state", {}).get("original_text", ""),
                "edit_demand": None,
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
        wizard = _load_demand_wizard(user.id)
        active_wizard = _inflate_wizard_for_view(wizard) if wizard and wizard.get("mode") == "edit" and wizard.get("target_demand_id") == demand_id else None
        return _render(
            request,
            "new_demand.html",
            {
                "title": "Editar demanda",
                "wizard_mode": "edit",
                "active_wizard": active_wizard,
                "submit_action": f"/demands/{demand_id}/edit",
                "submit_label": "Analizar y actualizar",
                "page_title": "Edita tu demanda",
                "page_note": "Reescribe la necesidad y en el siguiente paso podrás revisar todos los campos detectados antes de guardar.",
                "initial_text": (active_wizard or {}).get("state", {}).get("original_text", "") or demand.get("original_text") or demand.get("normalized_payload", {}).get("description", ""),
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
        if not user:
            _flash(request, "Necesitas iniciar sesión para publicar una demanda.", "error")
            return _redirect("/login")

        if len(demand_text.strip()) < 8:
            _flash(request, "Escribe tu demanda con un poco más de detalle.", "error")
            return _redirect("/demands/new")

        state = SessionState(original_text=demand_text.strip())
        return _prepare_demand_questionnaire(request, user.id, agent, state, wizard_mode="create")

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
        if len(demand_text.strip()) < 8:
            _flash(request, "Escribe tu demanda con un poco más de detalle.", "error")
            return _redirect(f"/demands/{demand_id}/edit")

        state = SessionState(original_text=demand_text.strip())
        return _prepare_demand_questionnaire(request, user.id, agent, state, wizard_mode="edit", target_demand_id=demand_id)

    @app.post("/demands/review")
    async def review_demand_route(request: Request) -> RedirectResponse:
        form = await request.form()
        _validate_csrf(request, str(form.get("csrf_token", "")))
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para continuar tu demanda.", "error")
            return _redirect("/login")

        wizard = _load_demand_wizard(user.id)
        if not wizard:
            _flash(request, "No hay ninguna demanda pendiente de completar.", "error")
            return _redirect("/demands/new")

        wizard_view = _inflate_wizard_for_view(wizard)
        state = _session_to_state(wizard["state"])
        field_entries = list(wizard_view.get("field_entries", []))
        field_errors: dict[str, str] = {}
        submitted_answers: dict[str, Any] = {}

        for entry in field_entries:
            field_name = entry.get("field_name", "")
            control = entry.get("control", {})
            submitted_answers[field_name] = _extract_field_answer(form, field_name, control)

        for entry in field_entries:
            field_name = entry.get("field_name", "")
            control = entry.get("control", {})
            answer = submitted_answers.get(field_name)
            existing_value = state.known_fields.get(field_name)
            option_error = _validate_control_answer(answer, control, field_name, state.known_fields, submitted_answers)
            if option_error:
                field_errors[field_name] = option_error
                continue
            if answer:
                if control.get("kind") == "zone_selector" and isinstance(answer, dict):
                    state.known_fields[field_name] = answer.get("label", "")
                    state.known_fields["location_json"] = answer
                    state.known_fields["location_value"] = answer.get("label", "")
                    state.known_fields["location"] = answer.get("label", "")
                elif control.get("kind") == "budget_range" and isinstance(answer, dict):
                    state.known_fields[field_name] = answer
                    state.known_fields["budget_min"] = answer.get("min", "")
                    state.known_fields["budget_max"] = answer.get("max", "")
                else:
                    state.known_fields[field_name] = answer
                    if field_name == "budget_max":
                        state.known_fields["budget_max"] = answer
                state.questions_asked.append(entry.get("question", field_name))
                state.user_answers.append(_format_answer_for_review(answer, control))
            elif entry.get("category") == "required" and not existing_value:
                field_errors[field_name] = "Este campo es obligatorio antes de publicar la demanda."

        if field_errors:
            _flash(request, "Completa los campos obligatorios marcados antes de publicar la demanda.", "error")
            _save_demand_wizard(
                user.id,
                {
                **wizard,
                "state": _state_to_session(state),
                "field_errors": field_errors,
                },
            )
            return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))

        return _prepare_demand_review(
            request,
            user.id,
            agent,
            state,
            wizard_mode=wizard.get("mode", "create"),
            target_demand_id=wizard.get("target_demand_id"),
            asked_entries=field_entries,
        )

    @app.post("/demands/wizard/edit-text")
    async def demand_wizard_edit_text(
        request: Request,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            return _redirect("/login")
        wizard = _load_demand_wizard(user.id)
        if not wizard:
            return _redirect("/demands/new")
        wizard["step"] = "text"
        _save_demand_wizard(user.id, wizard)
        return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))

    @app.post("/demands/wizard/back-to-questions")
    async def demand_wizard_back_to_questions(
        request: Request,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            return _redirect("/login")
        wizard = _load_demand_wizard(user.id)
        if not wizard:
            return _redirect("/demands/new")
        wizard["step"] = "questions"
        _save_demand_wizard(user.id, wizard)
        return _redirect(_wizard_return_path(wizard.get("mode", "create"), wizard.get("target_demand_id")))

    @app.get("/demands/{demand_id}", response_class=HTMLResponse)
    async def demand_detail_redirect(demand_id: int) -> RedirectResponse:
        return _redirect(f"/#demand-{demand_id}")

    @app.post("/demands/{demand_id}/offers")
    async def create_offer_route(
        request: Request,
        demand_id: int,
        message: str = Form(...),
        redirect_to: str = Form(""),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        target = redirect_to.strip() or f"/#demand-{demand_id}"
        if not user:
            _flash(request, "Necesitas iniciar sesión para enviar una oferta.", "error")
            return _redirect("/login")

        if len(message.strip()) < 12:
            _flash(request, "La oferta debe explicar qué ofreces con algo más de detalle.", "error")
            return _redirect(target)

        try:
            create_offer(
                demand_id=demand_id,
                supplier_user_id=user.id,
                message=message,
            )
        except ValueError as exc:
            _flash(request, str(exc), "error")
            return _redirect(target)

        _flash(request, "Tu oferta ha quedado registrada.", "success")
        return _redirect("/my-offers")

    @app.get("/offers/{offer_id}", response_class=HTMLResponse)
    async def offer_thread_page(request: Request, offer_id: int) -> HTMLResponse:
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para ver esta conversación.", "error")
            return _redirect("/login")

        thread = get_offer_thread(offer_id, user.id)
        if not thread:
            raise HTTPException(status_code=404, detail="Conversación no encontrada")

        return _render(
            request,
            "offer_thread.html",
            {
                "title": f"Conversación · {thread['demand_summary']}",
                "thread": thread,
            },
        )

    @app.post("/offers/{offer_id}/messages")
    async def offer_message_route(
        request: Request,
        offer_id: int,
        body: str = Form(...),
        redirect_to: str = Form(""),
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        target = redirect_to.strip() or request.headers.get("referer") or f"/offers/{offer_id}"
        if not user:
            _flash(request, "Necesitas iniciar sesión para responder en la conversación.", "error")
            return _redirect("/login")
        if not body.strip():
            _flash(request, "El mensaje no puede estar vacío.", "error")
            return _redirect(target)
        if not create_offer_message(offer_id, user.id, body):
            _flash(request, "No se ha podido enviar el mensaje.", "error")
            return _redirect("/my-demands")
        return _redirect(target)

    @app.post("/api/offers/{offer_id}/messages")
    async def offer_message_api(
        request: Request,
        offer_id: int,
        body: str = Form(...),
        csrf_token: str = Form(...),
    ) -> dict[str, Any]:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Autenticación requerida")
        if not body.strip():
            raise HTTPException(status_code=400, detail="Mensaje vacío")
        if not create_offer_message(offer_id, user.id, body):
            raise HTTPException(status_code=400, detail="No se ha podido enviar el mensaje")
        thread = get_offer_thread(offer_id, user.id)
        if not thread:
            raise HTTPException(status_code=404, detail="Conversación no encontrada")
        return {"thread": _to_json_ready(thread), "notifications": get_notification_summary(user.id)}

    @app.post("/demands/{demand_id}/delete")
    async def delete_demand_route(
        request: Request,
        demand_id: int,
        csrf_token: str = Form(...),
    ) -> RedirectResponse:
        _validate_csrf(request, csrf_token)
        user = _get_current_user(request)
        if not user:
            _flash(request, "Necesitas iniciar sesión para borrar una demanda.", "error")
            return _redirect("/login")

        if delete_web_demand(demand_id, user.id):
            _flash(request, "La demanda abierta se ha eliminado.", "success")
        else:
            _flash(request, "Solo puedes borrar tus demandas abiertas.", "error")
        return _redirect("/my-demands")

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
        request.session["user_id"] = user.id
        _flash(request, f"Sesión iniciada con {SOCIAL_PROVIDERS[provider]['label']}.", "success")
        return _redirect("/my-demands")

    return app


def _build_oauth_registry():
    if OAuth is None:
        return None

    oauth = OAuth()
    enabled = {}
    for provider, config in SOCIAL_PROVIDERS.items():
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


def _get_current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return get_user_by_id(user_id)


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
        parsed.get("geojson")
        or parsed.get("bbox")
        or (
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
    intent_type: str,
    zone_filter: Optional[dict[str, Any]],
    intent_domains: Optional[list[str]] = None,
    intent_types: Optional[list[str]] = None,
) -> bool:
    if not saved_filter:
        return False
    saved_domains, saved_types = _saved_filter_categories(saved_filter)
    return (
        (q or "").strip() == (saved_filter.get("query_text") or "").strip()
        and (location or "").strip() == (saved_filter.get("location") or "").strip()
        and saved_domains == _normalize_string_list(intent_domains)
        and saved_types == _normalize_string_list(intent_types)
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
        if os.getenv(f"{provider.upper()}_CLIENT_ID") and os.getenv(f"{provider.upper()}_CLIENT_SECRET"):
            providers.append({"key": provider, "label": config["label"], "icon": config["icon"]})
    return providers


def _render(request: Request, template_name: str, context: dict[str, Any]) -> HTMLResponse:
    current_user = _get_current_user(request)
    wizard = _load_demand_wizard(current_user.id) if current_user else None
    if wizard:
        wizard = _inflate_wizard_for_view(wizard)
    notification_summary = (
        get_notification_summary(current_user.id) if current_user else {"my_demands_unread": 0, "my_offers_unread": 0, "items": []}
    )
    return templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "current_user": current_user,
            "flash_messages": _pop_flashes(request),
            "csrf_token": _get_csrf_token(request),
            "oauth_providers": _enabled_social_providers(),
            "demand_wizard": wizard,
            "debug_normalization": _normalization_debug_enabled(),
            "notification_summary": notification_summary,
            **context,
        },
    )


def _keyword_suggestions() -> list[str]:
    registry = get_master_schema_registry()
    suggestions: list[str] = []
    seen: set[str] = set()
    for domain_name in registry.domains.values():
        candidate = str(domain_name).strip()
        key = candidate.lower()
        if candidate and key not in seen:
            suggestions.append(candidate)
            seen.add(key)
    for schema in registry.intent_schemas.values():
        candidate = str(schema.display_name).strip()
        key = candidate.lower()
        if candidate and key not in seen:
            suggestions.append(candidate)
            seen.add(key)
    return sorted(suggestions)


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


def _saved_filter_categories(saved_filter: Optional[dict[str, Any]]) -> tuple[list[str], list[str]]:
    if not saved_filter:
        return [], []
    domains = _normalize_string_list(saved_filter.get("intent_domains") or [])
    types = _normalize_string_list(saved_filter.get("intent_types") or [])
    return domains, types


def _category_catalog() -> list[dict[str, Any]]:
    registry = get_master_schema_registry()
    items: list[dict[str, Any]] = []
    for domain_code, domain_name in sorted(registry.domains.items(), key=lambda item: item[1].lower()):
        types = [
            {
                "intent_type": schema.intent_type,
                "display_name": schema.display_name,
            }
            for schema in registry.intent_schemas.values()
            if schema.intent_domain == domain_code
        ]
        types.sort(key=lambda item: item["display_name"].lower())
        items.append(
            {
                "code": domain_code,
                "name": domain_name,
                "intent_types": types,
            }
        )
    return items


def _category_summary(selected_domains: Optional[list[str]], selected_types: Optional[list[str]]) -> str:
    domains = _normalize_string_list(selected_domains)
    types = _normalize_string_list(selected_types)
    if not domains and not types:
        return "Todas"
    catalog = {item["code"]: item["name"] for item in _category_catalog()}
    registry = get_master_schema_registry()
    if types:
        first_schema = registry.resolve_intent_schema(types[0])
        first_domain_name = catalog.get(first_schema.intent_domain, first_schema.intent_domain)
        if len(types) == 1:
            return f"{first_domain_name} / {first_schema.display_name}"
        if len(domains) == 1:
            return f"{catalog.get(domains[0], domains[0])} + {len(types) - 1}"
        return f"{len(types)} tipos"
    if len(domains) == 1:
        return f"{catalog.get(domains[0], domains[0])}"
    return f"{len(domains)} dominios"


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


def _build_offers_workspace(offers: list[dict[str, Any]], selected_offer_id: int | None) -> dict[str, Any]:
    chosen_offer = None
    if selected_offer_id:
        chosen_offer = next((o for o in offers if o["offer_id"] == selected_offer_id), None)
    return {"selected_offer": chosen_offer}


def _prepare_demand_questionnaire(
    request: Request,
    user_id: int,
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
            user_id,
            agent,
            state,
            response,
            wizard_mode=wizard_mode,
            target_demand_id=target_demand_id,
        )
    _save_demand_wizard(user_id, wizard_data)
    return _redirect(_wizard_return_path(wizard_mode, target_demand_id))


def _prepare_demand_review(
    request: Request,
    user_id: int,
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
        response.next_question_field = target
        response.next_question = get_field_prompt(
            target,
            state.original_text,
            draft.intent_type,
            draft.intent_domain,
        )["question"]
        response.enough_information = False
    return response, draft


def _finalize_published_demand(
    request: Request,
    user_id: int,
    agent: DemandAgent,
    state: SessionState,
    response: LLMResponse,
    wizard_mode: str = "create",
    target_demand_id: int | None = None,
    asked_entries: list[dict[str, Any]] | None = None,
    prebuilt_demand: DemandResult | None = None,
) -> HTMLResponse | RedirectResponse:
    demand = prebuilt_demand or agent.build_final_demand(state, response)
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
    _clear_demand_wizard(user_id)
    return _render(
        request,
        "new_demand.html",
        {
            "title": "Nueva demanda" if wizard_mode == "create" else "Editar demanda",
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
    state = _session_to_state(view.get("state", {}))
    registry = get_master_schema_registry()
    if view.get("step") == "review":
        response = _response_from_wizard(view)
        schema = registry.resolve_intent_schema(response.intent_type)
        view["field_entries"] = []
        view["answered_fields"] = list(view.get("answered_fields", []))
        view["schema_domain_display_name"] = registry.domains.get(schema.intent_domain, schema.intent_domain)
        view["schema_display_name"] = schema.display_name
        view["schema_required_fields"] = list(schema.active_required_fields(state.known_fields))
        view["schema_debug_outline"] = _schema_debug_outline(schema)
        view["published_message"] = _published_confirmation_message(state, view.get("review_demand", {}))
    else:
        response = _response_from_wizard(view)
        schema = registry.resolve_intent_schema(response.intent_type)
        view["field_entries"] = _build_field_entries(state, response, view.get("field_errors", {}))
        view["answered_fields"] = []
        view["schema_domain_display_name"] = registry.domains.get(schema.intent_domain, schema.intent_domain)
        view["schema_display_name"] = schema.display_name
        view["schema_required_fields"] = list(schema.active_required_fields(state.known_fields))
        view["schema_debug_outline"] = _schema_debug_outline(schema)
    return view


def _schema_debug_outline(schema) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []

    def append_item(name: str, mode: str) -> None:
        marker = "*" if mode == "always" else "o" if mode == "conditional" else ""
        items.append({"name": name, "marker": marker})

    append_item("location", schema.location_policy.required_mode)
    append_item("budget", schema.budget_policy.required_mode)
    for field in schema.fields:
        append_item(field.name, field.required)
    return items


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

    if schema.location_required_for(merged_known) and "location_value" not in ordered_required:
        ordered_required.insert(0, "location_value")

    budget_entry_name = _budget_entry_name(schema)
    if budget_entry_name and budget_entry_name not in ordered_required and budget_entry_name not in optional_universe:
        if schema.budget_required_for(merged_known):
            insert_at = 1 if schema.location_required_for(merged_known) else 0
            ordered_required.insert(insert_at, budget_entry_name)
        else:
            optional_universe.insert(0, budget_entry_name)

    latent_conditional_required: list[str] = []
    for field in schema.fields:
        if field.required != "conditional":
            continue
        if field.name not in ordered_required and field.name not in latent_conditional_required:
            latent_conditional_required.append(field.name)
    if schema.location_policy.required_mode == "conditional" and "location_value" not in ordered_required and "location_value" not in latent_conditional_required:
        latent_conditional_required.insert(0, "location_value")
    if schema.budget_policy.required_mode == "conditional" and budget_entry_name and budget_entry_name not in ordered_required and budget_entry_name not in latent_conditional_required:
        latent_conditional_required.append(budget_entry_name)

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
    if not schema.budget_fix_or_range:
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
    prompt = get_field_prompt(field_name, raw_text, schema.intent_type, schema.intent_domain)
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
    base = get_field_prompt("location_value", raw_text, schema.intent_type, schema.intent_domain)
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
    if not (
        payload.get("label")
        or payload.get("geojson")
        or payload.get("bbox")
        or (
            payload.get("center", {}).get("lat") is not None
            and payload.get("center", {}).get("lon") is not None
        )
    ):
        return default_zone_payload()
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


def _load_demand_wizard(user_id: int) -> Optional[dict[str, Any]]:
    return get_demand_wizard_record(user_id)


def _save_demand_wizard(user_id: int, wizard: dict[str, Any]) -> None:
    save_demand_wizard_record(user_id, wizard)


def _clear_demand_wizard(user_id: int) -> None:
    clear_demand_wizard_record(user_id)


def _field_display_label(field_name: str) -> str:
    question = get_field_prompt(field_name).get("question", "")
    normalized = question.strip().strip("¿").strip("?").strip()
    if normalized:
        return normalized[:1].lower() + normalized[1:]
    return field_name.replace("_", " ")


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
    intent_type: str,
    zone_filter: Optional[dict[str, Any]],
    saved_filter_id: int | None,
    intent_domains: Optional[list[str]] = None,
    intent_types: Optional[list[str]] = None,
) -> dict[str, Any]:
    def page_url(target_page: int) -> str:
        params: dict[str, Any] = {"page": target_page}
        if q:
            params["q"] = q
        if location:
            params["location"] = location
        if intent_type:
            params["intent_type"] = intent_type
        if intent_domains:
            params["intent_domains_json"] = json.dumps(_normalize_string_list(intent_domains), ensure_ascii=False)
        if intent_types:
            params["intent_types_json"] = json.dumps(_normalize_string_list(intent_types), ensure_ascii=False)
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
