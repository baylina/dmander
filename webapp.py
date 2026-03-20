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
from llm_client import OpenAIClient
from master_schema import get_master_schema_registry
from models import DemandResult, LLMResponse, SessionState
from normalization_rules import dynamic_required_fields, get_field_prompt
from field_normalizers import parse_date_value
from field_specs import is_budget_field, is_date_field, is_location_field
from location_geometry import zone_has_geometry, zones_intersect
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
        saved_filter_id: str = "",
        page: int = 1,
    ) -> HTMLResponse:
        current_user = _get_current_user(request)
        current_saved_filter = None
        selected_saved_filter_id = int(saved_filter_id) if str(saved_filter_id).strip().isdigit() else None
        has_explicit_search_inputs = bool(
            q.strip()
            or location.strip()
            or location_zone_json.strip()
            or intent_type.strip()
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
            intent_type=intent_type,
            zone_filter=zone_filter,
        )
        has_active_search = bool(q.strip() or location.strip() or intent_type.strip() or has_zone_filter or selected_saved_filter_id)
        effective_location_text = "" if has_zone_filter else location
        all_demands = get_public_demands(
            q,
            effective_location_text,
            intent_type,
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
                "title": "Ver todas",
                "search_filters": {
                    "q": q,
                    "location": location,
                    "intent_type": intent_type,
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
                "pagination": _home_pagination(page, total_pages, q, location, intent_type, zone_filter, selected_saved_filter_id),
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
            if update_filter(user.id, filter_id, name, query_text, location, intent_type, zone_filter=zone_filter):
                saved_filter_target_id = filter_id
                _flash(request, "Filtro actualizado.", "success")
            else:
                _flash(request, "No he podido actualizar ese filtro.", "error")
        else:
            saved_filter_target_id = save_filter(user.id, name, query_text, location, intent_type, zone_filter=zone_filter)
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

        for entry in field_entries:
            field_name = entry.get("field_name", "")
            control = entry.get("control", {})
            answer = _extract_field_answer(form, field_name, control)
            existing_value = state.known_fields.get(field_name)
            option_error = _validate_control_answer(answer, control, field_name)
            if option_error:
                field_errors[field_name] = option_error
                continue
            if answer:
                if control.get("kind") == "zone_selector" and isinstance(answer, dict):
                    state.known_fields[field_name] = answer.get("label", "")
                    state.known_fields["location_json"] = answer
                    state.known_fields["location_value"] = answer.get("label", "")
                    state.known_fields["location"] = answer.get("label", "")
                else:
                    state.known_fields[field_name] = answer
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
) -> bool:
    if not saved_filter:
        return False
    return (
        (q or "").strip() == (saved_filter.get("query_text") or "").strip()
        and (location or "").strip() == (saved_filter.get("location") or "").strip()
        and (intent_type or "").strip() == (saved_filter.get("intent_type") or "").strip()
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
    try:
        response = agent.analyze(state)
    except RuntimeError as exc:
        _flash(request, f"No he podido revisar la demanda: {exc}", "error")
        return _redirect(_wizard_return_path(wizard_mode, target_demand_id))

    _apply_wizard_inference(state, response)
    agent.update_state(state, response)

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
    )


def _finalize_published_demand(
    request: Request,
    user_id: int,
    agent: DemandAgent,
    state: SessionState,
    response: LLMResponse,
    wizard_mode: str = "create",
    target_demand_id: int | None = None,
    asked_entries: list[dict[str, Any]] | None = None,
) -> HTMLResponse | RedirectResponse:
    demand = agent.build_final_demand(state, response)
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
    if view.get("step") == "review":
        response = _response_from_wizard(view)
        schema = get_master_schema_registry().resolve_intent_schema(response.intent_type)
        view["field_entries"] = []
        view["answered_fields"] = list(view.get("answered_fields", []))
        view["schema_display_name"] = schema.display_name
        view["schema_required_fields"] = list(schema.required_fields)
        view["published_message"] = _published_confirmation_message(state, view.get("review_demand", {}))
    else:
        response = _response_from_wizard(view)
        schema = get_master_schema_registry().resolve_intent_schema(response.intent_type)
        view["field_entries"] = _build_field_entries(state, response, view.get("field_errors", {}))
        view["answered_fields"] = []
        view["schema_display_name"] = schema.display_name
        view["schema_required_fields"] = list(schema.required_fields)
    return view


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
    for field_name in schema.required_fields:
        if _has_content(state.known_fields.get(field_name)) or _has_content(response.known_fields.get(field_name)):
            continue
        if is_date_field(field_name):
            parsed, issue = parse_date_value(state.original_text, field_name)
            if parsed and not issue:
                state.known_fields[field_name] = parsed
                continue
        if field_name == "people":
            people = _infer_people_from_text(state.original_text)
            if people is not None:
                state.known_fields[field_name] = people


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

    required_universe: list[str] = list(schema.required_fields)
    for field_name in dynamic_required_fields(schema.intent_domain, schema.intent_type, response.known_fields):
        if field_name not in required_universe:
            required_universe.append(field_name)

    ordered_required: list[str] = []
    for field_name in required_universe:
        if field_name not in ordered_required:
            ordered_required.append(field_name)

    entries: list[dict[str, Any]] = []
    required_missing_set = set(response.required_missing_fields)
    for field_name in ordered_required:
        prompt = get_field_prompt(field_name, state.original_text, response.intent_type, response.intent_domain)
        issue = issue_map.get(field_name, {})
        current_value = (
            state.known_fields.get("location_json") if is_location_field(field_name) else None
        ) or (
            issue.get("raw_value")
            or state.known_fields.get(field_name)
            or response.known_fields.get(field_name)
            or response.dates.get(field_name)
            or ""
        )
        issue_message = field_errors.get(field_name)
        additional_required = (field_name in required_missing_set or not _has_content(current_value) or field_name in issue_map) and not issue_message
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
                "control": control,
            }
        )
    return entries


def _field_control(field_name: str, current_value: Any, intent_type: str, intent_domain: str, original_text: str = "") -> dict[str, Any]:
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
    if field_name == "dates":
        start_value = ""
        end_value = ""
        if isinstance(current_value, dict):
            start_value = _coerce_date_input_value(current_value.get("start_date") or current_value.get("checkin") or current_value.get("date_from"))
            end_value = _coerce_date_input_value(current_value.get("end_date") or current_value.get("checkout") or current_value.get("date_to"))
        return {
            "kind": "date_range",
            "min": date.today().isoformat(),
            "start_value": start_value,
            "end_value": end_value,
        }
    if is_date_field(field_name):
        return {
            "kind": "date",
            "min": date.today().isoformat(),
            "value": _coerce_date_input_value(current_value),
        }
    options = _select_options_for_field(field_name, intent_type, intent_domain)
    if options:
        return {
            "kind": "select",
            "value": _normalize_select_value(current_value),
            "options": options,
        }
    return {"kind": "textarea"}


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
    if kind == "date_range":
        start = str(form.get(f"field__{field_name}__start", "")).strip()
        end = str(form.get(f"field__{field_name}__end", "")).strip()
        if start or end:
            return {"start_date": start, "end_date": end}
        return None
    return str(form.get(f"field__{field_name}", "")).strip()


def _validate_control_answer(answer: Any, control: dict[str, Any], field_name: str) -> Optional[str]:
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
        if end_date < start_date:
            return "La fecha final debe ser igual o posterior a la fecha inicial."
    if kind == "date" and answer:
        try:
            date.fromisoformat(str(answer))
        except ValueError:
            return "Selecciona una fecha válida."
    return None


def _format_answer_for_review(value: Any, control: dict[str, Any]) -> str:
    kind = control.get("kind")
    if kind == "zone_selector" and isinstance(value, dict):
        return zone_display_value(value)
    if kind == "date" and value:
        formatted = _format_review_date(value)
        return formatted or str(value)
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
) -> dict[str, Any]:
    def page_url(target_page: int) -> str:
        params: dict[str, Any] = {"page": target_page}
        if q:
            params["q"] = q
        if location:
            params["location"] = location
        if intent_type:
            params["intent_type"] = intent_type
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
