"""
database.py — Persistencia PostgreSQL para DMANDER.

Mantiene compatibilidad con el bot de Telegram original y añade soporte
para la aplicación web: usuarios, cuentas OAuth, demandas públicas,
ofertas y mensajes simples entre demandante y ofertante.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from embedding_service import (
    build_demand_search_text,
    cosine_similarity,
    embed_document_text,
    embed_query_text,
    embedding_model_name,
    rerank_demand_candidates,
)
from location_geometry import radius_limit_km, zone_has_geometry, zones_intersect
from master_schema import get_master_schema_registry
from models import APITokenInfo, DemandResult, OfferMessageResult, OfferResult, PublicDemand, SessionState, UserProfile
from zone_selector import compact_zone_label, zone_to_storage_fields

logger = logging.getLogger(__name__)

DEFAULT_DEMAND_EXPIRY_HOURS = 48
EXPIRED_CONVERSATION_GRACE_DAYS = 7
PASSWORD_RESET_TOKEN_HOURS = 2
MAGIC_LOGIN_TOKEN_MINUTES = 30
DEMAND_TEXT_MAX_LENGTH = 200
MESSAGE_TEXT_MAX_LENGTH = 200
_POSTGIS_AVAILABLE: Optional[bool] = None
SUPERADMIN_EMAILS = {
    email.strip().lower()
    for email in os.getenv("SUPERADMIN_EMAILS", "baylina@gmail.com").split(",")
    if email.strip()
}


CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    full_name     TEXT NOT NULL,
    password_hash TEXT,
    avatar_url    TEXT,
    auth_source   TEXT NOT NULL DEFAULT 'local',
    role          TEXT NOT NULL DEFAULT 'user',
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    last_login_at TIMESTAMP,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_OAUTH_ACCOUNTS_SQL = """
CREATE TABLE IF NOT EXISTS oauth_accounts (
    id               SERIAL PRIMARY KEY,
    user_id          INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider         TEXT NOT NULL,
    provider_user_id TEXT NOT NULL,
    provider_email   TEXT,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(provider, provider_user_id)
);
"""

CREATE_DEMANDS_SQL = """
CREATE TABLE IF NOT EXISTS demands (
    id               SERIAL PRIMARY KEY,
    public_id        TEXT UNIQUE,
    telegram_user_id BIGINT,
    user_id          INTEGER REFERENCES users(id) ON DELETE SET NULL,
    intent_domain    TEXT,
    intent_type      TEXT NOT NULL,
    summary          TEXT NOT NULL,
    location         TEXT,
    budget_min       REAL,
    budget_max       REAL,
    budget_unit      TEXT NOT NULL DEFAULT 'total',
    urgency          TEXT,
    location_mode    TEXT,
    location_label   TEXT,
    location_lat     DOUBLE PRECISION,
    location_lon     DOUBLE PRECISION,
    location_radius_km INTEGER,
    location_radius_bucket TEXT,
    location_source  TEXT,
    location_raw_query TEXT,
    location_admin_level TEXT,
    location_bbox    JSONB DEFAULT '[]'::jsonb,
    location_geojson JSONB DEFAULT '{}'::jsonb,
    location_json    JSONB DEFAULT '{}'::jsonb,
    attributes       JSONB DEFAULT '{}',
    normalized_payload JSONB DEFAULT '{}'::jsonb,
    llm_metadata     JSONB DEFAULT '{}'::jsonb,
    schema_version   TEXT,
    original_text    TEXT,
    conversation     JSONB DEFAULT '[]',
    status           TEXT NOT NULL DEFAULT 'open',
    is_pinned        BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at       TIMESTAMP DEFAULT (NOW() + interval '48 hours'),
    created_via      TEXT NOT NULL DEFAULT 'telegram',
    created_at       TIMESTAMP DEFAULT NOW()
);
"""

CREATE_OFFERS_SQL = """
CREATE TABLE IF NOT EXISTS offers (
    id               SERIAL PRIMARY KEY,
    demand_id        INTEGER NOT NULL REFERENCES demands(id) ON DELETE CASCADE,
    supplier_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    message          TEXT NOT NULL,
    supplier_is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
    supplier_hidden  BOOLEAN NOT NULL DEFAULT FALSE,
    demand_owner_last_read_at TIMESTAMP,
    supplier_last_read_at TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(demand_id, supplier_user_id)
);
"""

CREATE_OFFER_MESSAGES_SQL = """
CREATE TABLE IF NOT EXISTS offer_messages (
    id             SERIAL PRIMARY KEY,
    offer_id       INTEGER NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
    sender_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body           TEXT NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_SAVED_FILTERS_SQL = """
CREATE TABLE IF NOT EXISTS saved_filters (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    query_text  TEXT,
    location    TEXT,
    location_mode TEXT,
    location_label TEXT,
    location_lat DOUBLE PRECISION,
    location_lon DOUBLE PRECISION,
    location_radius_km INTEGER,
    location_radius_bucket TEXT,
    location_source TEXT,
    location_raw_query TEXT,
    location_admin_level TEXT,
    location_bbox JSONB DEFAULT '[]'::jsonb,
    location_geojson JSONB DEFAULT '{}'::jsonb,
    location_json JSONB DEFAULT '{}'::jsonb,
    intent_domains JSONB DEFAULT '[]'::jsonb,
    intent_types JSONB DEFAULT '[]'::jsonb,
    intent_type TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_DEMAND_WIZARDS_SQL = """
CREATE TABLE IF NOT EXISTS demand_wizards (
    user_id     INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_PASSWORD_RESET_TOKENS_SQL = """
CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email       TEXT NOT NULL,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMP NOT NULL,
    used_at     TIMESTAMP,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_API_TOKENS_SQL = """
CREATE TABLE IF NOT EXISTS api_tokens (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    token_hash   TEXT NOT NULL UNIQUE,
    token_prefix TEXT NOT NULL,
    last_used_at TIMESTAMP,
    revoked_at   TIMESTAMP,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_MAGIC_LOGIN_TOKENS_SQL = """
CREATE TABLE IF NOT EXISTS magic_login_tokens (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    purpose     TEXT NOT NULL DEFAULT 'telegram_web_login',
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMP NOT NULL,
    used_at     TIMESTAMP,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_APP_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS app_migrations (
    key         TEXT PRIMARY KEY,
    applied_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

CREATE_INDEXES_SQL = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_demands_public_id_unique ON demands(public_id) WHERE public_id IS NOT NULL;",
    "CREATE INDEX IF NOT EXISTS idx_demands_user_id ON demands(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_demands_status_created_at ON demands(status, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_offers_demand_id ON offers(demand_id);",
    "CREATE INDEX IF NOT EXISTS idx_offer_messages_offer_id ON offer_messages(offer_id, created_at ASC);",
    "CREATE INDEX IF NOT EXISTS idx_saved_filters_user_id ON saved_filters(user_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_demands_location_lat_lon ON demands(location_lat, location_lon);",
    "CREATE INDEX IF NOT EXISTS idx_demand_wizards_updated_at ON demand_wizards(updated_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_user_id ON password_reset_tokens(user_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_api_tokens_user_id ON api_tokens(user_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_magic_login_tokens_user_id ON magic_login_tokens(user_id, created_at DESC);",
]

POSTGIS_ALTERS_SQL = [
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_center_geom geometry(Point, 4326);",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_shape_geom geometry(Geometry, 4326);",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_center_geom geometry(Point, 4326);",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_shape_geom geometry(Geometry, 4326);",
]

POSTGIS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_demands_location_shape_geom ON demands USING GIST (location_shape_geom);",
    "CREATE INDEX IF NOT EXISTS idx_saved_filters_location_shape_geom ON saved_filters USING GIST (location_shape_geom);",
]

ALTERS_SQL = [
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS public_id TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS intent_domain TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open';",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT FALSE;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP DEFAULT (NOW() + interval '48 hours');",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS created_via TEXT NOT NULL DEFAULT 'telegram';",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS normalized_payload JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS llm_metadata JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS schema_version TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS budget_unit TEXT NOT NULL DEFAULT 'total';",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_mode TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_label TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_lat DOUBLE PRECISION;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_lon DOUBLE PRECISION;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_radius_km INTEGER;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_radius_bucket TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_source TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_raw_query TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_admin_level TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_bbox JSONB DEFAULT '[]'::jsonb;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_geojson JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS location_json JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user';",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP;",
    "ALTER TABLE demands ALTER COLUMN telegram_user_id DROP NOT NULL;",
    "ALTER TABLE offers ADD COLUMN IF NOT EXISTS message TEXT;",
    "ALTER TABLE offers ADD COLUMN IF NOT EXISTS supplier_is_pinned BOOLEAN NOT NULL DEFAULT FALSE;",
    "ALTER TABLE offers ADD COLUMN IF NOT EXISTS supplier_hidden BOOLEAN NOT NULL DEFAULT FALSE;",
    "ALTER TABLE offers ADD COLUMN IF NOT EXISTS demand_owner_last_read_at TIMESTAMP;",
    "ALTER TABLE offers ADD COLUMN IF NOT EXISTS supplier_last_read_at TIMESTAMP;",
    "ALTER TABLE offers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();",
    "ALTER TABLE offers DROP COLUMN IF EXISTS validity_hours;",
    "ALTER TABLE offers DROP COLUMN IF EXISTS expires_at;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_mode TEXT;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_label TEXT;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_lat DOUBLE PRECISION;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_lon DOUBLE PRECISION;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_radius_km INTEGER;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_radius_bucket TEXT;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_source TEXT;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_raw_query TEXT;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_admin_level TEXT;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_bbox JSONB DEFAULT '[]'::jsonb;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_geojson JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS location_json JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS intent_domains JSONB DEFAULT '[]'::jsonb;",
    "ALTER TABLE saved_filters ADD COLUMN IF NOT EXISTS intent_types JSONB DEFAULT '[]'::jsonb;",
]


def _get_connection():
    """Obtiene una conexión a PostgreSQL usando DATABASE_URL."""
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/dmander")
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)


def _enable_postgis(conn) -> bool:
    global _POSTGIS_AVAILABLE
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        conn.commit()
        _POSTGIS_AVAILABLE = True
        return True
    except Exception as exc:  # pragma: no cover - depende del sistema
        conn.rollback()
        _POSTGIS_AVAILABLE = False
        logger.warning("PostGIS no está disponible en este entorno: %s", exc)
        return False


def _postgis_available() -> bool:
    return bool(_POSTGIS_AVAILABLE)


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _role_for_email(email: str) -> str:
    return "superadmin" if _normalize_email(email) in SUPERADMIN_EMAILS else "user"


def _hash_reset_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_api_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_magic_login_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _telegram_placeholder_email(telegram_user_id: int) -> str:
    return f"telegram_{int(telegram_user_id)}@telegram.local"


def _telegram_display_name(username: str | None, first_name: str | None, last_name: str | None) -> str:
    first = str(first_name or "").strip()
    last = str(last_name or "").strip()
    full_name = " ".join(piece for piece in (first, last) if piece).strip()
    if full_name:
        return full_name
    username_value = str(username or "").strip()
    if username_value:
        return f"@{username_value}"
    return "Usuario de Telegram"


def _generate_demand_public_id() -> str:
    return f"dmd_{secrets.token_urlsafe(9).replace('-', '').replace('_', '')[:12]}"


def _next_demand_public_id(cur) -> str:
    public_id = _generate_demand_public_id()
    while True:
        cur.execute("SELECT 1 FROM demands WHERE public_id = %s", (public_id,))
        if not cur.fetchone():
            return public_id
        public_id = _generate_demand_public_id()


def hash_password(password: str) -> str:
    """Genera un hash con scrypt usando solo librerías estándar."""
    salt = secrets.token_bytes(16)
    derived = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1)
    return (
        "scrypt$"
        f"{base64.b64encode(salt).decode('ascii')}$"
        f"{base64.b64encode(derived).decode('ascii')}"
    )


def verify_password(password: str, stored_hash: Optional[str]) -> bool:
    if not stored_hash or not stored_hash.startswith("scrypt$"):
        return False

    _, salt_b64, digest_b64 = stored_hash.split("$", 2)
    salt = base64.b64decode(salt_b64.encode("ascii"))
    expected = base64.b64decode(digest_b64.encode("ascii"))
    candidate = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1)
    return hmac.compare_digest(candidate, expected)


def init_db() -> None:
    """Crea el esquema completo si no existe."""
    try:
        conn = _get_connection()
        postgis_available = _enable_postgis(conn)
        with conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_USERS_SQL)
                cur.execute(CREATE_OAUTH_ACCOUNTS_SQL)
                cur.execute(CREATE_DEMANDS_SQL)
                cur.execute(CREATE_OFFERS_SQL)
                cur.execute(CREATE_OFFER_MESSAGES_SQL)
                cur.execute(CREATE_SAVED_FILTERS_SQL)
                cur.execute(CREATE_DEMAND_WIZARDS_SQL)
                cur.execute(CREATE_PASSWORD_RESET_TOKENS_SQL)
                cur.execute(CREATE_API_TOKENS_SQL)
                cur.execute(CREATE_MAGIC_LOGIN_TOKENS_SQL)
                cur.execute(CREATE_APP_MIGRATIONS_SQL)
                for statement in ALTERS_SQL:
                    cur.execute(statement)
                if postgis_available:
                    for statement in POSTGIS_ALTERS_SQL:
                        cur.execute(statement)
                for statement in CREATE_INDEXES_SQL:
                    cur.execute(statement)
                if postgis_available:
                    for statement in POSTGIS_INDEXES_SQL:
                        cur.execute(statement)
                cur.execute("UPDATE users SET role = 'user' WHERE role = 'superadmin'")
                if SUPERADMIN_EMAILS:
                    cur.execute(
                        """
                        UPDATE users
                        SET role = 'superadmin',
                            updated_at = NOW()
                        WHERE lower(email) = ANY(%s)
                        """,
                        (list(SUPERADMIN_EMAILS),),
                    )
                _ensure_demand_public_ids(cur)
                _apply_product_pivot_reset(cur)
        conn.close()
        logger.info("✅ Base de datos inicializada correctamente.")
    except psycopg2.OperationalError as e:
        logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
        raise


def _apply_product_pivot_reset(cur) -> None:
    cur.execute("SELECT 1 FROM app_migrations WHERE key = %s", ("pivot_free_text_v1",))
    if cur.fetchone():
        return
    cur.execute("DELETE FROM offer_messages")
    cur.execute("DELETE FROM offers")
    cur.execute("DELETE FROM demands")
    cur.execute("DELETE FROM saved_filters")
    cur.execute("DELETE FROM demand_wizards")
    cur.execute("INSERT INTO app_migrations (key) VALUES (%s)", ("pivot_free_text_v1",))
    logger.info("🧹 Migración pivot_free_text_v1 aplicada: demandas, conversaciones y filtros antiguos eliminados.")


def _ensure_demand_public_ids(cur) -> None:
    cur.execute("SELECT id FROM demands WHERE public_id IS NULL OR public_id = ''")
    rows = cur.fetchall()
    for row in rows:
        public_id = _next_demand_public_id(cur)
        cur.execute("UPDATE demands SET public_id = %s WHERE id = %s", (public_id, row["id"]))


def _schema_registry():
    return get_master_schema_registry()

def _budget_unit_label(unit: str) -> str:
    normalized = str(unit or "total").strip().lower()
    labels = {
        "total": "",
        "hour": "por hora",
        "day": "por día",
        "night": "por noche",
        "month": "al mes",
        "item": "por producto",
        "service": "por servicio",
    }
    return labels.get(normalized, "")


def _lightweight_payload_for_row(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    attributes = dict(data.get("attributes") or {})
    llm_metadata = dict(data.get("llm_metadata") or {})
    original_text = str(data.get("original_text") or attributes.get("description") or data.get("summary") or "").strip()
    summary = str(data.get("summary") or original_text).strip() or original_text
    location_json = dict(data.get("location_json") or {})
    location_label = str(data.get("location_label") or location_json.get("label") or data.get("location") or "").strip()
    budget_max = data.get("budget_max")
    budget_unit = str(data.get("budget_unit") or llm_metadata.get("budget_unit") or "total").strip().lower() or "total"
    return {
        "raw_text": original_text,
        "summary": summary,
        "location_value": location_label or None,
        "location_label": location_label or None,
        "location_json": location_json,
        "budget_max_amount": budget_max,
        "budget_max": budget_max,
        "budget_unit": budget_unit,
        "budget_unit_label": _budget_unit_label(budget_unit),
        "suggested_missing_details": list(llm_metadata.get("suggested_missing_details") or []),
        "llm_metadata": llm_metadata,
    }


def _enrich_search_metadata(
    *,
    raw_text: str,
    summary: str,
    location_label: str = "",
    budget_max: Any = None,
    budget_unit: str = "total",
    llm_metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload = dict(llm_metadata or {})
    search_text = build_demand_search_text(
        raw_text,
        summary=summary,
        location_label=location_label,
        budget_max=budget_max,
        budget_unit=budget_unit,
        suggested_missing_details=list(payload.get("suggested_missing_details") or []),
    )
    payload["search_text"] = search_text
    payload["embedding_model"] = embedding_model_name()
    payload["search_embedding"] = embed_document_text(search_text)
    payload["embedding_updated_at"] = datetime.now(timezone.utc).isoformat()
    return payload


def _expire_open_demands(cur) -> None:
    cur.execute(
        """
        UPDATE demands
        SET status = 'expired'
        WHERE status = 'open'
          AND expires_at IS NOT NULL
          AND expires_at <= NOW()
        """
    )


def _hydrate_demand_row(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["public_id"] = str(data.get("public_id") or "")
    data["attributes"] = dict(data.get("attributes") or {})
    data["llm_metadata"] = dict(data.get("llm_metadata") or {})
    data["normalized_payload"] = _lightweight_payload_for_row(data)
    data["intent_domain"] = ""
    data["intent_type"] = "free_text"
    data["original_text"] = data["normalized_payload"].get("raw_text", "")
    if not data.get("location"):
        data["location"] = data["normalized_payload"].get("location_value")
    if not data.get("location_json"):
        data["location_json"] = data["normalized_payload"].get("location_json", {})
    if not data.get("location_mode"):
        data["location_mode"] = (
            data.get("location_json", {}).get("mode")
            or ("radius_from_point" if data.get("location_lat") is not None and data.get("location_lon") is not None else "unspecified")
        )
    if not data.get("location_label"):
        data["location_label"] = data["normalized_payload"].get("location_label") or data.get("location")
    if not data.get("location_admin_level"):
        data["location_admin_level"] = data.get("location_json", {}).get("admin_level")
    if data.get("location_lat") is None:
        data["location_lat"] = (data.get("location_json") or {}).get("center", {}).get("lat")
    if data.get("location_lon") is None:
        data["location_lon"] = (data.get("location_json") or {}).get("center", {}).get("lon")
    if data.get("location_radius_km") is None:
        data["location_radius_km"] = (data.get("location_json") or {}).get("radius_km")
    if not data.get("location_radius_bucket"):
        data["location_radius_bucket"] = (data.get("location_json") or {}).get("radius_bucket")
    if not data.get("location_source"):
        data["location_source"] = (data.get("location_json") or {}).get("source")
    if not data.get("location_raw_query"):
        data["location_raw_query"] = (data.get("location_json") or {}).get("raw_query")
    if not data.get("location_bbox"):
        data["location_bbox"] = []
    if not data.get("location_geojson"):
        data["location_geojson"] = {}
    data["location_display"] = compact_zone_label(
        data.get("location_label") or data.get("location"),
        data.get("location_raw_query"),
    )
    data["budget_unit"] = str(data.get("budget_unit") or data["normalized_payload"].get("budget_unit") or "total").strip().lower() or "total"
    data["suggested_missing_details"] = list(data["normalized_payload"].get("suggested_missing_details") or [])
    effective_status = data.get("status") or "open"
    data["effective_status"] = effective_status
    data["is_pinned"] = bool(data.get("is_pinned"))
    data["is_active"] = effective_status == "open"
    data["can_interact"] = effective_status == "open"
    data["can_pause"] = effective_status == "open"
    data["can_reactivate"] = effective_status == "paused"
    data["can_delete"] = effective_status != "deleted"
    data["can_pin"] = effective_status != "deleted"
    return data


def _demand_zone_fields(demand: DemandResult) -> dict[str, Any]:
    return zone_to_storage_fields(demand.location_json or demand.attributes.get("location_zone"))


def _matches_zone_filter(row: dict[str, Any], zone_filter: Optional[dict[str, Any]]) -> bool:
    if not zone_filter:
        return True
    demand_zone = row.get("location_json") or {}
    if not demand_zone or not zone_has_geometry(demand_zone):
        return False
    if not demand_zone.get("center") and row.get("location_lat") is not None and row.get("location_lon") is not None:
        demand_zone = {
            "mode": row.get("location_mode") or "radius_from_point",
            "label": row.get("location_label") or row.get("location") or "",
            "center": {"lat": row.get("location_lat"), "lon": row.get("location_lon")},
            "radius_km": row.get("location_radius_km"),
            "radius_bucket": row.get("location_radius_bucket"),
            "source": row.get("location_source") or "",
            "raw_query": row.get("location_raw_query") or "",
            "admin_level": row.get("location_admin_level") or "",
            "bbox": row.get("location_bbox") or [],
            "geojson": row.get("location_geojson") or {},
        }
    if not demand_zone or not zone_has_geometry(demand_zone):
        return False
    return zones_intersect(demand_zone, zone_filter)


def _matches_category_filter(
    row: dict[str, Any],
    selected_domains: Optional[list[str]] = None,
    selected_types: Optional[list[str]] = None,
) -> bool:
    selected_domains = [item for item in (selected_domains or []) if item]
    selected_types = [item for item in (selected_types or []) if item]
    if selected_types:
        return (row.get("intent_type") or "") in selected_types
    if selected_domains:
        return (row.get("intent_domain") or "") in selected_domains
    return True


def _demand_search_document(row: dict[str, Any]) -> str:
    llm_metadata = dict(row.get("llm_metadata") or {})
    suggestions = " ".join(str(item) for item in llm_metadata.get("suggested_missing_details") or [])
    return " ".join(
        part
        for part in [
            llm_metadata.get("search_text"),
            row.get("summary"),
            row.get("original_text"),
            row.get("location"),
            row.get("location_label"),
            suggestions,
        ]
        if part
    )


def _semantic_match_score(row: dict[str, Any], query_embedding: list[float] | tuple[float, ...]) -> float:
    if not query_embedding:
        return 0.0
    llm_metadata = dict(row.get("llm_metadata") or {})
    document_embedding = llm_metadata.get("search_embedding") or []
    return cosine_similarity(query_embedding, document_embedding)


def _normalize_search_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return text


def _search_tokens(value: Any) -> list[str]:
    normalized = _normalize_search_text(value)
    if not normalized:
        return []
    tokens = re.findall(r"[a-z0-9]+", normalized)
    return [token for token in tokens if len(token) >= 3]


def _search_roots(value: Any) -> set[str]:
    roots: set[str] = set()
    for token in _search_tokens(value):
        roots.add(token)
        if token.endswith("es") and len(token) > 4:
            roots.add(token[:-1])
            roots.add(token[:-2])
        elif token.endswith("s") and len(token) > 3:
            roots.add(token[:-1])
    return roots


def _fallback_text_match_score(row: dict[str, Any], query_text: str) -> float:
    query_normalized = _normalize_search_text(query_text)
    document = _normalize_search_text(_demand_search_document(row))
    if not query_normalized or not document:
        return 0.0
    if query_normalized in document:
        return 1.0

    query_roots = _search_roots(query_text)
    document_roots = _search_roots(document)
    if not query_roots or not document_roots:
        return 0.0

    overlap = len(query_roots & document_roots) / max(len(query_roots), 1)
    return overlap if overlap >= 0.25 else 0.0


def _location_shape_sql(alias: str) -> str:
    return f"""
        CASE
            WHEN {alias}.location_shape_geom IS NOT NULL THEN {alias}.location_shape_geom
            WHEN COALESCE({alias}.location_mode, {alias}.location_json->>'mode') = 'area'
                AND (
                    ({alias}.location_geojson IS NOT NULL AND {alias}.location_geojson::text NOT IN ('{{}}', 'null'))
                    OR ({alias}.location_json ? 'geojson')
                )
            THEN ST_SetSRID(
                ST_GeomFromGeoJSON(
                    COALESCE(NULLIF({alias}.location_geojson::text, '{{}}'), ({alias}.location_json->'geojson')::text)
                ),
                4326
            )
            WHEN {alias}.location_lon IS NOT NULL AND {alias}.location_lat IS NOT NULL
            THEN ST_Buffer(
                ST_SetSRID(ST_MakePoint({alias}.location_lon, {alias}.location_lat), 4326)::geography,
                CASE
                    WHEN COALESCE({alias}.location_radius_bucket, {alias}.location_json->>'radius_bucket') = '200_plus' THEN 1000000
                    ELSE COALESCE(
                        {alias}.location_radius_km,
                        NULLIF({alias}.location_json->>'radius_km', '')::integer,
                        10
                    ) * 1000
                END
            )::geometry
            ELSE NULL
        END
    """


def _search_shape_sql(zone_filter: dict[str, Any]) -> tuple[str, list[Any]]:
    mode = zone_filter.get("mode") or "radius_from_point"
    if mode == "area":
        geojson = zone_filter.get("geojson")
        bbox = zone_filter.get("bbox") or []
        if isinstance(geojson, dict) and geojson:
            return "ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)", [json.dumps(geojson, ensure_ascii=False)]
        if isinstance(bbox, list) and len(bbox) == 4:
            return "ST_MakeEnvelope(%s, %s, %s, %s, 4326)", [bbox[0], bbox[1], bbox[2], bbox[3]]
    center = zone_filter.get("center") or {}
    lon = center.get("lon")
    lat = center.get("lat")
    if lon is None or lat is None:
        return "", []
    radius_m = 1000000 if zone_filter.get("radius_bucket") == "200_plus" else int(radius_limit_km(zone_filter.get("radius_km"), zone_filter.get("radius_bucket")) * 1000)
    return "ST_Buffer(ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)::geometry", [lon, lat, radius_m]


def _apply_zone_geometries(cur, table_name: str, row_id: int, zone: dict[str, Any]) -> None:
    if not _postgis_available():
        return
    if table_name not in {"demands", "saved_filters"}:
        return

    mode = zone.get("location_mode")
    geojson = zone.get("location_geojson") or {}
    bbox = zone.get("location_bbox") or []
    lat = zone.get("location_lat")
    lon = zone.get("location_lon")
    radius_m = 1000000 if zone.get("location_radius_bucket") == "200_plus" else int(radius_limit_km(zone.get("location_radius_km"), zone.get("location_radius_bucket")) * 1000)

    if mode == "area" and isinstance(geojson, dict) and geojson:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET location_center_geom = CASE
                    WHEN %s IS NOT NULL AND %s IS NOT NULL THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    ELSE NULL
                END,
                location_shape_geom = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
            WHERE id = %s
            """,
            (
                lon,
                lat,
                lon,
                lat,
                json.dumps(geojson, ensure_ascii=False),
                row_id,
            ),
        )
        return

    if mode == "area" and isinstance(bbox, list) and len(bbox) == 4:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET location_center_geom = CASE
                    WHEN %s IS NOT NULL AND %s IS NOT NULL THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    ELSE NULL
                END,
                location_shape_geom = ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            WHERE id = %s
            """,
            (lon, lat, lon, lat, bbox[0], bbox[1], bbox[2], bbox[3], row_id),
        )
        return

    if lat is not None and lon is not None:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET location_center_geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                location_shape_geom = ST_Buffer(
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s
                )::geometry
            WHERE id = %s
            """,
            (lon, lat, lon, lat, radius_m, row_id),
        )
        return

    cur.execute(
        f"""
        UPDATE {table_name}
        SET location_center_geom = NULL,
            location_shape_geom = NULL
        WHERE id = %s
        """,
        (row_id,),
    )


def _row_to_user(row: dict[str, Any]) -> UserProfile:
    return UserProfile.model_validate(dict(row))


def _row_to_api_token(row: dict[str, Any]) -> APITokenInfo:
    return APITokenInfo.model_validate(dict(row))


def create_user(email: str, password: str, full_name: str) -> UserProfile:
    normalized_email = _normalize_email(email)
    password_hash = hash_password(password)
    role = _role_for_email(normalized_email)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (email, full_name, password_hash, auth_source, role, last_login_at)
                    VALUES (%s, %s, %s, 'local', %s, NOW())
                    RETURNING id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at;
                    """,
                    (normalized_email, full_name.strip(), password_hash, role),
                )
                return _row_to_user(cur.fetchone())
    finally:
        conn.close()


def get_user_by_email(email: str) -> Optional[UserProfile]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at
                FROM users
                WHERE email = %s
                """,
                (_normalize_email(email),),
            )
            row = cur.fetchone()
            return _row_to_user(row) if row else None
    finally:
        conn.close()


def get_user_by_id(user_id: int) -> Optional[UserProfile]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            return _row_to_user(row) if row else None
    finally:
        conn.close()


def record_user_login(user_id: int) -> None:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET last_login_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (user_id,),
                )
    finally:
        conn.close()


def authenticate_user(email: str, password: str) -> Optional[UserProfile]:
    user = get_user_by_email(email)
    if not user or not user.is_active or not verify_password(password, user.password_hash):
        return None
    record_user_login(user.id)
    return get_user_by_id(user.id) or user


def get_or_create_oauth_user(
    provider: str,
    provider_user_id: str,
    email: str,
    full_name: str,
    avatar_url: Optional[str] = None,
) -> UserProfile:
    normalized_email = _normalize_email(email)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.email, u.full_name, u.password_hash, u.avatar_url, u.auth_source, u.role, u.is_active, u.last_login_at, u.created_at
                    FROM oauth_accounts oa
                    JOIN users u ON u.id = oa.user_id
                    WHERE oa.provider = %s AND oa.provider_user_id = %s
                    """,
                    (provider, provider_user_id),
                )
                row = cur.fetchone()
                if row:
                    user = _row_to_user(row)
                    if not user.is_active:
                        return user
                    cur.execute(
                        "UPDATE users SET last_login_at = NOW(), updated_at = NOW() WHERE id = %s",
                        (user.id,),
                    )
                    return get_user_by_id(user.id) or user

                cur.execute(
                    """
                    SELECT id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at
                    FROM users
                    WHERE email = %s
                    """,
                    (normalized_email,),
                )
                row = cur.fetchone()
                if row:
                    user = _row_to_user(row)
                else:
                    role = _role_for_email(normalized_email)
                    cur.execute(
                        """
                        INSERT INTO users (email, full_name, avatar_url, auth_source, role, last_login_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        RETURNING id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at
                        """,
                        (normalized_email, full_name.strip() or normalized_email, avatar_url, provider, role),
                    )
                    user = _row_to_user(cur.fetchone())

                cur.execute(
                    """
                    INSERT INTO oauth_accounts (user_id, provider, provider_user_id, provider_email)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (provider, provider_user_id) DO NOTHING
                    """,
                    (user.id, provider, provider_user_id, normalized_email),
                )

                if avatar_url:
                    cur.execute(
                        "UPDATE users SET avatar_url = COALESCE(avatar_url, %s), updated_at = NOW() WHERE id = %s",
                        (avatar_url, user.id),
                    )
                    user.avatar_url = user.avatar_url or avatar_url

                if user.is_active:
                    cur.execute(
                        "UPDATE users SET last_login_at = NOW(), updated_at = NOW() WHERE id = %s",
                        (user.id,),
                    )
                return get_user_by_id(user.id) or user
    finally:
        conn.close()


def get_or_create_telegram_user(
    telegram_user_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> UserProfile:
    provider_user_id = str(int(telegram_user_id))
    placeholder_email = _telegram_placeholder_email(telegram_user_id)
    full_name = _telegram_display_name(username, first_name, last_name)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.email, u.full_name, u.password_hash, u.avatar_url, u.auth_source, u.role, u.is_active, u.last_login_at, u.created_at
                    FROM oauth_accounts oa
                    JOIN users u ON u.id = oa.user_id
                    WHERE oa.provider = 'telegram' AND oa.provider_user_id = %s
                    """,
                    (provider_user_id,),
                )
                row = cur.fetchone()
                if row:
                    user = _row_to_user(row)
                    cur.execute(
                        """
                        UPDATE users
                        SET full_name = %s,
                            auth_source = 'telegram',
                            last_login_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (full_name, user.id),
                    )
                    return get_user_by_id(user.id) or user

                cur.execute(
                    """
                    SELECT id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at
                    FROM users
                    WHERE email = %s
                    """,
                    (placeholder_email,),
                )
                row = cur.fetchone()
                if row:
                    user = _row_to_user(row)
                    cur.execute(
                        """
                        UPDATE users
                        SET full_name = %s,
                            auth_source = 'telegram',
                            last_login_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (full_name, user.id),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO users (email, full_name, auth_source, role, last_login_at)
                        VALUES (%s, %s, 'telegram', 'user', NOW())
                        RETURNING id, email, full_name, password_hash, avatar_url, auth_source, role, is_active, last_login_at, created_at
                        """,
                        (placeholder_email, full_name),
                    )
                    user = _row_to_user(cur.fetchone())

                cur.execute(
                    """
                    INSERT INTO oauth_accounts (user_id, provider, provider_user_id, provider_email)
                    VALUES (%s, 'telegram', %s, %s)
                    ON CONFLICT (provider, provider_user_id) DO UPDATE
                    SET provider_email = EXCLUDED.provider_email
                    """,
                    (user.id, provider_user_id, placeholder_email),
                )

                return get_user_by_id(user.id) or user
    finally:
        conn.close()


def create_magic_login_token(user_id: int, purpose: str = "telegram_web_login") -> str:
    plain_token = secrets.token_urlsafe(32)
    token_hash = _hash_magic_login_token(plain_token)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=MAGIC_LOGIN_TOKEN_MINUTES)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO magic_login_tokens (user_id, purpose, token_hash, expires_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (user_id, purpose, token_hash, expires_at),
                )
        return plain_token
    finally:
        conn.close()


def consume_magic_login_token(token: str, purpose: str = "telegram_web_login") -> Optional[UserProfile]:
    token_hash = _hash_magic_login_token(token)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.id AS magic_token_id,
                           u.id, u.email, u.full_name, u.password_hash, u.avatar_url, u.auth_source, u.role, u.is_active, u.last_login_at, u.created_at
                    FROM magic_login_tokens m
                    JOIN users u ON u.id = m.user_id
                    WHERE m.token_hash = %s
                      AND m.purpose = %s
                      AND m.used_at IS NULL
                      AND m.expires_at > NOW()
                    """,
                    (token_hash, purpose),
                )
                row = cur.fetchone()
                if not row:
                    return None
                magic_token_id = row.pop("magic_token_id", None)
                user = _row_to_user(row)
                if not user.is_active:
                    return None
                if magic_token_id is not None:
                    cur.execute(
                        """
                        UPDATE magic_login_tokens
                        SET used_at = NOW()
                        WHERE id = %s
                        """,
                        (magic_token_id,),
                    )
                cur.execute(
                    """
                    UPDATE users
                    SET last_login_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (user.id,),
                )
                return get_user_by_id(user.id) or user
    finally:
        conn.close()


def change_user_password(user_id: int, current_password: str, new_password: str) -> bool:
    user = get_user_by_id(user_id)
    if not user or not user.is_active or not verify_password(current_password, user.password_hash):
        return False
    new_hash = hash_password(new_password)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET password_hash = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (new_hash, user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def create_password_reset_token(email: str) -> tuple[Optional[UserProfile], Optional[str]]:
    user = get_user_by_email(email)
    if not user or not user.is_active:
        return None, None
    token = secrets.token_urlsafe(32)
    token_hash = _hash_reset_token(token)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=PASSWORD_RESET_TOKEN_HOURS)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE password_reset_tokens
                    SET used_at = NOW()
                    WHERE user_id = %s
                      AND used_at IS NULL
                    """,
                    (user.id,),
                )
                cur.execute(
                    """
                    INSERT INTO password_reset_tokens (user_id, email, token_hash, expires_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (user.id, user.email, token_hash, expires_at),
                )
    finally:
        conn.close()
    return user, token


def get_password_reset_user(token: str) -> Optional[UserProfile]:
    token_hash = _hash_reset_token(token)
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.email, u.full_name, u.password_hash, u.avatar_url, u.auth_source, u.role, u.is_active, u.last_login_at, u.created_at
                FROM password_reset_tokens prt
                JOIN users u ON u.id = prt.user_id
                WHERE prt.token_hash = %s
                  AND prt.used_at IS NULL
                  AND prt.expires_at > NOW()
                  AND u.is_active = TRUE
                ORDER BY prt.created_at DESC
                LIMIT 1
                """,
                (token_hash,),
            )
            row = cur.fetchone()
            return _row_to_user(row) if row else None
    finally:
        conn.close()


def reset_password_with_token(token: str, new_password: str) -> bool:
    token_hash = _hash_reset_token(token)
    new_hash = hash_password(new_password)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id
                    FROM password_reset_tokens
                    WHERE token_hash = %s
                      AND used_at IS NULL
                      AND expires_at > NOW()
                    ORDER BY created_at DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (token_hash,),
                )
                row = cur.fetchone()
                if not row:
                    return False
                user_id = row["user_id"]
                cur.execute(
                    """
                    UPDATE users
                    SET password_hash = %s,
                        updated_at = NOW()
                    WHERE id = %s
                      AND is_active = TRUE
                    """,
                    (new_hash, user_id),
                )
                if cur.rowcount <= 0:
                    return False
                cur.execute(
                    """
                    UPDATE password_reset_tokens
                    SET used_at = NOW()
                    WHERE token_hash = %s
                    """,
                    (token_hash,),
                )
                return True
    finally:
        conn.close()


def create_api_token(user_id: int, name: str) -> tuple[APITokenInfo, str]:
    user = get_user_by_id(user_id)
    if not user or not user.is_active:
        raise ValueError("Usuario no disponible")

    token_secret = secrets.token_urlsafe(32)
    plain_token = f"dmdr_pat_{token_secret}"
    token_hash = _hash_api_token(plain_token)
    token_prefix = plain_token[:16]
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_tokens (user_id, name, token_hash, token_prefix)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, user_id, name, token_prefix, last_used_at, revoked_at, created_at
                    """,
                    (user_id, (name or "Token API").strip(), token_hash, token_prefix),
                )
                token = _row_to_api_token(cur.fetchone())
                return token, plain_token
    finally:
        conn.close()


def list_api_tokens(user_id: int) -> list[APITokenInfo]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, user_id, name, token_prefix, last_used_at, revoked_at, created_at
                FROM api_tokens
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,),
            )
            return [_row_to_api_token(row) for row in cur.fetchall()]
    finally:
        conn.close()


def revoke_api_token(user_id: int, token_id: int) -> bool:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE api_tokens
                    SET revoked_at = NOW()
                    WHERE id = %s
                      AND user_id = %s
                      AND revoked_at IS NULL
                    """,
                    (token_id, user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def authenticate_api_token(token: str) -> Optional[UserProfile]:
    token_hash = _hash_api_token(token)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.email, u.full_name, u.password_hash, u.avatar_url, u.auth_source, u.role, u.is_active, u.last_login_at, u.created_at,
                           t.id AS api_token_id
                    FROM api_tokens t
                    JOIN users u ON u.id = t.user_id
                    WHERE t.token_hash = %s
                      AND t.revoked_at IS NULL
                      AND u.is_active = TRUE
                    LIMIT 1
                    """,
                    (token_hash,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                token_id = row.pop("api_token_id", None)
                if token_id is not None:
                    cur.execute(
                        """
                        UPDATE api_tokens
                        SET last_used_at = NOW()
                        WHERE id = %s
                        """,
                        (token_id,),
                    )
                return _row_to_user(row)
    finally:
        conn.close()


def list_admin_users(query: str = "") -> list[dict[str, Any]]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            normalized_query = f"%{(query or '').strip().lower()}%"
            cur.execute(
                """
                SELECT u.id, u.email, u.full_name, u.auth_source, u.role, u.is_active, u.created_at, u.last_login_at,
                       COUNT(DISTINCT d.id)::INTEGER AS demand_count,
                       COUNT(DISTINCT o.id)::INTEGER AS offer_count
                FROM users u
                LEFT JOIN demands d ON d.user_id = u.id
                LEFT JOIN offers o ON o.supplier_user_id = u.id
                WHERE (%s = '%%' OR lower(u.email) LIKE %s OR lower(u.full_name) LIKE %s)
                GROUP BY u.id
                ORDER BY lower(u.email) ASC
                """,
                (normalized_query, normalized_query, normalized_query),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_admin_user_detail(user_id: int) -> Optional[dict[str, Any]]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, email, full_name, auth_source, role, is_active, created_at, last_login_at
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            payload = dict(row)
            cur.execute(
                """
                SELECT id, public_id, summary, intent_type, status, created_at
                FROM demands
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,),
            )
            payload["demands"] = [dict(item) for item in cur.fetchall()]
            cur.execute(
                """
                SELECT o.id, o.demand_id, d.summary AS demand_summary, d.intent_type, d.status AS demand_status, o.created_at
                FROM offers o
                JOIN demands d ON d.id = o.demand_id
                WHERE o.supplier_user_id = %s
                ORDER BY o.created_at DESC
                """,
                (user_id,),
            )
            payload["offers"] = [dict(item) for item in cur.fetchall()]
            return payload
    finally:
        conn.close()


def set_user_active_status(user_id: int, is_active: bool) -> bool:
    user = get_user_by_id(user_id)
    if not user:
        return False
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET is_active = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (is_active, user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def set_user_role(user_id: int, role: str) -> bool:
    normalized_role = "superadmin" if (role or "").strip().lower() == "superadmin" else "user"
    user = get_user_by_id(user_id)
    if not user:
        return False
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET role = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (normalized_role, user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def delete_user_permanently(user_id: int) -> bool:
    user = get_user_by_id(user_id)
    if not user or user.role == "superadmin":
        return False
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM offer_messages WHERE offer_id IN (SELECT id FROM offers WHERE demand_id IN (SELECT id FROM demands WHERE user_id = %s))", (user_id,))
                cur.execute("DELETE FROM offers WHERE demand_id IN (SELECT id FROM demands WHERE user_id = %s)", (user_id,))
                cur.execute("DELETE FROM demands WHERE user_id = %s", (user_id,))
                cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                return cur.rowcount > 0
    finally:
        conn.close()


def save_demand(
    telegram_user_id: int,
    demand: DemandResult,
    state: SessionState,
) -> dict[str, Any]:
    """Guarda una demanda del bot de Telegram."""
    conversation = [
        {"question": q, "answer": a}
        for q, a in zip(state.questions_asked, state.user_answers)
    ]

    conn = _get_connection()
    zone = _demand_zone_fields(demand)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO demands (
                        telegram_user_id, user_id, intent_domain, intent_type, summary, location,
                        budget_min, budget_max, urgency, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        attributes, normalized_payload, schema_version,
                        original_text, conversation, status, expires_at, created_via
                    )
                    VALUES (
                        %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, 'open', NOW() + interval '48 hours', 'telegram'
                    )
                    RETURNING id, created_at;
                    """,
                    (
                        telegram_user_id,
                        demand.intent_domain,
                        demand.intent_type,
                        demand.summary,
                        demand.location_value or demand.location,
                        demand.budget_min,
                        demand.budget_max,
                        demand.urgency,
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(demand.attributes, ensure_ascii=False),
                        json.dumps(demand.model_dump(mode="json"), ensure_ascii=False),
                        demand.schema_version or _schema_registry().version,
                        state.original_text,
                        json.dumps(conversation, ensure_ascii=False),
                    ),
                )
                row = cur.fetchone()
                _apply_zone_geometries(cur, "demands", row["id"], zone)
                logger.info(f"💾 Demanda guardada con id={row['id']}")
                return dict(row)
    finally:
        conn.close()


def save_telegram_demand_lightweight(
    telegram_user_id: int,
    user_id: int,
    demand: DemandResult,
    state: SessionState,
) -> PublicDemand:
    """Guarda una demanda creada desde Telegram usando el flujo ligero actual."""
    attributes = {
        **(demand.attributes or {}),
        "description": state.original_text,
    }
    conn = _get_connection()
    zone = _demand_zone_fields(demand)
    try:
        llm_metadata = _enrich_search_metadata(
            raw_text=state.original_text,
            summary=demand.summary,
            location_label=demand.location_value or demand.location or zone.get("location_label") or "",
            budget_max=demand.budget_max,
            budget_unit=demand.budget_unit,
            llm_metadata=demand.llm_metadata or {},
        )
    except Exception as exc:
        logger.warning("No he podido enriquecer metadatos de búsqueda para Telegram: %s", exc)
        llm_metadata = dict(demand.llm_metadata or {})
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO demands (
                        public_id, telegram_user_id, user_id, intent_domain, intent_type, summary, location,
                        budget_min, budget_max, budget_unit, urgency, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        attributes, normalized_payload, llm_metadata, schema_version, original_text,
                        conversation, status, expires_at, created_via
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        '[]', 'open', NOW() + interval '48 hours', 'telegram'
                    )
                    RETURNING id, public_id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, budget_unit, urgency, status, is_pinned, expires_at, created_at, attributes, normalized_payload, llm_metadata, original_text
                    """,
                    (
                        _next_demand_public_id(cur),
                        telegram_user_id,
                        user_id,
                        "",
                        "free_text",
                        demand.summary,
                        demand.location_value or demand.location,
                        None,
                        demand.budget_max,
                        demand.budget_unit,
                        None,
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(attributes, ensure_ascii=False),
                        json.dumps({}, ensure_ascii=False),
                        json.dumps(llm_metadata, ensure_ascii=False),
                        "",
                        state.original_text,
                    ),
                )
                row = dict(cur.fetchone())
                _apply_zone_geometries(cur, "demands", row["id"], zone)
                row = _hydrate_demand_row(row)
                row["offer_count"] = 0
                return PublicDemand.model_validate(row)
    finally:
        conn.close()


def create_web_demand(
    user_id: int,
    summary: str,
    description: str,
    location: str = "",
    budget_max: Optional[float] = None,
    budget_unit: str = "total",
    zone_filter: Optional[dict[str, Any]] = None,
    suggested_missing_details: Optional[list[str]] = None,
    include_embeddings: bool = True,
) -> PublicDemand:
    """Crea una demanda simple desde la web."""
    description = str(description or "").strip()
    if len(description) > DEMAND_TEXT_MAX_LENGTH:
        raise ValueError(f"La demanda no puede superar {DEMAND_TEXT_MAX_LENGTH} caracteres.")
    attributes = {"description": description.strip()}
    zone = zone_to_storage_fields(zone_filter)
    llm_metadata = {"suggested_missing_details": list(suggested_missing_details or [])}
    if include_embeddings:
        llm_metadata = _enrich_search_metadata(
            raw_text=description.strip(),
            summary=summary.strip(),
            location_label=location.strip() or zone["location_label"] or "",
            budget_max=budget_max,
            budget_unit=budget_unit,
            llm_metadata=llm_metadata,
        )
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO demands (
                        public_id, user_id, telegram_user_id, intent_domain, intent_type, summary, location,
                        budget_min, budget_max, budget_unit, urgency, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        attributes, normalized_payload, llm_metadata, schema_version, original_text,
                        conversation, status, expires_at, created_via
                    )
                    VALUES (
                        %s, %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        '[]', 'open', NOW() + interval '48 hours', 'web'
                    )
                    RETURNING id, public_id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, budget_unit, urgency, status, is_pinned, expires_at, created_at, attributes, normalized_payload, llm_metadata, original_text
                    """,
                    (
                        _next_demand_public_id(cur),
                        user_id,
                        "",
                        "free_text",
                        summary.strip(),
                        location.strip() or zone["location_label"] or None,
                        None,
                        budget_max,
                        budget_unit.strip() or "total",
                        None,
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(attributes, ensure_ascii=False),
                        json.dumps({}, ensure_ascii=False),
                        json.dumps(llm_metadata, ensure_ascii=False),
                        "",
                        description.strip(),
                    ),
                )
                row = dict(cur.fetchone())
                _apply_zone_geometries(cur, "demands", row["id"], zone)
                row = _hydrate_demand_row(row)
                row["offer_count"] = 0
                return PublicDemand.model_validate(row)
    finally:
        conn.close()


def save_web_demand_from_agent(
    user_id: int,
    demand: DemandResult,
    state: SessionState,
) -> PublicDemand:
    """Guarda una demanda web creada a partir del analizador ligero."""
    attributes = {
        **demand.attributes,
        "description": state.original_text,
    }
    conn = _get_connection()
    zone = _demand_zone_fields(demand)
    llm_metadata = _enrich_search_metadata(
        raw_text=state.original_text,
        summary=demand.summary,
        location_label=demand.location_value or demand.location or zone.get("location_label") or "",
        budget_max=demand.budget_max,
        budget_unit=demand.budget_unit,
        llm_metadata=demand.llm_metadata or {},
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO demands (
                        public_id, user_id, telegram_user_id, intent_domain, intent_type, summary, location,
                        budget_min, budget_max, budget_unit, urgency, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        attributes, normalized_payload, llm_metadata, schema_version, original_text,
                        conversation, status, expires_at, created_via
                    )
                    VALUES (
                        %s, %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, 'open', NOW() + interval '48 hours', 'web'
                    )
                    RETURNING id, public_id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, budget_unit, urgency, status, is_pinned, expires_at, created_at, attributes, normalized_payload, llm_metadata, original_text
                    """,
                    (
                        _next_demand_public_id(cur),
                        user_id,
                        "",
                        "free_text",
                        demand.summary,
                        demand.location_value or demand.location,
                        None,
                        demand.budget_max,
                        demand.budget_unit,
                        None,
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(attributes, ensure_ascii=False),
                        json.dumps({}, ensure_ascii=False),
                        json.dumps(llm_metadata, ensure_ascii=False),
                        "",
                        state.original_text,
                        "[]",
                    ),
                )
                row = dict(cur.fetchone())
                _apply_zone_geometries(cur, "demands", row["id"], zone)
                row = _hydrate_demand_row(row)
                row["offer_count"] = 0
                return PublicDemand.model_validate(row)
    finally:
        conn.close()


def get_demands_by_user(user_id: int, telegram_user_id: int | None = None) -> list[dict[str, Any]]:
    """Obtiene demandas creadas por un usuario autenticado, incluyendo su origen Telegram."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, intent_domain, intent_type, summary, location, location_label, location_lat, location_lon,
                       public_id,
                       location_radius_km, location_json, budget_min, budget_max, budget_unit, urgency,
                       status, expires_at, attributes, normalized_payload, llm_metadata, original_text, created_at
                FROM demands
                WHERE user_id = %s OR (%s IS NOT NULL AND telegram_user_id = %s)
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (user_id, telegram_user_id, telegram_user_id),
            )
            rows = cur.fetchall()
            return [_hydrate_demand_row(dict(r)) for r in rows]
    finally:
        conn.close()


def get_public_demands(
    query_text: str = "",
    location: str = "",
    viewer_user_id: Optional[int] = None,
    zone_filter: Optional[dict[str, Any]] = None,
) -> list[PublicDemand]:
    """Lista demandas activas y públicas con filtros simples."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            where_parts = [
                "d.status = 'open'",
                "(d.expires_at IS NULL OR d.expires_at > NOW())",
                "(%s = '' OR COALESCE(d.location_label, d.location, '') ILIKE %s)",
            ]
            params: list[Any] = [
                viewer_user_id or -1,
                viewer_user_id or -1,
                location,
                f"%{location}%",
            ]

            if zone_filter and _postgis_available():
                search_shape_sql, search_params = _search_shape_sql(zone_filter)
                if search_shape_sql:
                    where_parts.append(
                        f"({_location_shape_sql('d')} IS NOT NULL AND ST_Intersects({_location_shape_sql('d')}, {search_shape_sql}))"
                    )
                    params.extend(search_params)

            query = f"""
                SELECT d.id, d.public_id, d.user_id, d.intent_domain, d.summary, d.intent_type, d.original_text, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_mode, d.location_admin_level, d.location_bbox, d.location_geojson, d.location_json, d.budget_min, d.budget_max, d.budget_unit,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.llm_metadata,
                       COALESCE(u.full_name, 'Demandante') AS owner_name,
                       COUNT(o.id)::INTEGER AS offer_count,
                       BOOL_OR(CASE WHEN o.supplier_user_id = %s THEN TRUE ELSE FALSE END) AS viewer_has_offer,
                       MAX(CASE WHEN o.supplier_user_id = %s THEN o.id ELSE NULL END) AS viewer_offer_id
                FROM demands d
                LEFT JOIN users u ON u.id = d.user_id
                LEFT JOIN offers o ON o.demand_id = d.id
                WHERE {' AND '.join(where_parts)}
                GROUP BY d.id, u.full_name
                ORDER BY d.created_at DESC
                """
            cur.execute(query, params)
            rows = [_hydrate_demand_row(dict(row)) for row in cur.fetchall()]
            if query_text.strip():
                query_embedding = embed_query_text(query_text)
                ranked_rows: list[dict[str, Any]] = []
                rerank_already_applied = False
                rerank_attempted = False

                if query_embedding:
                    for row in rows:
                        score = _semantic_match_score(row, query_embedding)
                        if score >= 0.12:
                            row["_match_score"] = score
                            ranked_rows.append(row)
                    ranked_rows = sorted(
                        ranked_rows,
                        key=lambda item: (
                            float(item.get("_match_score", 0.0)),
                            item.get("created_at") or datetime.min,
                        ),
                        reverse=True,
                    )
                else:
                    rerank_input = [
                        {
                            "id": row["id"],
                            "summary": row.get("summary"),
                            "text": row.get("original_text") or row.get("summary"),
                        }
                        for row in rows[:120]
                    ]
                    rerank_attempted = True
                    rerank_scores = rerank_demand_candidates(query_text, rerank_input)
                    if rerank_scores:
                        ranked_rows = []
                        for row in rows:
                            rerank_score = float(rerank_scores.get(row["id"], 0.0))
                            if rerank_score >= 0.22:
                                row["_rerank_score"] = rerank_score
                                ranked_rows.append(row)
                        ranked_rows = sorted(
                            ranked_rows,
                            key=lambda item: (
                                float(item.get("_rerank_score", 0.0)),
                                item.get("created_at") or datetime.min,
                            ),
                            reverse=True,
                        )
                        rerank_already_applied = True
                    else:
                        ranked_rows = []
                        for row in rows:
                            fallback_score = _fallback_text_match_score(row, query_text)
                            if fallback_score > 0:
                                row["_fallback_score"] = fallback_score
                                ranked_rows.append(row)
                        ranked_rows = sorted(
                            ranked_rows,
                            key=lambda item: (
                                float(item.get("_fallback_score", 0.0)),
                                item.get("created_at") or datetime.min,
                            ),
                            reverse=True,
                        )

                if ranked_rows and not rerank_already_applied and not rerank_attempted:
                    rerank_input = [
                        {
                            "id": row["id"],
                            "summary": row.get("summary"),
                            "text": row.get("original_text") or row.get("summary"),
                        }
                        for row in ranked_rows[:25]
                    ]
                    rerank_scores = rerank_demand_candidates(query_text, rerank_input)
                    if rerank_scores:
                        filtered_rows: list[dict[str, Any]] = []
                        for row in ranked_rows:
                            rerank_score = float(rerank_scores.get(row["id"], 0.0))
                            row["_rerank_score"] = rerank_score
                            if rerank_score >= 0.22:
                                filtered_rows.append(row)
                        ranked_rows = sorted(
                            filtered_rows,
                            key=lambda item: (
                                float(item.get("_rerank_score", 0.0)),
                                float(item.get("_match_score", item.get("_fallback_score", 0.0))),
                                item.get("created_at") or datetime.min,
                            ),
                            reverse=True,
                        )
                rows = ranked_rows
            if zone_filter:
                rows = [row for row in rows if _matches_zone_filter(row, zone_filter)]
            return [PublicDemand.model_validate(row) for row in rows]
    finally:
        conn.close()


def list_admin_demands(limit: int = 200) -> list[dict[str, Any]]:
    """Lista demandas para inspección interna de normalización."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            cur.execute(
                """
                SELECT d.id, d.public_id, d.user_id, d.intent_domain, d.summary, d.intent_type, d.original_text, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_json, d.budget_min, d.budget_max, d.budget_unit,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.llm_metadata,
                       COALESCE(u.full_name, 'Demandante') AS owner_name,
                       COUNT(o.id)::INTEGER AS offer_count
                FROM demands d
                LEFT JOIN users u ON u.id = d.user_id
                LEFT JOIN offers o ON o.demand_id = d.id
                GROUP BY d.id, u.full_name
                ORDER BY d.created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [_hydrate_demand_row(dict(row)) for row in cur.fetchall()]
    finally:
        conn.close()


def reindex_demand_embeddings(limit: int | None = None) -> int:
    """Regenera search_text y embeddings de las demandas existentes."""
    conn = _get_connection()
    updated = 0
    try:
        with conn:
            with conn.cursor() as cur:
                _expire_open_demands(cur)
                sql = """
                    SELECT id, summary, original_text, location, location_label, budget_max, budget_unit, llm_metadata
                    FROM demands
                    WHERE status <> 'deleted'
                    ORDER BY created_at DESC
                """
                params: list[Any] = []
                if limit is not None:
                    sql += " LIMIT %s"
                    params.append(limit)
                cur.execute(sql, params)
                rows = cur.fetchall()
                for row in rows:
                    llm_metadata = _enrich_search_metadata(
                        raw_text=str(row.get("original_text") or row.get("summary") or "").strip(),
                        summary=str(row.get("summary") or "").strip(),
                        location_label=str(row.get("location_label") or row.get("location") or "").strip(),
                        budget_max=row.get("budget_max"),
                        budget_unit=str(row.get("budget_unit") or "total"),
                        llm_metadata=dict(row.get("llm_metadata") or {}),
                    )
                    cur.execute(
                        """
                        UPDATE demands
                        SET llm_metadata = %s
                        WHERE id = %s
                        """,
                        (json.dumps(llm_metadata, ensure_ascii=False), row["id"]),
                    )
                    updated += 1
        return updated
    finally:
        conn.close()


def get_demand_id_by_public_id(public_id: str) -> Optional[int]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM demands WHERE public_id = %s", (str(public_id or "").strip(),))
            row = cur.fetchone()
            return int(row["id"]) if row else None
    finally:
        conn.close()


def get_demand_detail(demand_id: str, viewer_user_id: Optional[int] = None) -> Optional[dict[str, Any]]:
    """Obtiene una demanda y metadatos para la vista de detalle a partir de su public_id."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            cur.execute(
                """
                SELECT d.id, d.public_id, d.intent_domain, d.summary, d.intent_type, d.original_text, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_json, d.budget_min, d.budget_max, d.budget_unit,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.llm_metadata, d.user_id,
                       COALESCE(u.full_name, 'Demandante') AS owner_name,
                       COUNT(o.id)::INTEGER AS offer_count
                FROM demands d
                LEFT JOIN users u ON u.id = d.user_id
                LEFT JOIN offers o ON o.demand_id = d.id
                WHERE d.public_id = %s
                GROUP BY d.id, u.full_name
                """,
                (demand_id,),
            )
            row = cur.fetchone()
            if not row:
                return None

            data = _hydrate_demand_row(dict(row))
            if data["effective_status"] == "deleted":
                return None
            data["is_owner"] = viewer_user_id is not None and data["user_id"] == viewer_user_id

            if viewer_user_id is not None:
                cur.execute(
                    """
                    SELECT id
                    FROM offers
                    WHERE demand_id = %s AND supplier_user_id = %s
                    """,
                    (data["id"], viewer_user_id),
                )
                offer_row = cur.fetchone()
                data["viewer_has_offer"] = offer_row is not None
                data["viewer_offer_id"] = offer_row["id"] if offer_row else None
            else:
                data["viewer_has_offer"] = False
                data["viewer_offer_id"] = None

            return data
    finally:
        conn.close()


def get_editable_demand(demand_id: int, user_id: int) -> Optional[dict[str, Any]]:
    """Obtiene una demanda abierta editable por su propietario."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            cur.execute(
                """
                SELECT d.id, d.public_id, d.user_id, d.intent_domain, d.summary, d.intent_type, d.original_text, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_json, d.budget_min, d.budget_max, d.budget_unit,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.llm_metadata,
                       COALESCE(u.full_name, 'Demandante') AS owner_name
                FROM demands d
                LEFT JOIN users u ON u.id = d.user_id
                WHERE d.id = %s AND d.user_id = %s AND d.status = 'open'
                  AND (d.expires_at IS NULL OR d.expires_at > NOW())
                """,
                (demand_id, user_id),
            )
            row = cur.fetchone()
            return _hydrate_demand_row(dict(row)) if row else None
    finally:
        conn.close()


def update_web_demand_from_agent(
    demand_id: int,
    user_id: int,
    demand: DemandResult,
    state: SessionState,
) -> Optional[PublicDemand]:
    """Actualiza una demanda web abierta usando el modelo ligero."""
    attributes = {
        **demand.attributes,
        "description": state.original_text,
    }

    conn = _get_connection()
    zone = _demand_zone_fields(demand)
    llm_metadata = _enrich_search_metadata(
        raw_text=state.original_text,
        summary=demand.summary,
        location_label=demand.location_value or demand.location or zone.get("location_label") or "",
        budget_max=demand.budget_max,
        budget_unit=demand.budget_unit,
        llm_metadata=demand.llm_metadata or {},
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE demands
                    SET intent_domain = %s,
                        intent_type = %s,
                        summary = %s,
                        location = %s,
                        budget_min = %s,
                        budget_max = %s,
                        budget_unit = %s,
                        urgency = %s,
                        location_mode = %s,
                        location_label = %s,
                        location_lat = %s,
                        location_lon = %s,
                        location_radius_km = %s,
                        location_radius_bucket = %s,
                        location_source = %s,
                        location_raw_query = %s,
                        location_admin_level = %s,
                        location_bbox = %s,
                        location_geojson = %s,
                        location_json = %s,
                        attributes = %s,
                        normalized_payload = %s,
                        llm_metadata = %s,
                        schema_version = %s,
                        original_text = %s,
                        conversation = %s
                    WHERE id = %s
                      AND user_id = %s
                      AND status = 'open'
                      AND (expires_at IS NULL OR expires_at > NOW())
                    RETURNING id, public_id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, budget_unit, urgency, status, expires_at, created_at, attributes, normalized_payload, llm_metadata, original_text
                    """,
                    (
                        "",
                        "free_text",
                        demand.summary,
                        demand.location_value or demand.location,
                        None,
                        demand.budget_max,
                        demand.budget_unit,
                        None,
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(attributes, ensure_ascii=False),
                        json.dumps({}, ensure_ascii=False),
                        json.dumps(llm_metadata, ensure_ascii=False),
                        "",
                        state.original_text,
                        "[]",
                        demand_id,
                        user_id,
                    ),
                )
                row = cur.fetchone()
                if not row:
                    return None
                _apply_zone_geometries(cur, "demands", row["id"], zone)
                data = _hydrate_demand_row(dict(row))
                data["offer_count"] = 0
                return PublicDemand.model_validate(data)
    finally:
        conn.close()


def get_dashboard_data(user_id: int, demand_status_filter: str = "active", offer_filter: str = "visible") -> dict[str, Any]:
    """Recoge demandas y conversaciones del usuario, separando activos e históricos."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            my_demands = _fetch_owned_demands_with_conversations(cur, user_id)
            my_offers = _fetch_supplier_offer_threads(cur, user_id)
            demand_status_filter = (demand_status_filter or "active").strip().lower()
            offer_filter = (offer_filter or "visible").strip().lower()
            my_demands_filtered = _filter_owner_demands_by_status(my_demands, demand_status_filter)
            my_offers_filtered = _filter_supplier_offers(my_offers, offer_filter)

            return {
                "my_demands_active": my_demands_filtered,
                "my_demands_archived": [d for d in my_demands if not d["is_active"]],
                "my_offers_active": my_offers_filtered,
                "my_offers_archived": [o for o in my_offers if not o["is_active"]],
                "demand_status_filter": demand_status_filter,
                "offer_filter": offer_filter,
                "my_demands_status_counts": _owner_demand_status_counts(my_demands),
                "my_offers_status_counts": _supplier_offer_status_counts(my_offers),
            }
    finally:
        conn.close()


def get_notification_summary(user_id: int) -> dict[str, Any]:
    """Devuelve conteos y últimas conversaciones no leídas para refresco en vivo."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::INTEGER AS unread_threads
                FROM offers o
                JOIN demands d ON d.id = o.demand_id
                WHERE d.user_id = %s
                  AND EXISTS (
                    SELECT 1
                    FROM offer_messages m
                    WHERE m.offer_id = o.id
                      AND m.sender_user_id <> %s
                      AND (o.demand_owner_last_read_at IS NULL OR m.created_at > o.demand_owner_last_read_at)
                  )
                """,
                (user_id, user_id),
            )
            demands_unread = cur.fetchone()["unread_threads"]

            cur.execute(
                """
                SELECT COUNT(*)::INTEGER AS unread_threads
                FROM offers o
                WHERE o.supplier_user_id = %s
                  AND EXISTS (
                    SELECT 1
                    FROM offer_messages m
                    WHERE m.offer_id = o.id
                      AND m.sender_user_id <> %s
                      AND (o.supplier_last_read_at IS NULL OR m.created_at > o.supplier_last_read_at)
                  )
                """,
                (user_id, user_id),
            )
            offers_unread = cur.fetchone()["unread_threads"]

            cur.execute(
                """
                SELECT * FROM (
                    SELECT o.id AS offer_id, d.summary AS demand_summary, u.full_name AS sender_name,
                           MAX(m.created_at) AS last_message_at, '/my-demands#offer-' || o.id AS target
                    FROM offers o
                    JOIN demands d ON d.id = o.demand_id
                    JOIN offer_messages m ON m.offer_id = o.id
                    JOIN users u ON u.id = m.sender_user_id
                    WHERE d.user_id = %s
                      AND m.sender_user_id <> %s
                      AND (o.demand_owner_last_read_at IS NULL OR m.created_at > o.demand_owner_last_read_at)
                    GROUP BY o.id, d.summary, u.full_name
                    UNION ALL
                    SELECT o.id AS offer_id, d.summary AS demand_summary, u.full_name AS sender_name,
                           MAX(m.created_at) AS last_message_at, '/my-offers#offer-' || o.id AS target
                    FROM offers o
                    JOIN demands d ON d.id = o.demand_id
                    JOIN offer_messages m ON m.offer_id = o.id
                    JOIN users u ON u.id = m.sender_user_id
                    WHERE o.supplier_user_id = %s
                      AND m.sender_user_id <> %s
                      AND (o.supplier_last_read_at IS NULL OR m.created_at > o.supplier_last_read_at)
                    GROUP BY o.id, d.summary, u.full_name
                ) unread
                ORDER BY last_message_at DESC
                LIMIT 10
                """,
                (user_id, user_id, user_id, user_id),
            )
            items = [dict(row) for row in cur.fetchall()]
            for item in items:
                item["signature"] = f"{item['offer_id']}|{item['last_message_at']}"

            return {
                "my_demands_unread": demands_unread,
                "my_offers_unread": offers_unread,
                "items": items,
            }
    finally:
        conn.close()


def list_saved_filters(user_id: int) -> list[dict[str, Any]]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, query_text, location, location_label, location_lat, location_lon,
                       location_mode, location_radius_km, location_radius_bucket, location_source, location_raw_query,
                       location_admin_level, location_bbox, location_geojson, location_json,
                       intent_domains, intent_types,
                       intent_type, created_at
                FROM saved_filters
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_filter(
    user_id: int,
    name: str,
    query_text: str,
    location: str,
    intent_type: str,
    zone_filter: Optional[dict[str, Any]] = None,
    intent_domains: Optional[list[str]] = None,
    intent_types: Optional[list[str]] = None,
) -> int:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                zone = zone_to_storage_fields(zone_filter)
                cur.execute(
                    """
                    INSERT INTO saved_filters (
                        user_id, name, query_text, location, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        intent_domains, intent_types,
                        intent_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        name.strip(),
                        query_text.strip(),
                        location.strip(),
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(intent_domains or [], ensure_ascii=False),
                        json.dumps(intent_types or [], ensure_ascii=False),
                        intent_type.strip(),
                    ),
                )
                cur.execute("SELECT currval(pg_get_serial_sequence('saved_filters','id')) AS id")
                row_id = cur.fetchone()["id"]
                _apply_zone_geometries(cur, "saved_filters", row_id, zone)
                return row_id
    finally:
        conn.close()


def update_filter(
    user_id: int,
    filter_id: int,
    name: str,
    query_text: str,
    location: str,
    intent_type: str,
    zone_filter: Optional[dict[str, Any]] = None,
    intent_domains: Optional[list[str]] = None,
    intent_types: Optional[list[str]] = None,
) -> bool:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                zone = zone_to_storage_fields(zone_filter)
                cur.execute(
                    """
                    UPDATE saved_filters
                    SET name = %s,
                        query_text = %s,
                        location = %s,
                        location_mode = %s,
                        location_label = %s,
                        location_lat = %s,
                        location_lon = %s,
                        location_radius_km = %s,
                        location_radius_bucket = %s,
                        location_source = %s,
                        location_raw_query = %s,
                        location_admin_level = %s,
                        location_bbox = %s,
                        location_geojson = %s,
                        location_json = %s,
                        intent_domains = %s,
                        intent_types = %s,
                        intent_type = %s
                    WHERE id = %s AND user_id = %s
                    """,
                    (
                        name.strip(),
                        query_text.strip(),
                        location.strip(),
                        zone["location_mode"],
                        zone["location_label"],
                        zone["location_lat"],
                        zone["location_lon"],
                        zone["location_radius_km"],
                        zone["location_radius_bucket"],
                        zone["location_source"],
                        zone["location_raw_query"],
                        zone["location_admin_level"],
                        json.dumps(zone["location_bbox"], ensure_ascii=False),
                        json.dumps(zone["location_geojson"], ensure_ascii=False),
                        json.dumps(zone["location_json"], ensure_ascii=False),
                        json.dumps(intent_domains or [], ensure_ascii=False),
                        json.dumps(intent_types or [], ensure_ascii=False),
                        intent_type.strip(),
                        filter_id,
                        user_id,
                    ),
                )
                if cur.rowcount > 0:
                    _apply_zone_geometries(cur, "saved_filters", filter_id, zone)
                return cur.rowcount > 0
    finally:
        conn.close()


def delete_filter(user_id: int, filter_id: int) -> bool:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM saved_filters
                    WHERE id = %s AND user_id = %s
                    """,
                    (filter_id, user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def get_saved_filter(user_id: int, filter_id: int) -> Optional[dict[str, Any]]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, query_text, location, location_label, location_lat, location_lon,
                       location_mode, location_radius_km, location_radius_bucket, location_source, location_raw_query,
                       location_admin_level, location_bbox, location_geojson, location_json,
                       intent_domains, intent_types,
                       intent_type
                FROM saved_filters
                WHERE id = %s AND user_id = %s
                """,
                (filter_id, user_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_demand_wizard(user_id: int) -> Optional[dict[str, Any]]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload
                FROM demand_wizards
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            payload = row.get("payload")
            return dict(payload) if isinstance(payload, dict) else None
    finally:
        conn.close()


def save_demand_wizard(user_id: int, payload: dict[str, Any]) -> None:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO demand_wizards (user_id, payload, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (user_id)
                    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                    """,
                    (user_id, json.dumps(payload, ensure_ascii=False)),
                )
    finally:
        conn.close()


def clear_demand_wizard(user_id: int) -> None:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM demand_wizards
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
    finally:
        conn.close()


def get_offers_for_owner(demand_id: int, owner_user_id: int) -> list[OfferResult]:
    """Lista las ofertas de una demanda si la demanda pertenece al usuario."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.id, o.demand_id, o.supplier_user_id, u.full_name AS supplier_name,
                       u.email AS supplier_email, o.message, o.created_at
                FROM offers o
                JOIN demands d ON d.id = o.demand_id
                JOIN users u ON u.id = o.supplier_user_id
                WHERE d.id = %s AND d.user_id = %s
                ORDER BY o.created_at DESC
                """,
                (demand_id, owner_user_id),
            )
            return [OfferResult.model_validate(dict(row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_owned_demands_with_conversations(cur, user_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT d.id, d.public_id, d.intent_domain, d.summary, d.intent_type, d.original_text, d.location, d.budget_min, d.budget_max, d.budget_unit,
               d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.llm_metadata,
               (d.status = 'open' AND (d.expires_at IS NULL OR d.expires_at > NOW())) AS is_active,
               COUNT(o.id)::INTEGER AS offer_count,
               BOOL_OR(COALESCE(om.has_unread, FALSE)) AS has_unread
        FROM demands d
        LEFT JOIN offers o ON o.demand_id = d.id
        LEFT JOIN LATERAL (
            SELECT EXISTS (
                SELECT 1
                FROM offer_messages m
                WHERE m.offer_id = o.id
                  AND m.sender_user_id <> %s
                  AND (o.demand_owner_last_read_at IS NULL OR m.created_at > o.demand_owner_last_read_at)
            ) AS has_unread
        ) om ON TRUE
        WHERE d.user_id = %s
          AND d.status <> 'deleted'
          AND (
            d.status <> 'expired'
            OR d.expires_at IS NULL
            OR d.expires_at > NOW() - interval '7 days'
          )
        GROUP BY d.id
        ORDER BY d.is_pinned DESC, d.created_at DESC
        """,
        (user_id, user_id),
    )
    demands = [_hydrate_demand_row(dict(row)) for row in cur.fetchall()]

    for demand in demands:
        demand["conversations"] = _fetch_demand_conversations(cur, demand["id"], user_id)
    return demands


def _fetch_demand_conversations(cur, demand_id: int, owner_user_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT o.id AS offer_id, o.created_at, o.updated_at, o.message,
               u.full_name AS supplier_name, u.email AS supplier_email,
               COALESCE(last_message.body, o.message) AS last_message,
               COALESCE(last_message.created_at, o.updated_at, o.created_at) AS last_message_at,
               COALESCE(unread.unread_count, 0)::INTEGER AS unread_count
        FROM offers o
        JOIN users u ON u.id = o.supplier_user_id
        LEFT JOIN LATERAL (
            SELECT m.body, m.created_at
            FROM offer_messages m
            WHERE m.offer_id = o.id
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT 1
        ) last_message ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS unread_count
            FROM offer_messages m
            WHERE m.offer_id = o.id
              AND m.sender_user_id <> %s
              AND (o.demand_owner_last_read_at IS NULL OR m.created_at > o.demand_owner_last_read_at)
        ) unread ON TRUE
        WHERE o.demand_id = %s
        ORDER BY COALESCE(last_message.created_at, o.updated_at, o.created_at) DESC
        """,
        (owner_user_id, demand_id),
    )
    conversations = [dict(row) for row in cur.fetchall()]
    for conversation in conversations:
        conversation["messages"] = _fetch_offer_messages(cur, conversation["offer_id"])
    return conversations


def _fetch_supplier_offer_threads(cur, user_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT o.id AS offer_id, o.demand_id, d.public_id AS demand_public_id, o.message, o.created_at, o.updated_at,
               o.supplier_is_pinned, o.supplier_hidden,
               d.summary AS demand_summary, d.intent_domain, d.intent_type, d.original_text, d.location, d.budget_min, d.budget_max, d.budget_unit,
               d.urgency, d.attributes, d.normalized_payload, d.llm_metadata,
               COALESCE(owner_user.full_name, 'Demandante') AS owner_name,
               d.status, d.is_pinned, d.expires_at AS demand_expires_at,
               (d.status = 'open' AND (d.expires_at IS NULL OR d.expires_at > NOW())) AS is_active,
               COALESCE(last_message.body, o.message) AS last_message,
               COALESCE(last_message.created_at, o.updated_at, o.created_at) AS last_message_at,
               COALESCE(unread.unread_count, 0)::INTEGER AS unread_count
        FROM offers o
        JOIN demands d ON d.id = o.demand_id
        LEFT JOIN users owner_user ON owner_user.id = d.user_id
        LEFT JOIN LATERAL (
            SELECT m.body, m.created_at
            FROM offer_messages m
            WHERE m.offer_id = o.id
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT 1
        ) last_message ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS unread_count
            FROM offer_messages m
            WHERE m.offer_id = o.id
              AND m.sender_user_id <> %s
              AND (o.supplier_last_read_at IS NULL OR m.created_at > o.supplier_last_read_at)
        ) unread ON TRUE
        WHERE o.supplier_user_id = %s
          AND d.status <> 'deleted'
          AND (
            d.status <> 'expired'
            OR d.expires_at IS NULL
            OR d.expires_at > NOW() - interval '7 days'
          )
        ORDER BY o.supplier_is_pinned DESC, COALESCE(last_message.created_at, o.updated_at, o.created_at) DESC
        """,
        (user_id, user_id),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["normalized_payload"] = _lightweight_payload_for_row(
            {
                "id": row.get("demand_id"),
                "summary": row.get("demand_summary"),
                "original_text": row.get("original_text"),
                "location": row.get("location"),
                "budget_max": row.get("budget_max"),
                "budget_unit": row.get("budget_unit"),
                "attributes": row.get("attributes") or {},
                "normalized_payload": row.get("normalized_payload") or {},
                "llm_metadata": row.get("llm_metadata") or {},
            }
        )
        row["messages"] = _fetch_offer_messages(cur, row["offer_id"])
        row["supplier_is_pinned"] = bool(row.get("supplier_is_pinned"))
        row["supplier_hidden"] = bool(row.get("supplier_hidden"))
        row["effective_status"] = str(row.get("status") or "open").lower()
        row["can_pin"] = not row["supplier_hidden"]
        row["can_hide"] = not row["supplier_hidden"]
        row["can_unhide"] = row["supplier_hidden"]
        row["can_interact"] = row["effective_status"] == "open"
    return rows


def _owner_demand_status_counts(demands: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"active": 0, "paused": 0, "expired": 0, "all": len(demands)}
    for demand in demands:
        status_name = str(demand.get("effective_status") or demand.get("status") or "").lower()
        if status_name == "open":
            counts["active"] += 1
        elif status_name in counts:
            counts[status_name] += 1
    return counts


def _filter_owner_demands_by_status(demands: list[dict[str, Any]], status_filter: str) -> list[dict[str, Any]]:
    target = (status_filter or "active").strip().lower()
    if target == "all":
        return list(demands)
    status_alias = {"active": "open", "paused": "paused", "expired": "expired"}
    expected = status_alias.get(target, "open")
    return [demand for demand in demands if str(demand.get("effective_status") or demand.get("status") or "").lower() == expected]


def _supplier_offer_status_counts(offers: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"visible": 0, "pinned": 0, "hidden": 0, "all": len(offers)}
    for offer in offers:
        hidden = bool(offer.get("supplier_hidden"))
        pinned = bool(offer.get("supplier_is_pinned"))
        if hidden:
            counts["hidden"] += 1
        else:
            counts["visible"] += 1
            if pinned:
                counts["pinned"] += 1
    return counts


def _filter_supplier_offers(offers: list[dict[str, Any]], offer_filter: str) -> list[dict[str, Any]]:
    target = (offer_filter or "visible").strip().lower()
    if target == "all":
        return list(offers)
    if target == "hidden":
        return [offer for offer in offers if bool(offer.get("supplier_hidden"))]
    if target == "pinned":
        return [offer for offer in offers if bool(offer.get("supplier_is_pinned")) and not bool(offer.get("supplier_hidden"))]
    return [offer for offer in offers if not bool(offer.get("supplier_hidden"))]


def _fetch_offer_messages(cur, offer_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT om.id, om.offer_id, om.sender_user_id, u.full_name AS sender_name,
               om.body, om.created_at
        FROM offer_messages om
        JOIN users u ON u.id = om.sender_user_id
        WHERE om.offer_id = %s
        ORDER BY om.created_at ASC, om.id ASC
        """,
        (offer_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def create_offer(demand_id: int, supplier_user_id: int, message: str) -> OfferResult:
    """Crea o actualiza la oferta inicial de un ofertante para una demanda."""
    message = str(message or "").strip()
    if len(message) > MESSAGE_TEXT_MAX_LENGTH:
        raise ValueError(f"El mensaje no puede superar {MESSAGE_TEXT_MAX_LENGTH} caracteres.")
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _expire_open_demands(cur)
                cur.execute(
                    """
                    SELECT id, user_id, status, expires_at,
                           (expires_at IS NULL OR expires_at > NOW()) AS is_active
                    FROM demands
                    WHERE id = %s
                    """,
                    (demand_id,),
                )
                demand_row = cur.fetchone()
                if not demand_row:
                    raise ValueError("La demanda no existe.")
                if demand_row["status"] == "deleted":
                    raise ValueError("La demanda ya no está disponible.")
                if demand_row["status"] != "open":
                    raise ValueError("La demanda ya no está abierta.")
                if not demand_row["is_active"]:
                    raise ValueError("La demanda ha caducado.")
                if demand_row["user_id"] == supplier_user_id:
                    raise ValueError("No puedes ofertar sobre tu propia demanda.")

                cur.execute(
                    """
                    INSERT INTO offers (demand_id, supplier_user_id, message, supplier_last_read_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (demand_id, supplier_user_id)
                    DO UPDATE SET
                        message = EXCLUDED.message,
                        supplier_last_read_at = NOW(),
                        updated_at = NOW(),
                        created_at = NOW()
                    RETURNING id, demand_id, supplier_user_id, message, created_at
                    """,
                    (demand_id, supplier_user_id, message),
                )
                row = dict(cur.fetchone())

                cur.execute(
                    """
                    SELECT full_name AS supplier_name, email AS supplier_email
                    FROM users
                    WHERE id = %s
                    """,
                    (supplier_user_id,),
                )
                user_row = dict(cur.fetchone())
                row.update(user_row)
                offer = OfferResult.model_validate(row)

                cur.execute(
                    """
                    INSERT INTO offer_messages (offer_id, sender_user_id, body)
                    VALUES (%s, %s, %s)
                    """,
                    (offer.id, supplier_user_id, message),
                )
                return offer
    finally:
        conn.close()


def get_offer_thread(
    offer_id: int,
    viewer_user_id: int,
    participant_user_id: Optional[int] = None,
    mark_read: bool = True,
) -> Optional[dict[str, Any]]:
    """Obtiene un hilo de oferta si el usuario participa en él como D u O."""
    participant_id = participant_user_id or viewer_user_id
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.id, o.demand_id, d.public_id AS demand_public_id, o.supplier_user_id, o.message, o.created_at,
                           d.user_id AS demand_owner_user_id, d.summary AS demand_summary, d.intent_domain, d.intent_type,
                           d.original_text, d.location, d.budget_min, d.budget_max, d.budget_unit, d.urgency, d.attributes, d.normalized_payload, d.llm_metadata,
                           d.status AS demand_status, d.expires_at AS demand_expires_at,
                           su.full_name AS supplier_name, su.email AS supplier_email,
                           du.full_name AS demand_owner_name
                    FROM offers o
                    JOIN demands d ON d.id = o.demand_id
                    JOIN users su ON su.id = o.supplier_user_id
                    LEFT JOIN users du ON du.id = d.user_id
                    WHERE o.id = %s
                      AND (o.supplier_user_id = %s OR d.user_id = %s)
                      AND d.status <> 'deleted'
                      AND (
                        d.status <> 'expired'
                        OR d.expires_at IS NULL
                        OR d.expires_at > NOW() - interval '7 days'
                      )
                    """,
                    (offer_id, participant_id, participant_id),
                )
                row = cur.fetchone()
                if not row:
                    return None

                data = dict(row)
                data["normalized_payload"] = _lightweight_payload_for_row(
                    {
                        "id": data.get("demand_id"),
                        "summary": data.get("demand_summary"),
                        "original_text": data.get("original_text"),
                        "location": data.get("location"),
                        "budget_max": data.get("budget_max"),
                        "budget_unit": data.get("budget_unit"),
                        "attributes": data.get("attributes") or {},
                        "normalized_payload": data.get("normalized_payload") or {},
                        "llm_metadata": data.get("llm_metadata") or {},
                    }
                )
                data["effective_status"] = str(data.get("demand_status") or "open").lower()
                data["can_reply"] = data["effective_status"] == "open"
                if mark_read:
                    mark_offer_thread_as_read(cur, offer_id, participant_id, data["demand_owner_user_id"], data["supplier_user_id"])
                cur.execute(
                    """
                    SELECT om.id, om.offer_id, om.sender_user_id, u.full_name AS sender_name,
                           om.body, om.created_at
                    FROM offer_messages om
                    JOIN users u ON u.id = om.sender_user_id
                    WHERE om.offer_id = %s
                    ORDER BY om.created_at ASC, om.id ASC
                    """,
                    (offer_id,),
                )
                data["messages"] = [OfferMessageResult.model_validate(dict(msg)) for msg in cur.fetchall()]
                data["viewer_is_owner"] = data["demand_owner_user_id"] == participant_id
                data["read_only"] = participant_id != viewer_user_id
                return data
    finally:
        conn.close()


def create_offer_message(offer_id: int, sender_user_id: int, body: str) -> bool:
    """Añade un mensaje al hilo de una oferta si el usuario participa en ella."""
    body = str(body or "").strip()
    if len(body) > MESSAGE_TEXT_MAX_LENGTH:
        return False
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.id, d.user_id AS demand_owner_user_id, o.supplier_user_id
                           , d.status AS demand_status, d.expires_at,
                           (d.expires_at IS NULL OR d.expires_at > NOW()) AS demand_is_active
                    FROM offers o
                    JOIN demands d ON d.id = o.demand_id
                    WHERE o.id = %s
                    """,
                    (offer_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False
                if sender_user_id not in {row["demand_owner_user_id"], row["supplier_user_id"]}:
                    return False
                if row["demand_status"] != "open":
                    return False
                if not row["demand_is_active"]:
                    return False

                cur.execute(
                    """
                    INSERT INTO offer_messages (offer_id, sender_user_id, body)
                    VALUES (%s, %s, %s)
                    """,
                    (offer_id, sender_user_id, body),
                )
                if sender_user_id == row["demand_owner_user_id"]:
                    cur.execute(
                        "UPDATE offers SET demand_owner_last_read_at = NOW(), updated_at = NOW() WHERE id = %s",
                        (offer_id,),
                    )
                else:
                    cur.execute(
                        "UPDATE offers SET supplier_last_read_at = NOW(), updated_at = NOW() WHERE id = %s",
                        (offer_id,),
                    )
                return True
    finally:
        conn.close()


def mark_offer_thread_as_read(cur, offer_id: int, viewer_user_id: int, demand_owner_user_id: int, supplier_user_id: int) -> None:
    if viewer_user_id == demand_owner_user_id:
        cur.execute(
            "UPDATE offers SET demand_owner_last_read_at = NOW() WHERE id = %s",
            (offer_id,),
        )
    elif viewer_user_id == supplier_user_id:
        cur.execute(
            "UPDATE offers SET supplier_last_read_at = NOW() WHERE id = %s",
            (offer_id,),
        )


def delete_demand(demand_id: int, telegram_user_id: int) -> bool:
    """Borra una demanda del bot si pertenece al usuario."""
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM demands WHERE id = %s AND telegram_user_id = %s",
                    (demand_id, telegram_user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def delete_web_demand(demand_id: int, user_id: int) -> bool:
    """Marca una demanda como eliminada si pertenece al usuario."""
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _expire_open_demands(cur)
                cur.execute(
                    """
                    UPDATE demands
                    SET status = 'deleted',
                        is_pinned = FALSE
                    WHERE id = %s AND user_id = %s AND status <> 'deleted'
                    """,
                    (demand_id, user_id),
                )
                return cur.rowcount > 0
    finally:
        conn.close()


def update_web_demand_lifecycle(user_id: int, demand_id: int, action: str) -> bool:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _expire_open_demands(cur)
                normalized_action = (action or "").strip().lower()
                if normalized_action == "pause":
                    cur.execute(
                        """
                        UPDATE demands
                        SET status = 'paused'
                        WHERE id = %s
                          AND user_id = %s
                          AND status = 'open'
                          AND (expires_at IS NULL OR expires_at > NOW())
                        """,
                        (demand_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "reactivate":
                    cur.execute(
                        """
                        UPDATE demands
                        SET status = 'open'
                        WHERE id = %s
                          AND user_id = %s
                          AND status = 'paused'
                          AND (expires_at IS NULL OR expires_at > NOW())
                        """,
                        (demand_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "delete":
                    cur.execute(
                        """
                        UPDATE demands
                        SET status = 'deleted',
                            is_pinned = FALSE
                        WHERE id = %s
                          AND user_id = %s
                          AND status <> 'deleted'
                        """,
                        (demand_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "pin":
                    cur.execute(
                        """
                        UPDATE demands
                        SET is_pinned = TRUE
                        WHERE id = %s
                          AND user_id = %s
                          AND status <> 'deleted'
                        """,
                        (demand_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "unpin":
                    cur.execute(
                        """
                        UPDATE demands
                        SET is_pinned = FALSE
                        WHERE id = %s
                          AND user_id = %s
                        """,
                        (demand_id, user_id),
                    )
                    return cur.rowcount > 0
                return False
    finally:
        conn.close()


def update_supplier_offer_workspace(user_id: int, offer_id: int, action: str) -> bool:
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                normalized_action = (action or "").strip().lower()
                if normalized_action == "hide":
                    cur.execute(
                        """
                        UPDATE offers
                        SET supplier_hidden = TRUE,
                            supplier_is_pinned = FALSE,
                            updated_at = NOW()
                        WHERE id = %s
                          AND supplier_user_id = %s
                          AND supplier_hidden = FALSE
                        """,
                        (offer_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "unhide":
                    cur.execute(
                        """
                        UPDATE offers
                        SET supplier_hidden = FALSE,
                            updated_at = NOW()
                        WHERE id = %s
                          AND supplier_user_id = %s
                          AND supplier_hidden = TRUE
                        """,
                        (offer_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "pin":
                    cur.execute(
                        """
                        UPDATE offers
                        SET supplier_is_pinned = TRUE,
                            updated_at = NOW()
                        WHERE id = %s
                          AND supplier_user_id = %s
                          AND supplier_hidden = FALSE
                        """,
                        (offer_id, user_id),
                    )
                    return cur.rowcount > 0
                if normalized_action == "unpin":
                    cur.execute(
                        """
                        UPDATE offers
                        SET supplier_is_pinned = FALSE,
                            updated_at = NOW()
                        WHERE id = %s
                          AND supplier_user_id = %s
                        """,
                        (offer_id, user_id),
                    )
                    return cur.rowcount > 0
                return False
    finally:
        conn.close()


def admin_delete_demand(demand_id: int) -> bool:
    """Borra una demanda desde administración junto con sus ofertas y mensajes en cascada."""
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM demands
                    WHERE id = %s
                    """,
                    (demand_id,),
                )
                return cur.rowcount > 0
    finally:
        conn.close()
