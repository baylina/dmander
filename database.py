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
import secrets
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from demand_normalizer import normalize_existing_demand_record
from location_geometry import radius_limit_km, zone_has_geometry, zones_intersect
from master_schema import get_master_schema_registry
from models import DemandResult, OfferMessageResult, OfferResult, PublicDemand, SessionState, UserProfile
from zone_selector import compact_zone_label, zone_to_storage_fields

logger = logging.getLogger(__name__)

DEFAULT_DEMAND_EXPIRY_HOURS = 48
EXPIRED_CONVERSATION_GRACE_DAYS = 7
_POSTGIS_AVAILABLE: Optional[bool] = None


CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    full_name     TEXT NOT NULL,
    password_hash TEXT,
    avatar_url    TEXT,
    auth_source   TEXT NOT NULL DEFAULT 'local',
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
    telegram_user_id BIGINT,
    user_id          INTEGER REFERENCES users(id) ON DELETE SET NULL,
    intent_domain    TEXT,
    intent_type      TEXT NOT NULL,
    summary          TEXT NOT NULL,
    location         TEXT,
    budget_min       REAL,
    budget_max       REAL,
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

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_demands_user_id ON demands(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_demands_status_created_at ON demands(status, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_offers_demand_id ON offers(demand_id);",
    "CREATE INDEX IF NOT EXISTS idx_offer_messages_offer_id ON offer_messages(offer_id, created_at ASC);",
    "CREATE INDEX IF NOT EXISTS idx_saved_filters_user_id ON saved_filters(user_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_demands_location_lat_lon ON demands(location_lat, location_lon);",
    "CREATE INDEX IF NOT EXISTS idx_demand_wizards_updated_at ON demand_wizards(updated_at DESC);",
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
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS intent_domain TEXT;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open';",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT FALSE;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP DEFAULT (NOW() + interval '48 hours');",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS created_via TEXT NOT NULL DEFAULT 'telegram';",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS normalized_payload JSONB DEFAULT '{}'::jsonb;",
    "ALTER TABLE demands ADD COLUMN IF NOT EXISTS schema_version TEXT;",
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
        conn.close()
        logger.info("✅ Base de datos inicializada correctamente.")
    except psycopg2.OperationalError as e:
        logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
        raise


def _schema_registry():
    return get_master_schema_registry()


def _normalized_payload_for_row(row: dict[str, Any]) -> dict[str, Any]:
    return normalize_existing_demand_record(dict(row), _schema_registry())


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
    data["normalized_payload"] = _normalized_payload_for_row(data)
    data["intent_domain"] = data.get("intent_domain") or data["normalized_payload"].get("intent_domain", "")
    data["attributes"] = dict(data.get("attributes") or {})
    if not data.get("location"):
        data["location"] = data["normalized_payload"].get("location_value")
    if not data.get("location_json"):
        data["location_json"] = data["normalized_payload"].get("location_json", {})
    if not data.get("location_mode"):
        data["location_mode"] = (
            data.get("location_json", {}).get("mode")
            or data["normalized_payload"].get("location_mode")
            or ("radius_from_point" if data.get("location_lat") is not None and data.get("location_lon") is not None else "unspecified")
        )
    if not data.get("location_label"):
        data["location_label"] = data["normalized_payload"].get("location_label") or data.get("location")
    if not data.get("location_admin_level"):
        data["location_admin_level"] = data.get("location_json", {}).get("admin_level") or data["normalized_payload"].get("location_admin_level")
    if data.get("location_lat") is None:
        data["location_lat"] = data["normalized_payload"].get("location_lat")
    if data.get("location_lon") is None:
        data["location_lon"] = data["normalized_payload"].get("location_lon")
    if data.get("location_radius_km") is None:
        data["location_radius_km"] = data["normalized_payload"].get("location_radius_km")
    if not data.get("location_radius_bucket"):
        data["location_radius_bucket"] = data["normalized_payload"].get("location_radius_bucket")
    if not data.get("location_source"):
        data["location_source"] = data["normalized_payload"].get("location_source")
    if not data.get("location_raw_query"):
        data["location_raw_query"] = data["normalized_payload"].get("location_raw_query")
    if not data.get("location_bbox"):
        data["location_bbox"] = data.get("location_json", {}).get("bbox") or data["normalized_payload"].get("location_bbox") or []
    if not data.get("location_geojson"):
        data["location_geojson"] = data.get("location_json", {}).get("geojson") or data["normalized_payload"].get("location_geojson") or {}
    data["location_display"] = compact_zone_label(
        data.get("location_label") or data.get("location"),
        data.get("location_raw_query"),
    )
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


def create_user(email: str, password: str, full_name: str) -> UserProfile:
    normalized_email = _normalize_email(email)
    password_hash = hash_password(password)
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (email, full_name, password_hash, auth_source)
                    VALUES (%s, %s, %s, 'local')
                    RETURNING id, email, full_name, password_hash, avatar_url, auth_source, created_at;
                    """,
                    (normalized_email, full_name.strip(), password_hash),
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
                SELECT id, email, full_name, password_hash, avatar_url, auth_source, created_at
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
                SELECT id, email, full_name, password_hash, avatar_url, auth_source, created_at
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            return _row_to_user(row) if row else None
    finally:
        conn.close()


def authenticate_user(email: str, password: str) -> Optional[UserProfile]:
    user = get_user_by_email(email)
    if not user or not verify_password(password, user.password_hash):
        return None
    return user


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
                    SELECT u.id, u.email, u.full_name, u.password_hash, u.avatar_url, u.auth_source, u.created_at
                    FROM oauth_accounts oa
                    JOIN users u ON u.id = oa.user_id
                    WHERE oa.provider = %s AND oa.provider_user_id = %s
                    """,
                    (provider, provider_user_id),
                )
                row = cur.fetchone()
                if row:
                    return _row_to_user(row)

                cur.execute(
                    """
                    SELECT id, email, full_name, password_hash, avatar_url, auth_source, created_at
                    FROM users
                    WHERE email = %s
                    """,
                    (normalized_email,),
                )
                row = cur.fetchone()
                if row:
                    user = _row_to_user(row)
                else:
                    cur.execute(
                        """
                        INSERT INTO users (email, full_name, avatar_url, auth_source)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id, email, full_name, password_hash, avatar_url, auth_source, created_at
                        """,
                        (normalized_email, full_name.strip() or normalized_email, avatar_url, provider),
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

                return user
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


def create_web_demand(
    user_id: int,
    summary: str,
    description: str,
    location: str = "",
    budget_min: Optional[float] = None,
    budget_max: Optional[float] = None,
    urgency: str = "",
    intent_type: str = "general_request",
) -> PublicDemand:
    """Crea una demanda manual desde la web."""
    attributes = {"description": description.strip()}
    normalized_payload = normalize_existing_demand_record(
        {
            "summary": summary.strip(),
            "intent_type": intent_type,
            "intent_domain": None,
            "location": location.strip() or None,
            "budget_min": budget_min,
            "budget_max": budget_max,
            "urgency": urgency.strip() or None,
            "attributes": attributes,
            "normalized_payload": {},
            "original_text": description.strip(),
        },
        _schema_registry(),
    )
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                zone = zone_to_storage_fields(normalized_payload.get("location_json"))
                cur.execute(
                    """
                    INSERT INTO demands (
                        user_id, telegram_user_id, intent_domain, intent_type, summary, location,
                        budget_min, budget_max, urgency, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        attributes, normalized_payload, schema_version, original_text,
                        conversation, status, expires_at, created_via
                    )
                    VALUES (
                        %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        '[]', 'open', NOW() + interval '48 hours', 'web'
                    )
                    RETURNING id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, urgency, status, is_pinned, expires_at, created_at, attributes, normalized_payload
                    """,
                    (
                        user_id,
                        normalized_payload.get("intent_domain", ""),
                        normalized_payload.get("intent_type", intent_type),
                        normalized_payload.get("summary", summary.strip()),
                        normalized_payload.get("location_value", location.strip() or None),
                        normalized_payload.get("budget_min", budget_min),
                        normalized_payload.get("budget_max", budget_max),
                        normalized_payload.get("urgency", urgency.strip() or None),
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
                        json.dumps(normalized_payload, ensure_ascii=False),
                        _schema_registry().version,
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
    """Guarda una demanda web creada a partir del agente conversacional."""
    attributes = {
        **demand.attributes,
        "description": state.original_text,
    }
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
                        user_id, telegram_user_id, intent_domain, intent_type, summary, location,
                        budget_min, budget_max, urgency, location_mode, location_label, location_lat, location_lon,
                        location_radius_km, location_radius_bucket, location_source, location_raw_query,
                        location_admin_level, location_bbox, location_geojson, location_json,
                        attributes, normalized_payload, schema_version, original_text,
                        conversation, status, expires_at, created_via
                    )
                    VALUES (
                        %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, 'open', NOW() + interval '48 hours', 'web'
                    )
                    RETURNING id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, urgency, status, is_pinned, expires_at, created_at, attributes, normalized_payload
                    """,
                    (
                        user_id,
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
                        json.dumps(attributes, ensure_ascii=False),
                        json.dumps(demand.model_dump(mode="json"), ensure_ascii=False),
                        demand.schema_version or _schema_registry().version,
                        state.original_text,
                        json.dumps(conversation, ensure_ascii=False),
                    ),
                )
                row = dict(cur.fetchone())
                _apply_zone_geometries(cur, "demands", row["id"], zone)
                row = _hydrate_demand_row(row)
                row["offer_count"] = 0
                return PublicDemand.model_validate(row)
    finally:
        conn.close()


def get_demands_by_user(telegram_user_id: int) -> list[dict[str, Any]]:
    """Obtiene demandas creadas por un usuario de Telegram."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, intent_domain, intent_type, summary, location, budget_min, budget_max,
                       urgency, attributes, normalized_payload, original_text, created_at
                FROM demands
                WHERE telegram_user_id = %s
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (telegram_user_id,),
            )
            rows = cur.fetchall()
            return [_hydrate_demand_row(dict(r)) for r in rows]
    finally:
        conn.close()


def get_public_demands(
    query_text: str = "",
    location: str = "",
    intent_type: str = "",
    viewer_user_id: Optional[int] = None,
    zone_filter: Optional[dict[str, Any]] = None,
    intent_domains: Optional[list[str]] = None,
    intent_types: Optional[list[str]] = None,
) -> list[PublicDemand]:
    """Lista demandas activas y públicas con filtros simples."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            where_parts = [
                "d.status = 'open'",
                "(d.expires_at IS NULL OR d.expires_at > NOW())",
                "(%s = '' OR d.summary ILIKE %s OR d.original_text ILIKE %s OR CAST(d.attributes AS TEXT) ILIKE %s OR CAST(d.conversation AS TEXT) ILIKE %s)",
                "(%s = '' OR COALESCE(d.location_label, d.location, '') ILIKE %s)",
                "(%s = '' OR d.intent_type ILIKE %s)",
            ]
            params: list[Any] = [
                viewer_user_id or -1,
                viewer_user_id or -1,
                query_text,
                f"%{query_text}%",
                f"%{query_text}%",
                f"%{query_text}%",
                f"%{query_text}%",
                location,
                f"%{location}%",
                intent_type,
                f"%{intent_type}%",
            ]

            if zone_filter and _postgis_available():
                search_shape_sql, search_params = _search_shape_sql(zone_filter)
                if search_shape_sql:
                    where_parts.append(
                        f"({_location_shape_sql('d')} IS NOT NULL AND ST_Intersects({_location_shape_sql('d')}, {search_shape_sql}))"
                    )
                    params.extend(search_params)

            query = f"""
                SELECT d.id, d.user_id, d.intent_domain, d.summary, d.intent_type, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_mode, d.location_admin_level, d.location_bbox, d.location_geojson, d.location_json, d.budget_min, d.budget_max,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload,
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
            if zone_filter:
                rows = [row for row in rows if _matches_zone_filter(row, zone_filter)]
            if intent_types or intent_domains:
                rows = [row for row in rows if _matches_category_filter(row, intent_domains, intent_types)]
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
                SELECT d.id, d.user_id, d.intent_domain, d.summary, d.intent_type, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_json, d.budget_min, d.budget_max,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.original_text,
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


def get_demand_detail(demand_id: int, viewer_user_id: Optional[int] = None) -> Optional[dict[str, Any]]:
    """Obtiene una demanda y metadatos para la vista de detalle."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            _expire_open_demands(cur)
            cur.execute(
                """
                SELECT d.id, d.intent_domain, d.summary, d.intent_type, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_json, d.budget_min, d.budget_max,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.user_id,
                       COALESCE(u.full_name, 'Demandante') AS owner_name,
                       COUNT(o.id)::INTEGER AS offer_count
                FROM demands d
                LEFT JOIN users u ON u.id = d.user_id
                LEFT JOIN offers o ON o.demand_id = d.id
                WHERE d.id = %s
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
                    (demand_id, viewer_user_id),
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
                SELECT d.id, d.user_id, d.intent_domain, d.summary, d.intent_type, d.location, d.location_label, d.location_lat,
                       d.location_lon, d.location_radius_km, d.location_radius_bucket, d.location_source, d.location_raw_query,
                       d.location_json, d.budget_min, d.budget_max,
                       d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload, d.original_text,
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
    """Actualiza una demanda web abierta reutilizando la misma normalización."""
    attributes = {
        **demand.attributes,
        "description": state.original_text,
    }
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
                    UPDATE demands
                    SET intent_domain = %s,
                        intent_type = %s,
                        summary = %s,
                        location = %s,
                        budget_min = %s,
                        budget_max = %s,
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
                        schema_version = %s,
                        original_text = %s,
                        conversation = %s
                    WHERE id = %s
                      AND user_id = %s
                      AND status = 'open'
                      AND (expires_at IS NULL OR expires_at > NOW())
                    RETURNING id, user_id, intent_domain, summary, intent_type, location, location_mode, location_admin_level,
                              location_label, location_lat, location_lon, location_radius_km, location_radius_bucket,
                              location_source, location_raw_query, location_bbox, location_geojson, location_json,
                              budget_min, budget_max, urgency, status, expires_at, created_at, attributes, normalized_payload
                    """,
                    (
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
                        json.dumps(attributes, ensure_ascii=False),
                        json.dumps(demand.model_dump(mode="json"), ensure_ascii=False),
                        demand.schema_version or _schema_registry().version,
                        state.original_text,
                        json.dumps(conversation, ensure_ascii=False),
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
        SELECT d.id, d.intent_domain, d.summary, d.intent_type, d.location, d.budget_min, d.budget_max,
               d.urgency, d.status, d.is_pinned, d.expires_at, d.created_at, d.attributes, d.normalized_payload,
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
        SELECT o.id AS offer_id, o.demand_id, o.message, o.created_at, o.updated_at,
               o.supplier_is_pinned, o.supplier_hidden,
               d.summary AS demand_summary, d.intent_domain, d.intent_type, d.location, d.budget_min, d.budget_max,
               d.urgency, d.attributes, d.normalized_payload,
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
        row["normalized_payload"] = _normalized_payload_for_row(
            {
                "id": row.get("demand_id"),
                "summary": row.get("demand_summary"),
                "intent_domain": row.get("intent_domain"),
                "intent_type": row.get("intent_type"),
                "location": row.get("location"),
                "budget_min": row.get("budget_min"),
                "budget_max": row.get("budget_max"),
                "urgency": row.get("urgency"),
                "attributes": row.get("attributes") or {},
                "normalized_payload": row.get("normalized_payload") or {},
                "original_text": (row.get("attributes") or {}).get("description", row.get("demand_summary")),
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
                    (demand_id, supplier_user_id, message.strip()),
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
                    (offer.id, supplier_user_id, message.strip()),
                )
                return offer
    finally:
        conn.close()


def get_offer_thread(offer_id: int, viewer_user_id: int) -> Optional[dict[str, Any]]:
    """Obtiene un hilo de oferta si el usuario participa en él como D u O."""
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.id, o.demand_id, o.supplier_user_id, o.message, o.created_at,
                           d.user_id AS demand_owner_user_id, d.summary AS demand_summary, d.intent_domain, d.intent_type,
                           d.location, d.budget_min, d.budget_max, d.urgency, d.attributes, d.normalized_payload,
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
                    (offer_id, viewer_user_id, viewer_user_id),
                )
                row = cur.fetchone()
                if not row:
                    return None

                data = dict(row)
                data["normalized_payload"] = _normalized_payload_for_row(
                    {
                        "id": data.get("demand_id"),
                        "summary": data.get("demand_summary"),
                        "intent_domain": data.get("intent_domain"),
                        "intent_type": data.get("intent_type"),
                        "location": data.get("location"),
                        "budget_min": data.get("budget_min"),
                        "budget_max": data.get("budget_max"),
                        "urgency": data.get("urgency"),
                        "attributes": data.get("attributes") or {},
                        "normalized_payload": data.get("normalized_payload") or {},
                        "original_text": (data.get("attributes") or {}).get("description", data.get("demand_summary")),
                    }
                )
                data["effective_status"] = str(data.get("demand_status") or "open").lower()
                data["can_reply"] = data["effective_status"] == "open"
                mark_offer_thread_as_read(cur, offer_id, viewer_user_id, data["demand_owner_user_id"], data["supplier_user_id"])
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
                data["viewer_is_owner"] = data["demand_owner_user_id"] == viewer_user_id
                return data
    finally:
        conn.close()


def create_offer_message(offer_id: int, sender_user_id: int, body: str) -> bool:
    """Añade un mensaje al hilo de una oferta si el usuario participa en ella."""
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
                    (offer_id, sender_user_id, body.strip()),
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
