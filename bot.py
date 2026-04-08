"""
bot.py — Bot de Telegram para DMANDER (@dmanderbot).

Flujo simplificado:
1. /start o botón "Nueva demanda" -> pide texto libre
2. Usuario escribe la demanda
3. El bot analiza texto y la publica directamente
4. "Mis demandas" muestra últimas demandas con enlace a cada ficha web
"""

from __future__ import annotations

import html
import logging
import os
from urllib.parse import quote

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from llm_client import OpenAIClient
from models import DemandResult, SessionState
from database import (
    create_magic_login_token,
    get_demands_by_user,
    get_or_create_telegram_user,
    save_telegram_demand_lightweight,
)
from webapp import _analyze_lightweight_demand

logger = logging.getLogger(__name__)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["Nueva demanda", "Mis demandas"]],
    resize_keyboard=True,
    is_persistent=True,
)

BOT_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000").rstrip("/")

_llm_client: OpenAIClient | None = None


def get_llm_client() -> OpenAIClient:
    global _llm_client
    if _llm_client is None:
        _llm_client = OpenAIClient()
    return _llm_client


def clean_html(text: object) -> str:
    return html.escape(str(text or ""))


def demand_url(demand_public_id: str) -> str:
    return f"{BOT_BASE_URL}/demands/{demand_public_id}"


def telegram_magic_url(user_id: int, next_path: str = "/app/chats") -> str:
    token = create_magic_login_token(user_id)
    return f"{BOT_BASE_URL}/auth/telegram/{quote(token)}?next={quote(next_path, safe='/=?&')}"


def _telegram_identity(update: Update):
    user = update.effective_user
    return get_or_create_telegram_user(
        telegram_user_id=int(user.id),
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )


def format_budget(draft: DemandResult) -> str:
    if draft.budget_max is None:
        return "No indicado"
    unit_labels = {
        "total": "en total",
        "hour": "por hora",
        "night": "por noche",
        "day": "por día",
        "month": "al mes",
        "item": "por producto",
        "service": "por servicio",
    }
    unit = unit_labels.get(str(draft.budget_unit or "total").strip().lower(), "en total")
    return f"Hasta {draft.budget_max:g} € {unit}"


def fallback_summary(text: str) -> str:
    compact = " ".join(str(text or "").strip().split()).rstrip(".")
    if not compact:
        return "Demanda sin resumen."
    if len(compact) <= 96:
        return compact[:1].upper() + compact[1:] + "."
    shortened = compact[:93].rstrip(" ,;:")
    return shortened[:1].upper() + shortened[1:] + "..."


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["awaiting_demand"] = False
    telegram_user = _telegram_identity(update)
    user_name = clean_html(update.effective_user.first_name or "hola")
    await update.message.reply_text(
        f"Hola, {user_name}.\n\n"
        "Soy dmanderbot y te ayudo a publicar lo que necesitas de forma rápida.\n"
        "Puedes crear una nueva demanda, revisar las que ya has publicado y abrir la web sin registrarte aparte.",
        reply_markup=MAIN_KEYBOARD,
    )
    await update.message.reply_text(
        "Pulsa `Nueva demanda` y escribe tu necesidad en un único mensaje.\n"
        "Ejemplo: Busco electricista para revisar enchufes que no funcionan en Sabadell.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Abrir mi cuenta web", url=telegram_magic_url(telegram_user.id, "/app/chats"))]]
        ),
    )


async def nueva_demanda_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["awaiting_demand"] = True
    _telegram_identity(update)
    await update.message.reply_text(
        "Perfecto. Escribe ahora tu demanda en un único mensaje.\n"
        "Cuanto más clara sea, mejor quedará publicada.",
        reply_markup=MAIN_KEYBOARD,
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["awaiting_demand"] = False
    await update.message.reply_text(
        "Operación cancelada.",
        reply_markup=MAIN_KEYBOARD,
    )


async def mis_demandas_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["awaiting_demand"] = False
    telegram_user = _telegram_identity(update)
    demands = get_demands_by_user(telegram_user.id, telegram_user_id=update.effective_user.id)
    if not demands:
        await update.message.reply_text(
            "Todavía no has publicado demandas.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    await update.message.reply_text(
        "Estas son tus últimas demandas:",
        reply_markup=MAIN_KEYBOARD,
    )
    await update.message.reply_text(
        "También puedes abrir tu cuenta web y gestionarlas allí.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Abrir mi cuenta web", url=telegram_magic_url(telegram_user.id, "/app/chats"))]]
        ),
    )
    for item in demands:
        location = item.get("location_display") or item.get("location_label") or item.get("location") or "Sin ubicación"
        created_at = item["created_at"].strftime("%d/%m/%Y %H:%M") if item.get("created_at") else ""
        text = "\n".join(
            [
                f"🧾 <b>{clean_html(item.get('summary') or 'Demanda')}</b>",
                f"📍 {clean_html(location)}",
                f"📅 {clean_html(created_at)}",
            ]
        )
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Ver demanda", url=telegram_magic_url(telegram_user.id, f"/demands/{item['public_id']}"))]]
        )
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = str(update.message.text or "").strip()
    if text in {"Nueva demanda", "/nueva_demanda"}:
        await nueva_demanda_command(update, context)
        return
    if text in {"Mis demandas", "/mis_demandas"}:
        await mis_demandas_command(update, context)
        return

    if len(text) < 8:
        await update.message.reply_text("Escribe una demanda un poco más concreta.", reply_markup=MAIN_KEYBOARD)
        return
    if len(text) > 200:
        await update.message.reply_text("La demanda no puede superar 200 caracteres.", reply_markup=MAIN_KEYBOARD)
        return

    thinking = await update.message.reply_text("Analizando tu demanda...")
    telegram_user = _telegram_identity(update)
    try:
        draft = _analyze_lightweight_demand(get_llm_client(), text)
    except Exception as exc:
        logger.exception("Error analizando demanda en Telegram: %s", exc)
        draft = DemandResult(
            raw_text=text,
            summary=fallback_summary(text),
            description=text,
            location_mode="unspecified",
            location_json={},
            location=None,
            budget_max=None,
            budget_unit="total",
            attributes={},
            known_fields={},
            suggested_missing_details=[],
            confidence=0.0,
            llm_metadata={},
        )

    state = SessionState(original_text=draft.raw_text, known_fields=draft.known_fields, summary=draft.summary)
    try:
        persisted = save_telegram_demand_lightweight(
            telegram_user_id=update.effective_user.id,
            user_id=telegram_user.id,
            demand=draft,
            state=state,
        )
    except Exception as exc:
        logger.exception("Error guardando demanda de Telegram: %s", exc)
        await thinking.edit_text(f"No he podido publicar la demanda. Error: {clean_html(exc)}")
        return

    context.user_data["awaiting_demand"] = False
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Ver demanda", url=telegram_magic_url(telegram_user.id, f"/demands/{persisted.public_id}"))],
            [InlineKeyboardButton("Abrir mi cuenta web", url=telegram_magic_url(telegram_user.id, "/app/chats"))],
        ]
    )
    message_lines = [
        "✅ <b>Demanda publicada</b>",
        "",
        f"<b>Resumen:</b> {clean_html(persisted.summary)}",
    ]
    if persisted.location_display or persisted.location_label or persisted.location:
        message_lines.append(
            f"<b>Ubicación:</b> {clean_html(persisted.location_display or persisted.location_label or persisted.location)}"
        )
    message_lines.append(f"<b>Precio máximo:</b> {clean_html(format_budget(draft))}")
    await thinking.edit_text(
        "\n".join(
            message_lines
        ),
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await update.message.reply_text(
        "Puedes publicar otra demanda o consultar tus demandas.",
        reply_markup=MAIN_KEYBOARD,
    )


def build_telegram_app(token: str) -> Application:
    app = Application.builder().token(token).build()

    async def post_init(application: Application) -> None:
        await application.bot.set_my_commands(
            [
                BotCommand("start", "Abrir menú principal"),
                BotCommand("nueva_demanda", "Crear nueva demanda"),
                BotCommand("mis_demandas", "Ver mis demandas"),
                BotCommand("cancel", "Cancelar"),
            ]
        )

    app.post_init = post_init
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("nueva_demanda", nueva_demanda_command))
    app.add_handler(CommandHandler("mis_demandas", mis_demandas_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    return app
