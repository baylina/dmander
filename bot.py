"""
bot.py — Bot de Telegram para DMANDER (@santq_bot).

Implementa un flujo conversacional usando python-telegram-bot:
1. /start → bienvenida, pide demanda
2. Usuario escribe demanda → agente analiza
3. Si faltan campos → bot pregunta
4. Usuario responde → agente re-analiza → repite
5. Cuando hay suficiente info → guarda en DB → muestra JSON final
6. /cancel → cancela conversación
7. /mis_demandas → muestra demandas guardadas
"""

from __future__ import annotations

import json
import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from agent import DemandAgent
from database import get_demands_by_user, save_demand, delete_demand
from llm_client import OpenAIClient
from models import LLMResponse, SessionState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Estados de la conversación
# ---------------------------------------------------------------------------

WAITING_DEMAND = 0   # Esperando la demanda inicial
ANSWERING = 1        # Respondiendo preguntas del agente


# ---------------------------------------------------------------------------
# Inicialización del agente (singleton)
# ---------------------------------------------------------------------------

_agent: DemandAgent | None = None


def get_agent() -> DemandAgent:
    """Obtiene o crea la instancia del agente."""
    global _agent
    if _agent is None:
        llm_client = OpenAIClient()
        _agent = DemandAgent(llm_client)
    return _agent


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

# Teclado persistente con las opciones principales
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["/nueva_demanda", "/mis_demandas"]],
    resize_keyboard=True,
    is_persistent=True
)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Maneja /start y /nueva_demanda — da la bienvenida y pide la demanda."""
    user = update.effective_user.first_name
    logger.info(f"Comando de inicio recibido de {user} (ID: {update.effective_user.id})")
    
    # Limpiar cualquier estado previo si es /nueva_demanda
    context.user_data.clear()

    await update.message.reply_text(
        f"🔍 <b>¡Bienvenido a DMANDER, {user}!</b>\n\n"
        "Soy tu asistente para ayudarte a conectar demanda y oferta.\n\n"
        "📝 <b>Escribe tu demanda en lenguaje natural:</b>\n"
        "Ejemplo: <i>Busco profesor de matemáticas para mi hijo de 13 años en Sant Cugat</i>",
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD
    )
    return WAITING_DEMAND


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Maneja /cancel — cancela la conversación actual."""
    context.user_data.clear()
    await update.message.reply_text(
        "❌ Conversación cancelada. Escribe /start para empezar de nuevo."
    )
    return ConversationHandler.END


async def mis_demandas_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja /mis_demandas — muestra las demandas con opción de borrar."""
    user_id = update.effective_user.id

    try:
        demands = get_demands_by_user(user_id)
    except Exception as e:
        logger.error(f"Error consultando demandas: {e}")
        await update.message.reply_text("❌ Error al consultar tus demandas.")
        return

    if not demands:
        await update.message.reply_text(
            "📭 No tienes demandas guardadas todavía.",
            reply_markup=MAIN_KEYBOARD
        )
        return

    await update.message.reply_text("📋 <b>Tus últimas demandas:</b>", parse_mode="HTML")

    for d in demands:
        intent = clean_html(d['intent_type'])
        summary = clean_html(d['summary'])
        location = clean_html(d.get('location', 'N/A'))
        date_str = d["created_at"].strftime("%d/%m/%Y %H:%M") if d.get("created_at") else "?"
        
        text = (
            f"📌 <b>{intent}</b>\n"
            f"📝 {summary}\n"
            f"📍 {location} | 📅 {date_str}"
        )
        
        # Botón para borrar esta demanda específica
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Borrar", callback_data=f"delete_{d['id']}")]
        ])
        
        await update.message.reply_text(
            text, 
            parse_mode="HTML", 
            reply_markup=keyboard
        )

async def delete_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la pulsación del botón borrar."""
    query = update.callback_query
    user_id = update.effective_user.id
    
    # Extraer ID de la demanda del callback_data (delete_123)
    try:
        demand_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.answer("Error al procesar el ID.")
        return

    # Intentar borrar de la DB
    if delete_demand(demand_id, user_id):
        await query.answer("✅ Demanda eliminada")
        await query.edit_message_text(
            f"<s>{query.message.text}</s>\n\n🗑️ <i>Esta demanda ha sido eliminada.</i>",
            parse_mode="HTML"
        )
    else:
        await query.answer("❌ No se pudo borrar la demanda.")



# ---------------------------------------------------------------------------
# Flujo conversacional
# ---------------------------------------------------------------------------

async def receive_demand(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Recibe la demanda inicial del usuario y empieza el análisis."""
    user_text = update.message.text.strip()
    user_id = update.effective_user.id
    logger.info(f"Demanda recibida de {user_id}: {user_text[:50]}...")

    if not user_text:
        await update.message.reply_text("⚠️ No he recibido ningún texto. Escribe tu demanda:")
        return WAITING_DEMAND

    # Crear estado de sesión
    state = SessionState(original_text=user_text, telegram_user_id=user_id)
    context.user_data["state"] = state

    # Analizar
    return await _analyze_and_respond(update, context)


async def receive_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Recibe la respuesta a una pregunta del agente."""
    user_answer = update.message.text.strip()
    state: SessionState = context.user_data.get("state")

    if not state:
        await update.message.reply_text("⚠️ No hay conversación activa. Escribe /start.")
        return ConversationHandler.END

    if not user_answer:
        await update.message.reply_text("⚠️ Respuesta vacía. Por favor, responde a la pregunta:")
        return ANSWERING

    # Recuperar la última pregunta pendiente y registrarla
    last_question = context.user_data.get("pending_question", "")
    if last_question:
        state.questions_asked.append(last_question)
        state.user_answers.append(user_answer)

    # Analizar con la nueva info
    return await _analyze_and_respond(update, context)


import html

def clean_html(text: Any) -> str:
    """Escapa caracteres para que no rompan el HTML de Telegram."""
    return html.escape(str(text))


async def _analyze_and_respond(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Lógica común: analiza el estado actual y responde."""
    state: SessionState = context.user_data["state"]
    agent = get_agent()

    # Indicador de "pensando"
    thinking_msg = await update.message.reply_text("⏳ Analizando tu demanda...")

    # Llamar al agente
    try:
        response = agent.analyze(state)
    except RuntimeError as e:
        logger.error(f"Error en análisis: {e}")
        await thinking_msg.edit_text(f"❌ Error al analizar: {clean_html(e)}")
        return ConversationHandler.END

    # Actualizar estado
    agent.update_state(state, response)

    # Construir mensaje de análisis (usando HTML seguro)
    summary = clean_html(response.summary)
    intent = clean_html(response.intent_type)
    
    msg_parts = [f"🤖 <b>Análisis:</b>\n{summary}"]
    msg_parts.append(f"\n📌 <b>Tipo:</b> <code>{intent}</code> (confianza: {response.confidence:.0%})")

    if response.known_fields:
        msg_parts.append("\n✅ <b>Ya tengo:</b>")
        for k, v in response.known_fields.items():
            k_clean = clean_html(k)
            v_clean = clean_html(v)
            msg_parts.append(f"  • {k_clean}: <i>{v_clean}</i>")

    if response.missing_fields:
        msg_parts.append("\n❓ <b>Me falta:</b>")
        for f in response.missing_fields:
            f_clean = clean_html(f)
            msg_parts.append(f"  • {f_clean}")

    # ¿Suficiente información?
    if response.enough_information or not response.next_question:
        await thinking_msg.edit_text("\n".join(msg_parts), parse_mode="HTML")
        return await _finalize_demand(update, context, response)

    # ¿Máximo de iteraciones?
    if agent.has_reached_max_iterations(state):
        msg_parts.append("\n⚠️ <i>Máximo de preguntas alcanzado.</i>")
        await thinking_msg.edit_text("\n".join(msg_parts), parse_mode="HTML")
        return await _finalize_demand(update, context, response)

    # Hacer la pregunta
    question = clean_html(response.next_question)
    msg_parts.append(f"\n💬 <b>Pregunta:</b>\n{question}")
    context.user_data["pending_question"] = response.next_question

    await thinking_msg.edit_text("\n".join(msg_parts), parse_mode="HTML")
    return ANSWERING


async def _finalize_demand(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    last_response: LLMResponse,
) -> int:
    """Consolida la demanda final, la guarda en DB y la muestra."""
    state: SessionState = context.user_data["state"]
    agent = get_agent()
    user_id = update.effective_user.id

    # Análisis final de consolidación
    try:
        final_response = agent.analyze(state)
        demand = agent.build_final_demand(state, final_response)
    except RuntimeError:
        demand = agent.build_final_demand(state, last_response)

    # Guardar en base de datos
    try:
        db_result = save_demand(user_id, demand, state)
        demand.id = db_result["id"]
        demand.created_at = db_result["created_at"]
        db_msg = f"\n💾 <i>Demanda guardada con ID: {demand.id}</i>"
    except Exception as e:
        logger.error(f"Error guardando en DB: {e}")
        db_msg = "\n⚠️ <i>No se pudo guardar en la base de datos.</i>"

    # Formatear JSON final
    summary = clean_html(demand.summary)
    intent = clean_html(demand.intent_type)
    
    demand_dict = demand.model_dump(mode="json", exclude_none=True)
    json_str = clean_html(json.dumps(demand_dict, indent=2, ensure_ascii=False))

    await update.message.reply_text(
        f"✅ <b>DEMANDA FINAL ESTRUCTURADA</b>\n\n"
        f"📌 <b>Tipo:</b> <code>{intent}</code>\n"
        f"📝 <b>Resumen:</b> {summary}\n"
        f"{db_msg}\n\n"
        f"<pre>{json_str}</pre>",
        parse_mode="HTML",
    )

    # Limpiar estado
    context.user_data.clear()
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Construcción de la aplicación de Telegram
# ---------------------------------------------------------------------------

def build_telegram_app(token: str) -> Application:
    """Construye y configura la aplicación de Telegram."""

    app = Application.builder().token(token).build()

    # Configurar el menú de comandos del bot (botón azul)
    async def post_init(application: Application) -> None:
        await application.bot.set_my_commands([
            BotCommand("nueva_demanda", "Crear una nueva demanda"),
            BotCommand("mis_demandas", "Ver y gestionar mis demandas"),
            BotCommand("cancel", "Cancelar conversación actual"),
        ])

    app.post_init = post_init

    # Conversation handler para el flujo de demandas
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start_command),
            CommandHandler("nueva_demanda", start_command),
        ],
        states={
            WAITING_DEMAND: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_demand),
            ],
            ANSWERING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_answer),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_command),
            CommandHandler("start", start_command),
            CommandHandler("nueva_demanda", start_command),
        ],
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("mis_demandas", mis_demandas_command))
    app.add_handler(CallbackQueryHandler(delete_callback_handler, pattern="^delete_"))

    return app
