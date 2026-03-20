"""
main.py — Punto de entrada para DMANDER.

Modos de ejecución:
  python main.py          → Lanza el bot de Telegram
  python main.py --cli    → Modo CLI interactivo (POC original)
"""

from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv


def run_telegram_bot() -> None:
    """Arranca el bot de Telegram."""
    from bot import build_telegram_app
    from database import init_db

    # Inicializar base de datos
    try:
        init_db()
    except Exception as e:
        print(f"❌ Error inicializando la base de datos: {e}")
        print("   Asegúrate de que PostgreSQL está corriendo y DATABASE_URL es correcta.")
        sys.exit(1)

    # Obtener token del bot
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("❌ No se encontró TELEGRAM_BOT_TOKEN en .env")
        sys.exit(1)

    # Construir y arrancar
    app = build_telegram_app(token)
    print("🤖 Bot DMANDER (@santq_bot) arrancado. Ctrl+C para detener.")
    app.run_polling(drop_pending_updates=True)


def run_cli() -> None:
    """Ejecuta el modo CLI interactivo (POC original)."""
    from agent import DemandAgent
    from llm_client import OpenAIClient
    from models import SessionState
    from utils import (
        Colors,
        print_agent_analysis,
        print_agent_thinking,
        print_error,
        print_final_demand,
        print_header,
        print_iteration_info,
        print_question,
        print_warning,
    )

    print_header()

    try:
        llm_client = OpenAIClient()
    except ValueError as e:
        print_error(str(e))
        sys.exit(1)

    agent = DemandAgent(llm_client)

    print(f"{Colors.BOLD}Describe tu demanda en lenguaje natural:{Colors.RESET}")
    print(f'{Colors.DIM}(Ejemplo: "Busco profesor de matemáticas para mi hijo de 13 años en Sant Cugat"){Colors.RESET}')

    try:
        initial_text = input(f"\n{Colors.GREEN}> {Colors.RESET}").strip()
    except (KeyboardInterrupt, EOFError):
        print("\n\n👋 ¡Hasta luego!")
        sys.exit(0)

    if not initial_text:
        print_error("No has escrito ninguna demanda.")
        sys.exit(1)

    state = SessionState(original_text=initial_text)

    while True:
        print_iteration_info(state.iteration + 1, agent.max_iterations)
        print_agent_thinking()

        try:
            response = agent.analyze(state)
        except RuntimeError as e:
            print_error(str(e))
            break

        print_agent_analysis(
            summary=response.summary,
            known_fields=response.known_fields,
            intent_type=response.intent_type,
            confidence=response.confidence,
            missing_fields=response.missing_fields,
        )

        if response.enough_information or not response.next_question:
            agent.update_state(state, response)
            print(f"\n{Colors.GREEN}{Colors.BOLD}"
                  f"✅ El agente considera que ya tiene suficiente información."
                  f"{Colors.RESET}")
            break

        if agent.has_reached_max_iterations(state):
            agent.update_state(state, response)
            print_warning(
                f"Se alcanzó el máximo de {agent.max_iterations} iteraciones. "
                "Generando demanda con la información disponible."
            )
            break

        print_question(response.next_question)

        try:
            user_answer = input(f"\n{Colors.GREEN}> {Colors.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            print(f"\n\n{Colors.YELLOW}Interrumpido. Generando demanda con lo que tengo...{Colors.RESET}")
            agent.update_state(state, response)
            break

        if not user_answer:
            print_warning("Respuesta vacía. Continuando con la información disponible.")
            agent.update_state(state, response)
            break

        agent.update_state(state, response, user_answer)

    try:
        print_agent_thinking()
        final_response = agent.analyze(state)
        demand = agent.build_final_demand(state, final_response)
    except RuntimeError:
        from models import LLMResponse
        fallback = LLMResponse(
            intent_type=state.intent_type or "unknown",
            confidence=0.5,
            known_fields=state.known_fields,
            summary=state.summary or state.original_text,
            enough_information=True,
        )
        demand = agent.build_final_demand(state, fallback)

    print_final_demand(demand.model_dump())


def run_web() -> None:
    """Arranca la aplicación web."""
    try:
        import uvicorn
    except ImportError as e:
        print("❌ Falta uvicorn. Ejecuta `pip install -r requirements.txt`.")
        raise SystemExit(1) from e

    try:
        from webapp import build_app
        app = build_app()
    except Exception as e:
        print(f"❌ Error arrancando la web: {e}")
        sys.exit(1)

    host = os.getenv("WEB_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_PORT", "8000"))
    print(f"🌐 DMANDER web disponible en http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)


def main() -> None:
    """Punto de entrada principal."""
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    if "--cli" in sys.argv:
        run_cli()
    elif "--web" in sys.argv:
        run_web()
    else:
        run_telegram_bot()


if __name__ == "__main__":
    main()
