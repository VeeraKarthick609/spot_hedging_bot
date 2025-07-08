# This is the full content of the main.py file

import logging
import asyncio
from datetime import time
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    ConversationHandler, MessageHandler, filters
)

# Import all necessary components
import config
from bot.handlers import (
    start_command, help_command, price_command, monitor_risk_command,
    stop_monitoring_command, button_callback_handler, risk_check_job,
    portfolio_risk_command, hedge_options_command, select_strategy, select_expiry,
    select_strike, confirm_hedge, cancel_conversation, SELECT_STRATEGY, 
    SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE, auto_hedge_command,
    hedge_status_command, hedge_history_command, chart_command, 
    adjust_threshold_start, adjust_delta_received, adjust_var_received, 
    cancel_adjustment, send_daily_summary, ADJUST_DELTA, ADJUST_VAR,
    # Ensure reusable functions are imported if needed elsewhere, though they are primarily used inside handlers
    send_portfolio_report, execute_hedge_logic
)
from services.data_fetcher import data_fetcher_instance
from database import db_manager

# --- Setup Centralized Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot_activity.log")]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


def main() -> None:
    """The main function to set up and run the bot."""
    log.info("Starting bot...")
    application = Application.builder().token(config.TELEGRAM_TOKEN).build()

    # --- Conversation Handler for Adjusting Thresholds ---
    adjust_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("adjust_threshold", adjust_threshold_start)],
        states={
            ADJUST_DELTA: [MessageHandler(filters.TEXT & ~filters.COMMAND, adjust_delta_received)],
            ADJUST_VAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, adjust_var_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel_adjustment)],
        conversation_timeout=300 # 5 minute timeout
    )
    
    # --- Conversation Handler for Options Hedging (from Phase 3) ---
    options_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("hedge_options", hedge_options_command)],
        states={
            SELECT_STRATEGY: [CallbackQueryHandler(select_strategy, pattern="^strategy_")],
            SELECT_EXPIRY: [CallbackQueryHandler(select_expiry, pattern="^expiry_")],
            SELECT_STRIKE: [CallbackQueryHandler(select_strike, pattern="^strike_")],
            CONFIRM_HEDGE: [CallbackQueryHandler(confirm_hedge, pattern="^confirm_hedge")],
        },
        fallbacks=[CallbackQueryHandler(cancel_conversation, pattern="^cancel")],
        conversation_timeout=600 # 10 minute timeout
    )

    # Register conversation handlers first
    application.add_handler(adjust_conv_handler)
    application.add_handler(options_conv_handler)

    # --- Register ALL Standard Command Handlers ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("monitor_risk", monitor_risk_command))
    application.add_handler(CommandHandler("stop_monitoring", stop_monitoring_command))
    application.add_handler(CommandHandler("auto_hedge", auto_hedge_command))
    application.add_handler(CommandHandler("hedge_status", hedge_status_command))
    application.add_handler(CommandHandler("hedge_history", hedge_history_command))
    application.add_handler(CommandHandler("chart", chart_command))
    application.add_handler(CommandHandler("portfolio_risk", portfolio_risk_command))
    application.add_handler(CommandHandler("price", price_command))
    
    # --- Register the General Callback Handler for non-conversation buttons ---
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # --- Schedule Background Jobs ---
    job_queue = application.job_queue
    # Run risk check every 60 seconds
    job_queue.run_repeating(risk_check_job, interval=60, first=10)
    # Run daily summary every day at 08:00 UTC
    job_queue.run_daily(send_daily_summary, time=time(hour=8, minute=0, tzinfo=None)) # Using server's local time for simplicity
    log.info("Background jobs (risk check, daily summary) have been scheduled.")

    # --- Start the Bot ---
    log.info("Bot is polling for updates...")
    application.run_polling()

    # --- Graceful Shutdown ---
    log.info("Bot is shutting down...")
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(data_fetcher_instance.close_connections())
    else:
        loop.run_until_complete(data_fetcher_instance.close_connections())
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()