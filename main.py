# This is the full, final, and corrected content for main.py

import logging
import asyncio
from datetime import time
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    ConversationHandler, MessageHandler, filters
)
from telegram import InputFile

# Import all necessary components from our modules
import config
from bot.handlers import (
    # Core Commands
    start_command, help_command, monitor_risk_command, stop_monitoring_command,
    
    # Automation & Safety
    auto_hedge_command, set_large_trade_limit_command, adjust_threshold_start,
    adjust_delta_received, adjust_var_received, cancel_adjustment,
    
    # Reporting & Analytics
    hedge_status_command, hedge_history_command, chart_command, portfolio_risk_command,
    stress_test_command, export_data_command,
    
    # Machine Learning
    ml_mode_command,
    
    # Hedging & Utilities
    hedge_options_command, price_command,
    
    # Callback Handlers (for buttons)
    button_callback_handler, handle_export_callback,
    
    # Options Conversation Flow
    select_strategy, select_expiry, select_strike, confirm_hedge, cancel_conversation,
    select_put_strike, select_buy_put, select_sell_put, select_sell_call, select_buy_call,
    
    # Background Jobs
    risk_check_job, send_daily_summary,
    
    # Conversation States (constants)
    SELECT_STRATEGY, SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE, 
    ADJUST_DELTA, ADJUST_VAR, SELECT_PUT_STRIKE,
    SELECT_BUY_PUT, SELECT_SELL_PUT, SELECT_SELL_CALL, SELECT_BUY_CALL, CONFIRM_CONDOR
)
from services.data_fetcher import data_fetcher_instance
from database import db_manager
from reporting import reporting_manager

# --- Setup Centralized Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot_activity.log")]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


def main() -> None:
    """The main function to set up and run the entire bot application."""
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
        conversation_timeout=300
    )
    
    # --- Conversation Handler for All Options Strategies ---
    options_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("hedge_options", hedge_options_command)],
        states={
            SELECT_STRATEGY: [CallbackQueryHandler(select_strategy, pattern="^strategy_")],
            SELECT_EXPIRY: [CallbackQueryHandler(select_expiry, pattern="^expiry_")],
            SELECT_STRIKE: [CallbackQueryHandler(select_strike, pattern="^strike_")],
            SELECT_PUT_STRIKE: [CallbackQueryHandler(select_put_strike, pattern="^strike_")],
            SELECT_BUY_PUT: [CallbackQueryHandler(select_buy_put, pattern="^strike_")],
            SELECT_SELL_PUT: [CallbackQueryHandler(select_sell_put, pattern="^strike_")],
            SELECT_SELL_CALL: [CallbackQueryHandler(select_sell_call, pattern="^strike_")],
            SELECT_BUY_CALL: [CallbackQueryHandler(select_buy_call, pattern="^strike_")],
            CONFIRM_HEDGE: [CallbackQueryHandler(confirm_hedge, pattern="^confirm_hedge")],
            CONFIRM_CONDOR: [CallbackQueryHandler(confirm_hedge, pattern="^confirm_hedge")],
        },
        fallbacks=[CallbackQueryHandler(cancel_conversation, pattern="^cancel")],
        conversation_timeout=600
    )

    # Register conversation handlers first to ensure they have priority
    application.add_handler(adjust_conv_handler)
    application.add_handler(options_conv_handler)

    # --- Register ALL Standard Command Handlers ---
    command_handlers = {
        "start": start_command, "help": help_command, "monitor_risk": monitor_risk_command,
        "stop_monitoring": stop_monitoring_command, "auto_hedge": auto_hedge_command,
        "set_large_trade_limit": set_large_trade_limit_command, "hedge_status": hedge_status_command,
        "hedge_history": hedge_history_command, "chart": chart_command, "portfolio_risk": portfolio_risk_command,
        "stress_test": stress_test_command, "export_data": export_data_command, "price": price_command,
        "ml_mode": ml_mode_command # Add the new ML command
    }
    for command, handler in command_handlers.items():
        application.add_handler(CommandHandler(command, handler))
    
    # --- Register the General Callback Handler for non-conversation buttons ---
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # --- Schedule Background Jobs ---
    job_queue = application.job_queue
    job_queue.run_repeating(risk_check_job, interval=60, first=10)
    job_queue.run_daily(send_daily_summary, time=time(hour=8, minute=0))
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