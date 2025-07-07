import logging
import asyncio
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler

# Import configuration, handlers, and services
import config
from bot.handlers import (
    start_command, 
    help_command, 
    price_command,
    monitor_risk_command,
    stop_monitoring_command,
    button_callback_handler,
    risk_check_job,
    hedge_options_command, select_strategy, select_expiry, select_strike, confirm_hedge, cancel_conversation,
    SELECT_STRATEGY, SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE 
)
from services.data_fetcher import data_fetcher_instance

# --- Setup Centralized Logging ---
# ... (logging setup remains unchanged) ...
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_activity.log")
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

def main() -> None:
    """Start the bot."""
    log.info("Starting bot...")

    # Create the Application and pass it your bot's token.
    application = Application.builder().token(config.TELEGRAM_TOKEN).build()

    # --- Setup Conversation Handler for Options Hedging ---
    options_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("hedge_options", hedge_options_command)],
        states={
            SELECT_STRATEGY: [CallbackQueryHandler(select_strategy, pattern="^strategy_")],
            SELECT_EXPIRY: [CallbackQueryHandler(select_expiry, pattern="^expiry_")],
            SELECT_STRIKE: [CallbackQueryHandler(select_strike, pattern="^strike_")],
            CONFIRM_HEDGE: [CallbackQueryHandler(confirm_hedge, pattern="^confirm_hedge")],
        },
        fallbacks=[CallbackQueryHandler(cancel_conversation, pattern="^cancel")],
    )

    application.add_handler(options_conv_handler)

    # --- Register Command Handlers ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("price", price_command))
    application.add_handler(CommandHandler("monitor_risk", monitor_risk_command))
    application.add_handler(CommandHandler("stop_monitoring", stop_monitoring_command))

    # --- Register Callback Handler for Buttons ---
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # --- Schedule the Background Job ---
    job_queue = application.job_queue
    # The job will run every 60 seconds, starting 10 seconds after the bot launches.
    job_queue.run_repeating(risk_check_job, interval=60, first=10)
    log.info("Risk checking job scheduled to run every 60 seconds.")

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