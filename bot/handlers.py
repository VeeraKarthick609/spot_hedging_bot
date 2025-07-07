import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode
import asyncio

# Import our services and core logic
from services.data_fetcher import data_fetcher_instance
from core.risk_engine import risk_engine_instance

log = logging.getLogger(__name__)

# --- In-memory storage for demo purposes ---
# In a production bot, this would be a database (e.g., SQLite, PostgreSQL).
# Format: {chat_id: {"asset": "BTC", "symbol": "BTC/USDT", "size": 1.5, "threshold": 1000}}
user_positions = {}


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message when the /start command is issued."""
    user = update.effective_user
    welcome_message = (
        f"👋 Hello {user.first_name}!\n\n"
        f"Welcome to the *Spot Exposure Hedging Bot*.\n\n"
        f"*Available Commands:*\n"
        f"🔹 `/monitor_risk <ASSET> <SIZE> <THRESHOLD_USD>`\n"
        f"   Example: `/monitor_risk BTC 1.5 500`\n"
        f"🔹 `/stop_monitoring` - Stop tracking your position.\n"
        f"🔹 `/price <exchange> <symbol>` - Get a price.\n"
        f"   Example: `/price bybit BTC/USDT`\n"
        f"🔹 `/help` - Show this help message."
    )
    await update.message.reply_text(welcome_message, parse_mode=ParseMode.MARKDOWN)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_command(update, context)


async def monitor_risk_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sets up monitoring for a user's spot position."""
    chat_id = update.effective_chat.id
    try:
        # Example: /monitor_risk BTC 1.5 500
        asset = context.args[0].upper()
        size = float(context.args[1])
        threshold = float(context.args[2])

        # For simplicity, we hardcode the symbols. A real app would map assets to symbols.
        if asset != "BTC":
            await update.message.reply_text("❌ Sorry, only BTC monitoring is supported in this demo.")
            return
            
        position_data = {
            "asset": asset,
            "spot_symbol": "BTC/USDT",
            "perp_symbol": "BTC/USDT:USDT", # Bybit's linear perp symbol
            "size": size,
            "threshold": threshold,
        }
        user_positions[chat_id] = position_data
        
        await update.message.reply_text(
            f"✅ **Monitoring Enabled**\n\n"
            f"**Asset:** `{asset}`\n"
            f"**Position Size:** `{size}`\n"
            f"**Delta Threshold:** `${threshold:,.2f}`\n\n"
            f"I will now check your risk exposure every minute.",
            parse_mode=ParseMode.MARKDOWN
        )

    except (IndexError, ValueError):
        await update.message.reply_text(
            "❌ **Invalid format.**\n"
            "Usage: `/monitor_risk <ASSET> <SIZE> <THRESHOLD_USD>`\n"
            "Example: `/monitor_risk BTC 1.5 500`",
            parse_mode=ParseMode.MARKDOWN
        )


async def stop_monitoring_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stops monitoring for a user."""
    chat_id = update.effective_chat.id
    if chat_id in user_positions:
        del user_positions[chat_id]
        await update.message.reply_text("✅ Monitoring has been stopped.")
    else:
        await update.message.reply_text("ℹ️ You are not currently monitoring any position.")


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ... (this function remains unchanged from Phase 1) ...
    try:
        exchange_name = context.args[0]
        symbol = context.args[1]
    except IndexError:
        await update.message.reply_text("❌ **Invalid format.**\nUsage: `/price <exchange> <symbol>`\nExample: `/price bybit BTC/USDT`", parse_mode=ParseMode.MARKDOWN)
        return

    msg = await update.message.reply_text(f"Fetching price for `{symbol}` from `{exchange_name}`...", parse_mode=ParseMode.MARKDOWN)
    price = await data_fetcher_instance.get_price(exchange_name, symbol)
    if price is not None:
        response_text = f"📈 **Price Report**\n\n**Exchange:** `{exchange_name}`\n**Symbol:** `{symbol}`\n**Last Price:** `${price:,.4f}`"
    else:
        response_text = f"Could not fetch price for `{symbol}` on `{exchange_name}`. Please check the symbol and try again. For Bybit perps, use format like `BTC/USDT:USDT`."
    await context.bot.edit_message_text(text=response_text, chat_id=update.effective_chat.id, message_id=msg.message_id, parse_mode=ParseMode.MARKDOWN)


# ... (all imports and other functions like start_command, monitor_risk_command, etc., remain the same) ...

# --- BACKGROUND JOB ---
async def risk_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    This job runs periodically to check risk for all monitored positions using LIVE data.
    """
    if not user_positions:
        return

    log.info(f"Running REAL-TIME risk check job for {len(user_positions)} users.")

    for chat_id, position in user_positions.items():
        # 1. Fetch current LIVE prices for spot and perp
        spot_symbol = position['spot_symbol']
        perp_symbol = position['perp_symbol']
        
        spot_price_task = data_fetcher_instance.get_price('bybit', spot_symbol)
        perp_price_task = data_fetcher_instance.get_price('bybit', perp_symbol)
        spot_price, perp_price = await asyncio.gather(spot_price_task, perp_price_task)
        
        if spot_price is None or perp_price is None:
            log.warning(f"Could not fetch live prices for {position['asset']}. Skipping check for user {chat_id}.")
            continue

        # 2. Calculate current portfolio delta (exposure)
        current_delta_usd = position['size'] * spot_price

        # 3. Check if delta exceeds the user's threshold
        if abs(current_delta_usd) > position['threshold']:
            log.info(f"RISK THRESHOLD BREACHED for user {chat_id}. Delta: ${current_delta_usd:.2f}, Threshold: ${position['threshold']:.2f}")

            # --- Trigger Alert with Real-Time Calculations ---

            # 4. Get the current hedge ratio (beta) from the Risk Engine
            beta = await risk_engine_instance.calculate_beta(spot_symbol, perp_symbol)
            if beta is None:
                log.error(f"Could not calculate beta for {spot_symbol}/{perp_symbol}. Cannot recommend hedge.")
                await context.bot.send_message(chat_id, text="⚠️ Could not generate a hedge recommendation due to an internal error calculating beta.")
                continue

            # 5. Get the hedge recommendation from the Risk Engine using the calculated beta
            hedge_details = risk_engine_instance.calculate_perp_hedge(
                spot_position_usd=current_delta_usd,
                perp_price=perp_price,
                beta=beta
            )
            hedge_contracts = hedge_details['required_hedge_contracts']

            # --- 6. Prepare and send the detailed, live-data alert message ---
            message = (
                f"🚨 **Real-Time Risk Alert: {position['asset']}** 🚨\n\n"
                f"Your portfolio's delta exposure has exceeded its threshold.\n\n"
                f"**Position Size:** `{position['size']}` {position['asset']}\n"
                f"**Live Spot Price:** `${spot_price:,.2f}`\n"
                f"**Current Delta:** `${current_delta_usd:,.2f}`\n"
                f"**Risk Threshold:** `${position['threshold']:,.2f}`\n\n"
                f"--- **Hedge Recommendation** ---\n"
                f"**Hedge Ratio (Beta):** `{beta:.4f}`\n"
                f"**Action:** Short `{abs(hedge_contracts):.4f}` of `{position['perp_symbol']}`"
            )

            keyboard = [
                [InlineKeyboardButton("✅ Hedge Now (Simulated)", callback_data=f"hedge_now_{position['asset']}_{hedge_contracts:.4f}")],
                [InlineKeyboardButton("Dismiss", callback_data="dismiss_alert")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await context.bot.send_message(chat_id, text=message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query
    await query.answer() # Acknowledge the button press

    data = query.data

    if data.startswith("hedge_now"):
        # This is a SIMULATED action for Phase 2. Phase 4 will execute real trades.
        _, _, asset, size = data.split('_')  # Fixed: use '_' and account for all parts
        response_text = (
            f"✅ **Hedge Action Confirmed (Simulated)**\n\n"
            f"A market order to **short {abs(float(size))} {asset}** would be placed now.\n\n"
            f"_(This is a demo. No real trade was executed.)_"
        )
        await query.edit_message_text(text=response_text, parse_mode=ParseMode.MARKDOWN)

    elif data == "dismiss_alert":
        await query.edit_message_text(text="*Alert dismissed by user.*", parse_mode=ParseMode.MARKDOWN)