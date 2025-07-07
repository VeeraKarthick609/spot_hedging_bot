import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from telegram.constants import ParseMode
import asyncio
from telegram.ext import ConversationHandler, CallbackQueryHandler
from datetime import datetime

# Import our services and core logic
from services.data_fetcher import data_fetcher_instance
from core.risk_engine import risk_engine_instance

log = logging.getLogger(__name__)

# --- In-memory storage for demo purposes ---
# In a production bot, this would be a database (e.g., SQLite, PostgreSQL).
# Format: {chat_id: {"asset": "BTC", "symbol": "BTC/USDT", "size": 1.5, "threshold": 1000}}
user_positions = {}

# --- Options Hedging Conversation States ---
SELECT_STRATEGY, SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE = range(4)

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


async def hedge_options_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the options hedging conversation."""
    chat_id = update.effective_chat.id
    if chat_id not in user_positions:
        await update.message.reply_text("❌ Please set up a position to monitor first using `/monitor_risk`.")
        return ConversationHandler.END

    position = user_positions[chat_id]
    
    # For a long spot position, the logical hedges are buying a put or selling a call.
    keyboard = [
        [InlineKeyboardButton("Buy Protective Put (Downside Protection)", callback_data="strategy_put")],
        [InlineKeyboardButton("Sell Covered Call (Generate Income)", callback_data="strategy_call")],
        [InlineKeyboardButton("Cancel", callback_data="cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"You are holding **{position['size']} {position['asset']}**. "
        "Please choose an options hedging strategy:", 
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    return SELECT_STRATEGY

async def select_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the user's strategy choice and asks for an expiry date."""
    query = update.callback_query
    await query.answer()
    
    context.user_data['strategy'] = query.data # e.g., 'strategy_put'
    
    await query.edit_message_text("Fetching available expiry dates from Deribit...")
    
    # Fetch all BTC options and extract unique expiry dates
    instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
    if not instruments:
        await query.edit_message_text("❌ Could not fetch options data from Deribit. Please try again later.")
        return ConversationHandler.END

    expiries = sorted(list(set([i.split('-')[1] for i in instruments])))
    
    keyboard = []
    for expiry in expiries[:10]: # Limit to first 10 expiries for a clean interface
        # Convert '29NOV24' to a more readable format
        date_obj = datetime.strptime(expiry, '%y%m%d')
        readable_date = date_obj.strftime('%d %b %Y')
        keyboard.append([InlineKeyboardButton(readable_date, callback_data=f"expiry_{expiry}")])
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])

    await query.edit_message_text("Please select an expiry date:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_EXPIRY

async def select_expiry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles expiry choice and asks for a strike price."""
    query = update.callback_query
    await query.answer()

    expiry = query.data.split('_')[1]
    context.user_data['expiry'] = expiry
    
    await query.edit_message_text("Fetching available strike prices...")
    
    # Get current BTC price to suggest relevant strikes
    btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    if not btc_price:
        await query.edit_message_text("❌ Could not fetch BTC price. Please try again.")
        return ConversationHandler.END

    # Get all instruments and filter for the chosen expiry and option type
    option_type = 'P' if context.user_data['strategy'] == 'strategy_put' else 'C'
    instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
    
    relevant_strikes = []
    for i in instruments:
        parts = i.split('-')
        if parts[1] == expiry and parts[3] == option_type:
            relevant_strikes.append(int(parts[2]))
    
    # Find strikes closest to the current price (ATM, and a few OTM/ITM)
    strikes = sorted(relevant_strikes)
    closest_strike = min(strikes, key=lambda x:abs(x-btc_price))
    closest_index = strikes.index(closest_strike)
    
    # Show 2 strikes below, the ATM strike, and 2 strikes above
    display_strikes = strikes[max(0, closest_index-2):closest_index+3]
    
    keyboard = [[InlineKeyboardButton(f"Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])

    await query.edit_message_text(f"Current BTC Price: `${btc_price:,.2f}`\nPlease select a strike price:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    return SELECT_STRIKE

from datetime import datetime

async def select_strike(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles strike choice and shows final confirmation."""
    query = update.callback_query
    await query.answer()
    
    strike = int(query.data.split('_')[1])
    context.user_data['strike'] = strike
    
    await query.edit_message_text("Calculating hedge details...")
    
    # Construct the Deribit instrument name
    asset = user_positions[query.message.chat.id]['asset']
    
    # Parse and format expiry from YYMMDD to DMMMMYY (e.g., 250708 -> 8JUL25)
    raw_expiry = context.user_data['expiry']  # assume in YYMMDD format like 250708
    print(f"Raw expiry: {raw_expiry}")  # Debugging line

    # Parse as YYMMDD format
    expiry_date = datetime.strptime(raw_expiry, "%y%m%d")

    day = str(expiry_date.day)  # e.g., 8 (no leading zero)
    month = expiry_date.strftime("%b").upper()  # e.g., 'JUL'
    year = expiry_date.strftime("%y")  # e.g., '25'

    formatted_expiry = f"{day}{month}{year}"  # 8JUL25 ✅
    print(f"Formatted expiry: {formatted_expiry}")  # Debugging line
    # Construct the instrument name    
    option_type = 'P' if context.user_data['strategy'] == 'strategy_put' else 'C'
    instrument_name = f"{asset}-{formatted_expiry}-{strike}-{option_type}"
    
    # Fetch all data needed for calculation
    btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    option_ticker = await data_fetcher_instance.fetch_option_ticker(instrument_name)
    
    if not btc_price or not option_ticker:
        await query.edit_message_text("❌ Error fetching live data. Cannot proceed.")
        return ConversationHandler.END
        
    greeks = risk_engine_instance.calculate_option_greeks(btc_price, option_ticker)
    if not greeks:
        await query.edit_message_text("❌ Error calculating option greeks. Cannot proceed.")
        return ConversationHandler.END

    # Calculate how many contracts are needed to neutralize delta
    position_size = user_positions[query.message.chat.id]['size']
    contracts_needed = abs(position_size / greeks['delta'])
    total_cost = contracts_needed * greeks['price']

    # Prepare confirmation message
    action = "Buy" if option_type == 'P' else "Sell"
    
    # Portfolio delta after hedging
    original_delta = position_size
    hedge_delta = contracts_needed * greeks['delta']
    new_portfolio_delta = original_delta + hedge_delta
    
    message = (
        f"✅ **Hedge Confirmation**\n\n"
        f"**Strategy:** `{action} {instrument_name}`\n"
        f"**Quantity:** `{contracts_needed:.2f}` contracts\n"
        f"**Est. Cost/Premium:** `${total_cost:,.2f}`\n\n"
        f"--- **Risk Analysis** ---\n"
        f"**Option Delta:** `{greeks['delta']:.4f}`\n"
        f"**Original Portfolio Delta:** `{original_delta:.2f} BTC`\n"
        f"**Hedge Delta:** `{hedge_delta:.2f} BTC`\n"
        f"**New Portfolio Delta:** `{new_portfolio_delta:.4f} BTC`\n\n"
        f"This action will make your position nearly delta-neutral."
    )
    
    keyboard = [
        [InlineKeyboardButton("Confirm (Simulated)", callback_data="confirm_hedge")],
        [InlineKeyboardButton("Cancel", callback_data="cancel")]
    ]

    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    return CONFIRM_HEDGE


async def confirm_hedge(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Final confirmation of the simulated hedge."""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("✅ **Hedge action confirmed (Simulated).** No real trade was executed.")
    context.user_data.clear()
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Operation cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

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