import json
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
from database import db_manager

log = logging.getLogger(__name__)

# --- In-memory storage for demo purposes ---
# In a production bot, this would be a database (e.g., SQLite, PostgreSQL).
# Format: {chat_id: {"asset": "BTC", "symbol": "BTC/USDT", "size": 1.5, "threshold": 1000}}
user_positions = {}

# --- Options Hedging Conversation States ---
SELECT_STRATEGY, SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE = range(4)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends an updated welcome message with all commands."""
    user = update.effective_user
    help_text = (
        f"👋 Hello {user.first_name}! Welcome to the **Spot Exposure Hedging Bot**.\n\n"
        "**CONFIGURATION**\n"
        "`/monitor_risk <ASSET> <SIZE> <THRESHOLD_USD>` - Start monitoring a position.\n"
        "`/auto_hedge <on|off>` - Toggle fully automated hedging.\n"
        "`/stop_monitoring` - Stop all monitoring.\n\n"
        "**ACTIONS & REPORTING**\n"
        "`/hedge_now <ASSET> <SIZE>` - Manually trigger a hedge.\n"
        "`/hedge_options` - Start an interactive options hedge.\n"
        "`/hedge_status` - View your current settings.\n"
        "`/hedge_history` - View recent hedge actions.\n"
        "`/portfolio_risk` - Get a VaR and risk report.\n\n"
        "**UTILITIES**\n"
        "`/price <exchange> <symbol>` - Get a price.\n"
        "`/help` - Show this message."
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_command(update, context)


async def monitor_risk_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Sets up or updates monitoring for a user's spot position.
    This version correctly formats the data for the database and preserves the user's
    existing auto-hedge setting upon updates.
    """
    chat_id = update.effective_chat.id
    try:
        asset = context.args[0].upper()
        size_str = context.args[1]
        threshold_str = context.args[2]

        # IMPROVEMENT: Preserve the user's existing auto-hedge setting.
        # First, try to fetch the current settings from the database.
        existing_position = db_manager.get_position(chat_id)
        
        # If a setting exists, use its auto_hedge status. Otherwise, default to 0 (off).
        current_auto_hedge_status = existing_position['auto_hedge_enabled'] if existing_position else 0

        # This dictionary now uses the correct key 'auto_hedge_enabled' to match the database.
        position_data = {
            "chat_id": chat_id,
            "asset": asset,
            "spot_symbol": f"{asset}/USDT",
            "perp_symbol": f"{asset}/USDT:USDT",
            "size": float(size_str),
            "threshold": float(threshold_str),
            "auto_hedge_enabled": current_auto_hedge_status  # Preserve the existing setting
        }
        
        db_manager.upsert_position(chat_id, position_data)
        
        await update.message.reply_text(
            "✅ Monitoring settings have been updated.\n\n"
            "Your auto-hedge setting has been preserved. "
            "Use `/hedge_status` to see your current configuration.",
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
    db_manager.delete_position(update.effective_chat.id)
    await update.message.reply_text("✅ All monitoring has been stopped and settings cleared.")

async def auto_hedge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    position = db_manager.get_position(chat_id)
    if not position:
        await update.message.reply_text("❌ Please set up a position with `/monitor_risk` first.")
        return
    try:
        status = context.args[0].lower()
        if status not in ['on', 'off']: raise ValueError()
        
        position['auto_hedge_enabled'] = 1 if status == 'on' else 0
        db_manager.upsert_position(chat_id, position)
        
        mode = "ENABLED" if status == 'on' else "DISABLED"
        await update.message.reply_text(f"✅ **Automated hedging is now {mode}.**")
    except (IndexError, ValueError):
        await update.message.reply_text("❌ Usage: `/auto_hedge <on|off>`")

async def hedge_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    position = db_manager.get_position(update.effective_chat.id)
    if not position:
        await update.message.reply_text("ℹ️ You are not currently monitoring any position.")
        return
    mode = "ON" if position['auto_hedge_enabled'] else "OFF"
    status_text = (
        f"**📋 Hedging Status**\n\n"
        f"**Asset:** `{position['asset']}`\n"
        f"**Size:** `{position['size']}`\n"
        f"**Delta Threshold:** `${position['threshold']:,.2f}`\n"
        f"**Auto-Hedge Mode:** `{mode}`"
    )
    await update.message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

async def hedge_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    history = db_manager.get_hedge_history(update.effective_chat.id)
    if not history:
        await update.message.reply_text("ℹ️ No hedge history found.")
        return
    
    report = "**📜 Recent Hedge History**\n\n"
    for item in history:
        details = json.loads(item['details'])
        ts = datetime.strptime(item['timestamp'], '%Y-%m-%d %H:%M:%S').strftime('%d-%b %H:%M')
        cost = details.get('total_cost_usd', 0)
        report += (
            f"**{ts}** - `{item['action'].upper()}`\n"
            f"  - Size: `{abs(item['size']):.4f}`\n"
            f"  - Cost: `${cost:,.2f}`\n"
            f"  - Venue: `{details.get('venue', 'N/A').upper()}`\n---\n"
        )
    await update.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)

async def execute_hedge_logic(context: ContextTypes.DEFAULT_TYPE, chat_id: int, size: float, asset: str):
    """A reusable function to perform and log a simulated hedge."""
    perp_symbol = f"{asset}/USDT:USDT"
    execution_plan = await risk_engine_instance.find_best_execution_venue(perp_symbol, size)
    
    if not execution_plan:
        await context.bot.send_message(chat_id, "❌ Hedge failed: Could not determine an execution plan.")
        return None

    # Log the successful simulated hedge to the database
    db_manager.log_hedge(
        chat_id=chat_id,
        hedge_type='perp',
        action='short' if size < 0 else 'long',
        size=size,
        details=json.dumps(execution_plan)
    )
    return execution_plan

async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    # Parse as YYMMDD format
    expiry_date = datetime.strptime(raw_expiry, "%y%m%d")

    day = str(expiry_date.day)  # e.g., 8 (no leading zero)
    month = expiry_date.strftime("%b").upper()  # e.g., 'JUL'
    year = expiry_date.strftime("%y")  # e.g., '25'

    formatted_expiry = f"{day}{month}{year}"  
    
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

async def portfolio_risk_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Provides a comprehensive risk report for the user's portfolio."""
    chat_id = update.effective_chat.id
    if chat_id not in user_positions:
        await update.message.reply_text("❌ No position found. Use `/monitor_risk` to set one up.")
        return
        
    await update.message.reply_text("Crunching the numbers... generating your portfolio risk report.", parse_mode=ParseMode.MARKDOWN)

    # For this demo, we assume the portfolio is just the one monitored position.
    # A full system would pull all positions from a database.
    position = user_positions[chat_id]
    portfolio = [{'type': 'spot', 'asset': position['asset'], 'size': position['size']}]
    
    # Get live prices
    btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    if not btc_price:
        await update.message.reply_text("❌ Could not fetch live price data.")
        return
        
    prices = {'BTC/USDT': btc_price}
    
    # Calculate portfolio risk and VaR
    risk_data = await risk_engine_instance.calculate_portfolio_risk(portfolio, prices)
    var_data = await risk_engine_instance.calculate_historical_var(portfolio, prices)

    report_text = (
        f"**📊 Portfolio Risk Report**\n\n"
        f"**Total Portfolio Delta:** `${risk_data['total_delta_usd']:,.2f}`\n"
        f"_(This is your total directional exposure to the market.)_\n\n"
        f"--- **Value at Risk (VaR)** ---\n"
        f"**1-Day 95% VaR:** `${var_data:,.2f}`\n"
        f"_(Based on historical simulation, there is a 5% chance your portfolio could lose at least this amount in the next 24 hours.)_"
    )
    
    await update.message.reply_text(report_text, parse_mode=ParseMode.MARKDOWN)

# --- BACKGROUND JOB ---
async def risk_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    The main background job. It runs periodically, checks risk for all monitored positions
    from the database, and handles automated hedging or sends interactive alerts.
    """
    all_positions = db_manager.get_all_positions()
    if not all_positions:
        return  # No positions to check, exit quietly.

    log.info(f"Running risk check job for {len(all_positions)} positions stored in the database.")

    for position in all_positions:
        chat_id = position['chat_id']
        spot_symbol = position['spot_symbol']
        perp_symbol = position['perp_symbol']
        
        # 1. Fetch live spot price for the current position's asset.
        spot_price = await data_fetcher_instance.get_price('bybit', spot_symbol)
        
        if spot_price is None:
            log.warning(f"Could not fetch spot price for {spot_symbol}. Skipping check for chat_id {chat_id}.")
            continue

        # 2. Calculate the current delta exposure of the spot position.
        # Note: A more advanced version would also subtract the delta of existing hedges.
        current_delta_usd = position['size'] * spot_price

        # 3. Check if the exposure exceeds the user-defined threshold.
        if abs(current_delta_usd) > position['threshold']:
            log.info(f"RISK THRESHOLD BREACHED for chat_id {chat_id}. Delta: ${current_delta_usd:,.2f}, Threshold: ${position['threshold']:.2f}")

            # 4. Gather necessary data for the hedge calculation.
            perp_price_task = data_fetcher_instance.get_price('bybit', perp_symbol)
            beta_task = risk_engine_instance.calculate_beta(spot_symbol, perp_symbol)
            perp_price, beta = await asyncio.gather(perp_price_task, beta_task)
            
            if perp_price is None or beta is None:
                log.error(f"Could not fetch perp price or calculate beta for {perp_symbol}. Cannot proceed with hedge for {chat_id}.")
                continue

            # 5. Get the recommended hedge size from the risk engine.
            hedge_details = risk_engine_instance.calculate_perp_hedge(current_delta_usd, perp_price, beta)
            hedge_contracts = hedge_details['required_hedge_contracts']

            # --- 6. CRITICAL LOGIC: Decide between Auto-Hedge and Manual Alert ---
            if position['auto_hedge_enabled']:
                # AUTOMATED HEDGING PATH
                log.info(f"Auto-hedging is ON for {chat_id}. Executing hedge automatically...")
                await context.bot.send_message(chat_id, text="🚨 **Auto-Hedge Triggered!**\nFinding best execution venue...", parse_mode=ParseMode.MARKDOWN)
                
                # Use the reusable logic to execute and log the hedge.
                execution_plan = await execute_hedge_logic(context, chat_id, hedge_contracts, position['asset'])
                
                if execution_plan:
                    response_text = (
                        f"✅ **Auto-Hedge Executed (Simulated)**\n\n"
                        f"**Action:** Short `{abs(hedge_contracts):.4f}` {position['asset']}-PERP\n"
                        f"**Venue:** `{execution_plan['venue'].upper()}`\n"
                        f"**Est. Total Cost:** `${execution_plan['total_cost_usd']:,.2f}`\n\n"
                        f"This action has been logged. Use `/hedge_history` to view."
                    )
                    await context.bot.send_message(chat_id, text=response_text, parse_mode=ParseMode.MARKDOWN)
            else:
                # MANUAL HEDGING PATH
                log.info(f"Auto-hedging is OFF for {chat_id}. Sending interactive alert.")
                message = (
                    f"🚨 **Risk Alert: {position['asset']}** 🚨\n\n"
                    f"Your portfolio's delta exposure has exceeded its threshold.\n\n"
                    f"**Current Delta:** `${current_delta_usd:,.2f}`\n"
                    f"**Risk Threshold:** `${position['threshold']:,.2f}`\n\n"
                    f"**Recommended Action:**\n"
                    f"Short `{abs(hedge_contracts):.4f}` of `{position['perp_symbol']}`\n"
                )
                
                keyboard = [
                    [InlineKeyboardButton("✅ Hedge Now (Simulated)", callback_data=f"hedge_now_{position['asset']}_{hedge_contracts:.4f}")],
                    [InlineKeyboardButton("Dismiss", callback_data="dismiss_alert")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await context.bot.send_message(chat_id, text=message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

# --- UPDATE BUTTON HANDLER ---
async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Parses all CallbackQuery events from inline buttons.
    """
    query = update.callback_query
    # Acknowledge the button press to remove the "loading" icon on the user's side.
    await query.answer()

    data = query.data

    if data.startswith("hedge_now_"):
        # This block handles the "Hedge Now" button from a manual risk alert.
        await query.edit_message_text(text="Finding best execution venue and estimating costs...")
        
        try:
            _, _, asset, size_str = data.split('_')
            size = float(size_str)
            chat_id = query.message.chat.id
            
            # Use the centralized, reusable logic to perform the hedge simulation and DB logging.
            execution_plan = await execute_hedge_logic(context, chat_id, size, asset)
            
            if execution_plan:
                response_text = (
                    f"✅ <b>Hedge Execution Plan (Simulated)</b>\n\n"
                    f"<b>Action:</b> Short <code>{abs(size):.4f}</code> {asset}-PERP\n\n"
                    f"--- <b>Smart Order Routing Analysis</b> ---\n"
                    f"<b>Chosen Venue:</b> <code>{execution_plan['venue'].upper()}</code> (Best price)\n"
                    f"<b>Est. Fill Price:</b> <code>${execution_plan['avg_fill_price']:,.2f}</code>\n"
                    f"<b>Est. Slippage:</b> <code>${execution_plan['slippage_usd']:,.4f}</code>\n"
                    f"<b>Est. Taker Fee:</b> <code>${execution_plan['fees_usd']:,.4f}</code>\n\n"
                    f"<i>(This is a simulation. The action has been logged in /hedge_history.)</i>"
                )
                
                try:
                    await query.edit_message_text(text=response_text, parse_mode=ParseMode.HTML)
                except Exception as e:
                    # Fallback to plain text if HTML fails
                    plain_text = (
                        f"✅ Hedge Execution Plan (Simulated)\n\n"
                        f"Action: Short {abs(size):.4f} {asset}-PERP\n\n"
                        f"--- Smart Order Routing Analysis ---\n"
                        f"Chosen Venue: {execution_plan['venue'].upper()} (Best price)\n"
                        f"Est. Fill Price: ${execution_plan['avg_fill_price']:,.2f}\n"
                        f"Est. Slippage: ${execution_plan['slippage_usd']:,.4f}\n"
                        f"Est. Taker Fee: ${execution_plan['fees_usd']:,.4f}\n\n"
                        f"(This is a simulation. The action has been logged in /hedge_history.)"
                    )
                    await query.edit_message_text(text=plain_text)
            else:
                await query.edit_message_text(text="❌ Hedge failed: Could not determine an execution plan. Please try again later.")

        except (ValueError, IndexError) as e:
            log.error(f"Error parsing hedge_now callback data '{data}': {e}")
            await query.edit_message_text(text="❌ An error occurred while processing your request.")

    elif data == "dismiss_alert":
        # This block handles the "Dismiss" button.
        await query.edit_message_text(text="<i>Alert dismissed by user.</i>", parse_mode=ParseMode.HTML)