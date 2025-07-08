import os
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
from utils.pdf_generator import create_report_pdf


log = logging.getLogger(__name__)

# --- In-memory storage for demo purposes ---
# In a production bot, this would be a database (e.g., SQLite, PostgreSQL).
# Format: {chat_id: {"asset": "BTC", "symbol": "BTC/USDT", "size": 1.5, "threshold": 1000}}
user_positions = {}

# --- Options Hedging Conversation States ---
SELECT_STRATEGY, SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE = range(4)
ADJUST_DELTA, ADJUST_VAR = range(10, 12) # Use higher numbers to avoid conflict

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
        f"**Delta Threshold:** `${position['delta_threshold']:,.2f}`\n"
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
    The main background job, running the intelligent hedging logic for all users.
    This function now mirrors the logic of the successful backtester.
    """
    # 1. Get all active user configurations from the database
    all_positions = db_manager.get_all_positions()
    if not all_positions:
        return  # Exit early if no one is being monitored

    log.info(f"Running intelligent risk check for {len(all_positions)} users.")

    for position in all_positions:
        try:
            # --- 2. GATHER LIVE & HISTORICAL DATA ASYNCHRONOUSLY ---
            chat_id = position['chat_id']
            spot_symbol = position['spot_symbol']
            perp_symbol = position['perp_symbol']
            
            log.info(f"Processing position for chat_id: {chat_id}")

            # Fetch all required data concurrently for efficiency
            tasks = {
                "hist_data": data_fetcher_instance.fetch_historical_data('bybit', spot_symbol, '1h', limit=position['slow_ma'] + 5),
                "spot_price": data_fetcher_instance.get_price('bybit', spot_symbol),
                "perp_price": data_fetcher_instance.get_price('bybit', perp_symbol),
            }
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            
            # Unpack results and handle potential errors
            hist_df, spot_price, perp_price = results
            
            if isinstance(hist_df, Exception) or hist_df is None or hist_df.empty:
                log.warning(f"Could not fetch historical data for {spot_symbol}. Skipping check for {chat_id}. Error: {hist_df}")
                continue
            if isinstance(spot_price, Exception) or spot_price is None:
                log.warning(f"Could not fetch spot price for {spot_symbol}. Skipping check for {chat_id}. Error: {spot_price}")
                continue
            if isinstance(perp_price, Exception) or perp_price is None:
                log.warning(f"Could not fetch perp price for {perp_symbol}. Skipping check for {chat_id}. Error: {perp_price}")
                continue

            # --- 3. DETERMINE CURRENT MARKET REGIME ---
            should_be_hedging = True  # Default if filter is off
            regime_status_text = "N/A (Filter Off)"
            if position['use_regime_filter']:
                hist_df['fast_ma'] = hist_df['close'].rolling(window=position['fast_ma']).mean()
                hist_df['slow_ma'] = hist_df['close'].rolling(window=position['slow_ma']).mean()
                last_row = hist_df.iloc[-1]
                
                if pd.isna(last_row['slow_ma']):
                    log.info(f"Not enough data yet for MA calculation for {chat_id}. Skipping regime check.")
                else:
                    should_be_hedging = last_row['fast_ma'] < last_row['slow_ma']
                    regime_status_text = "BEARISH (Hedging ON)" if should_be_hedging else "BULLISH (Hedging OFF)"
            
            log.info(f"Chat {chat_id}: Market regime is '{regime_status_text}'")

            # --- 4. CALCULATE TARGET vs. CURRENT DELTA ---
            spot_value = position['size'] * spot_price
            # The delta we want to have after hedging
            target_delta = spot_value * (1 - position['hedge_ratio'])
            
            # Get current perp holdings. 
            # NOTE: For true accuracy, this should come from a live `fetch_positions` call.
            # We are using our own DB history as a reliable proxy for this project.
            last_hedge = db_manager.get_hedge_history(chat_id, limit=1)
            current_perp_holding = last_hedge[0]['size'] if last_hedge and last_hedge[0]['hedge_type'] == 'perp' else 0
            
            perp_value = current_perp_holding * perp_price
            current_delta = spot_value + perp_value

            # --- 5. DECIDE IF A TRADE IS NEEDED ---
            delta_to_correct = current_delta - target_delta
            trade_needed = False
            
            if should_be_hedging:
                # In a bearish market, we actively hedge. Trade if we drift from our target.
                if abs(delta_to_correct) > position['delta_threshold']:
                    trade_needed = True
                    log.info(f"Chat {chat_id}: Trade needed. Delta drift (${delta_to_correct:,.2f}) exceeds threshold (${position['threshold']:.2f}).")
            else:
                # In a bullish market, our goal is to have no hedge. Close any existing hedge.
                if abs(current_perp_holding) > 0.001:
                    delta_to_correct = perp_value  # The correction needed is to close the entire perp position
                    trade_needed = True
                    log.info(f"Chat {chat_id}: Trade needed. In bullish regime, closing existing hedge of {current_perp_holding:.4f} contracts.")
            
            # --- 6. EXECUTE ACTION IF NEEDED ---
            if trade_needed:
                contracts_to_trade = -delta_to_correct / perp_price
                
                if abs(contracts_to_trade) < 0.001: # Avoid dust trades
                    continue

                if position['auto_hedge_enabled']:
                    log.info(f"AUTO-HEDGING for {chat_id}: Executing trade for {contracts_to_trade:.4f} contracts.")
                    # Use the existing execution logic function
                    await execute_hedge_logic(context, chat_id, contracts_to_trade, position['asset'])
                    # Send a confirmation to the user
                    await context.bot.send_message(
                        chat_id,
                        text=f"✅ **Intelligent Auto-Hedge Executed!**\n\n**Reason:** Market is in `{regime_status_text}` and delta correction was required.\n**Action:** Placed simulated order for `{contracts_to_trade:.4f}` of `{perp_symbol}`.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    # Send an interactive alert for manual confirmation
                    message = (
                        f"🚨 **Hedge Recommendation** 🚨\n\n"
                        f"**Market Regime:** `{regime_status_text}`\n\n"
                        f"**Current Delta:** `${current_delta:,.2f}`\n"
                        f"**Target Delta:** `${target_delta:,.2f}` (based on {position['hedge_ratio']*100}% hedge ratio)\n\n"
                        f"**Recommended Action:**\n"
                        f"Trade `{contracts_to_trade:.4f}` of `{perp_symbol}` to align with your strategy."
                    )
                    keyboard = [
                        [InlineKeyboardButton("✅ Execute Hedge (Simulated)", callback_data=f"hedge_now_{position['asset']}_{contracts_to_trade:.4f}")],
                        [InlineKeyboardButton("Dismiss", callback_data="dismiss_alert")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await context.bot.send_message(chat_id, text=message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

        except Exception as e:
            log.error(f"An unexpected error occurred while processing position for chat_id {position.get('chat_id', 'N/A')}: {e}", exc_info=True)

# --- UPDATE BUTTON HANDLER ---
async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Parses all non-conversation inline button clicks."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press immediately
    data = query.data
    chat_id = query.message.chat.id

    if data.startswith("hedge_now"):
        await query.edit_message_text(text="*Finding best execution venue and estimating costs...*", parse_mode=ParseMode.MARKDOWN)
        
        _, _, asset, size_str = data.split('_')
        size = float(size_str)
        
        execution_plan = await execute_hedge_logic(context, chat_id, size, asset)
        
        if execution_plan:
            response_text = (
                f"✅ **Hedge Execution Plan (Simulated)**\n\n"
                f"**Action:** Short `{abs(size):.4f}` {asset}-PERP\n\n"
                f"--- **Smart Order Routing** ---\n"
                f"**Chosen Venue:** `{execution_plan['venue'].upper()}`\n"
                f"**Est. Fill Price:** `${execution_plan['avg_fill_price']:,.2f}`\n"
                f"**Est. Slippage:** `${execution_plan['slippage_usd']:,.4f}`\n\n"
                f"This action has been logged. Use `/hedge_history` to view."
            )
            await query.edit_message_text(text=response_text, parse_mode=ParseMode.MARKDOWN)
        else:
            await query.edit_message_text(text="❌ Hedge failed: Could not determine an execution plan.")

    elif data == "view_analytics":
        await query.edit_message_text(text="*Generating analytics report...*", parse_mode=ParseMode.MARKDOWN)
        # Call the reusable reporting function
        await send_portfolio_report(chat_id, context)

    elif data == "adjust_thresholds_prompt":
        # A callback query can't start a new user message, so we prompt them to use the command.
        await query.edit_message_text(
            text="*To adjust your thresholds, please send the command:* `/adjust_threshold`",
            parse_mode=ParseMode.MARKDOWN
        )

    elif data == "dismiss_alert":
        await query.edit_message_text(text="*Alert dismissed by user.*", parse_mode=ParseMode.MARKDOWN)

# --- Reusable Reporting Functions ---
async def send_portfolio_report(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Refactored logic to send the main portfolio risk report."""
    position = db_manager.get_position(chat_id)
    if not position:
        await context.bot.send_message(chat_id, "❌ No position found. Use `/monitor_risk` to set one up.")
        return
        
    await context.bot.send_message(chat_id, "Crunching the numbers... generating your portfolio risk report.", parse_mode=ParseMode.MARKDOWN)
    
    portfolio = [{'type': 'spot', 'asset': position['asset'], 'size': position['size']}]
    btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    if not btc_price:
        await context.bot.send_message(chat_id, "❌ Could not fetch live price data.")
        return
        
    prices = {'BTC/USDT': btc_price}
    risk_data = await risk_engine_instance.calculate_portfolio_risk(portfolio, prices)
    var_data = await risk_engine_instance.calculate_historical_var(portfolio, prices)

    report_text = (
        f"**📊 Portfolio Risk Report**\n\n"
        f"**Total Portfolio Delta:** `${risk_data['total_delta_usd']:,.2f}`\n\n"
        f"--- **Value at Risk (VaR)** ---\n"
        f"**1-Day 95% VaR:** `${var_data:,.2f}`\n"
        f"_(There's a 5% chance your portfolio could lose at least this much in 24 hours.)_"
    )
    await context.bot.send_message(chat_id, report_text, parse_mode=ParseMode.MARKDOWN)

async def monitor_risk_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    try:
        # /monitor_risk BTC 1.5 500 [VAR_THRESHOLD]
        args = context.args
        if len(args) < 3: raise ValueError("Not enough arguments")
        
        position_data = {
            "chat_id": chat_id, "asset": args[0].upper(),
            "spot_symbol": f"{args[0].upper()}/USDT", "perp_symbol": f"{args[0].upper()}/USDT:USDT",
            "size": float(args[1]), "delta_threshold": float(args[2]),
            # Optional VaR threshold
            "var_threshold": float(args[3]) if len(args) > 3 else None
        }
        db_manager.upsert_position(chat_id, position_data)
        await update.message.reply_text("✅ Monitoring enabled. Use `/hedge_status` to see your settings.", parse_mode=ParseMode.MARKDOWN)
    except (IndexError, ValueError):
        await update.message.reply_text("❌ Usage: `/monitor_risk <ASSET> <SIZE> <DELTA_USD> [VAR_USD]`", parse_mode=ParseMode.MARKDOWN)

async def chart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id, "Generating your hedge history chart...")
    history = db_manager.get_hedge_history(chat_id)
    chart_buffer = risk_engine_instance.generate_hedge_history_chart(history)
    
    if chart_buffer:
        await context.bot.send_photo(chat_id=chat_id, photo=chart_buffer, caption="Your net hedge position over time.")
    else:
        await context.bot.send_message(chat_id, "ℹ️ No hedge history found to generate a chart.")

async def adjust_threshold_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Enter your new **Delta Threshold** (e.g., `500`).\nType /skip to keep current.", parse_mode=ParseMode.MARKDOWN)
    return ADJUST_DELTA

async def adjust_delta_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    position = db_manager.get_position(chat_id)
    if text := update.message.text.lower():
        if text != '/skip':
            try:
                position['delta_threshold'] = float(text)
                db_manager.upsert_position(chat_id, position)
                await update.message.reply_text("✅ Delta threshold updated.")
            except ValueError:
                await update.message.reply_text("Invalid number. Please try again or /cancel.")
                return ADJUST_DELTA
    
    await update.message.reply_text("Enter your new **VaR Threshold** (e.g., `2000`).\nType /skip to keep current or /remove to disable.", parse_mode=ParseMode.MARKDOWN)
    return ADJUST_VAR

async def adjust_var_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    position = db_manager.get_position(chat_id)
    if text := update.message.text.lower():
        if text == '/skip':
            pass
        elif text == '/remove':
            position['var_threshold'] = None
            db_manager.upsert_position(chat_id, position)
            await update.message.reply_text("✅ VaR threshold removed.")
        else:
            try:
                position['var_threshold'] = float(text)
                db_manager.upsert_position(chat_id, position)
                await update.message.reply_text("✅ VaR threshold updated.")
            except ValueError:
                await update.message.reply_text("Invalid number. Please try again or /cancel.")
                return ADJUST_VAR

    await update.message.reply_text("All thresholds updated successfully! Use `/hedge_status` to view them.")
    return ConversationHandler.END

async def cancel_adjustment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Adjustment cancelled.")
    return ConversationHandler.END

async def send_daily_summary(context: ContextTypes.DEFAULT_TYPE):
    log.info("Running daily summary job...")
    positions = db_manager.get_all_positions()
    for pos in positions:
        if pos.get('daily_summary_enabled'):
            chat_id = pos['chat_id']
            await context.bot.send_message(chat_id, "☀️ **Good morning! Here is your daily risk summary:**")
            await send_portfolio_report(chat_id, context)

async def generate_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generates and sends a formal PDF report."""
    chat_id = update.effective_chat.id
    position = db_manager.get_position(chat_id)
    if not position:
        await update.message.reply_text("❌ No position found to report on.")
        return

    msg = await update.message.reply_text("... ⏳ Generating compliance report, please wait...")

    # --- 1. Gather all data ---
    try:
        # Position Data
        btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
        positions_for_report = [{
            'asset': position['asset'], 'type': 'SPOT', 'size': position['size'],
            'price': btc_price, 'value': position['size'] * btc_price
        }]
        
        # Risk Data
        prices = {'BTC/USDT': btc_price}
        portfolio_for_risk = [{'type': 'spot', 'asset': position['asset'], 'size': position['size']}]
        risk_data = await risk_engine_instance.calculate_portfolio_risk(portfolio_for_risk, prices)
        var_data = await risk_engine_instance.calculate_historical_var(portfolio_for_risk, prices)
        
        # History Data
        history_data = db_manager.get_hedge_history(chat_id, limit=50) # Get up to 50 recent trades

        report_data = {
            "positions": positions_for_report,
            "risk_metrics": {"delta": risk_data['total_delta_usd'], "var": var_data},
            "history": history_data
        }

        # --- 2. Create and send PDF ---
        filename = f"report_{chat_id}_{datetime.now().strftime('%Y%m%d')}.pdf"
        create_report_pdf(filename, report_data)
        
        await context.bot.send_document(chat_id, document=open(filename, 'rb'),
                                        caption="Here is your requested Portfolio Risk & Compliance Report.")
        await msg.delete() # Clean up the "please wait" message

    except Exception as e:
        log.error(f"Failed to generate report for {chat_id}: {e}")
        await msg.edit_text("❌ An error occurred while generating your report.")
    finally:
        # --- 3. Clean up the created file ---
        if os.path.exists(filename):
            os.remove(filename)

async def configure_strategy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Configures the parameters for the intelligent hedging strategy."""
    chat_id = update.effective_chat.id
    position = db_manager.get_position(chat_id)
    if not position:
        await update.message.reply_text("❌ Please set up a position with `/monitor_risk` first.")
        return
        
    try:
        # /configure_strategy <hedge_ratio> <use_regime_filter (on/off)>
        # Example: /configure_strategy 0.6 on
        hedge_ratio = float(context.args[0])
        use_filter_str = context.args[1].lower()

        if not (0.0 <= hedge_ratio <= 1.0):
            await update.message.reply_text("❌ Hedge ratio must be between 0.0 and 1.0.")
            return
        if use_filter_str not in ['on', 'off']:
            await update.message.reply_text("❌ Use 'on' or 'off' for the regime filter.")
            return

        position['hedge_ratio'] = hedge_ratio
        position['use_regime_filter'] = 1 if use_filter_str == 'on' else 0
        
        db_manager.upsert_position(chat_id, position)
        
        await update.message.reply_text(
            "✅ **Strategy Updated**\n\n"
            f"**Hedge Ratio:** `{position['hedge_ratio']}` (hedging {position['hedge_ratio']*100}% of exposure)\n"
            f"**Regime Filter:** `{'ON' if position['use_regime_filter'] else 'OFF'}`\n\n"
            "Your live bot will now use this logic.",
            parse_mode=ParseMode.MARKDOWN
        )
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: `/configure_strategy <hedge_ratio> <use_regime_filter (on|off)>`\nExample: `/configure_strategy 0.6 on`")