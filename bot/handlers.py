import os
import json
import logging
from telegram import InputFile, Update, InlineKeyboardButton, InlineKeyboardMarkup
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
import pandas as pd
from reporting import reporting_manager

log = logging.getLogger(__name__)

# --- Options Hedging Conversation States ---
# Use higher numbers to avoid conflict
SELECT_STRATEGY, SELECT_EXPIRY, SELECT_STRIKE, CONFIRM_HEDGE = range(4)
ADJUST_DELTA, ADJUST_VAR = range(10, 12) 
SELECT_PUT_STRIKE, SELECT_CALL_STRIKE = range(20, 22)
SELECT_BUY_PUT, SELECT_SELL_PUT, SELECT_SELL_CALL, SELECT_BUY_CALL, CONFIRM_CONDOR = range(30, 35)

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
    chat_id = update.effective_chat.id
    db_manager.delete_position(chat_id)
    db_manager.clear_holdings(chat_id) # Also clear the holdings state
    await update.message.reply_text("✅ All monitoring and portfolio state has been stopped and cleared.")

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
    db_manager.upsert_holding(
        chat_id=chat_id,
        symbol=perp_symbol, # Using Bybit's symbol format as the key
        asset_type='perp',
        quantity_change=size
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
    position = db_manager.get_position(chat_id)
    if not position:
        await update.message.reply_text("❌ Please set up a position to monitor first using `/monitor_risk`.")
        return ConversationHandler.END
    
    # For a long spot position, the logical hedges are buying a put or selling a call.
    keyboard = [
        [InlineKeyboardButton("Buy Protective Put (Downside Protection)", callback_data="strategy_put")],
        [InlineKeyboardButton("Sell Covered Call (Generate Income)", callback_data="strategy_call")],
        [InlineKeyboardButton("Create Collar (Put+Call)", callback_data="strategy_collar")],
        [InlineKeyboardButton("Create Iron Condor", callback_data="strategy_condor")],
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
    if context.user_data['strategy'] == 'strategy_condor':
        await query.edit_message_text("An Iron Condor is a 4-leg, range-bound strategy.\nFirst, let's choose an expiry date for all legs.")
    if context.user_data['strategy'] == 'strategy_collar':
        await query.edit_message_text("A collar protects your downside while capping your upside.\nFirst, let's choose the **Protective Put**.")
    
    else:
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
    
    if context.user_data['strategy'] == 'strategy_condor':
        # For condor, we need to get strikes for the long put selection
        await query.edit_message_text("Fetching available strike prices...")
        
        # Get current BTC price to suggest relevant strikes
        btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
        if not btc_price:
            await query.edit_message_text("❌ Could not fetch BTC price. Please try again.")
            return ConversationHandler.END

        # Get all instruments and filter for the chosen expiry and PUT options
        instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
        
        relevant_strikes = []
        for i in instruments:
            parts = i.split('-')
            if parts[1] == expiry and parts[3] == 'P':  # PUT options for condor
                relevant_strikes.append(int(parts[2]))
        
        # Find strikes closest to the current price (ATM, and a few OTM/ITM)
        strikes = sorted(relevant_strikes)
        closest_strike = min(strikes, key=lambda x:abs(x-btc_price))
        closest_index = strikes.index(closest_strike)
        
        # Show 2 strikes below, the ATM strike, and 2 strikes above
        display_strikes = strikes[max(0, closest_index-2):closest_index+3]
        
        keyboard = [[InlineKeyboardButton(f"Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
        keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])
        
        await query.edit_message_text(
            f"Current BTC Price: `${btc_price:,.2f}`\nStep 1/4: Select the **long Put** strike (your lower protection):", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=ParseMode.MARKDOWN
        )
        return SELECT_BUY_PUT
        
    elif context.user_data['strategy'] == 'strategy_collar':
        # For collar, get strikes for PUT selection
        await query.edit_message_text("Fetching available strike prices...")
        
        # Get current BTC price to suggest relevant strikes
        btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
        if not btc_price:
            await query.edit_message_text("❌ Could not fetch BTC price. Please try again.")
            return ConversationHandler.END

        # Get all instruments and filter for the chosen expiry and PUT options
        instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
        
        relevant_strikes = []
        for i in instruments:
            parts = i.split('-')
            if parts[1] == expiry and parts[3] == 'P':  # PUT options for collar
                relevant_strikes.append(int(parts[2]))
        
        # Find strikes closest to the current price (ATM, and a few OTM/ITM)
        strikes = sorted(relevant_strikes)
        closest_strike = min(strikes, key=lambda x:abs(x-btc_price))
        closest_index = strikes.index(closest_strike)
        
        # Show 2 strikes below, the ATM strike, and 2 strikes above
        display_strikes = strikes[max(0, closest_index-2):closest_index+3]
        
        keyboard = [[InlineKeyboardButton(f"Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
        keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])
        
        await query.edit_message_text(
            f"Current BTC Price: `${btc_price:,.2f}`\nPlease select a **Put Strike Price**:", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=ParseMode.MARKDOWN
        )
        return SELECT_PUT_STRIKE
        
    else:
        # For other strategies (single options)
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

        await query.edit_message_text(
            f"Current BTC Price: `${btc_price:,.2f}`\nPlease select a strike price:", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=ParseMode.MARKDOWN
        )
        return SELECT_STRIKE

async def select_strike(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles strike choice and shows final confirmation. Now uses db_manager."""
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat.id
    position = db_manager.get_position(chat_id)
    if not position:
        await query.edit_message_text("❌ Error: Could not find your monitored position. Please /start over.")
        return ConversationHandler.END

    if context.user_data['strategy'] == 'strategy_collar':
        # This is the second leg of the collar
        context.user_data['call_strike'] = int(query.data.split('_')[1])
        await query.edit_message_text("Calculating collar details...")
        
        # Get data for both options (put and call)
        asset = position['asset'] # USE DB DATA
        raw_expiry = context.user_data['expiry']
        
        # Parse and format expiry
        expiry_date = datetime.strptime(raw_expiry, "%y%m%d")
        day = str(expiry_date.day)
        month = expiry_date.strftime("%b").upper()
        year = expiry_date.strftime("%y")
        formatted_expiry = f"{day}{month}{year}"
        
        # Construct instrument names for both legs
        put_instrument = f"{asset}-{formatted_expiry}-{context.user_data['strike']}-P"
        call_instrument = f"{asset}-{formatted_expiry}-{context.user_data['call_strike']}-C"
        
        # Fetch market data
        btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
        put_ticker = await data_fetcher_instance.fetch_option_ticker(put_instrument)
        call_ticker = await data_fetcher_instance.fetch_option_ticker(call_instrument)
        
        if not all([btc_price, put_ticker, call_ticker]):
            await query.edit_message_text("❌ Error fetching live data. Cannot proceed.")
            return ConversationHandler.END
        
        # Calculate greeks for both options
        put_greeks = await risk_engine_instance.calculate_option_greeks(btc_price, put_ticker)
        call_greeks = await risk_engine_instance.calculate_option_greeks(btc_price, call_ticker)
        
        if not all([put_greeks, call_greeks]):
            await query.edit_message_text("❌ Error calculating option greeks. Cannot proceed.")
            return ConversationHandler.END
        
        # Calculate position sizes and costs
        position_size = position['size'] # USE DB DATA
        put_contracts = abs(position_size / put_greeks['delta'])
        call_contracts = abs(position_size / call_greeks['delta'])
        
        total_cost = (put_contracts * put_greeks['price']) - (call_contracts * call_greeks['price'])
        
        # Calculate net delta
        original_delta = position_size
        put_delta = put_contracts * put_greeks['delta']
        call_delta = call_contracts * call_greeks['delta']
        net_delta = original_delta + put_delta + call_delta
        
        message = (
            f"✅ **Collar Strategy Confirmation**\n\n"
            f"**Put Leg:** `Buy {put_instrument}`\n"
            f"**Put Quantity:** `{put_contracts:.2f}` contracts\n"
            f"**Call Leg:** `Sell {call_instrument}`\n"
            f"**Call Quantity:** `{call_contracts:.2f}` contracts\n"
            f"**Net Cost/Premium:** `${total_cost:,.2f}`\n\n"
            f"--- **Risk Analysis** ---\n"
            f"**Put Delta:** `{put_greeks['delta']:.4f}`\n"
            f"**Call Delta:** `{call_greeks['delta']:.4f}`\n"
            f"**Original Portfolio Delta:** `{original_delta:.2f} BTC`\n"
            f"**Net Hedge Delta:** `{put_delta + call_delta:.2f} BTC`\n"
            f"**Final Portfolio Delta:** `{net_delta:.4f} BTC`\n\n"
            f"This collar strategy will provide downside protection while capping upside potential."
        )
    else:
        strike = int(query.data.split('_')[1])
        context.user_data['strike'] = strike
        
        await query.edit_message_text("Calculating hedge details...")
        use_ml_vol = context.user_data.get('use_ml_vol', False)
        
        # Construct the Deribit instrument name
        asset = position['asset'] # USE DB DATA
        
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
            
        greeks = await risk_engine_instance.calculate_option_greeks(btc_price, option_ticker, use_ml_vol=use_ml_vol)
        if not greeks:
            await query.edit_message_text("❌ Error calculating option greeks. Cannot proceed.")
            return ConversationHandler.END
    
        # Calculate how many contracts are needed to neutralize delta
        position_size = position['size'] # USE DB DATA
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
    """
    Provides a comprehensive, state-aware risk report for the user's full portfolio,
    including all spot, perpetual, and options holdings.
    """
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("🔍 Analyzing your full portfolio... This may take a moment.")

    try:
        # --- 1. Fetch ALL holdings from the database ---
        holdings = db_manager.get_holdings(chat_id)
        if not holdings:
            await msg.edit_text("❌ You have no holdings to analyze. Use `/monitor_risk` to start.")
            return

        # --- 2. Gather live data for all assets concurrently ---
        btc_spot_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
        if not btc_spot_price:
            await msg.edit_text("❌ Critical error: Could not fetch the live price of BTC. Cannot generate report.")
            return
            
        prices = {'BTC/USDT': btc_spot_price, 'BTC/USDT:USDT': btc_spot_price} # Assume perp price tracks spot closely for this report

        portfolio_for_risk_calc = []
        portfolio_details = []
        
        # --- 3. Process each holding to calculate its value and risk contribution ---
        net_delta_btc, net_gamma_btc, net_vega_usd, net_theta_usd = 0.0, 0.0, 0.0, 0.0
        total_portfolio_value_usd = 0.0

        for holding in holdings:
            asset_type = holding['asset_type']
            symbol = holding['asset_symbol']
            quantity = holding['quantity']
            
            # Add raw data for VaR calculation
            portfolio_for_risk_calc.append({'type': asset_type, 'asset': 'BTC', 'size': quantity, 'symbol': symbol})

            if asset_type == 'spot':
                value_usd = quantity * btc_spot_price
                delta_btc = quantity
                portfolio_details.append({
                    'type': 'SPOT', 'symbol': symbol, 'quantity': quantity,
                    'value_usd': value_usd, 'delta_btc': delta_btc
                })
                net_delta_btc += delta_btc
                total_portfolio_value_usd += value_usd

            elif asset_type == 'perp':
                notional_usd = quantity * btc_spot_price
                delta_btc = quantity # Delta of a linear perp is 1:1
                portfolio_details.append({
                    'type': 'PERP', 'symbol': symbol, 'quantity': quantity,
                    'value_usd': notional_usd, 'delta_btc': delta_btc
                })
                net_delta_btc += delta_btc
                # Note: Perps don't add to portfolio value, they are liabilities/assets against cash (margin)

            elif asset_type == 'option':
                option_ticker = await data_fetcher_instance.fetch_option_ticker(symbol)
                if not option_ticker:
                    log.warning(f"Could not fetch ticker for option {symbol} for chat_id {chat_id}")
                    continue
                
                greeks = await risk_engine_instance.calculate_option_greeks(btc_spot_price, option_ticker)
                if not greeks:
                    log.warning(f"Could not calculate greeks for option {symbol} for chat_id {chat_id}")
                    continue

                option_value_usd = quantity * greeks['price'] * btc_spot_price # price is in BTC
                delta_btc = quantity * greeks['delta']
                
                portfolio_details.append({
                    'type': 'OPTION', 'symbol': symbol, 'quantity': quantity,
                    'value_usd': option_value_usd, 'delta_btc': delta_btc
                })
                net_delta_btc += delta_btc
                net_gamma_btc += quantity * greeks['gamma']
                net_vega_usd += quantity * greeks['vega']
                net_theta_usd += quantity * greeks['theta']
                total_portfolio_value_usd += option_value_usd
        
        # --- 4. Calculate final portfolio-level metrics ---
        net_delta_usd = net_delta_btc * btc_spot_price
        
        # Use the full portfolio for VaR calculation
        var_data = await risk_engine_instance.calculate_historical_var(portfolio_for_risk_calc, prices)

        # --- 5. Format the report ---
        report_text = f"**📊 Full Portfolio Risk Report**\n\n"
        
        # --- Summary Section ---
        report_text += (
            f"**Total Value (Spot & Options):** `${total_portfolio_value_usd:,.2f}`\n\n"
            f"**Net Delta:** `{net_delta_btc:,.4f}` BTC (`${net_delta_usd:,.2f}`)\n"
            f"_(Your total directional exposure. Positive is bullish, negative is bearish.)_\n\n"
            f"**Net Gamma:** `{net_gamma_btc:,.4f}`\n"
            f"_(Measures how fast your Delta will change. High gamma means high risk/reward.)_\n\n"
            f"**Net Vega:** `${net_vega_usd:,.2f}`\n"
            f"_(Your P&L change for a 1% rise in implied volatility.)_\n\n"
            f"**Net Theta:** `${net_theta_usd:,.2f}`\n"
            f"_(Your daily P&L decay from time passing.)_\n\n"
        )
        
        # --- Composition Section ---
        report_text += "**--- Portfolio Composition ---**\n"
        for detail in portfolio_details:
            sign = "＋" if detail['quantity'] > 0 else "－"
            value_display = f"Val: ${abs(detail['value_usd']):,.2f}"
            delta_display = f"Δ: {detail['delta_btc']:.3f} BTC"
            
            report_text += (
                f"🔹 **`{detail['type']}`** `{detail['symbol']}`\n"
                f"   `{sign} {abs(detail['quantity']):.4f}` | `{value_display}` | `{delta_display}`\n"
            )
        report_text += "\n"

        # --- VaR Section ---
        report_text += (
            f"**--- Value at Risk (VaR) ---**\n"
            f"**1-Day 95% VaR:** `${var_data:,.2f}`\n"
            f"_(Based on your net exposure, there's a 5% chance your portfolio could lose at least this amount in 24 hours.)_"
        )

        await msg.edit_text(report_text, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        log.error(f"Failed to generate full portfolio risk report for {chat_id}: {e}", exc_info=True)
        await msg.edit_text("❌ An unexpected error occurred while generating your report. The developers have been notified.")

# --- BACKGROUND JOB ---
async def risk_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    The main background job, now using a state-aware portfolio delta calculation.
    It loops through all user configurations and their corresponding portfolio holdings
    to make intelligent, incremental hedging decisions.
    """
    all_configs = db_manager.get_all_positions()
    if not all_configs:
        return  # No work to do if no users are monitoring.

    # Fetch primary asset prices once to be efficient
    btc_spot_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    btc_perp_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT:USDT')

    if not btc_spot_price or not btc_perp_price:
        log.error("Could not fetch primary BTC prices. Skipping this risk check cycle.")
        return

    for config in all_configs:
        chat_id = config['chat_id']
        
        # --- 1. Get current state of the entire portfolio from the database ---
        holdings = db_manager.get_holdings(chat_id)
        if not holdings:
            log.warning(f"No holdings found for configured user {chat_id}. Skipping.")
            continue

        # --- 2. Calculate NET portfolio delta ---
        net_portfolio_delta_usd = 0.0
        try:
            for holding in holdings:
                if holding['asset_type'] == 'spot':
                    net_portfolio_delta_usd += holding['quantity'] * btc_spot_price
                
                elif holding['asset_type'] == 'perp':
                    net_portfolio_delta_usd += holding['quantity'] * btc_perp_price
                
                elif holding['asset_type'] == 'option':
                    option_ticker = await data_fetcher_instance.fetch_option_ticker(holding['symbol'])
                    if option_ticker:
                        greeks = await risk_engine_instance.calculate_option_greeks(btc_spot_price, option_ticker, use_ml_vol=False)
                        if greeks:
                            net_portfolio_delta_usd += holding['quantity'] * greeks['delta'] * btc_spot_price
        except Exception as e:
            log.error(f"Error calculating net delta for user {chat_id}: {e}")
            continue # Skip to the next user

        log.info(f"User {chat_id}: Calculated Net Portfolio Delta = ${net_portfolio_delta_usd:,.2f}")

        # --- 3. Check if the NET delta exceeds the user's threshold ---
        if abs(net_portfolio_delta_usd) > config['delta_threshold']:
            log.info(f"NET DELTA THRESHOLD BREACHED for {chat_id}. Required hedge.")
            
            # --- 4. Calculate the required hedge for the REMAINING delta ---
            beta = 1.0  # Assuming 1:1 hedge ratio for BTC spot/perp
            hedge_details = risk_engine_instance.calculate_perp_hedge(
                spot_position_usd=net_portfolio_delta_usd,
                perp_price=btc_perp_price,
                beta=beta
            )
            hedge_contracts_to_trade = hedge_details['required_hedge_contracts']

            # --- 5. Execute or Alert based on user's auto_hedge setting ---
            if config['auto_hedge_enabled']:
                # The auto-hedge logic with large trade confirmation safety check
                hedge_value_usd = abs(hedge_contracts_to_trade * btc_perp_price)
                large_trade_limit = config.get('large_trade_threshold')
                
                if large_trade_limit and hedge_value_usd > large_trade_limit:
                    log.warning(f"LARGE TRADE DETECTED for {chat_id}. Reverting to manual confirmation.")
                    await context.bot.send_message(chat_id, f"⚠️ **Large Trade - Manual Confirmation Required!**\n\nThe required hedge of `${hedge_value_usd:,.2f}` exceeds your safety limit of `${large_trade_limit:,.2f}`.")
                    # Fall through to send the manual confirmation alert below
                else:
                    await context.bot.send_message(chat_id, "🚨 **Auto-Hedge Triggered!** Executing trade...")
                    await execute_hedge_logic(context, chat_id, hedge_contracts_to_trade, config['asset'])
                    continue # Move to the next user
            
            # --- Send Manual Alert if auto_hedge is OFF or if a large trade was detected ---
            message = (
                f"🚨 **Delta Risk Alert: {config['asset']}** 🚨\n\n"
                f"Your **net portfolio delta** of `${net_portfolio_delta_usd:,.2f}` has exceeded your threshold of `${config['delta_threshold']:,.2f}`.\n\n"
                f"**Recommended Rebalancing Trade:**\nShort `{abs(hedge_contracts_to_trade):.4f}` of `{config['perp_symbol']}`."
            )
            keyboard = [
                [InlineKeyboardButton("✅ Hedge Now (Simulated)", callback_data=f"hedge_now_{config['asset']}_{hedge_contracts_to_trade:.4f}")],
                [
                    InlineKeyboardButton("📊 View Analytics", callback_data="view_analytics"),
                    InlineKeyboardButton("⚙️ Adjust Thresholds", callback_data="adjust_thresholds_prompt")
                ],
                [InlineKeyboardButton("Dismiss", callback_data="dismiss_alert")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await context.bot.send_message(chat_id, text=message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

# --- UPDATE BUTTON HANDLER ---
async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Parses all non-conversation inline button clicks."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press immediately
    data = query.data
    chat_id = query.message.chat.id

    if data.startswith("stress_"):
        await handle_stress_test_callback(update, context)
        return
    
    if data.startswith("export_"):
        await handle_export_callback(update, context)
        return

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
    """
    Sets up monitoring for a user. This command clears any previous state
    and initializes a new portfolio with the specified spot position.
    """
    chat_id = update.effective_chat.id
    try:
        # Example usage: /monitor_risk BTC 1.5 500 [VAR_THRESHOLD]
        args = context.args
        if len(args) < 3:
            raise ValueError("Not enough arguments")

        asset = args[0].upper()
        size = float(args[1])
        delta_threshold = float(args[2])
        var_threshold = float(args[3]) if len(args) > 3 else None
        
        # --- 1. First, clear any pre-existing portfolio state for this user ---
        db_manager.clear_holdings(chat_id)
        log.info(f"Cleared existing holdings for chat_id: {chat_id} before starting new monitoring.")

        # --- 2. Set up the new monitoring configuration in the 'positions' table ---
        position_data = {
            "chat_id": chat_id,
            "asset": asset,
            "spot_symbol": f"{asset}/USDT",
            "perp_symbol": f"{asset}/USDT:USDT",
            "size": size,
            "delta_threshold": delta_threshold,
            "var_threshold": var_threshold,
        }
        db_manager.upsert_position(chat_id, position_data)

        # --- 3. Add the initial spot position to the new 'portfolio_holdings' state table ---
        db_manager.upsert_holding(
            chat_id=chat_id,
            symbol=position_data['spot_symbol'],
            asset_type='spot',
            quantity_change=position_data['size']
        )
        
        await update.message.reply_text(
            "✅ **Monitoring Enabled & Portfolio Reset**\n\n"
            "Your portfolio state has been initialized with your spot position. "
            "Use `/hedge_status` to see your settings.",
            parse_mode=ParseMode.MARKDOWN
        )

    except (IndexError, ValueError) as e:
        log.warning(f"Invalid monitor_risk command from {chat_id}: {e}")
        await update.message.reply_text(
            "❌ **Invalid Format.**\n"
            "Usage: `/monitor_risk <ASSET> <SIZE> <DELTA_THRESHOLD_USD> [VAR_THRESHOLD_USD]`\n"
            "Example: `/monitor_risk BTC 1.5 500 2000`",
            parse_mode=ParseMode.MARKDOWN
        )

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

async def set_large_trade_limit_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    position = db_manager.get_position(chat_id)
    if not position:
        await update.message.reply_text("❌ Please set up a position with `/monitor_risk` first.")
        return
    try:
        limit_str = context.args[0]
        if limit_str.lower() == 'off':
            limit = None
            message = "✅ Large trade limit has been removed."
        else:
            limit = float(limit_str)
            if limit <= 0: raise ValueError()
            message = f"✅ Large trade limit set to `${limit:,.2f}`."
        
        position['large_trade_threshold'] = limit
        db_manager.upsert_position(chat_id, position)
        await update.message.reply_text(message)
    except (IndexError, ValueError):
        await update.message.reply_text("❌ Usage: `/set_large_trade_limit <USD_VALUE>` or `/set_large_trade_limit off`")

async def select_put_strike(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Saves the put strike and asks for the call strike."""
    query = update.callback_query
    await query.answer()
    
    put_strike = int(query.data.split('_')[1])
    context.user_data['put_strike'] = put_strike

    await query.edit_message_text("Put strike selected. Now, fetching valid strikes for the **Covered Call**.")

    # Fetch all BTC option instruments for the selected expiry
    expiry = context.user_data['expiry']
    instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
    if not instruments:
        await query.edit_message_text("❌ Could not fetch options data from Deribit. Please try again later.")
        return ConversationHandler.END

    # Filter for CALLS with the same expiry and strike > put_strike
    call_strikes = []
    for i in instruments:
        parts = i.split('-')
        if parts[1] == expiry and parts[3] == 'C':
            strike = int(parts[2])
            if strike > put_strike:
                call_strikes.append(strike)

    if not call_strikes:
        await query.edit_message_text("❌ No valid call strikes found above your selected put strike. Please try a different put strike.")
        return ConversationHandler.END

    strikes = sorted(call_strikes)
    # Show up to 5 strikes above the put strike for user convenience
    display_strikes = strikes[:5]

    keyboard = [[InlineKeyboardButton(f"Call Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])

    await query.edit_message_text(
        f"Please select a **Call Strike Price** (must be > ${put_strike:,})",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SELECT_STRIKE  # Both collar and single-leg paths now converge here

async def export_data_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Provides buttons to export user data as CSV files."""
    keyboard = [
        [InlineKeyboardButton("Export My Settings Report", callback_data="export_settings")],
        [InlineKeyboardButton("Export My Trade History", callback_data="export_history")],
    ]
    await update.message.reply_text(
        "**Data Export & Reporting**\n\n"
        "Please choose a report to generate. The data will be sent as a CSV file, which can be used for compliance or personal record-keeping.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the button clicks for data export."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Generating your report...")

    chat_id = query.message.chat.id
    
    if query.data == 'export_settings':
        csv_buffer = reporting_manager.generate_position_report_csv(chat_id)
        filename = f"position_report_{chat_id}.csv"
        caption = "Your current risk configuration and settings."
    elif query.data == 'export_history':
        csv_buffer = reporting_manager.generate_trade_history_csv(chat_id)
        filename = f"trade_history_{chat_id}.csv"
        caption = "A complete ledger of your simulated hedge trades."
    else:
        return

    if csv_buffer:
        # We need to create an InputFile object for Telegram
        input_file = InputFile(csv_buffer, filename=filename)
        await context.bot.send_document(chat_id=chat_id, document=input_file, caption=caption)
    else:
        await context.bot.send_message(chat_id, "ℹ️ No data found to generate the report.")
        
async def select_buy_put(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the first leg of the Iron Condor: Buying a Put."""
    query = update.callback_query
    await query.answer()
    
    strike = int(query.data.split('_')[1])
    context.user_data['buy_put_strike'] = strike
    
    await query.edit_message_text("Fetching valid strikes for the short put...")
    
    expiry = context.user_data['expiry']
    instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
    
    # Filter for put strikes that are HIGHER than the one we just bought
    relevant_strikes = sorted([
        int(i.split('-')[2]) for i in instruments 
        if i.split('-')[1] == expiry and i.split('-')[3] == 'P' and int(i.split('-')[2]) > strike
    ])

    # Suggest a few relevant strikes
    display_strikes = relevant_strikes[:5] # Show the next 5 available strikes
    keyboard = [[InlineKeyboardButton(f"Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])

    await query.edit_message_text(f"✅ Step 1/4: Long Put @ ${strike:,}\n\nStep 2/4: Select the **short Put** strike.", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_SELL_PUT

async def select_buy_call(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the final leg, calculates the full strategy, and shows confirmation."""
    query = update.callback_query
    await query.answer()
    
    context.user_data['buy_call_strike'] = int(query.data.split('_')[1])
    
    await query.edit_message_text("Calculating all 4 legs of the Iron Condor...")
    use_ml_vol = context.user_data.get('use_ml_vol', False)
    
    # --- Full Iron Condor Calculation Logic ---
    try:
        ud = context.user_data
        expiry = ud['expiry']
        asset = 'BTC'
        
        # 1. Construct all 4 instrument names
        names = {
            'buy_put': f"{asset}-{expiry}-{ud['buy_put_strike']}-P",
            'sell_put': f"{asset}-{expiry}-{ud['sell_put_strike']}-P",
            'sell_call': f"{asset}-{expiry}-{ud['sell_call_strike']}-C",
            'buy_call': f"{asset}-{expiry}-{ud['buy_call_strike']}-C",
        }
        
        # 2. Fetch all data concurrently
        btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
        tickers = await asyncio.gather(*[data_fetcher_instance.fetch_option_ticker(n) for n in names.values()])
        
        # 3. Calculate all greeks concurrently
        greeks_tasks = [risk_engine_instance.calculate_option_greeks(btc_price, t, use_ml_vol=use_ml_vol) for t in tickers]
        all_greeks = await asyncio.gather(*greeks_tasks)
        
        # 4. Aggregate results
        # Iron Condor: Buy Put, Sell Put, Sell Call, Buy Call (in that order)
        # Net premium: -buy_put + sell_put + sell_call - buy_call
        net_premium = (-all_greeks[0]['price'] + all_greeks[1]['price'] + all_greeks[2]['price'] - all_greeks[3]['price'])
        net_delta = (-all_greeks[0]['delta'] + all_greeks[1]['delta'] + all_greeks[2]['delta'] - all_greeks[3]['delta'])
        net_gamma = (-all_greeks[0]['gamma'] + all_greeks[1]['gamma'] + all_greeks[2]['gamma'] - all_greeks[3]['gamma'])
        net_vega = (-all_greeks[0]['vega'] + all_greeks[1]['vega'] + all_greeks[2]['vega'] - all_greeks[3]['vega'])
        net_theta = (-all_greeks[0]['theta'] + all_greeks[1]['theta'] + all_greeks[2]['theta'] - all_greeks[3]['theta'])

        message = (
            f"✅ **Iron Condor Confirmation**\n\n"
            f"This is a range-bound strategy that profits if BTC stays between your short strikes.\n\n"
            f"**Legs:**\n"
            f"🔹 `BUY  {names['buy_put']}`\n"
            f"🔹 `SELL {names['sell_put']}`\n"
            f"🔹 `SELL {names['sell_call']}`\n"
            f"🔹 `BUY  {names['buy_call']}`\n\n"
            f"--- **Strategy Profile** ---\n"
            f"**Net Premium Received:** `${net_premium:,.2f}`\n"
            f"**Net Delta:** `{net_delta:.4f}` (Should be near zero)\n\n"
            f"This action will open a 4-leg options position."
        )
        keyboard = [
            [InlineKeyboardButton("Confirm (Simulated)", callback_data="confirm_hedge")],
            [InlineKeyboardButton("Cancel", callback_data="cancel")]
        ]
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        log.error(f"Error calculating Iron Condor: {e}")
        await query.edit_message_text("❌ An error occurred while calculating the strategy. Please try again.")
        return ConversationHandler.END

    return CONFIRM_CONDOR # Use a unified confirmation state

async def select_sell_call(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the third leg of the Iron Condor: Selling a Call."""
    query = update.callback_query
    await query.answer()
    
    strike = int(query.data.split('_')[1])
    context.user_data['sell_call_strike'] = strike
    
    await query.edit_message_text("Fetching valid strikes for the long call...")
    
    expiry = context.user_data['expiry']
    instruments = await data_fetcher_instance.fetch_option_instruments('BTC')

    # Filter for call strikes that are HIGHER than the one we just sold
    relevant_strikes = sorted([
        int(i.split('-')[2]) for i in instruments 
        if i.split('-')[1] == expiry and i.split('-')[3] == 'C' and int(i.split('-')[2]) > strike
    ])
    
    display_strikes = relevant_strikes[:5]
    keyboard = [[InlineKeyboardButton(f"Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])

    await query.edit_message_text(f"✅ Step 3/4: Short Call @ ${strike:,}\n\nStep 4/4: Select the **long Call** strike.", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_BUY_CALL


async def select_sell_put(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the second leg of the Iron Condor: Selling a Put."""
    query = update.callback_query
    await query.answer()
    
    strike = int(query.data.split('_')[1])
    context.user_data['sell_put_strike'] = strike
    
    await query.edit_message_text("Fetching valid strikes for the short call...")
    
    expiry = context.user_data['expiry']
    btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    instruments = await data_fetcher_instance.fetch_option_instruments('BTC')
    
    # Filter for call strikes that are out-of-the-money (higher than current price)
    relevant_strikes = sorted([
        int(i.split('-')[2]) for i in instruments 
        if i.split('-')[1] == expiry and i.split('-')[3] == 'C' and int(i.split('-')[2]) > btc_price
    ])
    
    display_strikes = relevant_strikes[:5]
    keyboard = [[InlineKeyboardButton(f"Strike: ${s:,.0f}", callback_data=f"strike_{s}")] for s in display_strikes]
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="cancel")])

    await query.edit_message_text(f"✅ Step 2/4: Short Put @ ${strike:,}\n\nStep 3/4: Select the **short Call** strike.", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_SELL_CALL


async def stress_test_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Starts the interactive stress test by sending scenario buttons."""
    keyboard = [
        [InlineKeyboardButton("Scenario: Market Crash (-20%)", callback_data="stress_crash")],
        [InlineKeyboardButton("Scenario: Volatility Spike (+50%)", callback_data="stress_vol_spike")],
    ]
    await update.message.reply_text(
        "Please choose a stress test scenario for your current portfolio:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_stress_test_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the button clicks from the stress test command and sends the report."""
    query = update.callback_query
    await query.answer()
    
    chat_id = query.message.chat.id
    position = db_manager.get_position(chat_id)
    if not position:
        await query.edit_message_text("❌ No position found. Please set one up with /monitor_risk first.")
        return
        
    await query.edit_message_text("Running stress test scenario...")
    
    # We assume a simple portfolio for the live bot's stress test
    portfolio = [{'type': 'spot', 'asset': position['asset'], 'size': position['size']}]
    btc_price = await data_fetcher_instance.get_price('bybit', 'BTC/USDT')
    if not btc_price:
        await query.edit_message_text("❌ Could not fetch live price data to run the test.")
        return

    prices = {'BTC/USDT': btc_price}

    scenario = {}
    if query.data == 'stress_crash':
        scenario = {'name': 'Market Crash (-20% Price)', 'price_change_pct': -0.20}
    elif query.data == 'stress_vol_spike':
        scenario = {'name': 'Volatility Spike', 'iv_change_pct': 0.50}

    result = await risk_engine_instance.run_stress_test(portfolio, prices, scenario)

    report_text = (
        f"**🔬 Stress Test Result**\n\n"
        f"**Scenario:** `{result['scenario_name']}`\n"
        f"**Estimated P&L Impact:** `${result['stressed_pnl']:,.2f}`\n\n"
        f"This is an approximation based on your portfolio's current risk profile."
    )
    await query.edit_message_text(report_text, parse_mode=ParseMode.MARKDOWN)

async def export_data_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Provides buttons to export user data as CSV files."""
    keyboard = [
        [InlineKeyboardButton("Export My Settings Report", callback_data="export_settings")],
        [InlineKeyboardButton("Export My Trade History", callback_data="export_history")],
    ]
    await update.message.reply_text(
        "**Data Export & Reporting**\n\n"
        "Please choose a report to generate. The data will be sent as a CSV file.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def ml_mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggles whether to use ML volatility forecasts."""
    chat_id = update.effective_chat.id
    # We store the user's choice in context.user_data (temporary per session)
    # A more permanent solution would be a new column in the database.
    
    current_mode = context.user_data.get('use_ml_vol', False)
    new_mode = not current_mode
    context.user_data['use_ml_vol'] = new_mode
    
    status = "ENABLED" if new_mode else "DISABLED"
    message = (
        f"🧠 **ML Volatility Forecasting is now {status}.**\n\n"
        f"When enabled, all options calculations will use our GARCH model's "
        f"forecast instead of the market's implied volatility."
    )
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)