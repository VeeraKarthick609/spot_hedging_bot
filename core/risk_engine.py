import numpy as np
import pandas as pd
import logging
import time
import asyncio
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta
from datetime import datetime
import io
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from typing import List, Dict
# matplotlib.use('Agg') # Use non-interactive backend for matplotlib


# Import our real data source
from services.data_fetcher import data_fetcher_instance

log = logging.getLogger(__name__)

class RiskEngine:
    def __init__(self):
        # Cache for storing calculated beta values to avoid excessive API calls
        # Format: {"BTC/USDT:BTC/USDT:USDT": {"beta": 1.01, "timestamp": 167...}}
        self.beta_cache = {}
        self.cache_duration_seconds = 4 * 60 * 60  # Cache beta for 4 hours
        log.info("RiskEngine initialized with caching enabled.")

    async def calculate_beta(self, spot_symbol: str, perp_symbol: str, exchange: str = 'bybit') -> float | None:
        """
        Calculates the hedge ratio (beta) using real historical data from the exchange.
        Uses a cache to avoid re-calculating on every run.
        """
        cache_key = f"{exchange}:{spot_symbol}:{perp_symbol}"
        
        # 1. Check cache first
        if cache_key in self.beta_cache:
            cached_data = self.beta_cache[cache_key]
            if time.time() - cached_data['timestamp'] < self.cache_duration_seconds:
                log.info(f"Using cached beta for {cache_key}: {cached_data['beta']:.4f}")
                return cached_data['beta']

        log.info(f"Cache miss or expired for {cache_key}. Fetching fresh historical data...")

        # 2. Fetch historical data for both instruments concurrently
        try:
            spot_task = data_fetcher_instance.fetch_historical_data(exchange, spot_symbol, timeframe='1d', limit=90)
            perp_task = data_fetcher_instance.fetch_historical_data(exchange, perp_symbol, timeframe='1d', limit=90)
            
            spot_df, perp_df = await asyncio.gather(spot_task, perp_task)

            if spot_df is None or perp_df is None:
                log.error("Failed to fetch historical data for one or both symbols.")
                return None # Cannot calculate beta
        except Exception as e:
            log.error(f"Error during concurrent fetch for beta calculation: {e}")
            return None

        # 3. Align data and calculate beta
        # Merge dataframes on timestamp to ensure we are comparing the same time periods
        merged_df = pd.merge(spot_df[['timestamp', 'close']], perp_df[['timestamp', 'close']], on='timestamp', suffixes=('_spot', '_perp'))
        if len(merged_df) < 30: # Need enough data points for a meaningful calculation
            log.warning("Not enough overlapping historical data to calculate beta accurately.")
            return 1.0 # Default to 1.0 if data is sparse

        # Calculate daily returns
        returns = merged_df[['close_spot', 'close_perp']].pct_change().dropna()
        
        # Calculate covariance matrix
        covariance_matrix = returns.cov()
        
        # Beta = Cov(Spot, Perp) / Var(Perp)
        beta = covariance_matrix.loc['close_spot', 'close_perp'] / returns['close_perp'].var()
        
        log.info(f"Calculated new beta for {cache_key}: {beta:.4f}")

        # 4. Update cache
        self.beta_cache[cache_key] = {'beta': beta, 'timestamp': time.time()}

        return beta

    def calculate_perp_hedge(self, spot_position_usd: float, perp_price: float, beta: float) -> dict:
        """
        Calculates the required hedge for a spot position to become delta-neutral.
        (This function's internal logic remains the same, but it now relies on a real beta)
        """
        spot_delta_usd = spot_position_usd
        required_hedge_usd = -spot_delta_usd * beta
        required_hedge_contracts = required_hedge_usd / perp_price
        
        log.info(f"Hedge calculation: spot_delta_usd=${spot_delta_usd:,.2f}, beta={beta:.4f}, required_hedge_contracts={required_hedge_contracts:.4f}")

        return {
            "spot_delta_usd": spot_delta_usd,
            "required_hedge_usd": required_hedge_usd,
            "required_hedge_contracts": required_hedge_contracts
        }
    
    def calculate_option_greeks(self, underlying_price: float, option_ticker: dict) -> dict | None:
        """
        Calculates the greeks for a single option using its live ticker data.
        
        :param underlying_price: Current price of the asset (e.g., BTC).
        :param option_ticker: The full ticker dictionary from data_fetcher.fetch_option_ticker.
        :return: A dictionary containing the calculated greeks.
        """
        try:
            # Get greeks from the correct path in the data structure
            greeks_data = option_ticker.get("info", {}).get("greeks", {})
            
            return {
                "delta": float(greeks_data.get('delta', 0)),  # Delta per $1 price change (already correct scale)
                "gamma": float(greeks_data.get('gamma', 0)),  # Gamma per $1 price change (already correct scale)
                "vega": float(greeks_data.get("vega", 0)),    # Vega per 1% vol change (already correct scale)
                "theta": float(greeks_data.get("theta", 0)),  # Theta per day (already correct scale)
                "price": float(option_ticker.get('mark_price') or option_ticker.get('info', {}).get('mark_price', 0)) * float(underlying_price),  # Deribit price is in BTC, convert to USD
            }
        except Exception as e:
            log.error(f"Error calculating greeks for {option_ticker.get('info', {}).get('instrument_name', 'N/A')}: {e}")
            return None
        
    async def calculate_portfolio_risk(self, portfolio: list, prices: dict) -> dict:
        """
        Calculates a WIDE RANGE of aggregated risk metrics for an entire portfolio.
        """
        # Initialize aggregators
        total_delta_usd = 0.0
        total_gamma_usd = 0.0  # Gamma is measured in delta change per $1 move in underlying
        total_vega_usd = 0.0   # Vega is measured in USD value change per 1% move in IV
        total_theta_usd = 0.0  # Theta is measured in USD value decay per day
        
        btc_price = prices.get('BTC/USDT', 0)
        if btc_price == 0: return {}

        for position in portfolio:
            pos_type = position['type']
            size = position.get('size', 0)
            asset = position.get('asset', '')

            if pos_type == 'spot':
                total_delta_usd += size * btc_price
            
            elif pos_type == 'perp':
                total_delta_usd += size * btc_price # Assuming 1x beta for simplicity in this report

            elif pos_type == 'option':
                option_ticker = await data_fetcher_instance.fetch_option_ticker(position['symbol'])
                if option_ticker:
                    greeks = self.calculate_option_greeks(btc_price, option_ticker)
                    if greeks:
                        # Convert Greek units to portfolio-level USD values
                        total_delta_usd += size * greeks['delta'] * btc_price
                        # Gamma Value: 0.5 * Gamma * (S * 1%)^2. We simplify to show exposure.
                        total_gamma_usd += size * greeks['gamma'] * btc_price 
                        total_vega_usd += size * greeks['vega'] # Vega is already in $/1% change
                        total_theta_usd += size * greeks['theta'] # Theta is already in $/day
        
        return {
            "total_delta_usd": total_delta_usd,
            "total_gamma_usd": total_gamma_usd,
            "total_vega_usd": total_vega_usd,
            "total_theta_usd": total_theta_usd,
        }
    
    async def calculate_historical_var(self, portfolio: list, prices: dict, days: int = 90, confidence_level: float = 0.95) -> float | None:
        """
        Calculates 1-day Value at Risk (VaR) using Historical Simulation.
        """
        log.info(f"Calculating {confidence_level:.0%} VaR over {days} days...")
        # 1. Get total portfolio value
        total_value = 0
        for pos in portfolio:
            # A more robust version would value options and perps correctly
            if pos['type'] == 'spot':
                total_value += pos['size'] * prices.get(f"{pos['asset']}/USDT", 0)

        if total_value == 0: return 0.0

        # 2. Fetch historical data for the main asset (BTC for now)
        hist_df = await data_fetcher_instance.fetch_historical_data('bybit', 'BTC/USDT', '1d', limit=days)
        if hist_df is None or hist_df.empty:
            log.error("Could not fetch historical data for VaR calculation.")
            return None
        
        # 3. Calculate historical daily returns
        hist_df['returns'] = hist_df['close'].pct_change().dropna()
        
        # 4. Simulate portfolio P&L
        simulated_pnl = hist_df['returns'] * total_value
        
        # 5. Find the VaR at the specified confidence level
        var_value = simulated_pnl.quantile(1 - confidence_level)
        log.info(f"Calculated 1-Day 95% VaR: ${var_value:,.2f}")
        return var_value
    
    def estimate_slippage_and_cost(self, order_book: dict, size: float, side: str) -> dict:
        """
        Estimates the average fill price and slippage for a given order size by walking the order book.
        
        :param order_book: The order book dictionary from ccxt.
        :param size: The number of contracts/coins to trade.
        :param side: 'buy' or 'sell'.
        :return: A dict with cost analysis.
        """
        book_side = order_book['asks'] if side == 'buy' else order_book['bids']
        
        remaining_size = abs(size)
        total_cost = 0
        
        for price, volume in book_side:
            if remaining_size <= 0: break
            
            trade_volume = min(remaining_size, volume)
            total_cost += trade_volume * price
            remaining_size -= trade_volume
            
        if remaining_size > 0:
            log.warning("Order size is larger than available liquidity in the fetched order book.")
            # Assume the rest fills at the last price for estimation
            total_cost += remaining_size * book_side[-1][0]
        
        avg_fill_price = total_cost / abs(size)
        mid_price = (order_book['asks'][0][0] + order_book['bids'][0][0]) / 2
        slippage_usd = (avg_fill_price - mid_price) * abs(size)
        
        return {
            "avg_fill_price": avg_fill_price,
            "total_cost_usd": total_cost,
            "slippage_usd": slippage_usd
        }
    
    async def find_best_execution_venue(self, symbol: str, size: float) -> dict | None:
        """
        Compares execution costs across multiple exchanges (Bybit, OKX) and finds the best one.
        
        :param symbol: The symbol for the perpetual contract (e.g., 'BTC/USDT:USDT').
        :param size: The trade size (negative for short, positive for long).
        :return: A dict with the best venue and cost analysis.
        """
        side = 'buy' if size > 0 else 'sell'
        venues = ['bybit', 'okx']
        results = []

        # Convert Bybit symbol to OKX symbol format
        okx_symbol = symbol.replace('/USDT:USDT', '-USDT-SWAP')

        symbols = {'bybit': symbol, 'okx': okx_symbol}

        for venue in venues:
            book = await data_fetcher_instance.fetch_order_book(venue, symbols[venue])
            if book:
                # Mock fees, a real app would fetch these from the exchange
                fee_rate = 0.00055 # Example taker fee
                cost_analysis = self.estimate_slippage_and_cost(book, size, side)
                cost_analysis['fees_usd'] = abs(cost_analysis['total_cost_usd'] * fee_rate)
                cost_analysis['total_final_cost'] = cost_analysis['total_cost_usd'] + cost_analysis['fees_usd'] if side == 'buy' else cost_analysis['total_cost_usd'] - cost_analysis['fees_usd']
                cost_analysis['venue'] = venue
                results.append(cost_analysis)

        if not results:
            log.error("Could not get execution analysis from any venue.")
            return None

        # Return the venue with the best final price (highest for sell, lowest for buy)
        best_venue = min(results, key=lambda x: x['total_final_cost']) if side == 'buy' else max(results, key=lambda x: x['total_final_cost'])
        log.info(f"Best execution venue found: {best_venue['venue'].upper()} with estimated average price {best_venue['avg_fill_price']:.2f}")
        return best_venue
    
    async def run_stress_test(self, portfolio: list, prices: dict, scenario: dict) -> dict:
        """Calculates the P&L of a portfolio under a given market shock scenario."""
        # 1. Calculate the portfolio's current value
        initial_value = 0
        # A full implementation would require a dedicated `calculate_portfolio_value` function.
        # For simplicity, we'll use the delta as a proxy for value.
        risk_data = await self.calculate_portfolio_risk(portfolio, prices)
        initial_value = risk_data['total_delta_usd'] # Simplified approximation

        # 2. Define the stressed market conditions
        stressed_prices = prices.copy()
        stressed_prices['BTC/USDT'] *= (1 + scenario.get('price_change_pct', 0))
        
        # 3. Calculate the portfolio's value under stress
        # This is a simplified calculation. A full version would re-price every option.
        # New Delta = Old Delta + Gamma * dS + ...
        # For a quick estimate, we can use the greeks.
        dS = stressed_prices['BTC/USDT'] - prices['BTC/USDT']
        
        # P&L from Delta and Gamma are the main drivers of a stress test
        pnl_from_delta = risk_data['total_delta_usd'] * scenario.get('price_change_pct', 0)
        pnl_from_gamma = 0.5 * risk_data['total_gamma_usd'] * dS * scenario.get('price_change_pct', 0)
        
        stressed_pnl = pnl_from_delta + pnl_from_gamma

        return {
            "initial_value": initial_value,
            "stressed_pnl": stressed_pnl,
            "scenario_name": scenario['name']
        }
    
    def generate_hedge_history_chart(self, history_data: List[Dict]) -> io.BytesIO | None:
        """
        Generates a professional, themed PNG chart of hedge history with enhanced styling
        and informative elements, returns as an in-memory byte buffer for Telegram.
        """
        if not history_data:
            return None

        # 1. --- Data Preparation ---
        df = pd.DataFrame(history_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Calculate cumulative position and additional metrics
        df['net_hedge_position'] = df['size'].cumsum()
        df['position_change'] = df['size']
        
        # Calculate some summary statistics
        max_position = df['net_hedge_position'].max()
        min_position = df['net_hedge_position'].min()
        current_position = df['net_hedge_position'].iloc[-1]
        total_volume = df['size'].abs().sum()

        # 2. --- Enhanced Theming and Styling ---
        plt.style.use('dark_background')
        
        # Create figure with custom styling
        fig, ax = plt.subplots(figsize=(12, 8), facecolor='#0f1419')
        ax.set_facecolor('#0f1419')

        # 3. --- Enhanced Plotting ---
        # Main line with gradient-like effect
        line = ax.plot(
            df['timestamp'], df['net_hedge_position'], 
            linewidth=3, color='#00d4ff', alpha=0.9,
            label='Net Hedge Position'
        )[0]
        
        # Add markers for individual hedge actions with better visibility
        # Positive changes (long hedges) in green, negative (short hedges) in red
        for i, row in df.iterrows():
            if row['position_change'] > 0:
                color = '#00ff88'
                marker = '^'  # Up arrow for long positions
            else:
                color = '#ff4444'
                marker = 'v'  # Down arrow for short positions
            
            size = min(abs(row['position_change']) * 30 + 80, 200)  # Better size scaling
            ax.scatter(row['timestamp'], row['net_hedge_position'], 
                    c=color, s=size, alpha=0.8, marker=marker,
                    edgecolors='white', linewidth=2, zorder=5)

        # Add area fill under the line for better visual impact
        ax.fill_between(df['timestamp'], df['net_hedge_position'], 
                    alpha=0.2, color='#00d4ff')

        # Add zero line for reference
        ax.axhline(y=0, color='#666666', linestyle='--', alpha=0.5, linewidth=1)

        # 4. --- Enhanced Labels and Title ---
        # Multi-line title with current position
        title_text = f'Net Hedge Position Over Time\nCurrent: {current_position:,.0f} contracts'
        ax.set_title(title_text, color='white', fontsize=16, pad=25, 
                    fontweight='bold', linespacing=1.2)
        
        ax.set_xlabel('Date & Time (UTC)', color='#cccccc', fontsize=12, fontweight='bold')
        ax.set_ylabel('Net Position (Contracts)', color='#cccccc', fontsize=12, fontweight='bold')

        # 5. --- Advanced Grid and Styling ---
        # Multi-level grid system
        ax.grid(True, linestyle='-', alpha=0.1, color='white')
        ax.grid(True, linestyle='--', alpha=0.05, color='white', which='minor')
        
        # Customize spines
        for spine in ax.spines.values():
            spine.set_edgecolor('#333333')
            spine.set_linewidth(1.5)

        # 6. --- Enhanced Date Formatting ---
        # Better date formatting based on time range
        time_range = df['timestamp'].max() - df['timestamp'].min()
        if time_range.days > 7:
            date_format = mdates.DateFormatter('%m-%d')
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        elif time_range.days > 1:
            date_format = mdates.DateFormatter('%m-%d\n%H:%M')
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        else:
            date_format = mdates.DateFormatter('%H:%M')
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        
        ax.xaxis.set_major_formatter(date_format)
        
        # Rotate and style tick labels
        ax.tick_params(axis='x', colors='#ffffff', rotation=0, labelsize=10)
        ax.tick_params(axis='y', colors='#ffffff', labelsize=10)

        # 7. --- Add Summary Statistics Box ---
        # Create a text box with key statistics
        stats_text = f"""Position Summary:
            Current: {current_position:,.0f}
            Max: {max_position:,.0f}
            Min: {min_position:,.0f}
            Total Volume: {total_volume:,.0f}
            Total Trades: {len(df):,}"""
        
        # Position the text box in the upper right corner
        ax.text(0.98, 0.98, stats_text, transform=ax.transAxes, 
                verticalalignment='top', horizontalalignment='right',
                bbox=dict(boxstyle='round,pad=0.8', facecolor='#1a1a1a', 
                        edgecolor='#00d4ff', alpha=0.9, linewidth=1.5),
                fontsize=10, color='#ffffff', fontfamily='monospace')

        # 8. --- Add Legend ---
        # Create custom legend elements
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], color='#00d4ff', linewidth=3, label='Net Position'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='#00ff88', 
                markersize=8, label='Long Hedge', linestyle='None'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='#ff4444', 
                markersize=8, label='Short Hedge', linestyle='None')
        ]
        
        ax.legend(handles=legend_elements, loc='upper left', 
                facecolor='#1a1a1a', edgecolor='#00d4ff', 
                labelcolor='#ffffff', fontsize=10, framealpha=0.9)

        # 9. --- Final Touches ---
        # Adjust layout to prevent clipping
        plt.tight_layout()
        
        # Add subtle border around the entire plot
        fig.patch.set_edgecolor('#333333')
        fig.patch.set_linewidth(2)

        # 10. --- Save with High Quality ---
        buf = io.BytesIO()
        fig.savefig(buf, format='png', transparent=False, dpi=150, 
                    bbox_inches='tight', facecolor='#0f1419', 
                    edgecolor='#333333', pad_inches=0.2)
        buf.seek(0)
        
        plt.close(fig)
        return buf

# Create a single instance
risk_engine_instance = RiskEngine()