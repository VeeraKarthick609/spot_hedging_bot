import numpy as np
import pandas as pd
import logging
import time
import asyncio
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta
from datetime import datetime

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
        Calculates aggregated risk metrics for an entire portfolio.
        
        :param portfolio: A list of position dicts, e.g., 
                          [{'type': 'spot', 'asset': 'BTC', 'size': 1.5}, 
                           {'type': 'option', 'symbol': 'BTC-29NOV24-70000-P', 'size': -2}]
        :param prices: A dict of current prices, e.g., {'BTC/USDT': 60000}
        :return: A dict with aggregated greeks.
        """
        total_delta_usd = 0
        # In a full implementation, you would aggregate all greeks
        
        for position in portfolio:
            if position['type'] == 'spot':
                price = prices.get(f"{position['asset']}/USDT")
                if price:
                    total_delta_usd += position['size'] * price
            
            elif position['type'] == 'perp':
                # Assuming linear perps for simplicity
                price = prices.get(f"{position['asset']}/USDT:USDT") # Bybit symbol format
                if price:
                    total_delta_usd += position['size'] * price

            elif position['type'] == 'option':
                btc_price = prices.get('BTC/USDT')
                option_ticker = await data_fetcher_instance.fetch_option_ticker(position['symbol'])
                if btc_price and option_ticker:
                    greeks = self.calculate_option_greeks(btc_price, option_ticker)
                    if greeks:
                        # Delta of one option contract * number of contracts * price of underlying
                        total_delta_usd += greeks['delta'] * position['size'] * btc_price
        
        return {"total_delta_usd": total_delta_usd}
    
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

# Create a single instance
risk_engine_instance = RiskEngine()