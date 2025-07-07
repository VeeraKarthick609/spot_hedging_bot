import numpy as np
import pandas as pd
import logging
import time
import asyncio

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

# Create a single instance
risk_engine_instance = RiskEngine()