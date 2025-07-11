import pandas as pd
from backtest.backtester import Backtester

def main():
    # --- 1. Load Data ---
    print("Loading historical data...")
    try:
        spot_data = pd.read_csv("./data/BTC_USDT_1d.csv", parse_dates=['timestamp'])
        perp_data = pd.read_csv("./data/BTC_USDT_USDT_1d.csv", parse_dates=['timestamp'])
    except FileNotFoundError:
        print("Error: Data files not found. Please run 'scripts/download_data.py' first.")
        return

    # --- 2. Define Strategy Configuration ---
    # This is where you can experiment with different parameters
    strategy_config = {
        'initial_capital': 100000.0,
        'initial_spot_holding': 1.0,
        
        # --- NEW DYNAMIC PARAMETERS ---
        # 'hedge_ratio': 1.0 means full hedge. 0.5 means hedge 50% of the delta.
        'hedge_ratio': 0.6, 
        
        # 'delta_threshold': We still need a trigger to avoid constant, tiny trades.
        'delta_threshold': 1000.0,
        
        # 'regime_filter': Use a simple moving average crossover to detect market trend.
        # If True, only hedge when the market is in a "downtrend".
        'use_regime_filter': True,
        'fast_ma': 50,  # 50-hour moving average
        'slow_ma': 200, # 200-hour moving average
    }

    # --- 3. Initialize and Run the Backtester ---
    backtester = Backtester(spot_data, perp_data, strategy_config)
    backtester.run()

if __name__ == "__main__":
    main()