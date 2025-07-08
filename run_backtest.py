import pandas as pd
from backtest.backtester import Backtester

def main():
    # --- 1. Load Data ---
    print("Loading historical data...")
    try:
        spot_data = pd.read_csv("./data/BTC_USDT_1h.csv", parse_dates=['timestamp'])
        perp_data = pd.read_csv("./data/BTC_USDT_USDT_1h.csv", parse_dates=['timestamp'])
    except FileNotFoundError:
        print("Error: Data files not found. Please run 'scripts/download_data.py' first.")
        return

    # --- 2. Define Strategy Configuration ---
    # This is where you can experiment with different parameters
    strategy_config = {
        'initial_capital': 100000.0,      # Starting cash
        'initial_spot_holding': 1.0,      # e.g., We start by buying 1 BTC
        'delta_threshold': 500.0,         # Hedge if our exposure exceeds $500
    }

    # --- 3. Initialize and Run the Backtester ---
    backtester = Backtester(spot_data, perp_data, strategy_config)
    backtester.run()

if __name__ == "__main__":
    main()