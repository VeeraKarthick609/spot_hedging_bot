import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .portfolio import SimulatedPortfolio
from .execution import SimulatedExecutionHandler
from core.risk_engine import RiskEngine # Use our existing risk engine!

class Backtester:
    def __init__(self, spot_data, perp_data, strategy_config):
        self.spot_data = spot_data
        self.perp_data = perp_data
        self.strategy = strategy_config
        
        self.portfolio = SimulatedPortfolio(self.strategy['initial_capital'])
        self.execution_handler = SimulatedExecutionHandler()
        self.risk_engine = RiskEngine() # The brain remains the same

    def run(self):
        """Runs the backtest from start to finish."""
        print("Starting backtest...")
        
        # Merge data to align timestamps
        data = pd.merge(self.spot_data, self.perp_data, on='timestamp', suffixes=('_spot', '_perp'))
        
        # Set initial holding
        initial_spot_price = data.iloc[0]['close_spot']
        initial_spot_quantity = self.strategy['initial_spot_holding']
        self.portfolio.holdings['BTC_spot'] = initial_spot_quantity
        self.portfolio.cash -= initial_spot_quantity * initial_spot_price
        
        for i, row in data.iterrows():
            timestamp = row['timestamp']
            prices = {'BTC_spot': row['close_spot'], 'BTC_perp': row['close_perp']}
            
            # 1. Update portfolio value at current timestamp
            self.portfolio.update_market_value(prices)
            self.portfolio.log_performance(timestamp)

            # 2. Check hedging logic (mimicking the live bot's job)
            spot_value = self.portfolio.holdings.get('BTC_spot', 0) * prices['BTC_spot']
            perp_value = self.portfolio.holdings.get('BTC_perp', 0) * prices['BTC_perp']
            current_delta_usd = spot_value + perp_value

            if abs(current_delta_usd) > self.strategy['delta_threshold']:
                # 3. A hedge is triggered!
                hedge_amount_usd = -current_delta_usd # We want to fully neutralize the delta
                hedge_contracts_needed = hedge_amount_usd / prices['BTC_perp']
                
                # We only need to trade the DIFFERENCE to our current hedge
                current_perp_holding = self.portfolio.holdings.get('BTC_perp', 0)
                trade_quantity = hedge_contracts_needed - current_perp_holding

                if abs(trade_quantity) > 0.001: # Minimum trade size
                    # 4. Create and execute order
                    order = {'asset': 'BTC_perp', 'quantity': trade_quantity}
                    fill = self.execution_handler.execute_order(order, prices['BTC_perp'])
                    
                    # 5. Update portfolio with the fill
                    self.portfolio.on_fill(fill)
                    print(f"{timestamp}: HEDGED. Delta was {current_delta_usd:.2f}. Traded {trade_quantity:.4f} contracts.")

        print("Backtest finished.")
        self.generate_report()

    def generate_report(self):
        """Calculates and prints performance metrics."""
        perf = self.portfolio.history
        perf['returns'] = perf['total_value'].pct_change()
        
        # Metrics
        total_return = (perf['total_value'].iloc[-1] / perf['total_value'].iloc[0]) - 1
        sharpe_ratio = perf['returns'].mean() / perf['returns'].std() * np.sqrt(365*24) # For hourly data
        
        # Max Drawdown
        rolling_max = perf['total_value'].cummax()
        daily_drawdown = perf['total_value'] / rolling_max - 1.0
        max_drawdown = daily_drawdown.min()

        print("\n--- Backtest Performance Report ---")
        print(f"Total Return: {total_return:.2%}")
        print(f"Max Drawdown: {max_drawdown:.2%}")
        print(f"Sharpe Ratio (annualized): {sharpe_ratio:.2f}")

        # Plotting
        plt.figure(figsize=(12, 8))
        plt.plot(perf['timestamp'], perf['total_value'], label='Hedged Portfolio Value')
        plt.title('Portfolio Performance Over Time')
        plt.xlabel('Date')
        plt.ylabel('Portfolio Value (USD)')
        plt.legend()
        plt.grid(True)
        plt.show()