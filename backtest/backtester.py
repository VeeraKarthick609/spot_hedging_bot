import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import the simulated environment components
from .portfolio import SimulatedPortfolio
from .execution import SimulatedExecutionHandler

# Although not used for the hedging logic in this simplified backtest,
# it's kept for structural consistency and potential future use with options.
from core.risk_engine import RiskEngine

class Backtester:
    """
    Orchestrates the backtesting process using historical data, a strategy configuration,
    a simulated portfolio, and an execution handler.
    """
    def __init__(self, spot_data, perp_data, strategy_config):
        """
        Initializes the Backtester.
        
        :param spot_data: DataFrame with historical spot data (timestamp, close).
        :param perp_data: DataFrame with historical perpetual data (timestamp, close).
        :param strategy_config: A dictionary defining the strategy parameters.
        """
        self.spot_data = spot_data
        self.perp_data = perp_data
        self.strategy = strategy_config
        
        self.portfolio = SimulatedPortfolio(self.strategy['initial_capital'])
        self.execution_handler = SimulatedExecutionHandler()
        self.risk_engine = RiskEngine()

    def run(self):
        """
        Runs the backtest loop from start to finish, applying the intelligent hedging strategy.
        """
        print(f"--- Starting INTELLIGENT HEDGING backtest ---")
        print(f"Strategy Config: {self.strategy}")
        
        # Merge spot and perpetual data to align timestamps for each step
        data = pd.merge(self.spot_data, self.perp_data, on='timestamp', suffixes=('_spot', '_perp'))

        # --- Calculate Market Regime Filter (MA Crossover) if enabled ---
        if self.strategy.get('use_regime_filter', False):
            fast_ma_period = self.strategy['fast_ma']
            slow_ma_period = self.strategy['slow_ma']
            data['fast_ma'] = data['close_spot'].rolling(window=fast_ma_period).mean()
            data['slow_ma'] = data['close_spot'].rolling(window=slow_ma_period).mean()
            # A "bearish" regime is when the fast MA is below the slow MA
            data['is_bearish'] = data['fast_ma'] < data['slow_ma']
            print(f"Regime filter enabled with {fast_ma_period}/{slow_ma_period} MA crossover.")
        
        # --- Set initial portfolio holding (buy the spot asset) ---
        initial_spot_price = data.iloc[0]['close_spot']
        initial_spot_quantity = self.strategy['initial_spot_holding']
        self.portfolio.holdings['BTC_spot'] = initial_spot_quantity
        self.portfolio.cash -= initial_spot_quantity * initial_spot_price
        
        # --- Main Backtesting Loop ---
        for i, row in data.iterrows():
            # Skip initial period where Moving Averages are not yet calculated
            if pd.isna(row.get('slow_ma')) and self.strategy.get('use_regime_filter', False):
                continue

            timestamp = row['timestamp']
            prices = {'BTC_spot': row['close_spot'], 'BTC_perp': row['close_perp']}
            
            # 1. Update portfolio value and log performance for this timestep
            self.portfolio.update_market_value(prices)
            self.portfolio.log_performance(timestamp)

            # 2. Decide if we should be actively hedging in this market condition
            should_hedge_now = True # Default to always being able to hedge
            if self.strategy.get('use_regime_filter', False):
                # If filter is on, only hedge if the market is in a bearish regime
                should_hedge_now = row['is_bearish']

            # 3. Calculate target portfolio delta based on the partial hedge ratio
            spot_value = self.portfolio.holdings.get('BTC_spot', 0) * prices['BTC_spot']
            # Target delta is the portion of the spot position we want to REMAIN exposed to.
            # E.g., if hedge_ratio is 0.6, we want to keep 40% of our upside (target_delta = 0.4 * spot_value)
            target_delta = spot_value * (1 - self.strategy['hedge_ratio'])

            # 4. Calculate the current actual delta of the portfolio
            perp_value = self.portfolio.holdings.get('BTC_perp', 0) * prices['BTC_perp']
            current_delta = spot_value + perp_value
            
            # 5. Determine the amount of delta we need to correct to reach our target
            delta_to_correct = current_delta - target_delta
            
            # 6. Check if a trade is needed based on the regime and thresholds
            trade_needed = False
            if should_hedge_now:
                # In a "hedge-on" regime, we trade if our delta deviates too far from the target.
                if abs(delta_to_correct) > self.strategy['delta_threshold']:
                    trade_needed = True
            else:
                # In a "hedge-off" (bullish) regime, our goal is to have NO hedge.
                # If we have any existing hedge, we must close it.
                current_perp_holding = self.portfolio.holdings.get('BTC_perp', 0)
                if abs(current_perp_holding) > 0.001: # Check if a hedge position exists
                    # We want to get back to full spot delta, so we close the entire perp position.
                    delta_to_correct = perp_value 
                    trade_needed = True

            # 7. If a trade is needed, execute it
            if trade_needed:
                # The quantity to trade is the opposite of the delta we need to correct
                contracts_to_trade = -delta_to_correct / prices['BTC_perp']
                
                if abs(contracts_to_trade) > 0.001: # Avoid dust trades
                    order = {'asset': 'BTC_perp', 'quantity': contracts_to_trade}
                    fill = self.execution_handler.execute_order(order, prices['BTC_perp'])
                    self.portfolio.on_fill(fill)
                    
                    regime_str = "(BEARISH)" if should_hedge_now else "(BULLISH - CLOSING HEDGE)"
                    print(f"{timestamp}: HEDGED {regime_str}. Current Delta: ${current_delta:,.0f}, Target Delta: ${target_delta:,.0f}. Traded {contracts_to_trade:.4f} contracts.")

        print("--- Backtest finished ---")
        self.generate_report()

    def generate_report(self):
        """Calculates and prints a detailed performance and attribution report."""
        perf = self.portfolio.history.set_index('timestamp')
        
        # --- 1. Basic Performance Metrics for Hedged Portfolio ---
        total_return_hedged = (perf['total_value'].iloc[-1] / perf['total_value'].iloc[0]) - 1
        # Annualize Sharpe Ratio based on hourly data (252 trading days * 24 hours)
        sharpe_ratio = perf['total_value'].pct_change().mean() / perf['total_value'].pct_change().std() * np.sqrt(252*24)
        rolling_max = perf['total_value'].cummax()
        drawdown = perf['total_value'] / rolling_max - 1.0
        max_drawdown_hedged = drawdown.min()

        # --- 2. Unhedged "Buy and Hold" Benchmark ---
        initial_spot_price = self.spot_data['close'].iloc[0]
        spot_qty = self.strategy['initial_spot_holding']
        initial_cost = initial_spot_price * spot_qty
        unhedged_value = (self.portfolio.initial_capital - initial_cost) + (self.spot_data.set_index('timestamp')['close'] * spot_qty)
        total_return_unhedged = (unhedged_value.iloc[-1] / unhedged_value.iloc[0]) - 1
        unhedged_drawdown = (unhedged_value / unhedged_value.cummax() - 1.0).min()

        # --- 3. Performance Attribution Analysis ---
        final_portfolio_value = self.portfolio.total_value
        net_pnl_hedged = final_portfolio_value - self.portfolio.initial_capital
        
        spot_pnl = (self.spot_data['close'].iloc[-1] - initial_spot_price) * spot_qty
        hedge_pnl = self.portfolio.realized_hedge_pnl
        # Add unrealized P&L from any open perp position at the end of the backtest
        if 'BTC_perp' in self.portfolio.holdings:
            unrealized_pnl = (self.perp_data.iloc[-1]['close_perp'] - self.portfolio.perp_cost_basis) * self.portfolio.holdings['BTC_perp']
            hedge_pnl += unrealized_pnl
            
        total_costs = self.portfolio.total_commissions # Slippage is implicitly included in P&L
        
        print("\n--- Backtest Performance Report ---")
        print(f"{'Metric':<25} | {'Hedged':<15} | {'Unhedged (Buy & Hold)':<25}")
        print("-" * 70)
        print(f"{'Total Return':<25} | {total_return_hedged:<15.2%} | {total_return_unhedged:<25.2%}")
        print(f"{'Max Drawdown':<25} | {max_drawdown_hedged:<15.2%} | {unhedged_drawdown:<25.2%}")
        print(f"{'Sharpe Ratio (Annualized)':<25} | {sharpe_ratio:<15.2f} | {'N/A':<25}")
        
        print("\n--- P&L Performance Attribution ---")
        print(f"{'P&L from Spot Position':<30}: ${spot_pnl:10,.2f}")
        print(f"{'P&L from Hedging (Perps)':<30}: ${hedge_pnl:10,.2f}")
        print(f"{'Total Trading Costs (Fees)':<30}: ${-total_costs:10,.2f}")
        print("-" * 43)
        print(f"{'Net P&L (Hedged Portfolio)':<30}: ${net_pnl_hedged:10,.2f}")

        # --- 4. Plotting Results ---
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.figure(figsize=(15, 8))
        plt.plot(perf.index, perf['total_value'], label='Hedged Portfolio (Intelligent)', color='royalblue', linewidth=2)
        plt.plot(unhedged_value.index, unhedged_value.values, label='Unhedged (Buy & Hold)', color='darkorange', linestyle='--', alpha=0.9)
        plt.title('Hedged vs. Unhedged Portfolio Performance (Intelligent Strategy)', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Portfolio Value (USD)', fontsize=12)
        plt.legend(fontsize=12)
        plt.show()