import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .portfolio import SimulatedPortfolio
from .execution import SimulatedExecutionHandler
from core.risk_engine import RiskEngine

class Backtester:
    """
    Orchestrates the backtesting process using historical data, a strategy configuration,
    a simulated portfolio, and an execution handler.
    """
    def __init__(self, spot_data, perp_data, strategy_config):
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
        
        data = pd.merge(self.spot_data, self.perp_data, on='timestamp', suffixes=('_spot', '_perp'))

        if self.strategy.get('use_regime_filter', False):
            fast_ma = self.strategy['fast_ma']
            slow_ma = self.strategy['slow_ma']
            data['fast_ma'] = data['close_spot'].rolling(window=fast_ma).mean()
            data['slow_ma'] = data['close_spot'].rolling(window=slow_ma).mean()
            data['is_bearish'] = data['fast_ma'] < data['slow_ma']
            print(f"Regime filter enabled with {fast_ma}/{slow_ma} MA crossover.")
        
        initial_spot_price = data.iloc[0]['close_spot']
        initial_spot_quantity = self.strategy['initial_spot_holding']
        self.portfolio.holdings['BTC_spot'] = initial_spot_quantity
        self.portfolio.cash -= initial_spot_quantity * initial_spot_price
        
        for i, row in data.iterrows():
            if pd.isna(row.get('slow_ma')) and self.strategy.get('use_regime_filter', False):
                continue

            timestamp = row['timestamp']
            prices = {'BTC_spot': row['close_spot'], 'BTC_perp': row['close_perp']}
            
            self.portfolio.update_market_value(prices)
            self.portfolio.log_performance(timestamp)

            # 1. Determine the TARGET HEDGE RATIO based on the current regime.
            current_hedge_ratio = self.strategy['hedge_ratio'] # Default ratio
            regime_str = "(NEUTRAL)"
            if self.strategy.get('use_regime_filter', False):
                if row['is_bearish']:
                    # In a bearish regime, use the configured hedge ratio.
                    current_hedge_ratio = self.strategy['hedge_ratio']
                    regime_str = "(BEARISH - HEDGE ON)"
                else:
                    # In a bullish regime, we want NO hedge to capture all upside.
                    current_hedge_ratio = 0.0
                    regime_str = "(BULLISH - HEDGE OFF)"

            # 2. Calculate the TARGET HEDGE VALUE in USD.
            # This is how much of our spot position we want to be short via perps.
            spot_value = self.portfolio.holdings.get('BTC_spot', 0) * prices['BTC_spot']
            target_hedge_value_usd = -spot_value * current_hedge_ratio

            # 3. Calculate the CURRENT HEDGE VALUE in USD.
            current_hedge_value_usd = self.portfolio.holdings.get('BTC_perp', 0) * prices['BTC_perp']
            
            # 4. Find the DISCREPANCY between our current hedge and our target hedge.
            hedge_discrepancy = current_hedge_value_usd - target_hedge_value_usd
            
            # 5. Only trade if this discrepancy is LARGER than our delta threshold.
            # The threshold now defines a "neutral band" around our target.
            if abs(hedge_discrepancy) > self.strategy['delta_threshold']:
                # The amount to trade is the opposite of the discrepancy to bring it back to the target.
                trade_value_usd = -hedge_discrepancy
                contracts_to_trade = trade_value_usd / prices['BTC_perp']
                
                if abs(contracts_to_trade) > 0.001:
                    order = {'asset': 'BTC_perp', 'quantity': contracts_to_trade}
                    fill = self.execution_handler.execute_order(order, prices['BTC_perp'])
                    self.portfolio.on_fill(fill)
                    
                    print(f"{timestamp}: REBALANCE {regime_str}. Discrepancy: ${hedge_discrepancy:,.0f}. Traded {contracts_to_trade:.4f} contracts.")

        print("--- Backtest finished ---")
        self.generate_report()

    def generate_report(self):
        """
        Calculates and prints a detailed performance report, including a benchmark 
        comparison and a robust P&L attribution analysis.
        """
        if self.portfolio.history.empty:
            print("No history recorded. Cannot generate report.")
            return

        perf = self.portfolio.history.set_index('timestamp')
        
        # --- 1. Hedged Portfolio Performance Metrics ---
        total_return_hedged = (perf['total_value'].iloc[-1] / perf['total_value'].iloc[0]) - 1
        net_pnl_hedged = perf['total_value'].iloc[-1] - self.portfolio.initial_capital
        
        returns = perf['total_value'].pct_change().dropna()
        sharpe_ratio = 0
        if not returns.empty and returns.std() != 0:
            sharpe_ratio = returns.mean() / returns.std() * np.sqrt(365 * 24)

        rolling_max_hedged = perf['total_value'].cummax()
        drawdown_hedged = (perf['total_value'] - rolling_max_hedged) / rolling_max_hedged
        max_drawdown_hedged = drawdown_hedged.min()

        # --- 2. Unhedged "Buy and Hold" Benchmark ---
        initial_spot_price = self.spot_data['close'].iloc[0]
        spot_qty = self.strategy['initial_spot_holding']
        unhedged_portfolio_value = self.portfolio.initial_capital + (self.spot_data.set_index('timestamp')['close'] - initial_spot_price) * spot_qty
        total_return_unhedged = (unhedged_portfolio_value.iloc[-1] / unhedged_portfolio_value.iloc[0]) - 1
        rolling_max_unhedged = unhedged_portfolio_value.cummax()
        drawdown_unhedged = (unhedged_portfolio_value - rolling_max_unhedged) / rolling_max_unhedged
        max_drawdown_unhedged = drawdown_unhedged.min()

        # --- 3. Rigorous P&L Attribution Analysis ---
        final_spot_price = self.spot_data['close'].iloc[-1]
        pnl_from_spot = (final_spot_price - initial_spot_price) * spot_qty
        total_costs = self.portfolio.total_commissions + self.portfolio.total_slippage
        pnl_from_hedges = net_pnl_hedged - pnl_from_spot + total_costs
        
        print("\n" + "="*75)
        print("--- Backtest Performance Report ---".center(75))
        print("="*75)
        print(f"{'Metric':<25} | {'Hedged Portfolio':<20} | {'Unhedged (Buy & Hold)':<25}")
        print("-" * 75)
        print(f"{'Total Return':<25} | {total_return_hedged:<20.2%} | {total_return_unhedged:<25.2%}")
        print(f"{'Max Drawdown':<25} | {max_drawdown_hedged:<20.2%} | {max_drawdown_unhedged:<25.2%}")
        print(f"{'Sharpe Ratio (Annualized)':<25} | {sharpe_ratio:<20.2f} | {'N/A':<25}")
        
        print("\n--- P&L Attribution Analysis ---")
        print("-" * 55)
        print(f"{'P&L from Spot Position (Buy & Hold)':<35}: ${pnl_from_spot:12,.2f}")
        print(f"{'P&L from Hedging Activities':<35}: ${pnl_from_hedges:12,.2f}")
        print(f"{'Total Costs (Fees + Slippage)':<35}: ${-total_costs:12,.2f}")
        print("-" * 55)
        print(f"{'Net P&L (Sum of Above)':<35}: ${pnl_from_spot + pnl_from_hedges - total_costs:12,.2f}")
        print(f"{'Actual Net P&L (Checksum)':<35}: ${net_pnl_hedged:12,.2f}")
        print("="*75)

        # --- 4. Plotting Results ---
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(15, 8), facecolor='#1c1c1e')
        ax.set_facecolor('#1c1c1e')
        ax.plot(perf.index, perf['total_value'], label='Hedged Portfolio (Intelligent)', color='#00aaff', linewidth=2)
        ax.plot(unhedged_portfolio_value.index, unhedged_portfolio_value.values, label=f"Unhedged ({spot_qty} BTC)", color='#ffae00', linestyle='--', alpha=0.9)
        ax.set_title('Hedged vs. Unhedged Portfolio Performance (Intelligent Strategy)', color='white', fontsize=18)
        ax.set_xlabel('Date', color='white', fontsize=12)
        ax.set_ylabel('Portfolio Value (USD)', color='white', fontsize=12)
        legend = ax.legend(fontsize=12)
        plt.setp(legend.get_texts(), color='white')
        ax.grid(True, linestyle='--', alpha=0.2, color='gray')
        fig.tight_layout()
        plt.show()