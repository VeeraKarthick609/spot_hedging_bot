import pandas as pd
import logging

log = logging.getLogger(__name__)

class SimulatedPortfolio:
    """
    Manages the state of a simulated portfolio, including cash, holdings,
    costs, and performance history.
    """
    def __init__(self, initial_capital=100000.0):
        """
        Initializes the portfolio with starting capital.
        """
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.holdings = {}
        self.positions_value = 0.0
        
        # --- Cost and Performance Tracking Attributes ---
        self.total_commissions = 0.0
        # This was the missing line. We need to initialize total_slippage to zero.
        self.total_slippage = 0.0 
        
        self.history = pd.DataFrame(columns=['timestamp', 'total_value'])

    @property
    def total_value(self) -> float:
        """Calculates the current total equity of the portfolio."""
        return self.cash + self.positions_value

    def update_market_value(self, prices: dict):
        """Recalculates the market value of all holdings based on current prices."""
        self.positions_value = 0
        for asset, quantity in self.holdings.items():
            self.positions_value += quantity * prices.get(asset, 0)

    def log_performance(self, timestamp):
        """Records the current total portfolio value at a given timestamp."""
        new_row = pd.DataFrame([{'timestamp': timestamp, 'total_value': self.total_value}])
        self.history = pd.concat([self.history, new_row], ignore_index=True)

    def on_fill(self, fill_event: dict):
        """
        Updates the portfolio's state after a trade has been 'executed'.
        
        :param fill_event: A dictionary containing details of the filled order.
        """
        asset = fill_event['asset']
        quantity = fill_event['quantity']
        cost = fill_event['cost']
        commission = fill_event['commission']
        slippage_cost = fill_event['slippage_cost']

        # Update holdings
        self.holdings[asset] = self.holdings.get(asset, 0) + quantity
        # If a position is closed (quantity is ~0), remove it from holdings.
        if abs(self.holdings[asset]) < 1e-9:
            del self.holdings[asset]
        
        # Update cash: reduce by the cost of the trade AND the commission
        self.cash -= cost
        self.cash -= commission
        
        # Add to our cost trackers
        self.total_commissions += commission
        self.total_slippage += slippage_cost
        
        log.debug(f"FILL: {quantity:.4f} of {asset}. Cash: ${self.cash:,.2f}")
    
    