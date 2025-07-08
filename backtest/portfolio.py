import pandas as pd

class SimulatedPortfolio:
    def __init__(self, initial_capital=100000.0):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.holdings = {}  # e.g., {'BTC_spot': 1.0, 'BTC_perp': -1.0}
        self.positions_value = 0.0
        
        # --- NEW: For Performance Attribution ---
        self.total_commissions = 0.0
        self.realized_hedge_pnl = 0.0
        # Tracks the average entry price of our current perpetuals position
        self.perp_cost_basis = 0.0
        
        self.history = pd.DataFrame(columns=['timestamp', 'total_value'])

    @property
    def total_value(self):
        return self.cash + self.positions_value

    def update_market_value(self, prices: dict):
        """Recalculates the value of all holdings based on current market prices."""
        self.positions_value = 0
        for asset, quantity in self.holdings.items():
            self.positions_value += quantity * prices.get(asset, 0)

    def log_performance(self, timestamp):
        """Logs the current total portfolio value at a given timestamp."""
        new_row = pd.DataFrame([{'timestamp': timestamp, 'total_value': self.total_value}])
        self.history = pd.concat([self.history, new_row], ignore_index=True)

    def on_fill(self, fill_event: dict):
        """Updates holdings and cash based on a trade execution."""
        asset = fill_event['asset']
        quantity = fill_event['quantity']
        cost = fill_event['cost']
        commission = fill_event['commission']

        # Update holdings
        self.holdings[asset] = self.holdings.get(asset, 0) + quantity
        if self.holdings[asset] == 0:
            del self.holdings[asset]
        
        # Update cash
        self.cash -= (cost + commission)
    
    def on_fill(self, fill_event: dict):
        """Updates holdings and cash, and calculates realized P&L for attribution."""
        asset = fill_event['asset']
        quantity = fill_event['quantity']
        fill_price = fill_event['fill_price']
        commission = fill_event['commission']

        self.total_commissions += commission
        self.cash -= (quantity * fill_price) + commission
        
        # --- NEW: P&L Attribution Logic for Hedges ---
        if 'perp' in asset:
            current_holding = self.holdings.get(asset, 0)
            
            # If the trade closes or reduces a position, calculate realized P&L
            if current_holding != 0 and (quantity > 0) != (current_holding > 0):
                # Amount of the position being closed
                closed_quantity = min(abs(quantity), abs(current_holding))
                
                # Realized P&L = (Exit Price - Entry Price) * Quantity
                pnl = (fill_price - self.perp_cost_basis) * (-direction_of(current_holding) * closed_quantity)
                self.realized_hedge_pnl += pnl

            # Update cost basis for any new or increased position
            new_holding = current_holding + quantity
            if new_holding != 0:
                self.perp_cost_basis = ((self.perp_cost_basis * current_holding) + (quantity * fill_price)) / new_holding
            else:
                self.perp_cost_basis = 0
        
        # Update holdings
        self.holdings[asset] = self.holdings.get(asset, 0) + quantity
        if abs(self.holdings[asset]) < 1e-9: # Use tolerance for float comparison
            del self.holdings[asset]
            
def direction_of(n):
    return 1 if n > 0 else -1