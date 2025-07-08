import pandas as pd

class SimulatedPortfolio:
    def __init__(self, initial_capital=100000.0):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.holdings = {}  # e.g., {'BTC': 1.5, 'BTC-PERP': -1.5}
        self.positions_value = 0.0
        
        # For performance tracking
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