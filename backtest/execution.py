class SimulatedExecutionHandler:
    def __init__(self, fee_rate=0.0006, slippage_pct=0.0005):
        self.fee_rate = fee_rate
        self.slippage_pct = slippage_pct

    def execute_order(self, order_event: dict, market_price: float) -> dict:
        """Simulates the execution of an order."""
        quantity = order_event['quantity']
        direction = 1 if quantity > 0 else -1
        
        # Simulate slippage
        slippage = market_price * self.slippage_pct * direction
        fill_price = market_price + slippage
        
        cost = quantity * fill_price
        commission = abs(cost) * self.fee_rate
        
        return {
            'asset': order_event['asset'],
            'quantity': quantity,
            'fill_price': fill_price,
            'cost': cost,
            'commission': commission
        }