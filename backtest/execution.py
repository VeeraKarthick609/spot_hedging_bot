import logging

log = logging.getLogger(__name__)

class SimulatedExecutionHandler:
    """
    Simulates the process of order execution, accounting for market frictions
    like trading fees and price slippage.
    """
    def __init__(self, fee_rate=0.0006, slippage_pct=0.0005):
        """
        Initializes the handler with cost parameters.
        
        :param fee_rate: The trading fee as a percentage (e.g., 0.0006 for 0.06%).
        :param slippage_pct: The assumed slippage as a percentage.
        """
        self.fee_rate = fee_rate
        self.slippage_pct = slippage_pct

    def execute_order(self, order_event: dict, market_price: float) -> dict:
        """
        Simulates the execution of an order and returns a detailed 'fill' event.
        This now correctly calculates and includes the slippage cost.
        
        :param order_event: A dictionary with order details {'asset', 'quantity'}.
        :param market_price: The current mid-price of the asset.
        :return: A fill event dictionary including all costs.
        """
        quantity = order_event['quantity']
        direction = 1 if quantity > 0 else -1
        
        # 1. Simulate adverse slippage
        # Slippage makes the price worse for us: higher for buys, lower for sells.
        slippage_per_unit = market_price * self.slippage_pct * direction
        fill_price = market_price + slippage_per_unit
        
        # 2. Calculate total cost of the trade at the filled price
        cost = quantity * fill_price
        
        # 3. Calculate commission based on the total cost
        commission = abs(cost) * self.fee_rate
        
        # 4. Calculate the explicit cost of slippage
        # This is the monetary value lost due to the unfavorable price movement.
        slippage_cost = abs(quantity * slippage_per_unit) # This is the crucial calculation
        
        log.debug(
            f"EXECUTION: {quantity:.4f} of {order_event['asset']} at ${fill_price:,.2f}. "
            f"Slippage cost: ${slippage_cost:.4f}, Fee: ${commission:.4f}"
        )
        
        # 5. Return the complete fill event dictionary
        return {
            'asset': order_event['asset'],
            'quantity': quantity,
            'fill_price': fill_price,
            'cost': cost,
            'commission': commission,
            'slippage_cost': slippage_cost  # <<< --- THIS KEY IS NOW INCLUDED ---
        }