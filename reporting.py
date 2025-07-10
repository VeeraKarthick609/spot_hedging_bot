import pandas as pd
import io
import json
from database import db_manager

class ReportingManager:
    def generate_position_report_csv(self, chat_id: int) -> io.StringIO | None:
        """
        Generates a CSV report of the user's current position and risk settings.
        This serves as a basic risk disclosure document.
        """
        position_data = db_manager.get_position(chat_id)
        if not position_data:
            return None
        
        # Convert the dict to a pandas DataFrame for easy CSV export
        df = pd.DataFrame([position_data])
        
        # Reorder and rename columns for clarity
        report_df = df[[
            'chat_id', 'asset', 'size', 'delta_threshold', 'var_threshold',
            'large_trade_threshold', 'auto_hedge_enabled', 'daily_summary_enabled'
        ]]
        report_df.columns = [
            'User ID', 'Asset', 'Position Size', 'Delta Threshold (USD)', 'VaR Threshold (USD)',
            'Large Trade Limit (USD)', 'Auto-Hedge Enabled', 'Daily Summary Enabled'
        ]
        
        output = io.StringIO()
        report_df.to_csv(output, index=False)
        output.seek(0)
        return output

    def generate_trade_history_csv(self, chat_id: int) -> io.StringIO | None:
        """
        Generates a CSV report of all historical (simulated) trades for a user.
        This serves as a trade ledger for compliance purposes.
        """
        history_data = db_manager.get_hedge_history(chat_id)
        if not history_data:
            return None
        
        # Normalize the JSON 'details' column into separate columns
        flat_data = []
        for record in history_data:
            details = json.loads(record['details'])
            record.update(details)
            del record['details'] # Remove the original JSON string
            flat_data.append(record)
        
        df = pd.DataFrame(flat_data)
        
        # Select and reorder columns for the final report
        report_df = df[[
            'timestamp', 'hedge_type', 'action', 'size', 'avg_fill_price',
            'total_cost_usd', 'slippage_usd', 'fees_usd', 'venue'
        ]]
        report_df.columns = [
            'Timestamp (UTC)', 'Hedge Type', 'Action', 'Quantity', 'Avg Fill Price',
            'Total Cost (USD)', 'Slippage (USD)', 'Fees (USD)', 'Execution Venue'
        ]

        output = io.StringIO()
        report_df.to_csv(output, index=False)
        output.seek(0)
        return output

# Create a single instance
reporting_manager = ReportingManager()