import sqlite3
import logging
from typing import List, Dict, Any

log = logging.getLogger(__name__)
DB_FILE = "hedging_bot.db"

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.create_tables()

    def _get_connection(self):
        """Creates a database connection."""
        return sqlite3.connect(self.db_file)

    def create_tables(self):
        log.info("Initializing database and creating tables if they don't exist...")
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            chat_id INTEGER PRIMARY KEY,
            asset TEXT NOT NULL,
            spot_symbol TEXT NOT NULL,
            perp_symbol TEXT NOT NULL,
            size REAL NOT NULL,
            delta_threshold REAL NOT NULL,
            var_threshold REAL,
            auto_hedge_enabled INTEGER DEFAULT 0,
            daily_summary_enabled INTEGER DEFAULT 1,
            large_trade_threshold REAL,
            slow_ma INTEGER DEFAULT 20,
            fast_ma INTEGER DEFAULT 10,
            use_regime_filter INTEGER DEFAULT 0,
            hedge_ratio REAL DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
        # Table to store the history of all hedging actions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hedge_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hedge_type TEXT NOT NULL, -- 'perp' or 'option'
                action TEXT NOT NULL, -- 'short', 'buy_put', 'sell_call'
                size REAL NOT NULL,
                details TEXT, -- JSON string with price, cost, etc.
                FOREIGN KEY (chat_id) REFERENCES positions (chat_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                asset_symbol TEXT NOT NULL, -- e.g., 'BTC/USDT', 'BTC-PERP', 'BTC-29NOV24-70000-P'
                asset_type TEXT NOT NULL, -- 'spot', 'perp', 'option'
                quantity REAL NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, asset_symbol) -- A user can only have one entry per symbol
            )
        """)
        conn.commit()
        conn.close()
        log.info("Database initialized successfully.")

    def upsert_position(self, chat_id: int, data: Dict[str, Any]):
        """Inserts a new position or updates it if the chat_id already exists."""
        conn = self._get_connection()
        cursor = conn.cursor()
    
        # Set defaults for optional fields
        data.setdefault('var_threshold', None)
        data.setdefault('auto_hedge_enabled', 0)
        data.setdefault('daily_summary_enabled', 1)
        data.setdefault('large_trade_threshold', None)
        data.setdefault('slow_ma', 20)
        data.setdefault('fast_ma', 10)
        data.setdefault('use_regime_filter', 0)
        data.setdefault('hedge_ratio', 1.0)  # Default to full hedge
        
        cursor.execute("""
            INSERT INTO positions (
                chat_id, asset, spot_symbol, perp_symbol, size, 
                delta_threshold, var_threshold, auto_hedge_enabled, 
                daily_summary_enabled, large_trade_threshold,
                slow_ma, fast_ma, use_regime_filter, hedge_ratio
            )
            VALUES (
                :chat_id, :asset, :spot_symbol, :perp_symbol, :size, 
                :delta_threshold, :var_threshold, :auto_hedge_enabled, 
                :daily_summary_enabled, :large_trade_threshold,
                :slow_ma, :fast_ma, :use_regime_filter, :hedge_ratio
            )
            ON CONFLICT(chat_id) DO UPDATE SET
                asset=excluded.asset,
                spot_symbol=excluded.spot_symbol,
                perp_symbol=excluded.perp_symbol,
                size=excluded.size,
                delta_threshold=excluded.delta_threshold,
                var_threshold=excluded.var_threshold,
                auto_hedge_enabled=excluded.auto_hedge_enabled,
                daily_summary_enabled=excluded.daily_summary_enabled,
                large_trade_threshold=excluded.large_trade_threshold,
                slow_ma=excluded.slow_ma,
                fast_ma=excluded.fast_ma,
                use_regime_filter=excluded.use_regime_filter,
                hedge_ratio=excluded.hedge_ratio
        """, data)
        conn.commit()
        conn.close()
        log.info(f"Upserted position for chat_id: {chat_id}")

    def get_position(self, chat_id: int) -> Dict[str, Any] | None:
        """Retrieves a user's position by chat_id."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM positions WHERE chat_id = ?", (chat_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_all_positions(self) -> List[Dict[str, Any]]:
        """Retrieves all monitored positions for the background job."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM positions")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def delete_position(self, chat_id: int):
        """Deletes a user's monitored position."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM positions WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        log.info(f"Deleted position for chat_id: {chat_id}")

    def log_hedge(self, chat_id: int, hedge_type: str, action: str, size: float, details: str):
        """Logs a completed hedge action to the history table."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO hedge_history (chat_id, hedge_type, action, size, details)
            VALUES (?, ?, ?, ?, ?)
        """, (chat_id, hedge_type, action, size, details))
        conn.commit()
        conn.close()
        log.info(f"Logged hedge action for chat_id: {chat_id}")

    def get_hedge_history(self, chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieves the most recent hedge history for a user."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hedge_history WHERE chat_id = ? ORDER BY timestamp DESC LIMIT ?", (chat_id, limit))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def upsert_holding(self, chat_id: int, symbol: str, asset_type: str, quantity_change: float):
        """Adds or subtracts from a holding's quantity. Inserts if new, deletes if quantity is zero."""
        conn = self._get_connection()
        cursor = conn.cursor()
        # First, get current quantity
        cursor.execute("SELECT quantity FROM portfolio_holdings WHERE chat_id = ? AND asset_symbol = ?", (chat_id, symbol))
        result = cursor.fetchone()
        current_quantity = result[0] if result else 0.0
        
        new_quantity = current_quantity + quantity_change
        
        if abs(new_quantity) < 1e-9: # Effectively zero, so we remove it
            cursor.execute("DELETE FROM portfolio_holdings WHERE chat_id = ? AND asset_symbol = ?", (chat_id, symbol))
            log.info(f"Removed holding for {chat_id} on {symbol} as quantity is zero.")
        else:
            cursor.execute("""
                INSERT INTO portfolio_holdings (chat_id, asset_symbol, asset_type, quantity)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id, asset_symbol) DO UPDATE SET
                    quantity = ?,
                    updated_at = CURRENT_TIMESTAMP
            """, (chat_id, symbol, asset_type, new_quantity, new_quantity))
            log.info(f"Upserted holding for {chat_id}: {symbol} new quantity {new_quantity:.4f}")
        
        conn.commit()
        conn.close()

    def get_holdings(self, chat_id: int) -> List[Dict[str, Any]]:
        """Retrieves all current holdings for a user."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM portfolio_holdings WHERE chat_id = ?", (chat_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def clear_holdings(self, chat_id: int):
        """Deletes all holdings for a user. Used when monitoring stops."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM portfolio_holdings WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        log.info(f"Cleared all holdings for chat_id: {chat_id}")

# Create a single instance to be used across the application
db_manager = DatabaseManager(DB_FILE)