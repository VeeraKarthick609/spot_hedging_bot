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
                daily_summary_enabled INTEGER DEFAULT 1, -- Default to ON
                auto_hedge_enabled INTEGER DEFAULT 0,
                
                hedge_ratio REAL DEFAULT 1.0, -- Default to 100% hedge for old behavior
                use_regime_filter INTEGER DEFAULT 0, -- Default to OFF
                fast_ma INTEGER DEFAULT 50,
                slow_ma INTEGER DEFAULT 200,

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
        conn.commit()
        conn.close()
        log.info("Database initialized successfully.")

    def upsert_position(self, chat_id: int, data: Dict[str, Any]):
        """
        Inserts a new position or updates it if the chat_id already exists.
        Handles the full user configuration, including optional fields.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Ensure all required keys have a default value before saving to the DB.
        # This makes the function robust even if the input dict is missing fields.
        data.setdefault('var_threshold', None)
        data.setdefault('auto_hedge_enabled', 0)
        data.setdefault('daily_summary_enabled', 1)
        
        # Use the powerful "UPSERT" syntax (INSERT ON CONFLICT)
        cursor.execute("""
            INSERT INTO positions (
                chat_id, asset, spot_symbol, perp_symbol, size, 
                delta_threshold, var_threshold, auto_hedge_enabled, daily_summary_enabled
            )
            VALUES (
                :chat_id, :asset, :spot_symbol, :perp_symbol, :size, 
                :delta_threshold, :var_threshold, :auto_hedge_enabled, :daily_summary_enabled
            )
            ON CONFLICT(chat_id) DO UPDATE SET
                asset=excluded.asset,
                spot_symbol=excluded.spot_symbol,
                perp_symbol=excluded.perp_symbol,
                size=excluded.size,
                delta_threshold=excluded.delta_threshold,
                var_threshold=excluded.var_threshold,
                auto_hedge_enabled=excluded.auto_hedge_enabled,
                daily_summary_enabled=excluded.daily_summary_enabled
        """, data)
        
        conn.commit()
        conn.close()
        log.info(f"Upserted position for chat_id: {chat_id} with data: {data}")

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

# Create a single instance to be used across the application
db_manager = DatabaseManager(DB_FILE)