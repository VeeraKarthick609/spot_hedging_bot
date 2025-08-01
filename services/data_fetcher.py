import ccxt.async_support as ccxt
import logging
import pandas as pd

log = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self):
        self.exchanges = {
            'bybit': ccxt.bybit({'options': {'defaultType': 'spot'}}), # Default to spot
            'deribit': ccxt.deribit(),
            'okx': ccxt.okx(), # for smart order routing
        }
        log.info("DataFetcher initialized with exchanges: %s", list(self.exchanges.keys()))

    async def get_price(self, exchange_name: str, symbol: str) -> float | None:
        exchange_name = exchange_name.lower()
        if exchange_name not in self.exchanges:
            log.error(f"Exchange '{exchange_name}' not supported.")
            return None
        exchange = self.exchanges[exchange_name]
        try:
            ticker = await exchange.fetch_ticker(symbol)
            log.debug(f"Fetched ticker for {symbol} from {exchange_name}: {ticker['last']}")
            return ticker['last']
        except Exception as e:
            log.error(f"An unexpected error occurred fetching price for {symbol} on {exchange_name}: {e}")
            return None

    async def fetch_historical_data(self, exchange_name: str, symbol: str, timeframe: str = '1d', limit: int = 100) -> pd.DataFrame | None:
        """
        Fetches historical OHLCV data and returns it as a pandas DataFrame.
        """
        exchange_name = exchange_name.lower()
        if exchange_name not in self.exchanges:
            log.error(f"Exchange '{exchange_name}' not supported for historical data.")
            return None

        exchange = self.exchanges[exchange_name]
        try:
            # fetch_ohlcv returns a list of lists: [timestamp, open, high, low, close, volume]
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not ohlcv:
                log.warning(f"No historical data returned for {symbol} on {exchange_name}")
                return None
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            log.info(f"Successfully fetched {len(df)} historical data points for {symbol} from {exchange_name}")
            return df
        
        except Exception as e:
            log.error(f"Error fetching historical data for {symbol} on {exchange_name}: {e}")
            return None
        
    async def fetch_option_instruments(self, currency: str = 'BTC') -> list:
        """Fetches all active option instruments for a given currency from Deribit."""
        deribit = self.exchanges['deribit']
        try:
            await deribit.load_markets()
            instruments = [
                symbol for symbol in deribit.symbols 
                if symbol.startswith(currency) and deribit.markets[symbol]['option']
            ]
            log.info(f"Fetched {len(instruments)} option instruments for {currency}.")
            return instruments
        except Exception as e:
            log.error(f"Error fetching option instruments for {currency}: {e}")
            return []
    
    async def fetch_option_ticker(self, option_symbol: str) -> dict | None:
        """Fetches the full ticker for a specific option symbol from Deribit."""
        deribit = self.exchanges['deribit']
        try:
            ticker = await deribit.fetch_ticker(option_symbol)
            print(f"Fetched ticker for option {option_symbol}: {ticker}")
            return ticker
        except Exception as e:
            log.error(f"Error fetching ticker for option {option_symbol}: {e}")
            return None

    async def fetch_order_book(self, exchange_name: str, symbol: str, limit: int = 25) -> dict | None:
        """Fetches the order book for a given symbol."""
        exchange_name = exchange_name.lower()
        if exchange_name not in self.exchanges:
            log.error(f"Exchange '{exchange_name}' not supported for order book.")
            return None
        
        exchange = self.exchanges[exchange_name]
        try:
            order_book = await exchange.fetch_order_book(symbol, limit=limit)
            return order_book
        except Exception as e:
            log.error(f"Error fetching order book for {symbol} on {exchange_name}: {e}")
            return None

    async def close_connections(self):
        for name, exchange in self.exchanges.items():
            await exchange.close()

# Create a single instance
data_fetcher_instance = DataFetcher()