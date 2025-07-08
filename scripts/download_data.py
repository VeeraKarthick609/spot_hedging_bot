import ccxt.pro as ccxt
import pandas as pd
import os
import asyncio

async def download_historical_data(symbol, timeframe, start_date_str, output_folder="data"):
    """Downloads historical OHLCV data and saves it to a CSV file."""
    
    # Use current directory + data folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)  # Go up one level from scripts folder
    output_folder = os.path.join(project_dir, output_folder)
    
    exchange = ccxt.bybit({
        'enableRateLimit': True,
        'sandbox': False  # Set to True for testnet
    })
    
    start_timestamp = exchange.parse8601(start_date_str)
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Clean filename - replace problematic characters
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    filename = os.path.join(output_folder, f"{clean_symbol}_{timeframe}.csv")
    
    print(f"Downloading data for {symbol} since {start_date_str}...")
    print(f"Will save to: {filename}")

    all_ohlcv = []
    request_count = 0
    max_requests = 2000  # Safety limit
    
    while start_timestamp < exchange.milliseconds() and request_count < max_requests:
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since=start_timestamp, limit=1000)
            request_count += 1
            
            if not ohlcv:
                print("No more data available")
                break
                
            all_ohlcv.extend(ohlcv)
            start_timestamp = ohlcv[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            
            print(f"Request {request_count}: Fetched {len(ohlcv)} bars. Total: {len(all_ohlcv)}. Now at {exchange.iso8601(start_timestamp)}")
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.1)
            
        except Exception as e:
            print(f"An error occurred: {e}")
            # Wait a bit longer on error and try to continue
            await asyncio.sleep(1)
            break
    
    await exchange.close()
    
    if all_ohlcv:
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Remove duplicates that might occur
        df = df.drop_duplicates(subset=['timestamp'])
        df = df.sort_values('timestamp')
        
        df.to_csv(filename, index=False)
        print(f"✅ Data saved to {filename}")
        print(f"📊 Total records: {len(df)}")
        print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"💾 File size: {os.path.getsize(filename):,} bytes")
        print("-" * 50)
    else:
        print(f"❌ No data downloaded for {symbol}")

async def main():
    try:
        print("Starting data download...")
        print("=" * 50)
        
        # Download spot data
        await download_historical_data("BTC/USDT", "1h", "2023-01-01T00:00:00Z")
        
        # Download perpetual futures data
        await download_historical_data("BTC/USDT:USDT", "1h", "2023-01-01T00:00:00Z")
        
        print("🎉 All downloads completed!")
        
    except KeyboardInterrupt:
        print("❌ Download interrupted by user")
    except Exception as e:
        print(f"❌ An error occurred in main: {e}")

if __name__ == "__main__":
    asyncio.run(main())