import pandas as pd
import numpy as np
from arch import arch_model
import joblib
import asyncio
import os
import matplotlib.pyplot as plt

# The path to the core directory where the model will be saved
MODEL_OUTPUT_PATH = "../core/garch_model.pkl"

# We can re-use the download script logic if needed, but for now we assume data exists
# from download_data import download_historical_data

async def train_and_evaluate_model():
    """
    Downloads data, trains a GARCH(1,1) model, evaluates its out-of-sample
    forecasting performance with a rolling window, plots the results, 
    and saves the final model trained on all available data.
    """
    symbol = "BTC/USDT"
    timeframe = "1d"
    data_path = f"../data/{symbol.replace('/', '_')}_{timeframe}.csv"

    # --- 1. Load Data ---
    try:
        # Set 'timestamp' as the index column and parse dates immediately
        data = pd.read_csv(data_path, index_col='timestamp', parse_dates=True)
        print(f"Successfully loaded {len(data)} data points from {data_path}")
    except FileNotFoundError:
        print(f"❌ Error: Data file not found at {data_path}.")
        print("Please run 'download_data.py' first to download a long history (e.g., since 2020).")
        return

    # Calculate daily returns in percentage points for better model convergence
    returns = 100 * data['close'].pct_change().dropna()
    
    if len(returns) < 500:
        print("❌ Error: Not enough historical data to perform a meaningful evaluation. Please download more data.")
        return

    # --- 2. Out-of-Sample Forecasting Evaluation ---
    print("\n--- Starting Out-of-Sample Evaluation (this may take a few minutes) ---")
    
    # Split data: 80% for the initial training set, 20% for testing (forecasting)
    split_index = int(len(returns) * 0.8)
    
    predictions = []
    
    # Use a rolling window forecast. At each step, we re-train the model with one new data point.
    for i in range(len(returns) - split_index):
        current_train_data = returns.iloc[:(split_index + i)]
        
        # Define the GARCH(1,1) model blueprint. Using a Student's-t distribution ('t')
        # is often more robust for financial data than a normal distribution.
        model = arch_model(current_train_data, vol='Garch', p=1, q=1, dist='t')
        
        # Fit the model and get the results object. 'disp='off'' suppresses verbose output in the loop.
        results = model.fit(disp='off')
        
        # Call forecast() on the FITTED results object to predict the next step.
        forecast = results.forecast(horizon=1)
        
        # Extract the forecasted variance for the next day
        predicted_variance = forecast.variance.iloc[-1, 0]
        
        # Append the standard deviation (sqrt of variance) to our predictions list
        predictions.append(np.sqrt(predicted_variance))

        # Print progress update every 50 steps
        if (i + 1) % 50 == 0:
            print(f"Rolling forecast... {i + 1}/{len(returns) - split_index} complete.")

    # Create a DataFrame to hold the results for easy plotting and analysis
    test_set = returns.iloc[split_index:]
    results_df = pd.DataFrame({
        'predicted_vol': predictions
    }, index=test_set.index)
    
    # Use the absolute value of returns as a proxy for actual daily volatility
    results_df['actual_vol'] = np.abs(test_set)
    
    # --- 3. Plot the Evaluation Results ---
    print("\n--- Generating Forecast Evaluation Plot ---")
    plt.figure(figsize=(14, 7))
    plt.plot(results_df.index, results_df['actual_vol'], label='Actual Volatility (Proxy)', color='gray', alpha=0.6)
    plt.plot(results_df.index, results_df['predicted_vol'], label='Predicted Volatility (GARCH Forecast)', color='#00aaff', linewidth=2)
    plt.title('GARCH Model: Out-of-Sample Volatility Forecast vs. Actual')
    plt.xlabel('Date')
    plt.ylabel('Daily Volatility (%)')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.show()

    # --- 4. Train Final Model on ALL Data and Save ---
    print("\n--- Training Final Model on Full Dataset ---")
    final_model = arch_model(returns, vol='Garch', p=1, q=1, dist='t')
    final_results = final_model.fit(update_freq=10, disp='iter')
    
    print("\nFinal Model Summary:")
    print(final_results.summary())

    # Save the fitted model object, which contains all learned parameters.
    joblib.dump(final_results, MODEL_OUTPUT_PATH)
    print(f"\n✅ Final GARCH model trained and saved successfully to {MODEL_OUTPUT_PATH}")

async def main():
    await train_and_evaluate_model()

if __name__ == "__main__":
    # Ensure the script runs in an environment where asyncio can function.
    # Depending on the system, you might need to manage the event loop explicitly
    # if running inside certain IDEs, but `asyncio.run` is generally robust.
    asyncio.run(main())