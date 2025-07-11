
# Spot Exposure Hedging Bot

A sophisticated, automated risk management system designed to monitor and hedge the directional risk of cryptocurrency spot positions using perpetual futures and options. The entire system is controlled through an interactive Telegram bot interface.

This project implements advanced financial engineering concepts, including real-time risk analytics (Greeks, VaR), dynamic hedging strategies, smart execution logic simulation, and machine learning-based forecasting, to provide a professional-grade risk management tool.

##  Key Features

*   **Automated & Manual Hedging:** Set up delta-neutral hedging strategies for spot positions using perpetual futures or options. Choose between fully automated execution or manual confirmation for every trade.
*   **Comprehensive Risk Analytics:** Access on-demand portfolio risk reports, including full Greek exposures (Delta, Gamma, Vega, Theta), Value at Risk (VaR), and stress testing scenarios.
*   **Advanced Options Strategies:** Interactively build multi-leg options strategies like Protective Puts, Covered Calls, Collars, and Iron Condors through a guided conversation.
*   **Machine Learning Integration:**
    *   **Volatility Forecasting:** A GARCH model provides predictive insights into future volatility, allowing for more intelligent options pricing.
    *   **Optimal Hedge Timing:** A classification model analyzes market momentum to decide the best moment to execute a hedge, potentially improving entry prices.
*   **Full Telegram Control:** Every feature, from initial setup to advanced analytics, is controlled through a seamless and interactive Telegram bot interface using commands and inline buttons.
*   **Robust Backtesting Framework:** A complete, event-driven backtester to validate hedging strategies on historical data before deployment.
*   **Compliance & Reporting:** Export your full configuration and trade history to CSV files for record-keeping and compliance.

##  Getting Started

Follow these steps to set up and run your own instance of the hedging bot.

### 1. Prerequisites

*   Python 3.11.9 (recommended)
*   A Telegram account.

### 2. Installation

1.  **Create a Virtual Environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### 3. Configuration

1.  **Create a Telegram Bot:**
    *   Open Telegram and start a chat with `@BotFather`.
    *   Send `/newbot` and follow the instructions to create a new bot.
    *   BotFather will give you a **HTTP API Token**. Copy it.

2.  **Set Up Environment Variables:**
    *   In the project's root directory, create a file named `.env`.
    *   Add your Telegram token to this file:
      ```env
      TELEGRAM_TOKEN="YOUR_TELEGRAM_BOT_TOKEN_HERE"
      ```

### 4. Data & Model Preparation

Before running the bot for the first time, you need to download historical data and train the machine learning models.

1.  **Navigate to the scripts directory:**
    ```bash
    cd scripts
    ```

2.  **Download Historical Data:** This script fetches daily and hourly data for BTC/USDT.
    ```bash
    python download_data.py
    ```

3.  **Train the Volatility Model:**
    ```bash
    python train_volatility_model.py
    ```
    *(This will run an out-of-sample evaluation and display a chart. Close the chart to allow the script to finish and save the model.)*

### 5. Running the Bot

1.  **Navigate back to the project root:**
    ```bash
    cd ..
    ```

2.  **Start the bot:**
    ```bash
    python main.py
    ```
    Your bot is now live! Open Telegram and start a chat with it.


## Running the Backtester

The backtesting framework allows you to test strategies on historical data without risking capital.

1.  Ensure you have downloaded historical data (see step 4.2 above).
2.  Open the `run_backtest.py` file.
3.  Modify the `strategy_config` dictionary to test different parameters (e.g., initial capital, delta threshold).
4.  Run the script from the project root:
    ```bash
    python run_backtest.py
    ```
    The script will print a detailed performance report and display a P&L chart.
