import requests
import pandas as pd
from datetime import datetime, timedelta

# Tradier API key
TRADIER_API_KEY = "XY9MWydqz7fG3ARufuTaapMRT3us"

def fetch_tradier_daily_data(ticker, days_back=250):
    """
    Fetch daily historical data from Tradier for the specified ticker.
    days_back: Number of days to fetch (default 250 to cover 200-day SMA plus buffer).
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    url = "https://api.tradier.com/v1/markets/history"
    headers = {"Authorization": f"Bearer {TRADIER_API_KEY}", "Accept": "application/json"}
    params = {"symbol": ticker, "interval": "daily", "start": start_date, "end": end_date}
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Tradier API Error for {ticker}: {response.status_code}")
        return None
    
    data = response.json()
    if (data == None):
        return None
    if "history" not in data or "day" not in data["history"]:
        print(f"No Tradier results for {ticker}: {data.get('error', 'Unknown error')}")
        return None
    
    return data["history"]["day"]

def calculate_smas(ticker):
    """
    Calculate SMAs for a ticker and return a dictionary with results.
    Returns None if data fetch fails.
    """
    daily_data = fetch_tradier_daily_data(ticker, days_back=250)
    if not daily_data:
        return None
    
    # Create DataFrame from Tradier data
    df = pd.DataFrame(daily_data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')  # Ensure chronological order
    df = df.rename(columns={'close': 'Close'})
    
    # Current price (most recent close)
    current_price = df['Close'].iloc[-1]
    
    # Calculate SMAs
    sma_periods = [21, 50, 120, 200]
    sma_results = {'ticker': ticker, 'current_price': current_price}
    
    for period in sma_periods:
        sma_key = f"{period}_day_sma"
        if len(df) >= period:
            sma = df['Close'].rolling(window=period, min_periods=period).mean().iloc[-1]
            sma_results[sma_key] = sma
        else:
            sma_results[sma_key] = None  # Leave blank if insufficient data
    
    return sma_results

def main():
    # Input and output file names
    ticker_file = "tickers.txt"
    output_file = "sma_results.csv"
    
    # Read tickers from file
    try:
        with open(ticker_file, 'r') as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {ticker_file} not found. Please create a text file with tickers (one per line).")
        return
    
    if not tickers:
        print("No tickers found in the file.")
        return
    
    print(f"Processing {len(tickers)} tickers from {ticker_file}...")
    
    # Calculate SMAs for each ticker
    results = []
    for ticker in tickers:
        if (ticker == None or len(ticker)==0 ):
            break
        print(f"Fetching data for {ticker}...")
        sma_data = calculate_smas(ticker)
        if sma_data:
            results.append(sma_data)
    
    if not results:
        print("No valid data fetched for any ticker.")
        return
    
    # Create DataFrame and write to CSV
    df_results = pd.DataFrame(results, columns=['ticker', 'current_price', '21_day_sma', '50_day_sma', '120_day_sma', '200_day_sma'])
    
    # Round numeric columns to 2 decimal places, leaving None as is
    for col in ['current_price', '21_day_sma', '50_day_sma', '120_day_sma', '200_day_sma']:
        df_results[col] = df_results[col].apply(lambda x: round(x, 2) if pd.notnull(x) else '')
    
    df_results.to_csv(output_file, index=False)
    print(f"Results written to {output_file}")
    print(df_results.to_string(index=False))  # Optional: Print to console for verification

if __name__ == "__main__":
    main()