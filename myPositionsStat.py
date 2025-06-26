from SchwabApi import SchwabAPI
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from alphaCommon import alphaDb

# Configurable parameters
START_DATE = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')  # Extended to 200 days
END_DATE = datetime.now()  # Use current date and time as end date
SMA_SHORT = 50  # Short-term SMA
SMA_LONG = 100  # Long-term SMA

def getTickers():
    """Fetch unique tickers from MyPositions table in alphaDb."""
    db = alphaDb()
    res = db.queryDbSql('SELECT distinct Ticker from MyPositions')
    tickers = [row.Ticker for row in res]
    return tickers

# List of tickers to analyze
TICKERS = getTickers()
TICKERS=[ 'AEM','QQQ']

def fetch_schwab_data(ticker):
    """Fetch split-adjusted daily OHLC data from Schwab API."""
    api = SchwabAPI()
    ticker = ticker.rstrip().upper()
    if ticker == 'BRK.B':
        ticker = 'BRK/B'
    elif ticker in ['SWVXX', 'BOXX']:
        return None
    
    try:
        client = api.getClient()
        response = client.get_price_history_every_day(
            symbol=ticker,
            start_datetime=pd.to_datetime(START_DATE),
            end_datetime=END_DATE + pd.Timedelta(days=1)  # Buffer to include today
        )
        if response.status_code != 200:
            print(f"Failed to fetch data for {ticker}: {response.status_code}")
            return None
        
        # Parse JSON response
        history = response.json()
        candles = history.get('candles', [])
        if not candles:
            print(f"No price data available for {ticker}.")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(candles)
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
        df.set_index('datetime', inplace=True)
        return df[['close']]
    except AttributeError:
        print(f"Error: 'get_price_history_every_day' method not found for {ticker}.")
        return None
    except Exception as e:
        print(f"Error fetching Schwab data for {ticker}: {e}")
        return None

def calculate_sma(df, sma_short, sma_long):
    """Calculate SMA 50 and SMA 100 for the given price data."""
    df['sma_short'] = df['close'].rolling(window=sma_short, min_periods=sma_short).mean()
    df['sma_long'] = df['close'].rolling(window=sma_long, min_periods=sma_long).mean()
    return df

def find_last_crossover(df, ticker):
    """Find the last date and price where SMA 50 crossed SMA 100."""
    df = df.copy()  # Avoid modifying original DataFrame
    df['above'] = df['sma_short'] > df['sma_long']
    df['prev_above'] = df['above'].shift(1)
    df['crossover'] = (df['above'] != df['prev_above']) & ~df['sma_short'].isna() & ~df['sma_long'].isna()
    
    # Debug for AMD
    if ticker.upper() == 'AMD':
        print(f"\nDebug for {ticker}:")
        print(f"Total data points: {len(df)}")
        print(f"Valid SMA points: {len(df.dropna(subset=['sma_short', 'sma_long']))}")
        print(f"First 5 valid rows:\n{df.dropna(subset=['sma_short', 'sma_long'])[['close', 'sma_short', 'sma_long', 'above', 'prev_above', 'crossover']].head()}")
        print(f"Last 5 rows:\n{df[['close', 'sma_short', 'sma_long', 'above', 'prev_above', 'crossover']].tail()}")
        crossover_points = df[df['crossover']]
        print(f"Crossover points:\n{crossover_points[['close', 'sma_short', 'sma_long', 'above', 'prev_above']]}")
    
    crossover_points = df[df['crossover']]
    if crossover_points.empty:
        return None, None
    last_crossover = crossover_points.index[-1]
    crossover_price = df.loc[last_crossover, 'close']
    return last_crossover, crossover_price

def check_sma_crossover(ticker):
    """Check SMA 50 vs. SMA 100, with percentage difference, current price, and last crossover details."""
    # Fetch data
    df = fetch_schwab_data(ticker)
    if df is None or df.empty:
        return ticker, None, None, None, None, None, None, None

    # Filter data with timezone-aware dates
    eastern = pytz.timezone('US/Eastern')
    start_date_tz = pd.to_datetime(START_DATE).tz_localize(eastern)
    end_date_tz = END_DATE.replace(tzinfo=eastern)  # Localize END_DATE to Eastern
    df = df[(df.index >= start_date_tz) & (df.index <= end_date_tz)]

    # Calculate SMAs
    df = calculate_sma(df, SMA_SHORT, SMA_LONG)

    # Get latest values
    latest_date = df.index[-1]
    latest_sma_short = df['sma_short'].iloc[-1]
    latest_sma_long = df['sma_long'].iloc[-1]
    latest_close = df['close'].iloc[-1]

    # Check if we have enough data for SMA 100
    if pd.isna(latest_sma_long) or pd.isna(latest_sma_short):
        return ticker, None, None, None, None, None, None, None

    # Calculate percentage difference between SMAs
    pct_difference = ((latest_sma_short - latest_sma_long) / latest_sma_long) * 100

    # Find last crossover
    last_crossover_date, crossover_price = find_last_crossover(df, ticker)
    if crossover_price is not None:
        pct_from_crossover = ((latest_close - crossover_price) / crossover_price) * 100
    else:
        pct_from_crossover = None

    return ticker, latest_sma_short, latest_sma_long, latest_date, pct_difference, latest_close, crossover_price, pct_from_crossover

def main():
    # Convert END_DATE to Eastern Time for display
    eastern = pytz.timezone('US/Eastern')
    end_date_display = END_DATE.astimezone(eastern).strftime('%Y-%m-%d %H:%M:%S')

    # Display header
    print(f"\nSMA 50 vs. SMA 100 Analysis (as of {end_date_display}):")
    print(f"{'Ticker':<10} {'SMA 50':<12} {'SMA 100':<12} {'Status':<10} {'% Diff':<10} {'Current':<12} {'Cross Price':<12} {'% from Cross':<12}")
    print("-" * 93)

    # Loop through tickers
    for ticker in TICKERS:
        ticker, sma_short, sma_long, date, pct_difference, current_price, crossover_price, pct_from_crossover = check_sma_crossover(ticker)
        
        if sma_short is None or sma_long is None:
            status = "No Data"
            print(f"{ticker:<10} {'N/A':<12} {'N/A':<12} {status:<10} {'N/A':<10} {'N/A':<12} {'N/A':<12} {'N/A':<12}")
        else:
            status = "Above" if sma_short > sma_long else "Below"
            if crossover_price is None:
                crossover_str = "N/A"
                pct_from_cross_str = "N/A"
            else:
                crossover_str = f"{crossover_price:.2f}"
                pct_from_cross_str = f"{pct_from_crossover:.2f}"
            print(f"{ticker:<10} {sma_short:<12.2f} {sma_long:<12.2f} {status:<10} {pct_difference:<10.2f} {current_price:<12.2f} {crossover_str:<12} {pct_from_cross_str:<12}")

if __name__ == "__main__":
    main()