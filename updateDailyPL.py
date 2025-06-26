from SchwabApi import SchwabAPI
from datetime import datetime, timedelta
import pandas as pd
import pytz
from alphaCommon import alphaDb

# Function to determine the last trading day (skipping weekends)
def get_last_trading_day(today):
    """Returns the previous trading day, adjusting for weekends."""
    if today.weekday() == 0:  # Monday
        return today - timedelta(days=3)  # Friday
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)  # Friday
    else:
        return today - timedelta(days=1)  # Previous weekday

# Function to compare closing prices
def compare_stock_close(ticker):
    """
    Compare today's closing price with the last trading day's for a given ticker.
    
    Args:
        ticker (str): Stock ticker symbol (e.g., 'QQQ').
    
    Returns:
        tuple: (price_change, percent_change) in dollars and percentage.
               Returns (None, None) if data fetch fails.
    """
    # Initialize Schwab API client
    api = SchwabAPI()
    try:
        client = api.getClient()
    except Exception as e:
        print(f"Failed to initialize Schwab client for {ticker}: {e}")
        return None, None

    # Set current date to now (Eastern Time)
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).replace(hour=0, minute=0, second=0, microsecond=0)  # Start of day
    last_trading_day = get_last_trading_day(today)

    # Fetch price history
    try:
        response = client.get_price_history_every_day(
            symbol=ticker,
            start_datetime=last_trading_day - timedelta(days=3),  # Buffer for holidays/weekends
            end_datetime=today + timedelta(days=1)  # Include today
        )
        
        if response.status_code != 200:
            print(f"Failed to fetch data for {ticker}: {response.status_code}")
            return None, None
        
        # Parse JSON response
        history = response.json()
        candles = history.get('candles', [])
        if not candles:
            print(f"No price data available for {ticker}.")
            return None, None

        # Convert to DataFrame
        df = pd.DataFrame(candles)
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')  # Convert timestamp to datetime
        df['date'] = df['datetime'].dt.date

    except AttributeError:
        print(f"Error: 'get_price_history_every_day' method not found for {ticker}.")
        return None, None
    except Exception as e:
        print(f"Error fetching price history for {ticker}: {e}")
        return None, None

    # Extract closing prices
    today_close = df[df['date'] == today.date()]['close']
    if today_close.empty:
        print(f"No closing price available for {ticker} on {today.date()}.")
        return None, None
    today_close = today_close.iloc[-1]

    last_day_close = df[df['date'] == last_trading_day.date()]['close']
    if last_day_close.empty:
        # Handle holidays or missing data
        prior_days = df[df['date'] < today.date()]
        if prior_days.empty:
            print(f"No prior trading data available for {ticker} before {today.date()}.")
            return None, None
        last_day_close = prior_days['close'].iloc[-1]
        last_trading_day = prior_days['date'].iloc[-1]
    else:
        last_day_close = last_day_close.iloc[-1]

    # Calculate price and percentage change
    price_change = today_close - last_day_close
    percent_change = (price_change / last_day_close) * 100

    return price_change, percent_change

# Example usage
if __name__ == "__main__":
    db = alphaDb()
    db2 = alphaDb()
    rtn = db.queryMyPositions(" AssetType<>'OPTION' ")

    for row in rtn:
        #print (row.Symbol)
        symbol = row.Symbol.rstrip().upper()
        qSymbol = symbol
        if ( symbol == 'BRK.B'):
            qSymbol = 'BRK/B'
        qty = row.Qty
        


        price_change, percent_change = compare_stock_close(qSymbol)
        
        
        if price_change is not None and percent_change is not None:
            dailyPl = qty * price_change
            today = datetime.now(pytz.timezone('US/Eastern')).date()
            last_trading_day = get_last_trading_day(datetime.now(pytz.timezone('US/Eastern'))).date()
            print(f"\nStock: {symbol} Price Change (from {last_trading_day} to {today}): ${price_change:.2f}  Percentage Change: {percent_change:.2f}%")
            db2.queryDbSql(f"UPDATE MyPositions SET CurrentDayProfitLoss={dailyPl}  WHERE Symbol='{symbol}'", True)
        else:
            print("Could not compute price change.")