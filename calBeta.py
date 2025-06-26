import pandas as pd
from yahooquery import Ticker

# Load the CSV file
df = pd.read_csv("ticker.csv")
## 
# Ensure required columns exist
if 'ticker' not in df.columns or 'beta' not in df.columns:
    raise ValueError("The CSV file must contain columns named 'ticker' and 'beta'")

# Fetch beta values
def get_beta(ticker):
    try:
        stock = Ticker(ticker)
        return stock.key_stats[ticker].get('beta', None)
    except Exception as e:
        print(f"Error fetching beta for {ticker}: {e}")
        return None

# Update beta values only if empty or zero
df['beta'] = df.apply(lambda row: get_beta(row['ticker']) if pd.isna(row['beta']) or row['beta'] == 0 else row['beta'], axis=1)

# Save back to CSV
df.to_csv("ticker.csv", index=False)

print("Beta values have been updated in ticker.csv")
