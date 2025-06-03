import csv
from  alphaCommon import alphaDb 
from yahooquery import Ticker
import requests
import time

# Open the CSV file
db=alphaDb()
db2=alphaDb()
def is_numeric_string(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
    
with open('tags.csv', mode='r', newline='', encoding='utf-8-sig') as file:
    reader = csv.DictReader(file)  # Using DictReader to access columns by name

    # Loop through each row
    for row in reader:
        print (row)
         # Access PositionId and Tag for each row
        position_id = row['PositionId']
        tag = row['Tag']
        tag2=row['Tag2']
           # Print or process the values

        if ( position_id == None or len(position_id)==0 or not is_numeric_string(position_id)):
            continue
        print(f"PositionId: {position_id}, Tag: {tag}, Tag2:{tag2} {len(position_id)}")
        if ( tag != 'NULL' and tag2 != 'NULL'):
            db.queryDbSql(f"UPDATE MyPos Set Tag='{tag}' , Tag2='{tag2}' WHERE PositionId={position_id}", True)
        elif ( tag != 'NULL'):
            db.queryDbSql(f"UPDATE MyPos Set Tag='{tag}' WHERE PositionId={position_id}", True)
        elif ( tag2 != 'NULL'):
            db.queryDbSql(f"UPDATE MyPos Set Tag2='{tag2}' WHERE PositionId={position_id}", True)

rtn = db.queryDbSql(f"SELECT * from MyPos where OpenQty+CloseQty<>0")
hash={}
for row in rtn:
    hash[row.Symbol] = [row.PositionId, row.Tag, row.OpenDate,-row.OpenAmt/row.OpenQty] 

rtn=db.queryDbSql(f"SELECT * from MyPositions where OpenDate is NULL or Tag is NULL or Tag='None'")

for row in rtn:
    symbol = row.Symbol
    print (symbol)
    try:
      [positionId, tag, openDate, openPrice] = hash[symbol]
      if (row.AssetType.rstrip()=='OPTION'):
         openPrice /=100
      diff = abs(int(openPrice*1000) - int(row.AveragePrice*1000))
      if ( diff > 17 ):
        print(f"{symbol} {openPrice} != {row.AveragePrice}, diff is {diff}")
        db2.queryDbSql(f"UPDATE MyPositions Set Tag='{tag}' , AveragePrice={openPrice} , OpenDate='{openDate}', PositionId={positionId} WHERE Symbol='{symbol}'", True)
      else:
        db2.queryDbSql(f"UPDATE MyPositions Set Tag='{tag}' , OpenDate='{openDate}', PositionId={positionId} WHERE Symbol='{symbol}'", True)
    except Exception as err:
       print (err)


TABLE_NAME = "MyPositions"

def fetch_tickers():
    db = alphaDb()
    """Retrieve tickers from the database."""
    rows = db.queryDbSql(f"SELECT Ticker FROM {TABLE_NAME} WHERE Sector is NULL and ( AssetType='EQUITY' or AssetType='ETF') ")
    tickers = [row[0].rstrip() for row in rows]
    return tickers

def fetch_stock_data(ticker):
    """Fetch Beta, Sector, and Industry from Alpha Vantage."""
    print(f"Fetching data for {ticker}")
    if ticker=='BRK.B':
        ticker='BRK-B'
    api_key = "HHDKTQ7KNAQBMZNG"
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data for {ticker}: {response.status_code}")
            return "N/A", "N/A", "0"
        data = response.json()
        sector = data.get("Sector", "N/A")
        industry = data.get("Industry", "N/A")
        beta = data.get("Beta", "0")
        return sector, industry, beta
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return "N/A", "N/A", "0"

def update_database(ticker, sector, industry, beta):
    """Update the database with the fetched values."""
    db = alphaDb()
    db.queryDbSql(f"""
        UPDATE {TABLE_NAME} 
        SET Sector = '{sector}', Industry = '{industry}', Beta ={beta}  
        WHERE Ticker = '{ticker}'""", True)

def main():
    """Main function to process all tickers."""
    tickers = fetch_tickers()
    for ticker in tickers:
        #if ticker=='BRK.B':
        #    continue
        sector, industry, beta = fetch_stock_data(ticker)
        update_database(ticker, sector, industry, beta)
        print(f"Updated {ticker}: Sector={sector}, Industry={industry}, Beta={beta}")
        time.sleep(12)  # Alpha Vantage free tier: max 5 calls per minute

if __name__ == "__main__":
    main()

