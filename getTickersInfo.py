from datetime import datetime, date, timedelta
from prompt_toolkit import prompt
import dateutil.parser as parser
from urllib.parse import urljoin
from alphaCommon import alphaDb, RapidAPI, FetchingAlphaAPI, FetchPolygenAPI
import json
import math
db = alphaDb()
db2 = alphaDb()
api = FetchPolygenAPI()
rtn = db.queryDbSql('SELECT * from Tickers WHERE Exchange Is NULL')
tickers=[]
for row in rtn:
  ticker =row.Ticker.rstrip().upper()
  
  numShares=None
  exchange = None
  type=None
  try:
    res = api.getTickerDetails(ticker)
  except Exception as err:
    print (err)
    continue

  if ( 'results' in res):
    res = res['results']
  else:
    continue
  if ( 'weighted_shares_outstanding' in res):
    numShares = res['weighted_shares_outstanding']
  if ('type' in res ):
    type = res['type']
  if ('primary_exchange' in res ):
    exchange = res['primary_exchange'].upper()
  elif ( 'market' in res):
    exchange = res['market'].upper()
  print (ticker, numShares, exchange, type)
  sqls=[]
  if ( numShares != None):
    sqls.append(f"NumShares={numShares}")
  if ( exchange !=None ):
     sqls.append(f"Exchange='{exchange}'")
  if (type !=None ):
     sqls.append(f"Type='{type}'")
  if ( len(sqls)>0 ):
     sql = "UPDATE Tickers SET " + ",".join(sqls) + f"  WHERE Ticker= '{ticker}'"
     db2.queryDbSql(sql, True)
  
