import pyodbc
from datetime import datetime

import dateutil.parser as parser
import pytz
import requests
import json
from urllib.parse import urljoin
import math
import time
import random
import csv
from dateutil import parser

stragety = 'HARD'
stragety = 'SOFT'
gTradeSize = 1000

def readAlertsFromCsv():
  ret = []

  with open('alerts_complete.csv', 'r', newline='') as input_file:
      dict_reader = csv.DictReader(input_file) 
      
      for row in dict_reader:
        
        row['strategy'] = row['strategy'] .rstrip().lstrip()
        #print(row)
        datetime_object =parser.parse(row['date'])
        entry = {'ticker': row['ticker'], 'strategy':row['strategy'], 'date':datetime_object.strftime("%Y-%m-%d")}
        ret.append(entry)
        #print (datetime_object)
  return (ret)


class alphaDb :
  def __init__(self):
    self.conn = self._getSqlConn_()

  def _getSqlConn_(self):
    SERVER=r"JD_NEWHP\SQLEXPRESS"
    DATABASE="seekAlpha"
 
    #connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    connectionString = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
    conn = pyodbc.connect(connectionString) 
    return conn

  def queryTopTickers(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  Ticker, UpdatedDate, CurrentRank, IsCurrent  FROM dbo.TopStocks "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"

    res = cursor.execute(qstr)
    return res
  
  def queryDbSql(self,sql):
    cursor = self.conn.cursor()

    res = cursor.execute(sql)
    return res
  
  def insertDailyPrice(self, ticker, dateStr, open, close, high, low, volume):
    qstr = (
        'INSERT INTO dbo.DailyPrice(Ticker, Date, O, C, H, L, V) '
        ' VALUES (?, ?, ?, ?, ?, ?, ?)' )
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, ticker, dateStr, open, close, high, low, volume) ):
      rtn = cursor.commit()
      return True
    else:
      return None  

  def _convertTs_(self, ts):
    try:
      timeObj = parser.parse(ts)
      est = pytz.timezone('US/Eastern')
      fmt = '%Y-%m-%d %H:%M:%S'
      ts2 = timeObj.astimezone(est).strftime(fmt)
      print (f"converting {ts} into {ts2} ..........")
      return ts2
    except Exception as error:
      print ("Error in converting timestamp ", ts)
      return None
    
class FetchPolygenAPI:
    def __init__(self):


        self.endpoint = "https://api.polygon.io/v2/" 
        self.session = requests.Session()

    def request(self, method: str, path: str, params: dict, data=None) -> dict:
        url = urljoin(self.endpoint, path)

        response = self.session.request(method.upper(), url, params=params, data=data)

        if response.status_code != 200:
            raise Exception(
                response.status_code, response.content.decode("utf-8")
            )
        res_json = response.json()
        key = url.rsplit("/", 1)[-1]
        if res_json.get(key) == "null":
            res_json[key] = []
        return res_json

    def get(self, path: str, params: dict) -> dict:
        """makes a GET request to an endpoint"""
        params['apiKey']='8pUxvsizskbaE9fK5sZn0oHN4kk34VMH'
        return self.request("GET", path, params)
    
    def getDailyPrice(self, ticker, startDate, endDate):
        ticker = ticker.upper()
        url = f"aggs/ticker/{ticker}/range/1/day/{startDate}/{endDate}"
        #print (url)
        rts = self.get(url, { 'adjusted':'true', 'sort':'asc'})
        return rts

def fetchDailyPriceAndSave(ticker):
  api = FetchPolygenAPI()
  db1 = alphaDb()

  print (f"Fetch {ticker}........")
  res = api.getDailyPrice(ticker, "2022-01-01", datetime.now().strftime('%Y-%m-%d'))
  if ('results' in res):
    data = res['results']
    for dd in data:
      ts = dd['t']
      o = datetime.fromtimestamp(ts/1000)
      tstr=o.strftime('%Y-%m-%d')
      print (ticker, " ", tstr, " ", dd['o'], " ", dd['c'])
      try:
        db1.insertDailyPrice(ticker.upper(), tstr, dd['o'], dd['c'], dd['h'], dd['l'], dd['v'])
      except Exception as err:
        print (err)
      
    

from datetime import date, timedelta
def daterange(start_date: date, end_date: date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)

def initHoldings():
  holdings = {}
  for single_date in daterange(date(2024, 4, 1), date.today()+timedelta(1)):
    dateName = single_date.strftime("%Y-%m-%d")
    holdings[dateName] = 0
  return holdings
    #print (dateName)


def testBullishStrategy(holdings, ticker, date):
  #print (ticker, date)
  sql = f"SELECT Date, C from DailyPrice WHERE Ticker='{ticker}' AND Date > '{date}' order by Date"

  rtn = db.queryDbSql(sql)
  cnt = 0
  rows = []
  profit = None
  openProfit = None
  g_stopProfit = 0.15
  
  #g_stopProfit = 0.10
  #g_stopLoss = 0.1
  g_stopProfit = 0.07
  g_stopLoss = 0.05

  for row in rtn:
    rows.append(row)
  
  for row in rows:
    #print (row)
    if ( cnt == 0):
      entryPrice = row.C
      cnt+=1
      continue
    holdings[row.Date]+=1  
    if ( row.C > (1+g_stopProfit) * entryPrice ) :
      profit = (row.C - entryPrice)/entryPrice * gTradeSize
      print (f" ++++++ {ticker} PROFIT {profit} {date} ${entryPrice}>> {row.Date} ${row.C}, ({cnt}) Days")
  
      break
    elif ( row.C < (1-g_stopLoss) * entryPrice ) :
      profit = (row.C - entryPrice)/entryPrice * gTradeSize
      print (f" ------ {ticker} LOSS {profit} {date} ${entryPrice}>> {row.Date} ${row.C}, ({cnt}) Days")
   
      break

    cnt+=1

  

  if ( cnt < 1):
    print (ticker, date, "  --------- NOT enough data to test")
    return None
  elif (profit is None): ## this trade is still open
    openProfit =  (row.C - entryPrice)/entryPrice * gTradeSize
    if ( openProfit > 0 ):
      print (f" +++ooo {ticker} open Profit {openProfit} {date} ${entryPrice}>> {row.Date} ${row.C}, ({cnt}) Days")
      pass
    else:
      print (f" ---ooo {ticker} open LOSS {-openProfit} {date} ${entryPrice}>> {row.Date} ${row.C}, ({cnt}) Days")
      pass
   
  return [profit, cnt, openProfit]

rows = readAlertsFromCsv()

def getKey(a,b):
  return f"{a}:{b}"


testStrategies = {
  "MACD Momentum" : 0, ## NOT GOOD

  "Buy the Dip" : 1, 
  'Bullish Bursts': 1,
  'Bullish Bursts' : 1,
  'buy the selloff': 1,
  'Bearish Bursts' : 1,
  'Fade the Dip' : 1,
  'Bollinger Buy the Dip' : 1,
  'Pre Earnings: 3-day Call' : 1,
  'Long Strangle Technical' : 1,
  'Pre Earnings: 14-day Diagonal' : 1, ## NOT GOOD
  'Pre Earnings: 7-day Call'  : 1,## NOT GOOD
  'Post Earnings: Short Put Spread' : 1,
  'Pre Earnings: 14-day Call' : 1,
  'Pre Earnings: 14-day Diagonal AI' : 1, 
  'Bollinger Buy the Dip AI' : 1,
  'Fade the Dip AI' : 1 }


db = alphaDb()

gHoldings = initHoldings()
totHoldingDays = 0
totProfit = 0.0
trades = 0
nLoss = nWin = 0

totOpenHoldingDays = 0
totOpenProfit = 0.0
totOpenTrades = 0
nOpenLoss = nOpenWin = 0


tickTrade={}

for entry in rows:
  strategyName = entry['strategy']
  if ( strategyName not in testStrategies):
     print ("ERROR!! Unknown strategy ", strategyName)
  elif ( testStrategies[strategyName] ):

    delta = datetime.now() - parser.parse(entry['date'])
    if ( delta.days < 10):
      print ("Not enough days to test")
      continue

    key =  getKey(entry['ticker'], entry['date']) 
    if ( key  in tickTrade):
      print ("SKIP THIS TRADE ", key, ". ALREAY TRADED")
      continue
    tickTrade[key] =1
    rtn = testBullishStrategy(gHoldings, entry['ticker'], entry['date'])
    if ( rtn is None):
      ## not enought pricing information???? 
      fetchDailyPriceAndSave(entry['ticker'])
      pass
    else:
      profit = rtn[0]
      if ( profit is None):
        openProfit = rtn[2]
        if ( openProfit is None):
          raise Exception ("Both profit and open profit is None. Something is wrong")
        else:
          totOpenProfit += openProfit
          totOpenTrades += 1
          holdingDays = rtn[1]
          totOpenHoldingDays += holdingDays
          if ( openProfit < 0):
            nOpenLoss +=1
          else:
            nOpenWin +=1

      else:
        holdingDays = rtn[1]

        trades+=1
        totProfit += profit
        totHoldingDays += holdingDays
        if ( profit < 0):
          nLoss +=1
        else:
          nWin +=1
  else :
     print ("SKIP this strategy ",strategyName )

print ( "Total trades is ", trades, " Total profit is ", totProfit)
print ( f"Per trade profit  {totProfit/trades:.1f}" )
print ("Wins = ", nWin, " ,  Loss =", nLoss)
print ("Average Holding days", totHoldingDays/trades)


print ( "Total open trades is ", totOpenTrades, " Total profit is ", totOpenProfit)
print ( f"Per open trade profit  {totOpenProfit/trades:.1f}" )
print ("open Wins = ", nOpenWin, " ,  open Loss =", nOpenLoss)
print ("Averag eopen Holding days", totOpenHoldingDays/totOpenTrades)
maxHoldings = 0
for dateStr in gHoldings:
  if ( gHoldings[dateStr] > maxHoldings ):
    maxHoldings =  gHoldings[dateStr]
print (f"Maximum number of holdings is {maxHoldings}")