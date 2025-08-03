from datetime import datetime, date, timedelta
from prompt_toolkit import prompt
import dateutil.parser as parser
from urllib.parse import urljoin
from alphaCommon import alphaDb, RapidAPI, FetchingAlphaAPI, FetchPolygenAPI, MyTradierAPI
import json
import time
import random
# Function to determine the last trading day (skipping weekends)
def get_last_trading_day(today):
    """Returns the previous trading day, adjusting for weekends."""
    if today.weekday() == 0:  # Monday
        return today - timedelta(days=3)  # Friday
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)  # Friday
    else:
        return today - timedelta(days=1)  # Previous weekday

newDate = datetime.now()#  None # date(2024,11,22)
oldDate = get_last_trading_day(newDate )
oldDate =  date(2025,7,29)
quantRatingHi = 4.95
quantRatingLo = 4.0
sellerRatingHi = 4.0
sellerRatingLo = 3.0
authorsRatingHi = 3.01


## quickScan does not go through SeekingAlpha rating history page and does not save to DB
quickScan=True
 


## this is the second set that generate high return:
quantRatingHi2 = 4.90
momentumGrade = 1

## this is the Third set that generate high return:
authorsRatingHi2 = 3.5
sellerRatingHi2 = 3.5






def fetchAllRatedStocks(quantRating=3)->list:
  dbi=alphaDb()
  tickers=dbi.queryAllTickers()
  tickerHash={}
  for t in tickers:
    tickerHash[t]=1

  api = RapidAPI()
  payload = {
   # "close": {"gte": 1, "lte": 200000  }, 
    "quant_rating": {"gte": quantRating, "lte": 5  },
    "sell_side_rating": { "gte": 1,    "lte": 5    },
   # "value_category": { "gte": 1   ,  "lte": 20  },
   # "growth_category": { "gte": 1  ,  "lte": 20  },
   # "profitability_category": { "gte": 1 ,  "lte": 20   },
   # "momentum_category": {  "gte": 1,  "lte": 20  },
   # "eps_revisions_category": { "gte": 1,  "lte": 20   }
  }
  tickers = api.getScreenedStocks (payload)
  cnt = 0
  for en in tickers:
    t = next(iter(en))
    id = en[t]
    cnt +=1
    if t in tickerHash:
      dbi.queryDbSql(f"UPDATE Tickers SET Rank={cnt},Id={id} WHERE Ticker='{t}'", True)
    else:
      dbi.queryDbSql(f"INSERT INTO Tickers(Ticker, Rank,Id) Values('{t}', {cnt},{id})", True)
    
  print (tickers)

def prepareStockNames():
  tickers = fetchAllRatedStocks()
  db = alphaDb()

  rank = 1
  for tickerTuplet in tickers:
      db.createTickerEntry(tickerTuplet['ticker'],rank, tickerTuplet['id'])
      rank +=1

 
## fetch stock rating history for all tickers in our database
def fetchAlphaStockRatingsHistAll(day=None, doMakeUp=False):

  #fetchAlpha ("AAPL")
  db = alphaDb()
  api = FetchingAlphaAPI()
  tickers=[]
  startRank = 1
  endRank = 5000
  endPage = 2
  delay=1
  if ( day != None):
    startRank = 1+(day-1)*150
    endRank = startRank + 149
  else:
    startRank = 1
    endRank = 5000
    delay=10
    endPage = 3
  tickers = db.queryAllTickers(f" Rank>={startRank} and Rank <={endRank}")
  #print (tickers)
  #delay = 1
  cnt = 0
  totPages = 1
  print ("Search Rating information from SeekingAlpha for ", len(tickers), " tickers...")
  fromDate =  datetime.now() + timedelta(days=-110)
  fromDateStr = fromDate.strftime("%Y-%m-%d")

  for ticker in tickers:
    #ticker = 'AAPL'
    print (ticker)
    dates={}
    rtn = db.queryDbSql(f"SELECT Ticker, Date, dpsRevisionsGrade from Ratings where Date>='{fromDateStr}' and Ticker='{ticker}' ")#   and quantRating is NULL ")
    for en in rtn:
      dates[en.Date.strftime("%Y-%m-%d")]=1    
    for page in range(1,endPage):
      ts =  datetime.now()
      print (f"{ts} Searching {cnt+startRank} {ticker} of page {page}...")
      ratingHistList = api.fetchRatingHist(ticker, page)
      num =  len(ratingHistList)
      if (  num == 0 ):
        ## no more data
        print (f"No data for {ticker} from Seeking Alpha")
        break
      else:
        print (f"Got {num} data for {ticker} from Seeking Alpha")
      
      for entry in ratingHistList:
        try:
          date = entry['Date']
          
          if (date not in dates ):
            db.createRateHistoryEntry(entry)
            #db.updateRateHistoryEntry(entry)
          else:
            db.updateRateHistoryEntry(entry)
        except Exception as err:
          print (err)
      
    cnt+=1   
    time.sleep(random.randint(delay,delay+4)) ## to prevent the server to coke
  
    #exit()

def fixDailyPriceFromPolygen():
  api = FetchPolygenAPI()
  db1 = alphaDb()
  db2 = alphaDb()
  sql = '''select * from (select distinct(ticker) as t from dailyPrice) as tt
          left join Tickers 
          on tt.t=tickers.ticker
          where ticker is NULL'''
  n = 0
  res = db1.queryDbSql(sql)
  tickers=[]
  for row in res:
    
    #print(row)
    ticker = row.t.rstrip()
    print (ticker)
    tickers.append(ticker)

  for ticker in tickers:
    sql = f"INSERT INTO Tickers(Ticker, Rank) Values('{ticker}',1)"
    db2.queryDbSql(sql, True)


def fetchDailyPriceForTicker(ticker, startDate, endDate):

  api = FetchPolygenAPI()
  res = api.getDailyPrice(ticker,startDate, endDate)
  db2 = alphaDb()
  if ('results' in res):
    data = res['results']
    for dd in data:
      ts = dd['t']
      o = datetime.fromtimestamp(ts/1000)
      tstr=o.strftime('%Y-%m-%d')
      #print (tstr, " ", dd['o'], " ", dd['c'])
      try:
        db2.insertDailyPrice(ticker.upper(), tstr, dd['o'], dd['c'], dd['h'], dd['l'], dd['v'])
      except Exception as err:
        print (err)
      
def fetchDailyPriceFromPolygen():

  db1 = alphaDb()
  sql = f"SELECT distinct Ticker From Tickers WHERE Exchange is not NULL" # AND Ticker='SOFI' "
  res = db1.queryDbSql(sql)
  n = 0
  for row in res:
    #print(row)
    ticker = row.Ticker.rstrip()
    n+=1
    if ( n<3294):
      continue
    print (f"Fetch {n} {ticker}........")
    fetchDailyPriceForTicker(ticker,  "2025-03-19", datetime.now().strftime("%Y-%m-%d") )
    #time.sleep(14)

def fetchDailyPriceForTickerTradier(ticker, startDate, endDate):
    api = MyTradierAPI()
    res = api.getHistoricalDailyPrices(ticker, startDate, endDate)
    db2 = alphaDb()
    for dd in res:
        tstr = dd['date']
        try:
            db2.insertDailyPrice(
                ticker.upper(),
                tstr,
                dd.get('open', 0),
                dd.get('close', 0),
                dd.get('high', 0),
                dd.get('low', 0),
                dd.get('volume', 0)
            )
        except Exception as err:
            print(err)

def fetchDailyPriceFromTradier():
    db1 = alphaDb()
    sql = f"SELECT distinct Ticker From Tickers WHERE Exchange is not NULL"
    res = db1.queryDbSql(sql)
    n = 0
    for row in res:
        ticker = row.Ticker.rstrip()
        n += 1

        print(f"Fetch {n} {ticker}........")
        try:
            fetchDailyPriceForTickerTradier(ticker, "2025-03-19", datetime.now().strftime("%Y-%m-%d"))
        except Exception as err:
            print(err)
        #time.sleep(14)

def topTier (en):
    if ( en.quantRating == None or en.sellSideRating == None ):
     return False
    if ( ( (en.quantRating>=quantRatingHi and en.sellSideRating>=sellerRatingHi) or
        (en.quantRating>=quantRatingHi2 and en.sellSideRating>=sellerRatingHi
          and en.momentumGrade != None and en.momentumGrade <= momentumGrade ) )):
      if ( en.authorsRating == None or en.authorsRating >= authorsRatingHi ) :
        return True
      
      ## 2/1/2025: added this rule because of TWLO missing
      elif ( en.authorsRating >= authorsRatingHi2 and  en.sellSideRating>=sellerRatingHi2):
        return True
    return False


class Rating:
  def __init__(self, quantRating, sellSideRating, authorsRating=0, momentumGrade=None):
    self.quantRating = quantRating
    self.sellSideRating = sellSideRating
    self.authorsRating = authorsRating
    self.momentumGrade = momentumGrade


def getPositionStr(rating:dict, position:dict):
  str =""
  for tk in rating:
    if ( tk in position):
      str += f"{tk}({position[tk]}) "
    else:
      str += f"{tk} "
  return str


def getAndSaveTodayRatings(quantLo=2.5):
  api = FetchingAlphaAPI()
  tickers = api.getScreenedStocks(
    {
      "quant_rating": {"in": ["strong_buy","buy","hold"]  },
    }
  )

  ratings = api.getTodayRatings(tickers)
  saveTodayRatings(tickers, ratings)

def saveTodayRatings(tickers, ratings):
  allTickers = {}
  db = alphaDb()
  res = db.queryDbSql('SELECT Ticker from Tickers')
  for row in res:
    allTickers[row.Ticker.rstrip().upper()] = 1

  ind = 1
  for ticker in tickers:
    ticker=ticker.upper()
    rating = ratings[ticker]
    exchange:str = rating['exchange']
    exchange = exchange[:10]
   # exchange=""
    if ( ticker in allTickers):
      sql = f"UPDATE Tickers SET Rank={ind}, Exchange='{exchange}' WHERE Ticker='{ticker}'"
    else:
      sql = f"INSERT INTO Tickers(Rank, Exchange, Ticker) VALUES({ind}, '{exchange}', '{ticker}') "
    #sql = f"UPDATE Tickers SET Rank={ind} WHERE  Ticker='{ticker}'"
    ind +=1
    db.queryDbSql(sql, True)
    #continue
    dateStr = datetime.today().strftime("%Y-%m-%d")
      
    sql2 = (f"INSERT INTO Ratings (Ticker, Date, quantRating, authorsRating, sellSideRating, "
            "momentumGrade, profitabilityGrade, valueGrade, growthGrade, epsRevisionsGrade) "
            f"VALUES('{ticker}', '{dateStr}', {rating['quantRating']} , {rating['authorsRating']} , {rating['sellSideRating']}"
            f" , {rating['momentumGrade']}, {rating['profitabilityGrade']}, {rating['valueGrade']}, {rating['growthGrade']}"
            f", {rating['epsRevisionsGrade']})" )
    try:
      #db.queryDbSql(sql2, True)
      db.createRateHistoryEntry(rating)
    except Exception as err:
      print (err)



    
## compare the rate change of two different dates
def getRatingChanges(newDate=None, oldDate=None, quickScan=True, enforce = False):

  tickerRatings={}
  tickersOld = {}
  tickersNew = {}
  myPositions = {}
  myAlphaPicksPositions = {}
  if ( newDate is None):
    today = date.today().strftime("%Y-%m-%d")
  else:
    today = newDate

  if ( oldDate is None):
    yesterday = (date.today()+timedelta(-1)).strftime("%Y-%m-%d")
  else:
    yesterday = oldDate

  db = alphaDb()

  rtn = db.queryMyPositions()
  for row in rtn:
    posTicker = row.Ticker.rstrip().upper()
    tag = row.Tag
    if ( tag != None):
      tag = tag.rstrip().upper()
    if ( tag == 'ALPHA' ):
      myPositions[posTicker] = int(row.Qty)
    elif ( tag  == 'PICKS'):
      myAlphaPicksPositions[posTicker] = int(row.Qty)
  sqlForNewDay = f" Date='{today}'  AND quantRating>={quantRatingLo} AND sellSideRating>={sellerRatingLo} "
  sqlForOldDay = f" Date='{yesterday}'  AND quantRating>={quantRatingLo} AND sellSideRating>={sellerRatingLo}"
  

  rows = db.queryTickerRatings(sqlForOldDay)
  bhavingOldData = False
  
  for row in rows:
    tickersOld[row.Ticker.rstrip().upper()] = row
    bhavingOldData = True

  if ( not bhavingOldData):
    print ("There is no old date rating data. Do you want to search rating history (y/n? ")
    x = input ()
    if ( x !='Y' or x!='y'):
      return()
    
    cnt = 1
    for ticker in tickers:
    
      print (f"{cnt} of {len(needDataTickers)} : {ticker}")
      cnt +=1
      
      ratingHistList = alphaApi.fetchRatingHist(ticker, 1)

      for entry in ratingHistList:
        try:
          db.createRateHistoryEntry(entry)
          # pass
        except Exception as err:
          pass

 
    rows = db.queryTickerRatings(sqlForOldDay)
    for row in rows:
      tickersOld[row.Ticker.rstrip().upper()] = row

  #api = RapidAPI() 
  alphaApi= FetchingAlphaAPI( )
 
  tickers = alphaApi.getScreenedStocks(
    {
      "quant_rating": {"in": ["strong_buy"]},
      "sell_side_rating": { "in": ["strong_buy", "buy"]     },
      "close": { "gte": 4    },
      "authors_rating": { "in": ["strong_buy", "buy","hold"]   },
      "marketcap_display":{ "gte": 400000000}
    }
  )    

  rtn = alphaApi.getTodayRatings(tickers)
  saveTodayRatings(tickers, rtn)

  for t in rtn:
    rating=rtn[t]
    tickersNew[t]=Rating(rating['quantRating'], rating['sellSideRating'], rating['authorsRating'], rating['momentumGrade'])

  newTickers = {}
  keepTickers = {}
  downgradeTickers = {}
  discardTickers = {}

  for ticker in tickersNew:
    en = tickersNew[ticker]
    if ( topTier(en) ): 
      tickerRatings[ticker]=2
      if (ticker in tickersOld and topTier(tickersOld[ticker]) ):
        keepTickers[ticker] =en
      else:
        newTickers[ticker] = en
    else: ## by default, tickersNew should meet lowTier requirement!
      #en.quantRating>=quantRatingHi and en.sellSideRating>=sellerRatingHi):
      tickerRatings[ticker] = 1
      if (ticker in tickersOld and topTier(tickersOld[ticker])):
        # .quantRating>=quantRatingHi and tickersOld[ticker].sellSideRating>=sellerRatingLo):
        downgradeTickers[ticker] = en
  for ticker in tickersOld:
    if (ticker not in tickersNew ):
      tickerRatings[ticker] = 0
      discardTickers[ticker] = en
  
  totalPositions = myPositions | myAlphaPicksPositions
  str = getPositionStr(newTickers, totalPositions)
  print ("New Stars ==========\n", str )

  str = getPositionStr(keepTickers, totalPositions)
  print ("\r\nStay in postion Stars ==========\n", str)

  str = getPositionStr( downgradeTickers, totalPositions)
  print ("\r\nDowngrade to Hold ==========\n", str)

  str = getPositionStr( discardTickers, totalPositions)
  print ("\r\nTo be SOLD ==========\n", str)

  for posTicker in myPositions:
    if posTicker in discardTickers:
      print (f"\r\n --- NEED TO SELL {myPositions[posTicker]} shares of {posTicker}" )
    elif posTicker in tickersNew:
      r =  tickersNew[posTicker]
      #print ( f"{posTicker} {r.quantRating} {r.sellSideRating} {r.authorsRating} {r.momentumGrade}"  )
    else:
        print (f"\r\n{posTicker} is not in screened result!")

  print()
  return (tickerRatings)


def checkDailyRatingChanges(tickerRatings):
  api = RapidAPI() 
  rtn = api.getScreenedStocks(
    {
      "quant_rating": {"gte": quantRatingHi, "lte": 5  },
      "sell_side_rating": { "gte": sellerRatingHi,    "lte": 5    },
      "authors_rating":  { "gte": authorsRatingHi,    "lte": 5    },
      "close": { "gte": 3    },
    }
  )

  rtnHash={}
  for en in rtn:
    ticker = next(iter(en))
    rtnHash[ticker]=2
    if ( ticker not in tickerRatings or tickerRatings[ticker] != 2) :
      print ("........New high ticker found", ticker)


  rtn = api.getScreenedStocks(
    {
      "quant_rating": {"gte": quantRatingHi2, "lte": 5  },
      "momentum_category": {"gte": 1, "lte": momentumGrade}, ## the smalle the better
      "sell_side_rating": { "gte": sellerRatingHi,    "lte": 5    },
      "authors_rating":  { "gte": authorsRatingHi,    "lte": 5    },
      "close": { "gte": 3    },
    }
  )
  for ticker in rtn:
    rtnHash[ticker]=2
    if ( ticker not in tickerRatings or tickerRatings[ticker] != 2) :
      print ("........New high ticker found", ticker)


  for ticker in tickerRatings:
    if ( tickerRatings[ticker]==2 and ticker not in rtnHash ) :
      print ("........Downgraded high ticker found", ticker)


  rtn = api.getScreenedStocks(
    {
      "quant_rating": {"gte": quantRatingLo, "lte": 5  },
      "sell_side_rating": { "gte": sellerRatingLo,    "lte": 5    },

      "close": { "gte": 3    },
    }
  )
  for ticker in rtn:
    if ( ticker not in rtnHash):
      rtnHash[ticker]=1

  for ticker in tickerRatings:
    if (  tickerRatings[ticker]< 1 ):
      continue
    if ( ticker not in rtnHash ):
      if (tickerRatings[ticker] == 2) :
        print ("........Tanked ranking  ticker found", ticker)
      elif (tickerRatings[ticker] == 1) :
         print ("........Rating changed from above low to below low ", ticker)
    elif ( rtnHash[ticker] ==2 and tickerRatings[ticker]<2 ):
      print ("........Rating Increased to new Star", ticker)




helpText = '''Please give me your choce\n
          1. Run a quick daily scan
          2. Get today's top tickers and save results into database
          3. Run daily duty for today

          5. --- Run a sepecial complete scan for rating and open/close price 
          6. Only Fetch daily stock price and store into the database
          7. Fix Ticker table and daily pricing
          8. Search for stocks with quant rating >= 3
          q. Quit
          '''

choice = prompt (helpText)

if ( choice == '1'):
 
  print (f"STEP ONE: Read today's data from Market or database and compare to {oldDate.strftime("%Y-%m-%d")}")
  tickerRatings = getRatingChanges(newDate, oldDate, quickScan=True)
elif ( choice == '2'):
  getAndSaveTodayRatings()
elif ( choice == '3'):
  getAndSaveTodayRatings()
  mday=date.today().day
  #print (mday)
  fetchAlphaStockRatingsHistAll(mday)
elif ( choice == '5'):

  fetchAlphaStockRatingsHistAll()
  #fetchDailyPriceFromPolygen()
  fetchDailyPriceFromTradier()
  pass

elif ( choice == '6'):
  
  fetchDailyPriceFromTradier()
  pass
elif ( choice == '7'):

   
  fixDailyPriceFromPolygen()
  pass
elif (choice == '8'):
  fetchAllRatedStocks()
elif (choice == 'q'):
  quit()