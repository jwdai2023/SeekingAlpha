from alphaCommon import alphaDb, FetchPolygenAPI
from datetime import datetime,date, timedelta

 

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

def updateApPrice(isForClosePrice=False):
  db = alphaDb()
  if ( isForClosePrice):
    sql = '''SELECT ApPositions.Ticker as Ticker, C as Price, ApPositions.CloseDate as Date from ApPositions left join DailyPrice 
              on ApPositions.Ticker=DailyPrice.Ticker and ApPositions.CloseDate=DailyPrice.Date where CloseDate is not null and ClosePrice is not NULL'''

  else:
    sql = '''SELECT ApPositions.Ticker as Ticker, C as Price, ApPositions.OpenDate as Date from ApPositions left join DailyPrice 
              on ApPositions.Ticker=DailyPrice.Ticker and ApPositions.OpenDate=DailyPrice.Date and OpenPrice is not NULL'''
  rtn = db.queryDbSql(sql)
  db2 = alphaDb()
  for row in rtn:
    ticker = row.Ticker.rstrip()
    price = row.Price
    date = row.Date
   
    print (ticker, price, date)
    if ( price == None):
      res = fetchDailyPriceForTicker(ticker, '2022-01-01', '2025-01-03')
      pass
    else:
      if ( not isForClosePrice):
        sql2 = f"UPDATE ApPositions SET OpenPrice={price} WHERE Ticker='{ticker}' and OpenDate='{date}'" 
      else:
        sql2 = f"UPDATE ApPositions SET ClosePrice={price} WHERE Ticker='{ticker}' and CloseDate='{date}'" 
      db2.queryDbSql(sql2, True)

def updateShares():
  sql = 'SELECT * From ApPositions where Shares is NULL'
  db = alphaDb()
  db2 = alphaDb()
  
  res = db.queryDbSql(sql)
  budget = 10000
  for row in res:
    ticker = row.Ticker
    price = row.OpenPrice
    shares = int(budget/price)
    date = row.OpenDate
    sql2 = f"UPDATE ApPositions SET Shares={shares} WHERE Ticker='{ticker}' and OpenDate='{date}'"
    db2.queryDbSql(sql2,True)


def daterange(start_date: date, end_date: date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)

def calReturns():

  sql = (f"SELECT * From DailyPrice  "
          f" where Date  >= '2022-01-01'  and Date  <= '2025-01-03' and Ticker in (Select distinct Ticker from ApPositions )" 
          
        )
  db = alphaDb()
  res = db.queryDbSql(sql)
  rows = []
  for row in res:
    rows.append(row)

  sql = 'SELECT * FROM ApPositions'
  res = db.queryDbSql(sql)
  actions={}
  for row in res:
    if row.OpenDate !=None:
      if ( row.OpenDate in actions):
        actionList = actions[row.OpenDate]
      else:
        actionList=[]
      actionList.append({'Ticker':row.Ticker.rstrip(), 'Shares':row.Shares, 'Action': "Open", "Price": row.OpenPrice})
      actions[row.OpenDate] = actionList

    if row.CloseDate !=None:
      if ( row.CloseDate in actions):
        actionList = actions[row.CloseDate]
      else:
        actionList=[]
      actionList.append({'Ticker':row.Ticker.rstrip(), 'Shares':row.Shares, 'Action': "Close", "Price": row.ClosePrice})
      actions[row.CloseDate] = actionList
  prePositions=None
  positions=[]
  datePositions=[]
  positionHistory=[]
  startCash=350000
  currCash = startCash

  for single_date in daterange(date(2022, 1, 1), date(2025, 1, 3)):  
    dateName = single_date.strftime("%Y-%m-%d")
    todayRows = []
    
    for row in rows: 
      if ( row.Date == dateName):
        todayRows.append(row)
    
    if ( len(todayRows) == 0):
      continue ## there is no prcing info for this date. No trading....

  
    if ( dateName in actions):
      actionList = actions[dateName]

      for en in actionList:
        command = en['Action']
        price = en['Price']
        ticker = en['Ticker']
        shares = en['Shares']
        amt = shares*price
        if (command == 'Open'):
          
          currCash-= amt
          pos={'Ticker': ticker, 'Shares': shares, 'Price': price}
          datePositions.append(pos)
        else: #   (command == 'Close'):
          currCash+= amt
          ind = 0
          closeEn=None
          for en in datePositions:
            if ( en['Ticker'] == ticker and en['Shares']== shares):
              en['Shares'] = 0
              closeEn = en
              break
            ind +=1
          if ( closeEn != None):
            datePositions[ind] = closeEn
      

    if ( len(datePositions) != 0):
      for ind in range(0, len(datePositions)):
        en = datePositions[ind]
        ticker = en['Ticker']
        ## find the price for the current date
        for temp in todayRows:
          if ( ticker== temp.Ticker.rstrip()):
            en['Price'] = temp.C
            #print (f"UPDATE price for {ticker} to {temp.C}")
            break
        datePositions[ind] = en.copy( )

      positionHistory.append({'Date': single_date, 'Cash': currCash, 'Positions': datePositions.copy()})
          
  #print (str(positionHistory))
  minCash = startCash
  fOut=open("picksResult.txt","w+")
  for pos in positionHistory:
    posList = pos['Positions']
    amt = 0
    for en in posList:
      amt += en['Price']*en['Shares']
      #print (f"{en['Ticker']}  {en['Shares']} {en['Price']}")
    print (pos['Date'], 'Cash:', f"{pos['Cash']:.2f}", 'Stock:', f"{amt:.2f}", 'Total:', f"{amt+pos['Cash']:.2f}") 
    fOut.writelines(f"{pos['Date']}\t{pos['Cash']:.2f}\t{amt:.2f}\t{amt+pos['Cash']:.2f}\n") 
    if ( minCash > pos['Cash']):
      minCash = pos['Cash']
    #z=input()
  print ("Min cash=", minCash)

#updateApPrice()
#updateApPrice(True)
#updateShares()

calReturns()