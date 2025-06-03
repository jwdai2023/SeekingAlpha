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
from alphaCommon import alphaDb, FetchPolygenAPI

from datetime import date, timedelta


class TickerState:
  def __init__(self):
    self.ticker:str=None
    self.price:float = None
    self.currentDate:datetime=None
    self.numShares = 0
    self.entryDate:datetime = None
    self.entryPrice:float = None
    self.stopPrice:float = None
    self.trailingPrice:float = 0
    self.trades=[]
    self.quantRating:float = None
    self.sellSideRating:float = None
    self.authorsRating:float = None
    self.growthGrade:float = None
    self.momentumGrade:float = None
    self.epsRevisionsGrade = None
    self.profitabilityGrade = None
    self.valueGrade = None


class Holding:
  def __init__(self):
    self.entryDate=None
    self.entryPrice=0
    self.trailingPrice=0
    self.shares=0
    self.currPrice = 0
    self.currDate=None


################################



g_startDate = date(2025,4,1)
g_endDate = date(2025, 5, 29)
g_startCash = 350000
g_minCash = 20000
g_tradeSize = 6000
g_maxPostion = 2000


class Strategy:
  class TIER:
    TIER_1 = 1
    TIER_2 = 2
    TIER_3 = 3

  class ACTION:
    BUY = 1
    SELL = 2
    HOLD = 3
    KEEP = 4
    NONE = 0
    ADD = 5
    SCALEOUT = 6

  def __init__(self):

    self.startCash= 350000
    self.minCash= 20000
    self.tradeSize= 6000
    self.maxPostion=2000


    ## filters
    self.sellSideRating= 4.0
    self.sellSideRatingLo= 3
    self.quantRating:float=4.92
    self.quantRatingLo= 3.5
    self.authorsRating= 3.01
    self.authorRatingLo=1.0
    self.minPrice= 4

  ## the smaller the better!!!!
    self.growthGrade= 0
    self.momentumGrade= 1
    self.profitabilityGrade=0
    self.epsRevisionsGrade=0
    self.valueGrade=0

    ## strategies
    self.strategy= 'SOFT'  ## SOFT or HARD stop

    ## these setting are only effecive for 'SOFT' stop strategy
    self.stopLoss= 0.10
    self.takeProfit= 0.24
    self.trailingStop= 0.12

    ## force stop out even when the rating is strong. 
    ## Using any stop loss here would cause reduced performace
    ## back tests show that the loss could be as big as 30% within strong rating range.
    self.forceStopLoss=  0.35
    self.useStablizer=  0
    #self.useStablizer=  0.5
    
    self.addPosition= 0  ## this value > 1, then add # number times according to the below two values
    self.addPositionthreshold_lo=0.05
    self.addPositionthreshold_hi= 0.12
  
  def getTradeSize(self, ticker, date):
    return self.tradeSize
  
  def qualified(self, tickstate:TickerState)->TIER:
    if ( tickstate.quantRating == None or tickstate.sellSideRating == None ) :# or tickstate.authorsRating == None ):
      return Strategy.TIER.TIER_3
    elif (  tickstate.authorsRating != None and  tickstate.momentumGrade != None and 
          tickstate.epsRevisionsGrade != None and tickstate.valueGrade != None and 
          tickstate.profitabilityGrade != None and 
           tickstate.quantRating>= self.quantRating and
          (self.sellSideRating ==0 or  tickstate.sellSideRating>=self.sellSideRating) and
          (self.authorsRating == 0 or tickstate.authorsRating >= self.authorsRating) and
          (self.growthGrade == 0 or tickstate.growthGrade <= self.growthGrade) and
          (self.momentumGrade == 0 or tickstate.momentumGrade <= self.momentumGrade) and
          (self.epsRevisionsGrade == 0 or tickstate.epsRevisionsGrade <= self.epsRevisionsGrade) and
          (self.profitabilityGrade == 0 or tickstate.profitabilityGrade <= self.profitabilityGrade) and
          (self.valueGrade == 0 or tickstate.valueGrade <= self.valueGrade) and
          (self.minPrice ==0 or tickstate.price >= self.minPrice ) 
            ):

      return Strategy.TIER.TIER_1
    elif ( tickstate.quantRating< self.quantRatingLo or
           (self.sellSideRatingLo !=0 and tickstate.sellSideRating< self.sellSideRatingLo)
          ): ## this ticker does not qualify for the low threshold
      return Strategy.TIER.TIER_3
    else:
      return Strategy.TIER.TIER_2
  
  def decision(self, tickstate:TickerState, tier:TIER )->ACTION:
    action = Strategy.ACTION.NONE
    if ( tickstate is None or tickstate.numShares == 0 ): 
        ## I do not have a postion on this ticket yet
      if ( tier == Strategy.TIER.TIER_1 ):
        action = Strategy.ACTION.BUY
      else:
        action = Strategy.ACTION.NONE
    ### I have a position ::::::::::::

    elif ( tier == Strategy.TIER.TIER_3):
      delta = tickstate.currentDate - tickstate.entryDate
       ## this ticker does not qualify for the low threshold
      if (  tickstate.trailingPrice == 0):
        #holding.trailingPrice = holding.currPrice
        tickstate.trailingPrice = tickstate.price

      entryPrice = tickstate.entryPrice 

      if ( self.strategy == 'HARD'):
        action  = Strategy.ACTION.SELL
      elif (tickstate.price <= tickstate.entryPrice  *(1-self.stopLoss) ): ## sell for stop loss
        ## RULE #1, stop loss
        action  = Strategy.ACTION.SELL
      
      elif ( self.trailingStop <= 0.0001 and tickstate.price >= tickstate.entryPrice *(1+ self.takeProfit) ):
        ## RULE #2, TARGET hit 
        ## hit the target, sell to ake profit
        action  = Strategy.ACTION.SELL  ## sell for stop loss
      #elif  (  tickstate.price < tickstate.entryPrice ):
        ## RULE #3, hold if not hitting stop
        ## Adding this rule increases the over all retun a little bit. It means that if it is a loss but not hitting stop loss, 
        # let it run a little bit longer
        ## However, it will increast the avg loss amount to be greater than the win amount, but the win:loss chance is 2:1
        ## without this rule (default), the win:loss chance is 1:1 but win_amount:loss_amt is 2:1. I like the default config
       
        ## Actually, without this rule, rule #1 will not have effect because Rule #4 always dominate rule #1
      #  action[ticker] = 'HOLD'

      elif ( self.useStablizer 
            and ( delta != None and delta.days<21 and 
              tickstate.price >  tickstate.entryPrice *(1-self.useStablizer) and 
              (tickstate.price < tickstate.entryPrice*(1+self.useStablizer)))):
        action  = Strategy.ACTION.HOLD
        #print (f"{ticker} stablizer hold....")
      elif  ( self.trailingStop > 0 and ( tickstate.price < tickstate.entryPrice *(1+ self.takeProfit) ) ):
        # RULE #4, sell if the price is not high enough
        ## to change it from HOLD to SELL will increase a few percent.
        ## this means that if it is not a big winning stock, just sell it when its rating drops out of the zone  
        #action[ticker]  = 'HOLD'
        action  = Strategy.ACTION.SELL
      elif ( self.trailingStop > 0 and  tickstate.price < tickstate.trailingPrice*(1 - self.trailingStop ) ):
        ## RULE #5, Trailing stop
        action  = Strategy.ACTION.SELL
       
      else:
        action  = Strategy.ACTION.HOLD
        #print (f"{ticker}   hold....")

    elif ( self.forceStopLoss > 0 and tickstate.price<= tickstate.entryPrice  *(1-self.forceStopLoss) ):
        action  = Strategy.ACTION.SELL
    elif (tickstate.price >= 2* tickstate.entryPrice  ):
      if ( len(tickstate.trades) == 0 or tickstate.trades[-1]['shares']>0):
        action  = Strategy.ACTION.KEEP   # SCALEOUT   get rid off scaleout . It will reduce return
      else:
        action = Strategy.ACTION.KEEP
    elif ( tier == Strategy.TIER.TIER_1  or tier== Strategy.TIER.TIER_2 ):
      if ( len(tickstate.trades)<=self.addPosition and 
         ( tickstate.price > tickerState.entryPrice*1.20 and tickstate.price < tickerState.entryPrice*1.5) ):
          action = Strategy.ACTION.ADD
      else:
        action  = Strategy.ACTION.KEEP
    else:
        action  = Strategy.ACTION.KEEP
    return action

class topMomentumStrategy(Strategy):
  def __init__(self):
    super().__init__()
    self.addPosition = 0  
    self.addPosition = 2

class topMomentumStrategy2(Strategy):
  def __init__(self):
    super().__init__()
    #self.sellSideRating= 3.5
    self.authorsRating= 3.5
    self.tradeSize= 1000
    self.forceStopLoss = 0.3
    self.addPosition = 4 
  ## take momentumRating 2 if the quantRating is very high 
  def qualified(self, tickstate:TickerState)->Strategy.TIER:
    if ( tickstate.quantRating != None and tickstate.sellSideRating != None and tickstate.authorsRating != None
        and  tickstate.quantRating>=4.95 and  tickstate.sellSideRating>=self.sellSideRating
         and tickstate.authorsRating >= self.authorsRating and tickstate.momentumGrade<=2 ) :# or tickstate.authorsRating == None ):
      # print ("xxxx", tickstate.ticker, tickstate.quantRating, tickstate.sellSideRating, tickstate.authorsRating, tickstate.momentumGrade)
      tier = Strategy.TIER.TIER_1
    else:
      tier = super().qualified(tickstate)
    #print (tier, tickerState.quantRating, tickerState.authorsRating, tickerState.sellSideRating, tickerState.momentumGrade)
    return tier


class highGrowthStrategy(Strategy):
  def __init__(self):
    super().__init__()
    self.authorsRating= 3.0
    self.tradeSize= 4000
    self.quantRating:float=3.45
    self.quantRatingLo= 3.0
    self.growthGrade= 1
    self.momentumGrade= 1
   # self.profitabilityGrade=2
   # self.epsRevisionsGrade=3
    self.sellSideRating= 3.5
    self.sellSideRatingLo= 3


class specialStrategy(Strategy):
  def __init__(self):
    super().__init__()
    self.minPrice = 2
    self.quantRating = 4.92
    #self.momentumGrade= 1
 
  def qualified(self, tickstate:TickerState)->Strategy.TIER:
    if ( tickstate.quantRating == None or tickstate.sellSideRating == None ) :# or tickstate.authorsRating == None ):
      return Strategy.TIER.TIER_3
    elif (  tickstate.quantRating>= self.quantRating and
          (self.sellSideRating ==0 or  tickstate.sellSideRating>=self.sellSideRating) and
          ( tickstate.authorsRating == None or self.authorsRating == 0 or tickstate.authorsRating >= self.authorsRating) and
          (self.growthGrade == 0 or tickstate.growthGrade <= self.growthGrade) and
          (self.momentumGrade == 0 or tickstate.momentumGrade <= self.momentumGrade) and
          (self.epsRevisionsGrade == 0 or tickstate.epsRevisionsGrade <= self.epsRevisionsGrade) and
          (self.profitabilityGrade == 0 or tickstate.profitabilityGrade <= self.profitabilityGrade) and
          (self.valueGrade == 0 or tickstate.valueGrade <= self.valueGrade) and
          (self.minPrice ==0 or tickstate.price >= self.minPrice  ) 
            ):

      return Strategy.TIER.TIER_1
    elif ( tickstate.quantRating< self.quantRatingLo or
           (self.sellSideRatingLo !=0 and tickstate.sellSideRating< self.sellSideRatingLo)
          ): ## this ticker does not qualify for the low threshold
      return Strategy.TIER.TIER_3
    else:
      return Strategy.TIER.TIER_2
 
  
class alphaPicksStrategy(Strategy):
  def __init__(self):
    super().__init__()

    sql = 'SELECT * FROM ApPositions'
    db = alphaDb()
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
    self.actions=actions

  def getTradeSize(self, ticker:str, date:str):
    if ( t == 'SMCI'): ## SMCI was sold 50% and 50% in two batches
      return self.tradeSize/2
    else:
      return self.tradeSize
    
  def qualified(self, tickstate:TickerState)->Strategy.TIER:
    dateStr = tickstate.currentDate.strftime("%Y-%m-%d")
    ticker = tickstate.ticker.upper()
    if ( dateStr not in self.actions):
      return Strategy.TIER.TIER_2
    actionList=self.actions[dateStr]
    for en in actionList:
      if ( ticker == en['Ticker'].upper()):
        if ( en['Action'] == 'Open'):
          return Strategy.TIER.TIER_1
        elif (en['Action'] == 'Close'): 
          return Strategy.TIER.TIER_3
    return Strategy.TIER.TIER_2

  def decision(self, tickstate:TickerState, tier:Strategy.TIER )->Strategy.ACTION:
    action = Strategy.ACTION.NONE
    if ( tickstate is None or tickstate.numShares == 0 ): 
        ## I do not have a postion on this ticket yet
      if ( tier == Strategy.TIER.TIER_1 ):
        action = Strategy.ACTION.BUY
      else:
        action = Strategy.ACTION.NONE
    ### I have a position ::::::::::::
    elif ( tier == Strategy.TIER.TIER_3):
      action  = Strategy.ACTION.SELL
    else:
      action  = Strategy.ACTION.HOLD
    return action





class mixedStrategy(Strategy):
  def __init__(self):
    super().__init__()
  
  def qualified2(self, tickstate:TickerState)->Strategy.TIER:
    if ( tickstate.quantRating != None and tickstate.sellSideRating != None and tickstate.authorsRating != None
        and  tickstate.quantRating>=4.95 and  tickstate.sellSideRating>=self.sellSideRating
         and tickstate.authorsRating >= self.authorsRating and tickstate.momentumGrade<=2 ) :# or tickstate.authorsRating == None ):
      return Strategy.TIER.TIER_1
    else:
      return super().qualified(tickstate)

  def decision(self, tickstate:TickerState, tier:Strategy.TIER )->Strategy.ACTION:
    action = Strategy.ACTION.NONE
    if ( tickstate is None or tickstate.numShares == 0 ): 
        ## I do not have a postion on this ticket yet
      if ( tier == Strategy.TIER.TIER_1 ):
        action = Strategy.ACTION.BUY
      else:
        action = Strategy.ACTION.NONE
    ### I have a position ::::::::::::
    elif ( tier == Strategy.TIER.TIER_3):
      delta = tickstate.currentDate - tickstate.entryDate
       ## this ticker does not qualify for the low threshold
      if (  tickstate.trailingPrice == 0):
        #holding.trailingPrice = holding.currPrice
        tickstate.trailingPrice = tickstate.price

      entryPrice = tickstate.entryPrice 

      if (tickstate.price <= tickstate.entryPrice  *(1-self.stopLoss) ): ## sell for stop loss
        ## RULE #1, stop loss
        action  = Strategy.ACTION.SELL
      
      elif ( tickstate.price >= tickstate.entryPrice *(1+ self.takeProfit) ):
        if ( tickstate.price < tickstate.trailingPrice*(1-0.12)):
        ## RULE #2, TARGET hit 
        ## hit the target, sell to ake profit
          action  = Strategy.ACTION.SELL  ## sell for stop loss
        else:
          action =  Strategy.ACTION.HOLD

      elif ( tickstate.price >= tickstate.entryPrice *(1+ self.takeProfit/2) ):
        if ( tickstate.price < tickstate.trailingPrice*(1-0.06)):
        ## RULE #2, TARGET hit 
        ## hit the target, sell to ake profit
          action  = Strategy.ACTION.SELL  ## sell for stop loss
        else:
          action =  Strategy.ACTION.HOLD
      
      elif ( tickstate.price < tickstate.trailingPrice*(1-0.04)):
        ## RULE #5, Trailing stop
        action  = Strategy.ACTION.SELL
       
      else:
        action  = Strategy.ACTION.HOLD
        #print (f"{ticker}   hold....")

    elif ( self.forceStopLoss > 0 and tickstate.price<= tickstate.entryPrice  *(1-self.forceStopLoss) ):
        action  = Strategy.ACTION.SELL
    else:
        action  = Strategy.ACTION.KEEP


    return action


class Status :
  def __init__(self, startCash):
    self.wins = 0
    self.loses = 0
    self.holdingDays = 0
    self.winHoldingDays = 0
    self.lossHoldingDays = 0
    self.winAmt = 0
    self.lossAmt = 0
    self.maxPositions = 0
    self.cash = startCash
    self.minCash = self.cash
    self.stockVal = 0
    self.profit = 0
    self.added = 0
    self.sold = 0
    self.dailyPositions=[]
    self.minVal = 0
    self.maxVal = 0
    self.maxDrawndown = 0
    self.maxRealDrawndown = 0
    self.tickProfit={}


stat = Status(g_startCash)

db = alphaDb()
#res = db.queryDbSql('SELECT Distinct Date From Ratings Order by Date desc')
#for row in res:
#  print (row.Date)

def daterange(start_date: date, end_date: date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)

#sql = f"SELECT * From DailyPrice join Ratings on DailyPrice.Ticker=Ratings.Ticker and DailyPrice.Date=Ratings.Date Where Ratings.Date='{dateName}'"
  #print (sql)
sql = (f"SELECT * From DailyPrice join Ratings on DailyPrice.Ticker=Ratings.Ticker and DailyPrice.Date=Ratings.Date "
        f" where Ratings.Date  >= '{g_startDate.strftime("%Y-%m-%d")}'  and Ratings.Date  <= '{g_endDate.strftime("%Y-%m-%d")}' " 
         #    " AND Ratings.Ticker='NVDA' "
       )

res = db.queryDbSql(sql)
rows = []
for row in res:
  rows.append(row)

fOut = open(r"results1.txt", "w+")



holdings={} ##   holdings for tickers at the current date
stopMsg = ""
shortMsg = ""

#strategy = mixedStrategy()

#strategy = alphaPicksStrategy()
strategy = topMomentumStrategy()
#strategy = specialStrategy()
#strategy = topMomentumStrategy2()
#strategy = highGrowthStrategy()
for single_date in daterange(g_startDate, g_endDate):
# interate each date

  if (single_date.weekday() >= 5):
    continue

  todayRows = []
  dateName = single_date.strftime("%Y-%m-%d")
  for row in rows: 
    if ( row.Date == dateName):
      todayRows.append(row)
  if ( len(todayRows) == 0):
    continue ## there is no prcing info for this date. No trading....

  
  #print (dateName)

  nAdd=0
  nSold=0
  currMarket={}
  action = {}


  ## for each day, iterate each ticker to decide the action for this ticker
  for row in todayRows: 
    price =  row.O
    #print (row)
    tickerState:TickerState = None
    ticker = row.Ticker.rstrip().upper()
    if (  ticker in holdings):
      tickerState = holdings[ticker]

    else:
      tickerState = TickerState()
      tickerState.ticker = ticker

    tickerState.price = price
    tickerState.currentDate = single_date
    tickerState.quantRating = row.quantRating
    tickerState.sellSideRating = row.sellSideRating
    tickerState.authorsRating = row.authorsRating
    tickerState.growthGrade = row.growthGrade
    tickerState.momentumGrade = row.momentumGrade
    tickerState.epsRevisionsGrade = row.epsRevisionsGrade
    tickerState.profitabilityGrade = row.profitabilityGrade
    tickerState.valueGrade = row.valueGrade
    currMarket[ticker] = tickerState
    tier:Strategy.TIER = strategy.qualified(tickerState)
    action[ticker] = strategy.decision(tickerState, tier)


  newHoldings={}

  ## check the previous holding to see if any needs to be sold!
  posCount = len(holdings)
  for t in holdings:
    if ( not t in currMarket): ## for some reason, there is no market price today
      ## keep the existing postion AS IS
      action[t] = Strategy.ACTION.KEEP
      newHoldings[t] = holdings[t]
      continue
    tickerState =   currMarket[t]
    price = tickerState.price
    rating = tickerState.quantRating
    en:TickerState = holdings[t]

    if ( action[t] == Strategy.ACTION.SELL):
      # print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxx")
      shares = en.numShares
      entryPrice = en.entryPrice

      profit = (price-entryPrice)*shares
      profitPct = (price-entryPrice)/entryPrice*100
      print (f" --SOLD {t} {shares} shares at {price:.2f} with pofit {profit:.2f} {profitPct:.02f}% (rating={rating:.3f}")
      delta = single_date - en.entryDate
      if ( profit >=0):
        stat.wins += 1
        stat.winAmt += profit
        stat.winHoldingDays += delta.days
      else:
        stat.loses += 1
        stat.lossAmt += profit
        stat.lossHoldingDays += delta.days

      
      stat.holdingDays += delta.days
      if ( t in stat.tickProfit):
        stat.tickProfit[t] += profit
      else:
        stat.tickProfit[t] = profit

      stat.profit += profit 
      stat.sold += 1
      posCount -=1
      stat.cash += shares * price
    elif ( action[t] == Strategy.ACTION.SCALEOUT ):
      shares = int(en.numShares/5)
      entryPrice = en.entryPrice

      profit = (price-entryPrice)*shares
      profitPct = (price-entryPrice)/entryPrice*100
      print (f" --SCALE OUT {t} {shares} shares at {price:.2f} with pofit {profit:.2f} {profitPct:.02f}% (rating={rating:.3f}")

      stat.profit += profit 
      stat.cash += shares * price
      en.numShares -= shares
      en.trades.append({'shares':-shares, 'price':price, 'date':single_date})
      newHoldings[t] = en
    elif ( action[t] == Strategy.ACTION.HOLD or action[t] == Strategy.ACTION.KEEP):

      if ( action[t] == Strategy.ACTION.HOLD and price > en.trailingPrice):
        en.trailingPrice = price ## update the price
        #print ("UPDATE trailing price ........... ", price)
      #elif ( action[t] == 'KEEP'):
      ## IF the trailingPrice is reset after each time it goes out of strong rating, the return will decrease!!
      #  en.trailingPrice = 0 ## reset trailing stop
      newHoldings[t] = en ### keep this holding...

  ## do a second interation to add positions
  for row in todayRows:
    t = row.Ticker.rstrip().upper()

    if (action[t] != Strategy.ACTION.BUY and action[t] != Strategy.ACTION.ADD ):
      continue

    tickerState =   currMarket[t]
    price = tickerState.price
    rating = tickerState.quantRating
    if  ( posCount <= strategy.maxPostion and stat.cash > strategy.minCash + strategy.tradeSize): ## there is a room to add
      posCount+= 1
      tradeSize=strategy.getTradeSize(t, single_date )
      if ( action[t] == Strategy.ACTION.ADD ):
        tradeSize = tradeSize * 1
      newShares = int(tradeSize/price)
      if ( len(tickerState.trades)== 0 ):
        tickerState.numShares = newShares
        tickerState.entryPrice = price
        tickerState.entryDate = single_date
      else:
        value = newShares * price + tickerState.numShares * tickerState.entryPrice
        tickerState.numShares += newShares
        avgPrice = value / tickerState.numShares
        tickerState.entryPrice = avgPrice

      tickerState.trades.append({'shares':newShares, 'price':price, 'date':single_date})
      newHoldings[t] =  tickerState
      stat.added  +=1
      if ( action[t] == Strategy.ACTION.BUY ):
        print (" ### OPEN ", newShares ," shares of" , t, " at ", price, f" rating={tickerState.quantRating}, authors={tickerState.authorsRating}, sellSide={tickerState.sellSideRating}  momentum={tickerState.momentumGrade} ")
      else:
        print (" ### ADD ", newShares ," shares of" , t, " at ", price)
      stat.cash -= price * newShares
    elif  (action[t] == Strategy.ACTION.ADD): ## there is no room to add
      newHoldings[t] =  tickerState ## just keep it

  pos = {'Date': single_date}
  pos['Holdings'] = newHoldings
  pos['Cash'] = stat.cash

  stat.dailyPositions.append(pos)
  holdings=newHoldings
  stat.stockVal = 0
  for ticker in newHoldings:
    en = newHoldings[ticker]
    stat.stockVal += en.numShares*en.price
    #print (f"{ticker}  {stat.stockVal}")
  if ( stat.minCash > stat.cash):
    stat.minCash = stat.cash

  wholeVal = stat.cash + stat.stockVal
  if (  stat.minVal == 0 or stat.minVal >  wholeVal):
    stat.minVal = wholeVal
  if ( stat.maxVal <  wholeVal):
    stat.maxVal = wholeVal
  if (stat.maxVal > stat.minCash ):
    drawdown = (stat.maxVal - wholeVal)/(stat.maxVal -stat.minCash)
    realDrawdown = (stat.maxVal - wholeVal)/(stat.maxVal)
    if ( stat.maxDrawndown < drawdown):
      stat.maxDrawndown = drawdown
    if ( stat.maxRealDrawndown < realDrawdown):
      stat.maxRealDrawndown = realDrawdown
    
  print (single_date,f"==== Number of postions = {len(newHoldings)}  Cash {stat.cash:.2f}, Stock {stat.stockVal:.2f} Total {stat.cash+stat.stockVal:.2f}, min cash {stat.minCash:.2f}")
  #print (pos['Date'], " : ", pos['Holdings'])
  fOut.writelines(f"{single_date}\t{stat.cash:.2f}\t{stat.stockVal:.2f}\t{stat.cash+stat.stockVal:.2f}\r\n")
  if ( len(newHoldings)>stat.maxPositions):
    stat.maxPositions =  len(newHoldings)



# Define a custom function to serialize datetime objects 
def serialize_datetime(obj): 
    if isinstance(obj, (datetime, date)): 
        return obj.isoformat() 
    elif isinstance(obj, TickerState):
      return (f'{obj.entryDate}, {obj.entryPrice}, {obj.price}, {100*(obj.price-obj.entryPrice)/obj.entryPrice:.2f}%')
 
    raise TypeError("Type %s not serializable" % type(obj))
#exit()
#for pos in positions:
pos = stat.dailyPositions[-1]
#print (pos)
pos = stat.dailyPositions[-1]
print (json.dumps(pos, indent=2, default=serialize_datetime))
fOut.close()
print ("\nMaximum positions ==", stat.maxPositions, f" return Rate= {(stat.cash+stat.stockVal-g_startCash)*100/(g_startCash-stat.minCash):.2f}%",
       f"real return={(stat.cash+stat.stockVal-g_startCash)/g_startCash*100:.2f}% ")

print (f"Total Trades = {stat.added} Total Closed Trades = {stat.sold}")
print (f"Max value ={stat.maxVal:.2f}, Min value={stat.minVal:.2f}, max drawdown = {stat.maxDrawndown*100:.2f}%",
        f"real max drawdown = {stat.maxRealDrawndown*100:.2f}%")
if ( stat.wins != 0 and stat.loses != 0):
  print (f"Total Wins = {stat.wins} , avg=${stat.winAmt/stat.wins:.2f} Loses = {stat.loses}, avg=${stat.lossAmt/stat.loses:.2f}")
  print (f"Avg Holding Days = {stat.holdingDays/stat.sold:.2f}, Win holds day {stat.winHoldingDays/stat.wins:.2f}, Loss holds days {stat.lossHoldingDays/stat.loses:.2f}")

#for en in  pos['Holdings']:
#  print (" ", en, pos['Holdings'][en])
minTickProfit = 9999999
maxTickProfit = 0
bestTick = worstTick = None

for t in stat.tickProfit :
  if ( minTickProfit > stat.tickProfit[t]):
    minTickProfit = stat.tickProfit[t]
    worstTick = t
  if ( maxTickProfit < stat.tickProfit[t]):
    maxTickProfit = stat.tickProfit[t]
    bestTick = t


if ( bestTick != None and maxTickProfit> 0):
  print (f"{len(stat.tickProfit)} Tickers were traded. Best tick {bestTick} made ${maxTickProfit:.2f}.")
if ( worstTick != None and minTickProfit< 0):
  print (f"Worst tick {worstTick} lost ${-minTickProfit:.2f}")

print (shortMsg)
print (stopMsg)


