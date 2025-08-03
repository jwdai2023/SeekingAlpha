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
from collections import defaultdict
import pandas as pd


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

g_startDate = date(2024,1,1)
g_endDate = date(2025, 7, 25)
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
         ( tickstate.price > tickstate.entryPrice*1.20 and tickstate.price < tickstate.entryPrice*1.5) ):
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
    #self.addPosition = 2

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


def daterange(start_date: date, end_date: date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)


def optimize_data_loading():
    """Optimized data loading with pre-processing"""
    db = alphaDb()
    
    # Load all data at once with optimized query
    sql = (f"SELECT * From DailyPrice join Ratings on DailyPrice.Ticker=Ratings.Ticker and DailyPrice.Date=Ratings.Date "
            f" where Ratings.Date  >= '{g_startDate.strftime("%Y-%m-%d")}'  and Ratings.Date  <= '{g_endDate.strftime("%Y-%m-%d")}' "
            f" ORDER BY Ratings.Date, Ratings.Ticker")
    
    res = db.queryDbSql(sql)
    
    # Convert to pandas DataFrame for faster processing
    data = []
    for row in res:
        data.append({
            'Date': row.Date,
            'Ticker': row.Ticker.rstrip().upper(),
            'Price': row.O,
            'quantRating': row.quantRating,
            'sellSideRating': row.sellSideRating,
            'authorsRating': row.authorsRating,
            'growthGrade': row.growthGrade,
            'momentumGrade': row.momentumGrade,
            'epsRevisionsGrade': row.epsRevisionsGrade,
            'profitabilityGrade': row.profitabilityGrade,
            'valueGrade': row.valueGrade
        })
    
    df = pd.DataFrame(data)
    
    # Group by date for faster lookup
    data_by_date = {}
    for date_str, group in df.groupby('Date'):
        date_key = date_str.strftime("%Y-%m-%d")
        data_by_date[date_key] = group.to_dict('records')
    
    return data_by_date


def run_optimized_backtest():
    """Main optimized backtest function"""
    print("Loading data...")
    data_by_date = optimize_data_loading()
    
    stat = Status(g_startCash)
    strategy = topMomentumStrategy()
    holdings = {}
    
    fOut = open(r"results1_optimized.txt", "w+")
    
    # Pre-calculate trading dates (skip weekends)
    trading_dates = []
    for single_date in daterange(g_startDate, g_endDate):
        if single_date.weekday() < 5:  # Monday = 0, Friday = 4
            trading_dates.append(single_date)
    
    print(f"Processing {len(trading_dates)} trading days...")
    
    for single_date in trading_dates:
        dateName = single_date.strftime("%Y-%m-%d")
        
        # Fast lookup using pre-grouped data
        if dateName not in data_by_date:
            continue
            
        todayRows = data_by_date[dateName]
        if len(todayRows) == 0:
            continue
        
        # Pre-build current market state
        currMarket = {}
        action = {}
        
        # Process all tickers for today in one pass
        for row in todayRows:
            ticker = row['Ticker']
            
            # Reuse existing TickerState or create new one
            if ticker in holdings:
                tickerState = holdings[ticker]
            else:
                tickerState = TickerState()
                tickerState.ticker = ticker
            
            # Update ticker state efficiently
            tickerState.price = row['Price']
            tickerState.currentDate = single_date
            tickerState.quantRating = row['quantRating']
            tickerState.sellSideRating = row['sellSideRating']
            tickerState.authorsRating = row['authorsRating']
            tickerState.growthGrade = row['growthGrade']
            tickerState.momentumGrade = row['momentumGrade']
            tickerState.epsRevisionsGrade = row['epsRevisionsGrade']
            tickerState.profitabilityGrade = row['profitabilityGrade']
            tickerState.valueGrade = row['valueGrade']
            
            currMarket[ticker] = tickerState
            
            # Calculate strategy decisions
            tier = strategy.qualified(tickerState)
            action[ticker] = strategy.decision(tickerState, tier)
        
        # Process holdings efficiently
        newHoldings = {}
        posCount = len(holdings)
        
        # Process existing holdings
        for ticker, holding in holdings.items():
            if ticker not in currMarket:
                # Keep existing position if no market data
                action[ticker] = Strategy.ACTION.KEEP
                newHoldings[ticker] = holding
                continue
                
            tickerState = currMarket[ticker]
            price = tickerState.price
            rating = tickerState.quantRating
            
            if action[ticker] == Strategy.ACTION.SELL:
                # Process sell action
                shares = holding.numShares
                entryPrice = holding.entryPrice
                profit = (price - entryPrice) * shares
                profitPct = (price - entryPrice) / entryPrice * 100
                
                print(f" --SOLD {ticker} {shares} shares at {price:.2f} with profit {profit:.2f} {profitPct:.02f}% (rating={rating:.3f}")
                
                delta = single_date - holding.entryDate
                if profit >= 0:
                    stat.wins += 1
                    stat.winAmt += profit
                    stat.winHoldingDays += delta.days
                else:
                    stat.loses += 1
                    stat.lossAmt += profit
                    stat.lossHoldingDays += delta.days
                
                stat.holdingDays += delta.days
                stat.tickProfit[ticker] = stat.tickProfit.get(ticker, 0) + profit
                stat.profit += profit
                stat.sold += 1
                posCount -= 1
                stat.cash += shares * price
                
            elif action[ticker] == Strategy.ACTION.SCALEOUT:
                # Process scale out
                shares = int(holding.numShares / 5)
                entryPrice = holding.entryPrice
                profit = (price - entryPrice) * shares
                profitPct = (price - entryPrice) / entryPrice * 100
                
                print(f" --SCALE OUT {ticker} {shares} shares at {price:.2f} with profit {profit:.2f} {profitPct:.02f}% (rating={rating:.3f}")
                
                stat.profit += profit
                stat.cash += shares * price
                holding.numShares -= shares
                holding.trades.append({'shares': -shares, 'price': price, 'date': single_date})
                newHoldings[ticker] = holding
                
            elif action[ticker] in [Strategy.ACTION.HOLD, Strategy.ACTION.KEEP]:
                # Update trailing price if needed
                if action[ticker] == Strategy.ACTION.HOLD and price > holding.trailingPrice:
                    holding.trailingPrice = price
                newHoldings[ticker] = holding
        
        # Process new positions
        for row in todayRows:
            ticker = row['Ticker']
            
            if action[ticker] not in [Strategy.ACTION.BUY, Strategy.ACTION.ADD]:
                continue
                
            tickerState = currMarket[ticker]
            price = tickerState.price
            rating = tickerState.quantRating
            
            if posCount <= strategy.maxPostion and stat.cash > strategy.minCash + strategy.tradeSize:
                posCount += 1
                tradeSize = strategy.getTradeSize(ticker, single_date)
                if action[ticker] == Strategy.ACTION.ADD:
                    tradeSize = tradeSize * 1
                    
                newShares = int(tradeSize / price)
                
                if len(tickerState.trades) == 0:
                    tickerState.numShares = newShares
                    tickerState.entryPrice = price
                    tickerState.entryDate = single_date
                else:
                    value = newShares * price + tickerState.numShares * tickerState.entryPrice
                    tickerState.numShares += newShares
                    tickerState.entryPrice = value / tickerState.numShares
                
                tickerState.trades.append({'shares': newShares, 'price': price, 'date': single_date})
                newHoldings[ticker] = tickerState
                stat.added += 1
                
                if action[ticker] == Strategy.ACTION.BUY:
                    print(f" ### OPEN {newShares} shares of {ticker} at {price} rating={tickerState.quantRating}, authors={tickerState.authorsRating}, sellSide={tickerState.sellSideRating} momentum={tickerState.momentumGrade}")
                else:
                    print(f" ### ADD {newShares} shares of {ticker} at {price}")
                    
                stat.cash -= price * newShares
            elif action[ticker] == Strategy.ACTION.ADD:
                newHoldings[ticker] = tickerState
        
        # Update portfolio statistics
        pos = {'Date': single_date, 'Holdings': newHoldings, 'Cash': stat.cash}
        stat.dailyPositions.append(pos)
        holdings = newHoldings
        
        # Calculate current portfolio value
        stat.stockVal = sum(holding.numShares * holding.price for holding in newHoldings.values())
        
        if stat.minCash > stat.cash:
            stat.minCash = stat.cash
            
        wholeVal = stat.cash + stat.stockVal
        if stat.minVal == 0 or stat.minVal > wholeVal:
            stat.minVal = wholeVal
        if stat.maxVal < wholeVal:
            stat.maxVal = wholeVal
            
        if stat.maxVal > stat.minCash:
            drawdown = (stat.maxVal - wholeVal) / (stat.maxVal - stat.minCash)
            realDrawdown = (stat.maxVal - wholeVal) / stat.maxVal
            if stat.maxDrawndown < drawdown:
                stat.maxDrawndown = drawdown
            if stat.maxRealDrawndown < realDrawdown:
                stat.maxRealDrawndown = realDrawdown
        
        print(f"{single_date} ==== Number of positions = {len(newHoldings)} Cash {stat.cash:.2f}, Stock {stat.stockVal:.2f} Total {stat.cash+stat.stockVal:.2f}, min cash {stat.minCash:.2f}")
        fOut.writelines(f"{single_date}\t{stat.cash:.2f}\t{stat.stockVal:.2f}\t{stat.cash+stat.stockVal:.2f}\r\n")
        
        if len(newHoldings) > stat.maxPositions:
            stat.maxPositions = len(newHoldings)
    
    fOut.close()
    
    # Print final results
    print_results(stat)
    
    return stat


def print_results(stat):
    """Print final backtest results"""
    print(f"\nMaximum positions == {stat.maxPositions}")
    print(f"Return Rate = {(stat.cash+stat.stockVal-g_startCash)*100/(g_startCash-stat.minCash):.2f}%")
    print(f"Real return = {(stat.cash+stat.stockVal-g_startCash)/g_startCash*100:.2f}%")
    print(f"Total Trades = {stat.added} Total Closed Trades = {stat.sold}")
    print(f"Max value = {stat.maxVal:.2f}, Min value = {stat.minVal:.2f}")
    print(f"Max drawdown = {stat.maxDrawndown*100:.2f}%")
    print(f"Real max drawdown = {stat.maxRealDrawndown*100:.2f}%")
    
    if stat.wins != 0 and stat.loses != 0:
        print(f"Total Wins = {stat.wins}, avg=${stat.winAmt/stat.wins:.2f}")
        print(f"Loses = {stat.loses}, avg=${stat.lossAmt/stat.loses:.2f}")
        print(f"Avg Holding Days = {stat.holdingDays/stat.sold:.2f}")
        print(f"Win holds day {stat.winHoldingDays/stat.wins:.2f}")
        print(f"Loss holds days {stat.lossHoldingDays/stat.loses:.2f}")
    
    # Find best and worst tickers
    if stat.tickProfit:
        best_tick = max(stat.tickProfit.items(), key=lambda x: x[1])
        worst_tick = min(stat.tickProfit.items(), key=lambda x: x[1])
        
        if best_tick[1] > 0:
            print(f"{len(stat.tickProfit)} Tickers were traded. Best tick {best_tick[0]} made ${best_tick[1]:.2f}.")
        if worst_tick[1] < 0:
            print(f"Worst tick {worst_tick[0]} lost ${-worst_tick[1]:.2f}")


if __name__ == "__main__":
    # Run the optimized backtest
    run_optimized_backtest() 