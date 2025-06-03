from schwab import client, auth
import json
from SchwabApi import SchwabAPI
from datetime import datetime,timedelta
import time
import re
from  alphaCommon import alphaDb
import pandas as pd

def getMyPositionsFromDb():
  db = alphaDb()
  rtn = db.queryMyPositions()
  positions={}
  for row in rtn:
    symbol = row.Symbol.rstrip()
    positions[symbol] = row
  return positions

def saveMyDbPositions(positions):
  db = alphaDb()
  for symbol in positions:
    pos = positions[symbol]
    db.saveMyPosition(pos)

def removeMyDbPositions():
  db = alphaDb()
  db.removeMyPositions()

def sign():
  c = auth.easy_client(api_key, app_secret, callback_url, token_path)

  r = c.get_price_history_every_day('AAPL')
  r.raise_for_status()
  print(json.dumps(r.json(), indent=4))




def main2(api_key, app_secret, callback_url, token_path, requested_browser):

    auth.client_from_manual_flow2(api_key, app_secret, callback_url,  token_path)

    return 0


def testApi():

    api = SchwabAPI()
    for i in range(0,1):
      rtn = api.client.get_quotes('AAPL')
      print (rtn)
      rtn = api.client.get_account_numbers()
      print (rtn)

      #rtn = api.get_accounts()
      #print ( json.dumps(rtn, indent=2))
      rtn = api = api.client.get_positions('709C3841BA10F6E8AC2239215FCF6F38A75AB3E29B2653EAC4F7C332D0EDE1D8')


      for key in rtn:
         print (key)
      positions=rtn['securitiesAccount']['positions']
      for key in rtn['securitiesAccount']:
         print (key)
      cashVal = rtn['securitiesAccount']['currentBalances']['cashBalance']
      cnt = 0
      val = 0
      etfVal = optionVal = stockVal = fundVal = otherVal = 0
      etfDay = optionDay = stockDay = fundDay = otherDay = 0
      
      for pos in positions:
        ins=pos['instrument']
        if ( cnt < 3):
          print ( json.dumps(pos, indent=2))
        print (f"{ins['assetType']}\t{ins['symbol']}\t{pos['longQuantity']-pos['shortQuantity']}\t{pos['marketValue']}")
        match ins['assetType']:
          case 'COLLECTIVE_INVESTMENT':
            etfVal += pos['marketValue']
            etfDay += pos['currentDayProfitLoss']
          case 'OPTION':
            optionVal += pos['marketValue']
            optionDay += pos['currentDayProfitLoss']
          case 'EQUITY':
            stockVal+= pos['marketValue']
            stockDay += pos['currentDayProfitLoss']
          case  'MUTUAL_FUND':
            fundVal += pos['marketValue']
            fundDay += pos['currentDayProfitLoss']
          case _:
            otherVal += pos['marketValue']
            otherDay += pos['currentDayProfitLoss']

        val += pos['marketValue']
 
        cnt +=1
    pass
    print (f'Total  value ={val+cashVal:.02f}')
    print (f'Cash         value = {cashVal:.02f}')
    print (f'Total asset value ={val:.02f}')
    print (f'Total ETF    value ={etfVal:.02f}\tchange={etfDay:.02f}') 
    print (f'Total OPTION value ={optionVal:.02f}\tchange={optionDay:.02f}')
    print (f'Total STOCK  value ={stockVal:.02f}\tchange={stockDay:.02f}')
    print (f'Total FUND   value ={fundVal:.02f}\tchange={fundDay:.02f}')
    print (f'Total OTHER  value ={otherVal:.02f}\tchange={otherDay:.02f}')
    
    print (json.dumps(rtn['aggregatedBalance']))
    print (f'Calculated total = {etfVal+optionVal+stockVal+fundVal+otherVal+cashVal:.02f}')

def fetchTransaction(startDate, endDate):
  c = api.getClient()
  positions={}
  specials=[]
  r=c.get_transactions('47B5B8D53FE71EE00A47CCF5C0A97B8D1E090D916B692467A002B9CBC802D574',start_date=startDate, end_date=endDate)
  db = alphaDb()
  rtn = r.json()
  #print (rtn)
  totAmount = 0
  tList=[]
  enList=[]
  for en in rtn:
    time = en['time']
    ts= datetime.strptime(time, '%Y-%m-%dT%H:%M:%S%z')
    tList.append(ts)
    enList.append(en)

  df=pd.DataFrame({'time': tList, 'entity': enList})
  df = df.sort_values(by='time')

  for index, row in df.iterrows():
    en = row['entity']
    type=en['type']

    time=en['time']
    status=en['status']
    if ( 'positionId' in en):
      positionId=en['positionId']
    else:
      positionId = None
    activityId=en['activityId']
    if ( 'orderId' in en):
      orderId=en['orderId']
    else:
      orderId = None
    netAmount=en['netAmount']
    totAmount += netAmount

    items=en['transferItems']
    
    #print (f"{time} {type} {positionId} {status} {netAmount}")

    if ( type != 'TRADE' and type != 'RECEIVE_AND_DELIVER'):
#    if (   type != 'RECEIVE_AND_DELIVER'):
      continue
    for it in items:
      ins=it['instrument']
      assetType=ins['assetType']
      if ( assetType == 'COLLECTIVE_INVESTMENT'):
        assetType = 'ETF'
      elif (assetType == 'MUTUAL_FUND'):
        assetType = 'MUTUAL'
      if ( assetType=='CURRENCY'):
        amount = it['amount']
        #totAmount += -amount
        #feeType= it['feeType']
        #print (f"    {amount} {feeType}")
      else:
        symbol=ins['symbol']
        #if ( assetType != "OPTION" or symbol!='QQQ   250103P00509000'):
        #if ( symbol !='TSLA'):
        #  continue
        amount = it['amount']
        
        price = it['price']
        cost = it['cost']
        
        #totAmount += cost
        if ( type == 'RECEIVE_AND_DELIVER'):
          action = 'EXPIRE'
        else:
          positionEffect=it['positionEffect']
          if ( positionEffect == 'OPENING'):
            action = 'OPEN'

          elif ( positionEffect == 'CLOSING'):
            action = 'CLOSE'
          else:
            action = positionEffect
        #print ("cost =======", totAmount)
        if ( action == 'OPEN' and positionId != None):
          if ( positionId in positions):
            pos = positions[positionId]
            pos['qty'] += amount
            if ( 'adjusts' in pos):
                pos['adjusts'].append( {'adjQty': amount,  'adjAmt': cost, 'adjTime': time,  'adjPrice': price})
            else:
                   pos['adjusts'] = [{'adjQty': amount,  'adjAmt': cost, 'adjTime': time,  'adjPrice': price}]
          else:
            positions[positionId] = {'qty': amount, 'openQty': amount, 'symbol': symbol, 'cost':cost, 'openPrice': price, 'openTime': time}
        elif ( ( action == 'CLOSE' or action == 'EXPIRE' )and positionId != None):
          if ( positionId in positions):
            pos = positions[positionId]
            if ( pos['symbol'] != symbol):
              specials.append({'id': activityId, 'note': 'position Not found', 'positionId': positionId, 'qty': amount, 'openQty': amount, 'symbol': symbol, 'cost':cost, 'openPrice': price, 'openTime': time})
            else:
              pos['qty'] += amount
              if ( pos['qty']== 0):
                pos['closeAmt'] = cost
                pos['closeTime'] = time
                pos['closePrice'] = price
              else:
                if ( 'adjusts' in pos):
                  pos['adjusts'].append( {'adjQty': amount,  'adjAmt': cost, 'adjTime': time,  'adjPrice': price})
                else:
                   pos['adjusts'] = [{'adjQty': amount,  'adjAmt': cost, 'adjTime': time,  'adjPrice': price}]
          else:
             specials.append({'id': activityId, 'note': 'Unmatched PositionId', 'action':action, 'positionId': positionId, 'adjQty': amount,  'adjAmt': cost, 'adjTime': time,  'adjPrice': price})

        else:
          specials.append({'id': activityId, 'note': 'Unknow Position or action', 'action':action, 'positionId': positionId, 'qty': amount, 'qty': amount, 'symbol': symbol, 'cost':cost, 'price': price, 'time': time})
        print (f"  {activityId} {assetType}  {positionId} {time} {action} {symbol} {amount} {price} {cost} , {totAmount:.2f}")
        time = time.replace(r"+0000",'')
        if ( assetType == 'OPTION'):
          mo = re.search(r'(\w+)\s', symbol)
          if ( mo == None):
            re.search(r'/(\w+)(\D)(\d+)_', symbol)
          if ( mo == None) :
            mo = re.search(r'/(\w+)(\D)(\d+):', symbol)   
          if ( mo != None ):
            ticker = mo.group(1)
          else:
            ticker = symbol

        elif ( assetType == 'FUTURE'):
          #mo = re.search(r'/(\D+)(\D)(\d+):', symbol)
          mo = re.search(r'/(\w+)(\D)(\d+):', symbol)
          
          if ( mo == None):
            re.search(r'/(\w+)(\D)(\d+)_', symbol)
          ticker = mo.group(1)
          if ( mo != None ):
            ticker = mo.group(1)
          else:
            ticker = symbol
        else:
          ticker = symbol
        if ( positionId == None):
          sql = (f"INSERT INTO MyTrades(TradeId, Ts, Command, AssetType, Ticker, Symbol, Qty, Price)" 
          f" VALUES( {activityId} , '{time}', '{action}', '{assetType}', '{ticker}', '{symbol}', {amount} ,{price})")
        else:

          sql = (f"INSERT INTO MyTrades(TradeId, PositionId, Ts, Command, AssetType, Ticker, Symbol, Qty, Price)" 
              f" VALUES( {activityId} , {positionId} , '{time}', '{action}', '{assetType}', '{ticker}', '{symbol}', {amount} ,{price})")
          #continue
        try:
          db.queryDbSql(sql, True)
        except Exception as err:
          print (err)

 

  #print (json.dumps(positions, indent=2))
  #print (json.dumps(specials, indent=2))

  print ("Cash Flow =", totAmount)

def displayAndSavePositions(rtn):
  
  currPos = getMyPositionsFromDb()

  rtn = rtn[0]
  for key in rtn:
      print (key)
  positions=rtn['securitiesAccount']['positions']
  for key in rtn['securitiesAccount']:
      print (key)
  cashVal = rtn['securitiesAccount']['currentBalances']['cashBalance']
  cnt = 0
  val = 0
  etfVal = optionVal = stockVal = fundVal = otherVal = 0
  etfDay = optionDay = stockDay = fundDay = otherDay = 0
  storedPositions={}
  totalOptionMaintenanceRequirement:float = 0
  
  for pos in positions:
    dbP = alphaDb.MyPosition()
    ins=pos['instrument']
    if ( cnt < 3):
      print ( json.dumps(pos, indent=2))
    assetType = ins['assetType']
    maintenanceRequirement = pos['maintenanceRequirement']
    if ( ins['assetType'] == 'OPTION'):
      totalOptionMaintenanceRequirement+= float(maintenanceRequirement)
    print (f"{ins['assetType']}\t{ins['symbol']}\t{pos['longQuantity']-pos['shortQuantity']}\t{pos['marketValue']}\t{pos['currentDayProfitLoss']:.2f}\t[{maintenanceRequirement}]")
    dbP.Symbol =  ins['symbol']

    if ( dbP.Symbol in currPos):
      dbP.Tag = currPos[ dbP.Symbol ].Tag
      dbP.Beta= currPos[ dbP.Symbol ].Beta
      dbP.Sector=currPos[ dbP.Symbol ].Sector
      dbP.Industry=currPos[ dbP.Symbol ].Industry 
      dbP.Category = currPos[ dbP.Symbol ].Category
      
      #dbP.Signal = currPos[ dbP.Symbol ].Signal
      
   
    dbP.Qty = pos['longQuantity']-pos['shortQuantity']
    dbP.AveragePrice = pos['averagePrice']
    dbP.CurrentPrice = pos['marketValue']/ dbP.Qty
    dbP.Ticker = ins['symbol']
    dbP.MaintenanceMargin = pos['maintenanceRequirement']
    dbP.CurrentDayProfitLoss = pos['currentDayProfitLoss']
    if ( assetType == 'OPTION' ):
      rtnM = re.match(r'(\w+)\s+(\d\d)(\d\d)(\d\d)(\w)(\d{8})', ins['symbol'])
      if (rtnM != None):
        dbP.CurrentPrice = dbP.CurrentPrice/100
        dbP.Ticker = rtnM.group(1)
        optYear = int(rtnM.group(2))
        optMonth = int(rtnM.group(3))
        optDay = int(rtnM.group(4))
        dbP.Expiration=datetime(optYear+2000,optMonth,optDay)
        dbP.CallPut = rtnM.group(5)
        dbP.Strike = float(rtnM.group(6))/1000
        #print (vars(dbP))

    match ins['assetType']:
      case 'COLLECTIVE_INVESTMENT':
        etfVal += pos['marketValue']
        etfDay += pos['currentDayProfitLoss']
        dbP.AssetType = 'ETF'
      case 'OPTION':
        optionVal += pos['marketValue']
        optionDay += pos['currentDayProfitLoss']
        dbP.AssetType = 'OPTION'
      case 'EQUITY':
        stockVal+= pos['marketValue']
        stockDay += pos['currentDayProfitLoss']
        dbP.AssetType = 'EQUITY'
      case  'MUTUAL_FUND':
        fundVal += pos['marketValue']
        fundDay += pos['currentDayProfitLoss']
        dbP.AssetType = 'MUTUAL_F'
      case _:
        otherVal += pos['marketValue']
        otherDay += pos['currentDayProfitLoss']
        dbP.AssetType = ins['assetType']
    storedPositions[dbP.Symbol] = dbP
    val += pos['marketValue']

    cnt +=1
  removeMyDbPositions()
  saveMyDbPositions(storedPositions)
  print (f'\nTotal  value ={val+cashVal:.02f}')
  print (f'Cash         value = {cashVal:.02f}')
  print (f'Total asset value ={val:.02f}')
  print (f'Total ETF    value ={etfVal:.02f}\tchange={etfDay:.02f}') 
  print (f'Total OPTION value ={optionVal:.02f}\tchange={optionDay:.02f}')
  print (f'Total STOCK  value ={stockVal:.02f}\tchange={stockDay:.02f}')
  print (f'Total FUND   value ={fundVal:.02f}\tchange={fundDay:.02f}')
  print (f'Total OTHER  value ={otherVal:.02f}\tchange={otherDay:.02f}')
  print (f'Cash         value = {cashVal:.02f}')
  print (json.dumps(rtn['aggregatedBalance']))
  print (f'Calculated total = {etfVal+optionVal+stockVal+fundVal+otherVal+cashVal:.02f}')
  print (f"Total MaintenanceRequirement = {totalOptionMaintenanceRequirement:02f}")


class myPos:

  PositionId:int=None
  AssetType:str=None
  Ticker:str=None
  Symbol:str=None
  OpenDate:datetime=None
  OpenAmt:float=None
  OpenQty:float=None
  CloseDate:datetime=None
  CloseQty:float=0
  CloseAmt:float=0
  Tag:str=None
def printIt(*args):
   #print (*args)
   pass

def repairPositionId2():
  db = alphaDb()
  db2= alphaDb()
  cnt = 1
  rtn = db.queryDbSql( r" SELECT max(PositionId) as idMax from  MyTrades where Flag2 ='fix2' and PositionId<10000"   )
  for row in rtn:
    cnt=row.idMax + 1
  rtn = db.queryDbSql( " SELECT * from  MyTrades where PositionId is NULL "   )
  hash={}
 
  for row in rtn:
    symbol = row.Symbol
    positionIdTmp = 0
    if (symbol in hash):
      positionIdTmp = hash[symbol]
    else:
      positionIdTmp = cnt
      cnt +=1
      hash[symbol] = positionIdTmp
    print (symbol, positionIdTmp)
    sql =  f"UPDATE MyTrades SET PositionId={positionIdTmp}, Flag2='fix2' where TradeId={row.TradeId}"
    db2.queryDbSql(sql, True)

    
## somethimes Schwab transaction API does not provide positionId. I need to use the positionID of the 
# open postions in the database to match the positionId
def repairPositionId():
  db = alphaDb()
 
  pos={}
  rtn = db.queryDbSql( " SELECT * from  MyTrades where PositionId is NULL and Command='CLOSE' "   )
  rows=[]
  for row in rtn:
    rows.append(row)

  for row in rows:
    symbol = row.Symbol
    tradeId=row.TradeId
    cnt = 0
    rtn = db.queryDbSql( f" SELECT * from  MyPos where Symbol='{symbol}' and (OpenQty+CloseQty<>0 or CloseQty is NULL) "   )
    for rr in rtn:
      print (str(rr))
      cnt +=1
      positionId = rr.PositionId
    if ( cnt == 1) :  ## unique
      sql =  f"repairPositionId: UPDATE MyTrades SET PositionId={positionId}, Flag2='fix' where TradeId={tradeId}"
      db.queryDbSql(sql, True)
    elif ( cnt > 1):
      print (f"repairPositionId: multiple ({cnt}) matches found for symbol {symbol}")
    else:
      print (f"repairPositionId: No matche was found for symbol {symbol}")

def updateClosedTrades():


  db = alphaDb()
  db2 = alphaDb()
  pos={}
  rtn = db.queryDbSql( " SELECT * from  MyPos"   )
  for row in rtn:
    posId = row.PositionId
    pos[posId] = row

  rtn = db.queryDbSql( " SELECT * from  MyTrades where  PositionId is not NULL AND Flag1 is NULL and ( Command='CLOSE' or Command='EXPIRE') order by Ts"   )
  cnt = 1
  for row in rtn:
    print (f"{cnt}:Add close entry PositionId={row.PositionId}, ts={row.Ts} qty={row.Qty}")
    cnt += 1
    qty = int(row.Qty)
    price = row.Price
    if ( qty != None):
      amount = -qty * price
    else:
      raise(Exception(row))
    if ( row.AssetType=='OPTION'):
      amount =int(amount*100)
    #db2.createPositionEntry(row.AlertId, row.Ts, row.Ticker, row.Asset, row.BuyPower, row.Amount, row.Trader)
    #db2.updateAlertFlag(row.AlertId, 'Open')
    print (f"{row.PositionId} {row.Ts} {amount}")
    if ( row.PositionId in pos):
      p = pos[row.PositionId ]
      if ( p.CloseQty == None): 
        p.CloseQty = row.Qty
      else:
        p.CloseQty += row.Qty
      
      if (  p.CloseAmt == None):
        p.CloseAmt = amount
      else:
        p.CloseAmt += amount
      sql = f" UPDATE MyPos SET CloseDate='{row.Ts}', CloseAmt={p.CloseAmt}, CloseQty={p.CloseQty}  WHERE PositionId={row.PositionId}" 
      #print (sql)
      db2.queryDbSql( sql, True)  
                     
      
     
    else:
      print (f"Could not find the open position for this close trade {row.PositionId}")
      continue
    
      sql = (" INSERT INTO  MyPos(PositionId, AssetType, Ticker, Symbol,  CloseDate, CloseAmt, CloseQty) "
                       f"VALUES({row.PositionId}, '{row.AssetType}', '{row.Ticker}', '{row.Symbol}', '{row.Ts}', {amount},{row.Qty})")
      db2.queryDbSql(sql, True)
      #print (sql)
      posEn=myPos()
      posEn.PositionId = row.PositionId
      posEn.CloseAmt = amount
      posEn.CloseDate = row.Ts
      posEn.CloseQty = row.Qty
      posEn.AssetType=row.AssetType
      pos[row.PositionId] = posEn

      #db2.queryDbSql( sql, True  )
      
    db2.queryDbSql( f" UPDATE MyTrades SET Flag1='STORED' where TradeId={row.TradeId}", True   )
  print ("Completed creating close entries")

def createOpenEntries():


  db = alphaDb()
  db2 = alphaDb()
  pos={}
  rtn = db.queryDbSql( " SELECT * from  MyPos"   )
  for row in rtn:
    posId = row.PositionId
    pos[posId] = row

  rtn = db.queryDbSql( " SELECT * from  MyTrades where PositionId is not NULL and Flag1 is NULL and Command='OPEN' order by Ts"   )
  cnt = 1
  for row in rtn:

    qty = int(row.Qty)
    price = row.Price
    

    if ( qty != None):
      amount = -qty * price
    else:
      raise(Exception(row))
    if ( row.AssetType=='OPTION'):
      amount =int(amount*100)
    #db2.createPositionEntry(row.AlertId, row.Ts, row.Ticker, row.Asset, row.BuyPower, row.Amount, row.Trader)
    #db2.updateAlertFlag(row.AlertId, 'Open')
    print (f"{cnt}: Add open entry PositionId={row.PositionId}, ts={row.Ts} amount={amount}, OpenQty={row.Qty} ")
    cnt += 1
    if ( row.PositionId in pos):
      p = pos[row.PositionId ]
       
      p.OpenQty += row.Qty
      p.OpenAmt += amount

      db2.queryDbSql( f" UPDATE MyPos SET OpenAmt={p.OpenAmt}, OpenQty={p.OpenQty}  WHERE PositionId={row.PositionId}", True)  
                     
      
     
    else:
      sql = (" INSERT INTO  MyPos(PositionId, AssetType, Ticker, Symbol,  OpenDate, OpenAmt, OpenQty, CloseAmt, CloseQty) "
                       f"VALUES({row.PositionId}, '{row.AssetType}', '{row.Ticker}', '{row.Symbol}', '{row.Ts}', {amount},{row.Qty},0,0)")
      db2.queryDbSql(sql, True)
      #print (sql)
      posEn=myPos()
      posEn.PositionId = row.PositionId
      posEn.OpenAmt = amount
      posEn.OpenDate = row.Ts
      posEn.OpenQty = row.Qty
      posEn.AssetType=row.AssetType
      pos[row.PositionId] = posEn

      #db2.queryDbSql( sql, True  )
      
    db2.queryDbSql( f" UPDATE MyTrades SET Flag1='STORED' where TradeId={row.TradeId}", True   )
  print ("Completed creating open entries")

def updateAdjustTrades():
  db = alphaDb()
  db2 = alphaDb()
  rtn = db.queryBaAlerts(
  " Command='Adjust' and AlertId is not null   and Flag1 is  null  "
  # " And AlertId <>'#UgIkyyEK' "
  )
  adjusts={}

  for row in rtn:
    amountSide = row.AmountSide

    if ( amountSide != None):
      amountSide = amountSide.rstrip()
    else:
      raise(Exception(row))
    amt = row.Amount

    alertId = row.AlertId.rstrip()
    if ( alertId in adjusts ):
      adjust = adjusts[alertId]
      adjust['cnt'] += 1
      adjust['amount'] += amt
      adjusts[alertId] = adjust
    else:
      adjust={}
      adjust['cnt'] = 1
      adjust['amount'] = amt
      adjusts[alertId] = adjust
    
  ### check the existing records to see if there was any adjustment already
  
  rtn = db.queryBaPositions(
  " AdjustCnt is not null  "
  # " And AlertId <> '#UgIkyyEK' "
  )
 
  for row in rtn:
    alertId = row.AlertId.rstrip()
    if ( alertId in adjusts ):
      adjust = adjusts[alertId]
      adjust['cnt'] += row.AdjustCnt
      adjust['amount'] += row.AdjustAmt
      adjusts[alertId] = adjust

  cnt = 1
  for alertId in adjusts:
    print (f"updating entry {cnt} {alertId}")
    cnt += 1
    if ( adjusts[alertId]['cnt'] > 0 ):
      print (f"{alertId}\t{adjusts[alertId]['cnt']}\t{adjusts[alertId]['amount']}")   
      db2.updatePositionAdjustEntry(alertId, adjusts[alertId]['cnt'], adjusts[alertId]['amount'] )
    db2.updateAlertFlag(alertId, 'Adjust')

  print ("Completed updating adjust entries")

def copyTradesToPos():
  
  #updateBuyPower()
  repairPositionId()
  createOpenEntries()
  repairPositionId2()
 


  updateClosedTrades()



if __name__ == '__main__':
    api = SchwabAPI()
    c = api.getClient()

    #r =c.get_account_numbers( )
    #print(json.dumps(r.json(), indent=4))
    today = datetime.today()
    tomorrow= today+timedelta(days=1)
    year = tomorrow.year
    month = tomorrow.month
    day = tomorrow.day

    # Get yesterday's date
    yesterday = today - timedelta(days=4)
    yesterday_year = yesterday.year
    yesterday_month = yesterday.month
    yesterday_day = yesterday.day
    endDate = datetime(year,month,day,5,0,0)
    startDate = datetime(yesterday_year,yesterday_month,yesterday_day,5,0,0)
    #startDate = datetime(2024,9,20,5,30,0)
    #endDate = datetime(2024,9,24,21,0,0)
    fetchTransaction(startDate, endDate)
    copyTradesToPos()
   
    #r=c.get_transactions('47B5B8D53FE71EE00A47CCF5C0A97B8D1E090D916B692467A002B9CBC802D574',start_date=startDate, end_date=endDate)
    #print(json.dumps(r.json() ),"\n")
     
    r =c.get_accounts(fields=[c.Account.Fields.POSITIONS])

    r.raise_for_status()
    displayAndSavePositions(r.json())
    #exit()
    
    print ("OK")
    print ("Press any key to continue>>>>>")
    x = input()
    #r=c.get_option_chain('AAPL')
    #print (json.dumps(r.json()))
    #exit()
    #acct = '47B5B8D53FE71EE00A47CCF5C0A97B8D1E090D916B692467A002B9CBC802D574'
    #r = c.get_orders_for_account(acct)
    
    for i in range(0,100):
      #r = c.get_quotes(['AAPL', '/ES', '/MES', 'AAPL  270115P00310000', 'SPY   241209P00600000'], fields=[c.Quote.Fields.QUOTE])
      #r = c.get_quotes(['AAPL', '/ES', '/MES', s], fields=[c.Quote.Fields.QUOTE])
      #rtn = r.json()

      #for ticker in rtn:
      #  en = rtn[ticker]
      #  quote = en['quote']
      #  ask = quote['askPrice']
      #  last = quote['lastPrice']
      #  bid = quote['bidPrice']
      #  print (f"{ticker}\t{last}\t{bid}\t{ask}")

      #print (json.dumps(r.json(), indent=2))
      symbols = ['AAPL', '/ES', '/MES', s]
      symbols=['USO   241206P00070000', 'USO   241211P00070000']
      rtn = api.getQuotes(symbols)
      print (json.dumps(rtn))
      time.sleep(2)
   
