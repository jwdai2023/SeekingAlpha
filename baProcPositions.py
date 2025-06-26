
import json
from datetime import datetime, timedelta, timezone
import sys
from alphaCommon import alphaDb, BaAlert
import re
import math

def printIt(*args):
   #print (*args)
   pass

def updateClosedTrades():
  db = alphaDb()
  db2 = alphaDb()
  rtn = db.queryBaAlerts(
  " Command='Close' and AlertId is not  null  and Flag1 is null  "
  )
  cnt = 1
  for row in rtn:
    #print(row)

    print (f"Updating close entry {cnt} {row.AlertId}")
    cnt +=1
    db2.updatePositionCloseEntry(row.AlertId, row.Ts, row.Amount, row.ReturnPct)
    db2.updateAlertFlag(row.AlertId, 'Close')
  print ("Completed updating close entries")

def createOpenEntries():
  db = alphaDb()
  db2 = alphaDb()
  rtn = db.queryBaAlerts(
  " Command='Open' and AlertId is not null  and Flag1 is  null "
  )
  cnt = 1
  for row in rtn:
    print (f"create open entry {cnt}")
    cnt += 1
    amountSide = row.AmountSide
    if ( amountSide != None):
      amountSide = amountSide.rstrip()
    else:
      raise(Exception(row))
    db2.createPositionEntry(row.AlertId, row.Ts, row.Ticker, row.Asset, row.BuyPower, row.Amount, row.Trader)
    db2.updateAlertFlag(row.AlertId, 'Open')
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

#checkAmt()

#updateBuyPower()
createOpenEntries()
updateAdjustTrades()
updateClosedTrades()