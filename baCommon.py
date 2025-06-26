
import json
from datetime import datetime, timedelta, timezone
import sys
import re

class BaAlert:
    def __init__(self):
        self.MessageId=None
        self.Command=None
        self.AlertId= None
        self.Asset= None
        self.Ticker= None 
        self.TradeSide= None 
        self.Ts= None 
        self.TradeDetails= None 
        self.AmountSide= None 
        self.Amount= None 
        self.BuyPower= None 
        self.ReturnPct = None 
        self.Trader = ''
        self.Comments = ''

 
def printIt( *args):
  #print (*args)
  pass

class BaAlertProc:

  @staticmethod  
  def checkAmt( alert:BaAlert)->BaAlert:
    #asset, tradeDetails, ):
    asset = alert.Asset
  
  # if ( asset != 'Stock' and asset != 'Futures'):
  #  return alert
    #print (row)
  
    tradeDetails = json.loads(alert.TradeDetails.rstrip())
    legs = tradeDetails['legs']
  
    totAmt = 0
    for legName in legs:
      leg = legs[legName]
      qty:float = 1
      if ('qty' not in leg ):
        leg['qty'] = qty
      elif (leg['qty'] == 0 ):
        leg['qty'] = 1
      else:
        qtyStr:str = f"{leg['qty']}"
      
        if ( qtyStr[-1] == '%' ):
          qtyStr = qtyStr.replace('%','')
          qty = float(qtyStr)/100
        else:
          qty = float(qtyStr)
        leg['qty'] = qty
      
      amt = abs( leg['price'] * qty )

      if ( leg['action'] == 'Buy' or   leg['action'] == 'Long' ):
        amt = - amt
      totAmt += amt

    if ( totAmt > 0):
      amtSide = 'Credit'
    else:
      amtSide = 'Debit'
  
    if ( alert.Amount is not None):
      alert.Amount = float(alert.Amount)
      ## verify if the legs amout match the alert
      if ( ( abs(int(alert.Amount*100)) > abs(int(totAmt*100) ) + 2  ) or  
          ( abs(int(alert.Amount*100)) < abs(int(totAmt*100) ) - 2  ) ) :
        raise Exception (f"Error found! Msg {alert.MessageId} amount calucated is {totAmt} but amount in message is {alert.Amount}." + alert.TradeDetails)
      if ( amtSide == 'Credit'):
        alert.Amount = abs(alert.Amount)
      else:
        alert.Amount = -abs(alert.Amount)
    else:
      alert.Amount = totAmt
    if ( alert.AmountSide is not None):
      if ( alert.AmountSide != amtSide and int(100*totAmt) !=0):
        print (f"Error found! Msg {alert.MessageId} calculated {totAmt} Name is {amtSide} but name in message is {alert.AmountSide}." + alert.TradeDetails ) 

    alert.AmountSide = amtSide
    return alert



  @staticmethod
  def convertMsgToAlert (msgId:int, msgTs:datetime, msg:str)-> BaAlert:
    if ( not re.match('^BUYALERTS', msg)):
      return None

    tradeId=None
    legs={}
    trade={'legs': legs}
    adj = 0
    lines = msg.split("\n")

    if (groups := re.match(r'^Trade ID (\S+)', lines[1] ) ):
      tradeId = groups[1]
      adj = 1
    cmd = lines[1 + adj].rstrip()
    if ( groups:=re.match(r'^(\w+)\s', cmd)):
      cmd = groups[1]
    action = None
    leg = None
    stockPrice = None
    debitCredit = None
    amt = None
    comments = ""
    ticker = None 
    ticker 
    buyPower = None
    rtnPct = None 
    publisher=""

    if ( cmd != 'Open' and cmd != 'Close' and cmd != 'Adjust'): 
      return None

    #print (row.Date, " ",  msg)
    printIt ("-----------------------------------------", msgId)
    alertTitleTxt = lines[0]
    publisherTxt = lines[2+adj]
    assetTxt = lines[3+adj]
    
    if ( groups := re.match(r'^BUYALERTS: (\w+) (\w+)',alertTitleTxt ) ):
      action = groups[1]
      ticker = groups[2]
      if ( action == 'Pair'):
        return None ## ignore pair trades
    if ( groups := re.match(r'Published by: (.*)',publisherTxt ) ):
      publisher = groups[1]
      #print (publisher)
    if ( groups := re.match(r'Asset: (.*?)\s',assetTxt ) ):
      asset = groups[1]
      if ( asset == 'Crypto'):
        printIt ("Ignore Crypto")
        return None
      #print (asset)
    for i in range(4+adj, len(lines)):
      line = lines[i]
      if ( groups := re.match(r'Net Open (\w+): (.*)$', line) ) :
        debitCredit = groups[1]
        amt:str = groups[2]
        amt = amt.replace('$','')
        printIt (f"####### {debitCredit} {amt}")
      elif ( groups := re.match(r'Estimated Buying Power: \$(\S+)$', line) ):
        buyPower:str  = groups[1]
        buyPower = buyPower.replace(',','')
        printIt (f"BBBBB   {buyPower}")
      elif ( groups := re.match(r'L(\d+): (\w+) (\w+) (\S+) (\S+) \@ (\S+)', line) ):
        leg:str  = groups[1]
        optActName:str = groups[2]
        #optQty:str = groups[3]
        optName:str = groups[3]
        strike:str = groups[4]
        dateStr:str = groups[5]
        priceStr:str = groups[6]
        optQty = 1
        if ( subGroups:=re.match(r'(\d+)(\s*)(\w+)', optName)):
          optQty=subGroups[1]
          optName=subGroups[3].replace('x','').replace('X','')
        printIt (f"XXX   {leg} {optActName} {optName} {strike} {dateStr} {priceStr}")
        priceStr = priceStr.replace('$','').replace(',','').replace('&#039;','.')
        legs[leg] = { 'action': optActName,  'qty':optQty, 'name': optName, 'strike': float(strike), 'date': dateStr, 'price': float(priceStr) }
      elif ( groups := re.match(r'L(\d+): (\w+) (\S+) (\w+) (\S+) (\S+) \@ (\S+)', line) ):
        leg:str  = groups[1]
        optActName:str = groups[2]
        optQty:str = groups[3]
        optQty = optQty.replace('x','').replace('X','')
        if ( subgroups:= re.match(r'(.*)\%', optQty) ):
          qptPctNum = subgroups[1] 
          optQty = float(int(qptPctNum)/100)
        else:
          optQty = int(optQty)
        optName:str = groups[4]

        strike:str = groups[5]
        dateStr:str = groups[6]
        priceStr:str = groups[7]
        priceStr = priceStr.replace('$','').replace(',','')
        legs[leg] = { 'action': optActName, 'qty':optQty, 'name': optName, 'strike': float(strike), 'date': dateStr, 'price': float(priceStr) }
        printIt (f"XXXXX   {leg} {optActName} qty={optQty} {optName} {strike} {dateStr} {priceStr}")
      elif ( groups:= re.match( r'^Stop Loss: (\S+)', line ) ) : 
        stopLoss:str = groups[1]
        trade['Stop'] = float(stopLoss.replace('$','').replace(',',''))

        printIt("SSSTOP Loss:", trade['Stop'])
      elif ( groups:= re.match( r'^Target (Price|Points|Point): (\S+)', line ) ) : 
        target:str = groups[2]
        trade['Target'] = float(target.replace('$','').replace(',',''))

        printIt("TTTarget ", trade['Target'])
      elif ( groups:= re.match( r'^Stop Loss: (\S+)', line ) ) : 
        stopLoss:str = groups[1]
        trade['Stop'] = float(stopLoss.replace('$','').replace(',',''))

        printIt("SSSTOP Loss:", trade['Stop'])

      elif ( groups:= re.match( r'^Comments: (.*)', line ) ) : 
        comments += groups[1]+"\r\n"
        #     |(Target Price)|(Comments))', line) ):
      elif (  asset == 'Stock' or asset == 'Futures' ):
        if (groups := re.match(r'(\w+) (\w+) \@ (\S+)', line)  ):
            
            stockAction = groups[1]
            stockName = groups[2]
            stockPrice = groups[3]
            stockPrice = stockPrice.replace('$','').replace(',','').replace('&#039;','.')
            legs['0'] =  { 'action': stockAction,  'qty': 1, 'name': stockName, 'price': float(stockPrice) }
        elif (groups := re.match(r'(\w+) (\S+) (\w+) \@ (\S+)', line)   ):
            stockAction = groups[1]
            stockQty = groups[2]
            if ( stockQty[-1] == '%' ):
              stockQty = stockQty.replace('%','')
              qty = float(stockQty)/100
            else:
              qty = float(stockQty)

            stockName = groups[3]
            stockPrice = groups[4]
            stockPrice = stockPrice.replace('$','').replace(',','').replace('&#039;','.')
            legs['0'] =  { 'action': stockAction, 'qty':  qty, 'name': stockName, 'price': float(stockPrice) }
      elif ( cmd == 'Close' ):
        if ( groups := re.match(r'Net Close (\w+): (\S+)', line) ) :
          debitCredit = groups[1]
          amt = groups[2]
          amt = amt.replace('$','').replace(',','').replace('&#039;','.')
          printIt (f'cccccccccccc {debitCredit} {amt}')
        elif ( groups := re.match(r'Return: (\S+)\%', line) ) :
          rtnPct = groups[1]
          #print (f'rrrrrrrrr  {rtnPct}')
      elif ( cmd == 'Adjust'):
        if ( groups := re.match(r'Net Adjust (\w+): (\S+)', line) ) :
          debitCredit = groups[1]
          amt = groups[2]
          amt = amt.replace('$','').replace(',','').replace('&#039;','.')
          printIt (f'aaaaaaaaaa {debitCredit} {amt}')
      else:
        printIt (i, "CCCCCCC" , lines[i])
        comments += lines[i] +"\r\n"

  
    if ( cmd == 'Open'): 
        printIt (f"FFFOOO {msgId} {cmd} {asset} {ticker} {action} {tradeId} {debitCredit} {amt} {publisher} {buyPower} {comments}") 
    elif ( cmd == 'Close'): 
        printIt (f"FFFCCC {msgId} {cmd} {asset} {ticker} {action} {tradeId} {debitCredit} {amt} {publisher} {rtnPct} {comments}") 
    else:
        printIt (f"FFFAAA {msgId}  {cmd} {asset} {ticker} {action} {tradeId} {debitCredit} {amt} {publisher}  {comments}") 
    #print (f"{trade}")
    #printIt (msg)
    alert = BaAlert()
    alert.MessageId = msgId
    alert.AlertId = tradeId
    alert.Asset = asset
    alert.Ticker= ticker 
    alert.Command = cmd
    alert.TradeSide = action 
    alert.Ts= msgTs 
    alert.TradeDetails= json.dumps(trade )
    alert.AmountSide= debitCredit
    alert.Amount= amt
    alert.BuyPower= buyPower 
    alert.ReturnPct = rtnPct 
    alert.Trader = publisher
    alert.Comments = comments
    alert = BaAlertProc.checkAmt(alert)
    '''
    if ( debitCredit == None ):
        raise(Exception(msg))
    
    #print (f"{action} {ticker} {publisher} {asset}")
    if ( (action is None or leg is None) and (stockPrice is None) ):
        printIt (msg)
        raise (Exception(msg))
    '''

    return alert

