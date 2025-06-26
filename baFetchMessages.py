from telethon.sync import TelegramClient
from telethon.tl.types import PeerChannel
import pytz
import asyncio
import re
from alphaCommon import alphaDb, TradeStation, MyTradierAPI
import json
from baCommon import BaAlert, BaAlertProc
from datetime import datetime
from SchwabApi import SchwabAPI
phone_number = '+16092035951'



api_id = 26644246
api_hash = '7e3011f886ddf229150f25087c19b0a0'
session_name = 'my_session'
chat = 'me'
private_channel_id = 1557173806
#tsApi = TradeStation()
#tdApi = MyTradierAPI()
quoteApi = SchwabAPI()
g_saveToDb = True

async def preProcessAlert2(alert:BaAlert):
    print (f"{alert.AlertId } {alert.Asset} {alert.Ticker} by {alert.Trader}")
    symbols=[]
    details = json.loads(alert.TradeDetails)
    ticker = alert.Ticker

    if ( alert.Asset == 'Futures'):
        if ( ticker == 'MNQ' or ticker == 'NQ'):
            symbols = ['QQQ','TQQQ', 'SQQQ']
            details['legs']['0']['symbol'] = 'QQQ'
        elif ( ticker == 'ES' or ticker == 'MES'):
            symbols = ['SPY','SSO']
            details['legs']['0']['symbol'] = 'SPY'
        else:
            return
        
    elif ( alert.Asset == 'Stock'): 
        symbols=[alert.Ticker]
        details['legs']['0']['symbol'] = alert.Ticker
    elif ( alert.Asset == 'Options'): 


        for n in details['legs']:
            leg = details['legs'][n]
            name = leg['name']
            if ( name == 'Call'):
                name = 'C'
            else:
                name = 'P'
            strike = leg['strike']
            if (strike == int (strike)):
                strike = int(strike)
            elif ( strike*10 == int(strike*10)):
                strike = int(strike*10)/10
            dateObj = leg['date'].split('/')
            dateObj = datetime.strptime(leg['date'], '%m/%d/%y')
            symbol =tdApi.computeOptionSymbol(ticker, dateObj, leg['name'], leg['strike'])
            
            # symbol =f"{ticker}%20{int(dateObj[2]):02}{int(dateObj[0]):02}{int(dateObj[1]):02}{name}{strike}"
            price = leg['price']

            symbols.append(symbol)
            leg['symbol'] = symbol
            print (f"leg{n}: {leg['action']}  {leg['qty']} {leg['date']} {leg['name']} {strike} ")

    #quotes = tsApi.getQuotes(symbols)
    quotes = tdApi.getQuotes(symbols)
    marketData={}
    for quote in quotes:
        #symbol = quote["Symbol"]
        #print (f'{symbol} {quote["Last"]} {quote["Bid"]} {quote["Ask"]}')
        #marketData[symbol.replace(' ', '%20')] = {'Last':quote["Last"], 'Bid': quote["Bid"], 'Ask': quote["Ask"] }
        symbol = quote["symbol"]
        print (f'{symbol} {quote["last"]} {quote["bid"]} {quote["ask"]}')
        marketData[symbol.replace(' ', '%20')] = {'Last':quote["last"], 'Bid': quote["bid"], 'Ask': quote["ask"] }
    amt = 0
    for n in details['legs']:
        leg = details['legs'][n]
        action = leg['action']
        symbol = leg['symbol']
        if ( 'qty' in leg):
            qty = leg['qty']
        else:
            qty = 1

        if ( alert.Asset=='Futures' or alert.Asset== 'Stock'):
            if ( action == 'Buy'):
                calPrice = marketData[symbol]['Ask']
                amt -= float(qty)*float(calPrice)
            else:
                calPrice = marketData[symbol]['Bid']
                amt += float(qty)*float(calPrice)
        elif (len(details['legs'])> 1 and alert.Asset):
            calPrice = (float(marketData[symbol]['Ask']) + float(marketData[symbol]['Bid']) )/2
            if ( action == 'Buy'):
                amt -= float(qty)*float(calPrice)
            else:
                amt += float(qty)*float(calPrice)
        else:
            if ( action == 'Buy'):
                calPrice = marketData[symbol]['Ask']
                amt -= float(qty)*float(calPrice)
            else:
                calPrice = marketData[symbol]['Bid']
                amt += float(qty)*float(calPrice)

    print (f"The calculated amount for this transaction is  {amt:.2f}. Alert amt is {alert.Amount}")


async def preProcessAlert(alert:BaAlert):
    print (f"{alert.AlertId } {alert.Asset} {alert.Ticker} by {alert.Trader}")
    symbols=[]
    details = json.loads(alert.TradeDetails)
    ticker = alert.Ticker

    if ( alert.Asset == 'Futures'):
        symbol = '/'+ticker
        if ( ticker == 'MNQ' or ticker == 'NQ'):
            symbols = [symbol, 'QQQ','TQQQ', 'SQQQ']
        elif ( ticker == 'ES' or ticker == 'MES'):
            symbols = [symbol, 'SPY', 'SPXL']
        else:
            symbols=[symbol]
        details['legs']['0']['symbol'] = symbol
    elif ( alert.Asset == 'Stock'): 
        symbols=[alert.Ticker]
        details['legs']['0']['symbol'] = alert.Ticker
    elif ( alert.Asset == 'Options'): 
        for n in details['legs']:
            leg = details['legs'][n]
            name = leg['name']
            if ( name == 'Call'):
                name = 'C'
            else:
                name = 'P'
            strike = leg['strike']
            if (strike == int (strike)):
                strike = int(strike)
            elif ( strike*10 == int(strike*10)):
                strike = int(strike*10)/10
            dateObj = leg['date'].split('/')
            dateObj = datetime.strptime(leg['date'], '%m/%d/%y')
            symbol =quoteApi.computeOptionSymbol(ticker, dateObj, leg['name'], leg['strike'])
            
            # symbol =f"{ticker}%20{int(dateObj[2]):02}{int(dateObj[0]):02}{int(dateObj[1]):02}{name}{strike}"
            price = leg['price']

            symbols.append(symbol)
            leg['symbol'] = symbol
            print (f"leg{n}: {leg['action']}  {leg['qty']} {leg['date']} {leg['name']} {strike} ")

    #quotes = tsApi.getQuotes(symbols)
    quotes = quoteApi.getQuotes(symbols)
    marketData={}
    for quote in quotes:
        #symbol = quote["Symbol"]
        #print (f'{symbol} {quote["Last"]} {quote["Bid"]} {quote["Ask"]}')
        #marketData[symbol.replace(' ', '%20')] = {'Last':quote["Last"], 'Bid': quote["Bid"], 'Ask': quote["Ask"] }
        symbol = quote["symbol"]
        print (f'{symbol} {quote["last"]} {quote["bid"]} {quote["ask"]}')
        marketData[symbol] = {'Last':quote["last"], 'Bid': quote["bid"], 'Ask': quote["ask"] }
    amt = 0
    for n in details['legs']:
        leg = details['legs'][n]
        action = leg['action']
        symbol = leg['symbol']
        if ( 'qty' in leg):
            qty = leg['qty']
        else:
            qty = 1
        
        try:

            if ( alert.Asset=='Futures' or alert.Asset== 'Stock'):
                symbolFromRtn:str =  next(iter( marketData))
                if (symbolFromRtn[:3] == symbol[:3] ):
                    symbol  = symbolFromRtn
                    calPrice = marketData[symbol]['Ask']
                    amt -= float(qty)*float(calPrice)
                else:
                    calPrice = marketData[symbol]['Bid']
                    amt += float(qty)*float(calPrice)
            elif (len(details['legs'])> 1 and alert.Asset):
                calPrice = (float(marketData[symbol]['Ask']) + float(marketData[symbol]['Bid']) )/2
                if ( action == 'Buy'):
                    amt -= float(qty)*float(calPrice)
                else:
                    amt += float(qty)*float(calPrice)
            else:
                if ( action == 'Buy'):
                    calPrice = marketData[symbol]['Ask']
                    amt -= float(qty)*float(calPrice)
                else:
                    calPrice = marketData[symbol]['Bid']
                    amt += float(qty)*float(calPrice)
        except Exception as err:
            print ('ERROR:', err)

    print (f"The calculated amount for this transaction is  {amt:.2f}. Alert amt is {alert.Amount}")

async def main():

    EST = pytz.timezone("US/Eastern")
    db = alphaDb()
    idHash={}

    client = TelegramClient(session_name, api_id, api_hash)
    
    await client.start(phone_number)  # start the client

    # Check if the user is already authorized, otherwise prompt the user to authorize the client
    if not await client.is_user_authorized():
        await client.send_code_request(phone_number)
        await client.sign_in(phone_number, input('Enter the code: '))


    if (   g_saveToDb):
        rows = db.queryBaMessages("date>'2024-12-01' order by Date desc  ")
        for row in rows:
            print (row.Id, row.Date)
            idHash[row.Id] = 1
 

    # Get Chat Messages
    #messages = client.get_messages(chat)
    #for message in client.iter_messages(chat):
    #    print(message.sender_id, ':', message.text)

    # Get Private Channel Messages
    cnt = 1
    messages = await client.get_messages(PeerChannel(channel_id=private_channel_id))

    async for message in client.iter_messages(PeerChannel(channel_id=private_channel_id)):
        if ( cnt > 300):
            pass
            break
        cnt +=1
        if ( re.match(r"\* Reminder", message.message, re.IGNORECASE) != None ):
            continue
        ts = message.date.astimezone(EST).strftime('%Y-%m-%d %H:%M:%S')
        msg = message.message[:1999]
        try :
            if ( g_saveToDb):
                if ( message.id in idHash):
                    break
                db.createBaMsgEntry(message.id, ts, msg)
            else:
                alert:BaAlert = BaAlertProc.convertMsgToAlert(message.id, ts, msg)
                if ( alert == None):
                    print (f"ERROR: Could not convert the message to Alarm {message.id}: {msg}")
                    continue
 
                await preProcessAlert(alert)
            print (message.id, " ", message.date.astimezone(EST).strftime('%Y-%m-%d %H:%M:%S'))# , message.message[:45])
            print ("")
        except Exception as err:
            print (err)
            return
        #print (message)
        #print(message.sender_id, ':', message.text)
        #exit()
'''
    # Get Public Channel Messages
    messages = client.get_messages('Public_Channel_Name', limit=2)
    for message in client.iter_messages('Public_Channel_Name'):
        print(message.sender_id, ':', message.text)
'''


if __name__ == '__main__':
    asyncio.run(main())