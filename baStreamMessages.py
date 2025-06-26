import asyncio
import json
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient, events, utils
import sys
from alphaCommon import TradeStation, MyTradierAPI
from baCommon import BaAlert, BaAlertProc
import re
import pytz
from  SchwabApi import SchwabAPI

# Replace the values with your own API ID, API hash, and phone number

#tsApi = TradeStation()
#quoteApi = MyTradierAPI()
quoteApi = SchwabAPI()
phone_number = '+16092035951'


api_id = 26644246
api_hash = '7e3011f886ddf229150f25087c19b0a0'
session_name = 'my_session'
chat = 'me'
private_channel_id = 1557173806



# JSON file to store messages
json_file = 'messages.json'


async def save_message_to_json(client, message, group_name):
    
    
    # Append the new message
    new_message = {
        'group_name': group_name,
        'timestamp': message.date.isoformat(),
        'message': message.text
    }

    print (new_message)
    return

    try:
        # Read existing messages
        with open(json_file, 'r') as file:
            messages = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        messages = []

    messages.append(new_message)

    # Write back to the JSON file
    with open(json_file, 'w') as file:
        json.dump(messages, file, indent=4)

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
        if ( ticker == 'SPY' ):
            symbols.extend(['SPY', 'SPXL'])
        elif ( ticker == 'QQQ'):
            symbols.extend(['QQQ', 'TQQQ'])

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
    client = TelegramClient('session_name', api_id, api_hash)
    EST = pytz.timezone("US/Eastern")
    await client.start(phone_number)  # start the client

    # Check if the user is already authorized, otherwise prompt the user to authorize the client
    if not await client.is_user_authorized():
        await client.send_code_request(phone_number)
        await client.sign_in(phone_number, input('Enter the code: '))

    @client.on(events.NewMessage)
    async def handler(event):
        # Get the group name
        group_entity = await event.get_chat()
        group_name = utils.get_display_name(group_entity)
        message = event.message
        if (event.is_private or  event.chat.id != private_channel_id ):
            return
        if ( re.match(r"\* Reminder", message.message, re.IGNORECASE) != None ):
            return
        # Save the message to JSON file in real-time
        #await save_message_to_json(client, event.message, group_name)
        ts = message.date.astimezone(EST).strftime('%Y-%m-%d %H:%M:%S')
        msg = message.message[:1999]
        #db.createBaMsgEntry(message.id, ts, msg)
        #try :
        if (True):
            print ("\n\n=================================================================")
            print(msg)
            alert:BaAlert = BaAlertProc.convertMsgToAlert(message.id, ts, msg)
            if ( alert != None):
                print (alert.TradeDetails)
                await preProcessAlert(alert)

        #except Exception as err:
        #    print (f"ERROR: {err}")
        #print(f"New message saved: {event.message.text}")


    print("Listening for new messages...")
    await client.run_until_disconnected()


asyncio.run(main())
#tsApi.getQuotes(['SPX%20241203P6050'])
#tsApi.getQuotes(['QQQ'])