from schwab import client, auth
import json
from datetime import datetime
import time

class SchwabAPI:
    """
    Schwab-python is a python client for interacting with the Schwab API.
    """

    
    def __init__(self):
        self.api_key = 'oANl7y1HAr0utTsqMNn0jAQsFEE7O02w'
        self.app_secret = 'vh3e7ggWulmenxkx'
        self.callback_url = 'https://127.0.0.1:8080'
        self.token_path = 'C:\\Users\\jwdai\\AppData\\Roaming\\Trading\\Security\\Schwab\\schwab_token.json'
        #self.client =  auth.easy_client(api_key, app_secret, callback_url, token_path)
        self.client = auth.client_from_token_file(self.token_path,  self.api_key, self.app_secret)

    def getClient(self):
        return self.client
    
    def client_from_manual_flow(self):
        auth.client_from_manual_flow(self.api_key,self.app_secret, self.callback_url, self.token_path)
        
    def computeOptionSymbol(self, root:str, date:datetime, side:str, strike:float):
        root = root.upper().lstrip().rstrip()
        if ( root == 'SPX'):
            root = 'SPXW'
        dateStr = date.strftime("%y%m%d")
        strikeStr = f"{int(strike*1000):08}"
        if (side.upper()[0] == 'C'):
            sideStr = 'C'
        else:
            sideStr = 'P'
        return f"{root:6}{dateStr}{sideStr}{strikeStr}"
    
    def  getQuotes(self, symbols):
        r = self.client.get_quotes(symbols, fields=[self.client.Quote.Fields.QUOTE])
        rtn = r.json()
        quotes=[]
        try:
            for ticker in rtn:
                en = rtn[ticker]
                quote = en['quote']
                ask = quote['askPrice']
                last = quote['lastPrice']
                bid = quote['bidPrice']
                quotes.append({'symbol': ticker, 'ask':ask, 'bid':bid, 'last':last})
                #print (f"{ticker}\t{last}\t{bid}\t{ask}")
        except Exception as err:
            pass
        return quotes
    
    def  getFundamentals(self, symbols):
        r = self.client.get_quotes(symbols, fields=[self.client.Quote.Fields.QUOTE,self.client.Quote.Fields.FUNDAMENTAL])
        rtn = r.json()
        quotes=[]
        try:
            for ticker in rtn:
                en = rtn[ticker]
                quote = en['quote']
                ask = quote['askPrice']
                last = quote['lastPrice']
                bid = quote['bidPrice']
                quotes.append({'symbol': ticker, 'ask':ask, 'bid':bid, 'last':last})
                #print (f"{ticker}\t{last}\t{bid}\t{ask}")
        except Exception as err:
            pass
        return quotes