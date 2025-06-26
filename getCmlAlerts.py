import os.path
import os
import asyncio
import sys
from queue import Queue 
from threading import Thread 
import re
import pprint 
#from websockets.sync.client import connect
import websockets
import time
import logging as log
#from botDb import *
import csv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import re
import pyodbc
from datetime import datetime

import dateutil.parser as parser
import pytz
import requests
import json
import base64


class GmailHdr():
  def _getGmailHdl_(self):
 
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
  
    # If modifying these scopes, delete the file token.json.
    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"]

    opath = r'C:/Users/jwdai/AppData/Roaming/Trading/Security/Gmail/'
    tokenFile =  opath +"token.json"
    credentialFile = opath + "credentials.json"

    if os.path.exists(tokenFile):
      creds = Credentials.from_authorized_user_file(tokenFile, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    bGetNewToken = False
    if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
        try: 
          creds.refresh(Request()) 
        except:
          bGetNewToken = True
      else:
        bGetNewToken = True
    if ( bGetNewToken):
      flow = InstalledAppFlow.from_client_secrets_file(credentialFile, SCOPES)
      creds = flow.run_local_server(port=0)
      # Save the credentials for the next run
      with open(tokenFile, "w") as token:
        token.write(creds.to_json())
    if creds:
      return build("gmail", "v1", credentials=creds)


  def __init__(self):

    self.gmailHdr = self._getGmailHdl_()
    self._register_pubsub_()

  def _get_labels_(self):
    if ( not self.gmailHdr):
      return

    ## this section get labels from gmail

    results = self.gmailHdr.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])
    return labels

  def _register_pubsub_(self):
    if ( not self.gmailHdr):
      return
    ## this section register gmail as a publisher to my Google Cloud PUB/SUB 
    ## PUB/SUB expired in 7 days. Better call this everytime I login 
    request = {
    'labelIds': ['INBOX'],
    'topicName': 'projects/tradetest-406102/topics/tickeron',
    'labelFilterBehavior': 'INCLUDE'
    }
    res = self.gmailHdr.users().watch(userId='me', body=request).execute()
    print (res)
	
  def _keywordToTradeType_ (self,keyword):
    tradetype=None
    if ( keyword.lower() == 'shorted'):
      tradetype  = 'SHORT'
    elif (keyword.lower()  == 'bought'):
      tradetype = 'LONG'
    elif (keyword.lower()  == 'covered to close'):
      tradetype = 'COVER'
    elif (keyword.lower()  == 'bought to close'):
      tradetype = 'COVER'
    elif (keyword.lower()  == 'sold to close'):
      tradetype= 'SELL'
    return  tradetype

  def _parseAlertFromMsg_ (self, msg):
    alerts=[]
    body = msg['msg']
    #print (body)
    result =  re.search(pattern=r"Today Tab Triggered Alerts", string= msg['Subject'] , flags=re.IGNORECASE)
    if ( result ): ## new subject pattern
      ticker = strategy  = wins = losses = timex = None
      for line in body.splitlines():
        #print("LINE::: ",line)
    
        if ( not ticker ):
        #bdyRes = re.search('Tap here:<a href=\'http.*>(.*),\/a>',body, flags=re.IGNORECASE )
          bdyRes = re.search(r'Tap here: <a href.*>(.*)<',line, flags=re.IGNORECASE )
          if ( bdyRes):
            ticker = bdyRes.group(1)
            continue
        if ( ticker and not strategy):
          strategy = line.replace('&nbsp;','' ).rstrip()
          continue
        if ( ticker and strategy and not wins ):
          res = re.search(r'wins: <.*>(\d+)<',line, flags=re.IGNORECASE )
          if ( res):
            wins = res.group(1)
           
          continue
        if ( ticker and strategy and  wins and not losses ):
          res = re.search(r'losses: <.*>(\d+)<',line, flags=re.IGNORECASE )
          if ( res):
            losses = res.group(1)
          continue
        alert = {}
        if ( ticker and strategy and  wins and  losses and not timex ):
          res = re.search(r'time: (.*)',line, flags=re.IGNORECASE )
          if ( res):
            timex = res.group(1)
            print (ticker, strategy, wins, losses, timex)
            alert['ticker'] = ticker
            alert['strategy'] = strategy
            alert['wins'] = wins
            alert['losses'] = losses
            alert['time'] = timex
            alert['date'] = msg['Date']
            alert['m_id']= msg['id']
            alerts.append(alert)
            ticker = strategy  = wins = losses = timex = None
          continue
        
          
      return alerts, False
   
      
    return None, False


    ## remove INBOX label and move the message to tickron label
  def updateLabel(self, msgId, newlabel='Label_2165818822513491888'): ## default tickeron label
    if ( newlabel is None):
      label_body =   {
        'removeLabelIds': ['IMPORTANT', 'CATEGORY_UPDATES',  'UNREAD'],

        }
    else:
      label_body = {
      'removeLabelIds': ['IMPORTANT', 'CATEGORY_UPDATES', 'INBOX', 'UNREAD'],
      'addLabelIds': newlabel 
      }
    ret = self.gmailHdr.users().messages().modify(userId='me', id=msgId, body=label_body ).execute()


  def getAlerts(self, historyId):
    user_id =  'me'
    label_id_one = 'INBOX'
    label_id_two = 'UNREAD'
    reg =re.compile(r"support\@cmlviz\.com")
    #history = self.gmailHdr.users().history().list(userId='me', historyTypes='messageAdded').execute()
    profile = self.gmailHdr.users().getProfile(userId='me').execute()
    newHistoryId = profile['historyId']
 
    if ( historyId == newHistoryId):
      return newHistoryId, []
    
    # Getting all the unread messages from Inbox
    # labelIds can be changed accordingly
    unread_msgs = self.gmailHdr.users().messages().list(userId='me',labelIds=[label_id_one, label_id_two], maxResults=500).execute()
    if ( 'messages' not in unread_msgs) : ## no messages
      return newHistoryId,[]
    # We get a dictonary. Now reading values for the key 'messages'
    mssg_list = unread_msgs['messages']

    print ("Total unread messages in inbox: ", str(len(mssg_list)))

    final_list = [ ]
        
    for mssg in mssg_list:
        temp_dict = { }
        m_id = mssg['id'] # get id of individual message
        message = self.gmailHdr.users().messages().get(userId=user_id, id=m_id).execute() # fetch the message using API
        payld = message['payload'] # get payload of the message 
        headr = payld['headers'] # get header of the payload

        for cell in headr: # getting the Subject
          if cell['name'] == 'Subject':
              msg_subject = cell['value']
              temp_dict['Subject'] = msg_subject
          elif cell['name'] == 'Date':
              msg_date = cell['value']
              #date_parse = (parser.parse(msg_date))
              #utcTs = date_parse.utctimetuple()
              #utcTsStr = "%04d-%02d-%02d %02d:%02d:%02d UTC" % (utcTs.tm_year, utcTs.tm_mon, utcTs.tm_mday,  utcTs.tm_hour, utcTs.tm_min, utcTs.tm_sec )

              
              #m_date = (date_parse.date())
              #temp_dict['Date'] = str(m_date)
              temp_dict['Date'] = msg_date
          elif cell['name'] == 'From':
              msg_from = cell['value']
              temp_dict['Sender'] = msg_from
          else:
              pass

        temp_dict['Snippet'] = message['snippet'] # fetching message snippet
        if (('Date' not in temp_dict ) and 'Date' in headr ) :
          temp_dict['Date'] = headr['Date'] 
        result = reg.search( temp_dict['Sender'] )
        temp_dict['msg'] = ""
          # I added the below script.
        payload = message["payload"]
        if ( 'parts' in payload):
          for p in payload["parts"]:
              if p["mimeType"] in ["text/plain", "text/html"]:
                  data = base64.urlsafe_b64decode(p["body"]["data"]).decode("utf-8")
                  temp_dict['msg'] += data
        else:
          data = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8")
          temp_dict['msg'] += data

        temp_dict['id'] = m_id

        if ( result or temp_dict['Sender'].find('jwdai2007')>=0 ): ## for debug
          (alerts, junk) = self._parseAlertFromMsg_( msg=temp_dict)
          
          if (junk) :
            self.updateLabel(m_id, "Label_1631664924980846752") # apply error label and archive/mark unread
          elif (alerts):
           

            final_list.extend(alerts) # This will create a dictonary item in the final list
          else: 
            self.updateLabel(m_id, newlabel=None)

          #if (db[key(alert)]) 
        else:
          print (f"Email {msg_subject} from {temp_dict['Sender']}is not a CML msg")
          self.updateLabel(m_id, newlabel=None)
    print ("FINAL LIST IS ::::::::::::::::::")
    print (final_list)
    print ("END OF LIST")
    return newHistoryId, final_list



def saveAlertsToCsv(alerts):
  if ( alerts == None or  len(alerts)== 0 ):
    return 
  keys = alerts[0].keys()

  with open('alerts.csv', 'w', newline='') as output_file:
      dict_writer = csv.DictWriter(output_file, keys)
      dict_writer.writeheader()
      dict_writer.writerows(alerts)

def procGmail():
 
  gmail  = GmailHdr()
  historyId = 0

  try:
      historyId, alerts = gmail.getAlerts(historyId)
      saveAlertsToCsv(alerts)

  except HttpError as error:
    # TODO(developer) - Handle errors from gmail API.
    log.error(f"An error occurred: {error}")


procGmail()