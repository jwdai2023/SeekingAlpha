
import json
from datetime import datetime, timedelta, timezone
import sys
from alphaCommon import alphaDb, BaAlert
import re
from baCommon import BaAlert, BaAlertProc

# JSON file to store messages
json_file = 'messages.json'

bSaveToDb = True


def main():
  db = alphaDb()
  db2 = alphaDb()
  rtn = db.queryBaMessages(
    #'Id=13039 '
    # "Date>'2024-11-17' order by Date desc"  
   )
  for row in rtn:
    try:
      alert = BaAlertProc.convertMsgToAlert(row.Id, row.Date, row.Msg)
    except Exception as exrr:
      print ("ERROR: ", row.Id)
      raise(exrr)
    if ( alert != None):
      #print (alert)
      try:
        if ( bSaveToDb):
          db2.createAlertEntry(alert)
        pass
      except Exception as err:
        print (err)
        pass
        #exit()





main()