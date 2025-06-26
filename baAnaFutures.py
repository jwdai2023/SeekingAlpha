
import json
from datetime import datetime, timedelta, timezone
import sys
from alphaCommon import alphaDb, BaAlert
import re
import math

def printIt(*args):
   #print (*args)
   pass


def ana():
  db = alphaDb()


  rtn = db.queryBaPositions(
  r" closeDate is not null and asset='Futures' " +
  r" and (Ticker ='ES' or Ticker ='MES' or Ticker='NQ' or Ticker='MNQ')  " +
   r" and OpenDate>'2024-11-01' and Trader='James Williams'" +
   r" order by OpenDate "
  # " And AlertId <> '#UgIkyyEK' "
  )
  fout = open('williams.txt', "w+")
  cnt=1
  for row in rtn:
     print (cnt, row.AlertId, 'LONG' if row.OpenAmt<0 else 'SHORT', row.CloseAmt+row.OpenAmt, row.Ticker, row.OpenDate, row.CloseDate)
     fout.writelines(f"{row.AlertId}\t{'LONG' if row.OpenAmt>0 else 'SHORT'}\t{row.CloseAmt+row.OpenAmt}\t" +
                     f"{row.Ticker}\t{row.OpenDate}\t{int(row.OpenDate.timestamp()*1000)}\t{row.CloseDate}\t{int(row.CloseDate.timestamp()*1000)}\r\n")
     cnt +=1
  fout.close()  
 
   
ana()