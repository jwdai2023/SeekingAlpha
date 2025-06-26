from dataclasses import dataclass
from urllib.parse import urljoin

import requests
import json
from tradier_python.models import *
from datetime import datetime


class KinfoAPI:
    """
    Tradier-python is a python client for interacting with the Tradier API.
    """

    def __init__(self, token, endpoint=None):

        self.endpoint = endpoint if endpoint else 'https://api.kinfo.com'
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
        )

    def request(self, method: str, path: str, params: dict) -> dict:
        url = urljoin(self.endpoint, path)

        response = self.session.request(method.upper(), url, params=params)

        if response.status_code != 200:
            raise Exception(
                response.status_code, response.content.decode("utf-8")
            )
        res_json = response.json()
        return res_json

    def get(self, path: str, params: dict) -> dict:
        """makes a GET request to an endpoint"""
        return self.request("GET", path, params)

    def post(self, path: str, params: dict) -> dict:
        """makes a POST request to an endpoint"""
        return self.request("POST", path, params)

    def delete(self, path: str, params: dict):
        """makes a DELETE request to an endpoint"""
        return self.request("DELETE", path, params)

    def put(self, path: str, params):
        """makes a PUT request to an endpoint"""
        return self.request("PUT", path, params)

    def get_trades(self, profileId:int, page:int) -> Profile:
  
        url = f"/api/Portfolio/{profileId}/trades/{page}"
        data = self.get(url, {})
        if ( isinstance(data, str)):
            data = json.loads(data)
        return data

def getPage(page=0):
  token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6IjU3Nzk2IiwibmJmIjoxNzM1Nzc1Njc4LCJleHAiOjE3MzU4NjIwNzgsImlhdCI6MTczNTc3NTY3OCwiaXNzIjoiS0lORk8iLCJhdWQiOiJodHRwczovL2dva2luZm8uY29tIn0.Y8OCioRx-Gs85BPoOSqjvd91TWH6-6DEbwpzfziu--Y"
  profileId=17255
  api = KinfoAPI(token)
  rtn=api.get_trades(profileId,page)
  totalCount = rtn['totalCount']
  results = rtn['result']
  for t in results:
      print (t['entryDateTime'],t['exitDateTime'], t['symbol'], t['gainAmount'], t['quantity'], t['entryPrice'],t['exitPrice'],f"{t['gainPercent']*100:.2f}%", f"${t['quantity']*t['entryPrice']}" )
      #print (json.dumps(trade))
  #print(json.dumps(rtn))

for p in range(0,20):  
  getPage(p)