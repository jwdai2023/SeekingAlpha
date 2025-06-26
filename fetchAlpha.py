import requests
import json
from urllib.parse import urljoin
import datetime



class FetchingAlphaAPI:
    """
    Tradier-python is a python client for interacting with the Tradier API.
    """

    def __init__(self):


        self.endpoint = "https://seekingalpha.com/api/v3/" 
        self.session = requests.Session()
        self.session.headers.update(
            {


"accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
"accept-encoding":"gzip, deflate, br, zstd",
"accept-language":"en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
"cookie":
"machine_cookie=5016027438574; _ga=GA1.1.1590743390.1699883260; _pcid=%7B%22browserId%22%3A%22lowylqs8upq8hlkq%22%7D; _hjSessionUser_65666=eyJpZCI6IjA5MTZiMDQ1LTJiYjQtNWIxOC1iYWVmLWZjODJiYTFkY2VhOCIsImNyZWF0ZWQiOjE2OTk4ODMyNjAzOTEsImV4aXN0aW5nIjp0cnVlfQ==; hubspotutk=02ce2ff7585376b471a16cbd73ded066; __stripe_mid=a927a35d-026a-4dfe-8611-364643fa9f5d742dba; user_id=56320586; user_nick=bingxu.ct00; user_devices=1; u_voc=; has_paid_subscription=true; ever_pro=1; sapu=12; user_remember_token=b66ee10e22eb232b3664cfdf67f2510c3e2bd360; sailthru_hid=cb3fdc7373c486492690a8a8aa0873b96253c27a9c829dacdd03c6d3bee280cc4ef282cd7d93d6d999d1f12b; _pctx=%7Bu%7DN4IgrgzgpgThIC5QFYBsBmATABmQDlUVAAcYoAzASwA9EQQAaEAFwE9io6A1ADRAF9%2BTSLADKzAIbNIdMgHNKEZrCgATRiAiVlASXUIAdmAA2x-kA; _px2=eyJ1IjoiODAxZWRiNDAtZTZlMS0xMWVlLTkwNGUtYjMzYTY2ODliYzhlIiwidiI6IjM3N2U0ZGRkLTgyMmItMTFlZS1hMjFkLTg5Y2VkMGE3ZTlmYyIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiJmNjIyNDdjYWM3N2E4MTU3ODE5NmY1ODgzMTYwMTM2NjU5MTE1YTQ5OTdlZjA1OTc2NTliOWU5ZGNkNjdjOTk1In0=; _px=KAc6s9i7jAT4WE+XnefacR1LUrIPqiJR7nQPmeg34cPO36MzI7W19ZdioCnph2GsSbqUDt8DIsP5qARdN0OChQ==:1000:EOO9NfYHXZhu82qKpTEYOA7IHJg10BT9EgA/P9cPBO/lk6R674bNO/b+h05YZMlpV7ZxgdBn5s9QI6Ow7oe5gVbbNOOODvkTRCBhCFLY7RehLrSuctxQaM4kBPh4X0NIU1p6sLRL1aZsC1tgcW62YzllzkbIAxN7M1GJbUGEPTm4+z7eBxeyQ3DBJNrtsftGfSJzfkOBTFa4bYyhL4n6lmTnfNiYDMqgUoNhwa3EQ7CeEmAjhUiw/ROxBKBZ8Icvc/JZnv0EYFWUz2Dofw1YkA==; _pxhd=6d70a2b6f78b1cfd2110e137d19cfa4d06ebdd1d6688314ec21c9d85b8a4d293:d02fc780-65bc-11e9-b971-bb43e5539738; _hjDonePolls=1020340; _fbp=fb.1.1723468223457.42214377748168201; __tae=1728393722064; OptanonAlertBoxClosed=2024-10-08T13:23:22.449Z; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+08+2024+09%3A23%3A22+GMT-0400+(Eastern+Daylight+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0007%3A1&hosts=H40%3A1%2CH17%3A1%2CH13%3A1%2CH36%3A1%2CH55%3A1%2CH69%3A1%2CH45%3A1%2CH14%3A1%2CH15%3A1%2CH19%3A1%2CH47%3A1&genVendors=V12%3A1%2CV5%3A1%2CV7%3A1%2CV8%3A1%2CV13%3A1%2CV15%3A1%2CV3%3A1%2CV2%3A1%2CV6%3A1%2CV14%3A1%2CV1%3A1%2CV4%3A1%2CV9%3A1%2C; __tbc=%7Bkpex%7DeqkOP5pNjdf_J7DGsMNgG6cC3rqChOEKzp3ADvN311f-auC-3o7NSlys9XEInXHK; xbc=%7Bkpex%7D1EfXBMFRRIHcqHSj-bCC5_nSp69gvqSXT6vBBf3xZqr7W5KNXQiQMi3AGsp3Vv055FrEDGjbH3TR6wfEF31JTnJdR5G7eYCf_GBzKCAPbyMkbY-uFVQ0T3Ge0t1lKgFEdazhwFOsj_xFaHvEFcUZ4ocyzQlIp-3ZT28KfxaIOeaXPx4WCUXDn-12ajVuyRzPU1f_lWmqeNXkqdyf1kIIehU2LQ8G_P1e1E1Is4Onsa4Fk2cqQxIYt0Au3mgcq5A5MFlFR8Y8a7csrbtd5gC1mkcisyy7cLOxd-1pewEhhD44CAoEJoj1iOz2a2bjKS7DKFE-d2Eyuk5-YmAL5Ev5BAwGAjFdxjPcpUFm-Yoo-x9TeWUf1cUq5KcHq3LxVoxMwqt77Y2Uw0q3yq1x8fos3g; marketplace_author_slugs=david-alton-clark%2Ccory-cramer%2Cseeking-alpha%2Csteven-cress-quant-team%2Cseeking-alpha-videos; _gcl_aw=GCL.1730591672.Cj0KCQjwm5e5BhCWARIsANwm06hyN58PibaoAGid2-h7UFX_0wwdR8w-OymRmzYt0YY5z72s33mV_CkaAqpdEALw_wcB; _gcl_gs=2.1.k1$i1730591670$u84919963; _gcl_au=1.1.495372604.1731249655; _pxvid=db73e21f-a108-11ef-9891-e978ab184f46; _sasource=; __hssrc=1; pxcts=60b218be-a30f-11ef-ab3f-5ddabb82a5be; user_cookie_key=1e5z4py; gk_user_access=1*premium.archived.mp_1156.chat.mpb_1156.mpf_1156.mp_1262.chat.mpb_1262.mpf_1262.mpb_1409.mpf_1409*1731706812; gk_user_access_sign=727e974049844e261846888d92c815ce1d8fddef; _clck=103dq6t%7C2%7Cfqx%7C0%7C1451; _ig=56320586; session_id=0365665b-4a36-4ec9-9e13-584b8852e5f7; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1731717788249.1731766341607.539; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1731717788249.1731766341607.539; sailthru_pageviews=2; _ga_KGRFF2R2C5=GS1.1.1731766338.722.1.1731767847.60.0.0; sailthru_content=fea1d66c3fe2b9912878795f808ae003b820909b231e2039bd83a462b379f5a5f3302cc0da6d66f72921902840264d5805065878eb61b7b47d631e5af3ac6db2886fcd61ad113efa8c0750fd69046d959eada221935b05aaed9d45ccc59575091bbda4ad1a69f39f107ad1af35c4b49bbc2b00879e5119dc695ed544a4dc5b6aa744e5fc10220c745a2e254b25e6827029a3e7d86ad09991210f1e411ab7503a298ce4fd3534b6a524ff34050405fe8dadd6c7551d912079a6957c80b21652d98f3288391b6e81ab1f63ae4464f8c635b52624f3aa6be2d685985686dd2b45806d033c8f7ec9accb700e427f77fffe9e3c5f57e561d9315a8a9a86be8720511e; sailthru_visitor=ead8cdb4-fb30-4250-a4dc-7d6ea9d70232; _uetsid=467959409e1711efb682f515cc53ebd4; _uetvid=3663e3b0822b11ee8bdd99a23ab714fa; _clsk=1q3xij%7C1731767848104%7C13%7C0%7Cr.clarity.ms%2Fcollect; __hssc=234155329.2.1731766341607; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Fsymbol%2FNVDA%2Fratings%2Fquant-ratings%22%2C%22pageKey%22%3A%220a6a14ed-a773-46c2-bc71-83a22e609ae6%22%7D; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%225016027438574%22%2C%22machineCookieSessionId%22%3A%225016027438574%261731766308580%22%2C%22sessionStart%22%3A1731766308580%2C%22sessionEnd%22%3A1731769852752%2C%22firstSessionPageKey%22%3A%22b57d274c-5efe-46a2-98e9-32122dbe0233%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1731768052752%7D%7D; _px3=ad8bec8084c73519763ab29314c1b3ec26b7cc3c605cdeba484f4152f1877cdc:gFC+TApARSUB6Sp8zqHaOsYAUwwbk2ka8e84GtbkpbB/bNL5bkAEE4rHA8w0BdjzZFes3DQ44h8dw//jT6Mr4g==:1000:oi43AnChelHM9MrDr/LPCUtQXWK6ivnKzPHzLqmkiTOXbfuBfHdcYVIyzc4RD52eYnoO5TAEr5VVLdlx3hRswdxsKF4ebd97uRiO+llQEf0N8G1z3xKC32FeuyuhfAv8btMfrWvwfeYWXjUQt/lgdNtt8G1Ifiww6cf6ifmtPcpyhoKv1QyGBJQFrvQr0pEwZeYvlTecW/OImDyBuj774CoLVe6D1yiWc1L5pwJLF4E=; _pxde=5b14016424894bf19470b656e2b71f843296ec309f309f9df3dc11e65801fa2f:eyJ0aW1lc3RhbXAiOjE3MzE3NjgyNTIxMzEsImZfa2IiOjB9",
"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"


            }
        )

    def request(self, method: str, path: str, params: dict, data=None) -> dict:
        url = urljoin(self.endpoint, path)

        response = self.session.request(method.upper(), url, params=params, data=data)

        if response.status_code != 200:
            raise Exception(
                response.status_code, response.content.decode("utf-8")
            )
        res_json = response.json()
        key = url.rsplit("/", 1)[-1]
        if res_json.get(key) == "null":
            res_json[key] = []
        return res_json

    def get(self, path: str, params: dict) -> dict:
        """makes a GET request to an endpoint"""
        return self.request("GET", path, params)

    def post(self, path: str, params: dict, payload: dict) -> dict:
        """makes a POST request to an endpoint"""
        return self.request("POST", path, params, json.dumps(payload))

    def delete(self, path: str, params: dict):
        """makes a DELETE request to an endpoint"""
        return self.request("DELETE", path, params)

    def put(self, path: str, params):
        """makes a PUT request to an endpoint"""
        return self.request("PUT", path, params)


class RapidAPI:
    """
    Tradier-python is a python client for interacting with the Tradier API.
    """

    def __init__(self):


        self.endpoint = "https://seeking-alpha.p.rapidapi.com" 
        self.session = requests.Session()
        self.session.headers.update(
            {
              "x-rapidapi-key": "4bc10c59e2msh76d659a9fc486a5p10cea8jsn476d9167941b",
              "x-rapidapi-host": "seeking-alpha.p.rapidapi.com",
              "Content-Type": "application/json"
            }
        )

    def request(self, method: str, path: str, params: dict, data=None) -> dict:
        url = urljoin(self.endpoint, path)

        response = self.session.request(method.upper(), url, params=params, data=data)

        if response.status_code != 200:
            raise Exception(
                response.status_code, response.content.decode("utf-8")
            )
        res_json = response.json()
        key = url.rsplit("/", 1)[-1]
        if res_json.get(key) == "null":
            res_json[key] = []
        return res_json

    def get(self, path: str, params: dict) -> dict:
        """makes a GET request to an endpoint"""
        return self.request("GET", path, params)

    def post(self, path: str, params: dict, payload: dict) -> dict:
        """makes a POST request to an endpoint"""
        return self.request("POST", path, params, json.dumps(payload))

    def delete(self, path: str, params: dict):
        """makes a DELETE request to an endpoint"""
        return self.request("DELETE", path, params)

    def put(self, path: str, params):
        """makes a PUT request to an endpoint"""
        return self.request("PUT", path, params)


api = FetchingAlphaAPI()
params = {"page[number]":"1"}
#params={}
ticker = "AAPL"
ticker = ticker.lower()
rtn=api.get(f"symbols/aapl/rating/histories", params)
data = rtn['data']
print (data)
for row in data:
    attr = row ['attributes']
    ratings = attr['ratings']
    rt0 = {
        'Ticker': ticker,
        'Date': attr['asDate'],
        'authorsRating': 0, 
        "authorsCount":0,
        "quantRating":0,
        "sellSideRating":0,
        "divConsistencyCategoryGrade":0,
        "divGrowthCategoryGrade":0,
        "divSafetyCategoryGrade":0,
        "dividendYieldGrade":0,
        "dpsRevisionsGrade":0,
        "epsRevisionsGrade":0,
        "growthGrade":0,
        "momentumGrade":0,
        "profitabilityGrade":0,
        "valueGrade":0,
        "yieldOnCostGrade":0,
        "":0,
        "":0,

        }
    

    for key in ratings:
        rt0[key] = ratings[key]
        print (key, ":", ratings[key])

    print (rt0)
    exit()
#print (rtn)