import pyodbc
from datetime import datetime
from datetime import date
import dateutil.parser as parser
import pytz
import requests
from urllib.parse import urljoin
from baCommon import BaAlert
import json
import math
import http.client
import time
from tradier_python import TradierAPI
import random
class FetchingAlphaAPI:
  """
  Tradier-python is a python client for interacting with the Tradier API.
  """

  def __init__(self):


      self.endpoint = "https://seekingalpha.com/api/v3/" 
      self.session = requests.Session()
      self.session.headers.update(
          {


#"accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
"accept": "application/json",
"accept-encoding":"gzip, deflate, br, zstd",
"accept-language":"en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
"cookie":
"machine_cookie=5016027438574; _ga=GA1.1.1590743390.1699883260; _pcid=%7B%22browserId%22%3A%22lowylqs8upq8hlkq%22%7D; _hjSessionUser_65666=eyJpZCI6IjA5MTZiMDQ1LTJiYjQtNWIxOC1iYWVmLWZjODJiYTFkY2VhOCIsImNyZWF0ZWQiOjE2OTk4ODMyNjAzOTEsImV4aXN0aW5nIjp0cnVlfQ==; hubspotutk=02ce2ff7585376b471a16cbd73ded066; __stripe_mid=a927a35d-026a-4dfe-8611-364643fa9f5d742dba; has_paid_subscription=true; ever_pro=1; _pctx=%7Bu%7DN4IgrgzgpgThIC5QFYBsBmATABmQDlUVAAcYoAzASwA9EQQAaEAFwE9io6A1ADRAF9%2BTSLADKzAIbNIdMgHNKEZrCgATRiAiVlASXUIAdmAA2x-kA; _px2=eyJ1IjoiODAxZWRiNDAtZTZlMS0xMWVlLTkwNGUtYjMzYTY2ODliYzhlIiwidiI6IjM3N2U0ZGRkLTgyMmItMTFlZS1hMjFkLTg5Y2VkMGE3ZTlmYyIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiJmNjIyNDdjYWM3N2E4MTU3ODE5NmY1ODgzMTYwMTM2NjU5MTE1YTQ5OTdlZjA1OTc2NTliOWU5ZGNkNjdjOTk1In0=; _px=KAc6s9i7jAT4WE+XnefacR1LUrIPqiJR7nQPmeg34cPO36MzI7W19ZdioCnph2GsSbqUDt8DIsP5qARdN0OChQ==:1000:EOO9NfYHXZhu82qKpTEYOA7IHJg10BT9EgA/P9cPBO/lk6R674bNO/b+h05YZMlpV7ZxgdBn5s9QI6Ow7oe5gVbbNOOODvkTRCBhCFLY7RehLrSuctxQaM4kBPh4X0NIU1p6sLRL1aZsC1tgcW62YzllzkbIAxN7M1GJbUGEPTm4+z7eBxeyQ3DBJNrtsftGfSJzfkOBTFa4bYyhL4n6lmTnfNiYDMqgUoNhwa3EQ7CeEmAjhUiw/ROxBKBZ8Icvc/JZnv0EYFWUz2Dofw1YkA==; _pxhd=6d70a2b6f78b1cfd2110e137d19cfa4d06ebdd1d6688314ec21c9d85b8a4d293:d02fc780-65bc-11e9-b971-bb43e5539738; _hjDonePolls=1020340; _fbp=fb.1.1723468223457.42214377748168201; __tae=1728393722064; OptanonAlertBoxClosed=2024-10-08T13:23:22.449Z; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+08+2024+09%3A23%3A22+GMT-0400+(Eastern+Daylight+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0007%3A1&hosts=H40%3A1%2CH17%3A1%2CH13%3A1%2CH36%3A1%2CH55%3A1%2CH69%3A1%2CH45%3A1%2CH14%3A1%2CH15%3A1%2CH19%3A1%2CH47%3A1&genVendors=V12%3A1%2CV5%3A1%2CV7%3A1%2CV8%3A1%2CV13%3A1%2CV15%3A1%2CV3%3A1%2CV2%3A1%2CV6%3A1%2CV14%3A1%2CV1%3A1%2CV4%3A1%2CV9%3A1%2C; __tbc=%7Bkpex%7DeqkOP5pNjdf_J7DGsMNgG6cC3rqChOEKzp3ADvN311f-auC-3o7NSlys9XEInXHK; xbc=%7Bkpex%7D1EfXBMFRRIHcqHSj-bCC5_nSp69gvqSXT6vBBf3xZqr7W5KNXQiQMi3AGsp3Vv055FrEDGjbH3TR6wfEF31JTnJdR5G7eYCf_GBzKCAPbyMkbY-uFVQ0T3Ge0t1lKgFEdazhwFOsj_xFaHvEFcUZ4ocyzQlIp-3ZT28KfxaIOeaXPx4WCUXDn-12ajVuyRzPU1f_lWmqeNXkqdyf1kIIehU2LQ8G_P1e1E1Is4Onsa4Fk2cqQxIYt0Au3mgcq5A5MFlFR8Y8a7csrbtd5gC1mkcisyy7cLOxd-1pewEhhD44CAoEJoj1iOz2a2bjKS7DKFE-d2Eyuk5-YmAL5Ev5BAwGAjFdxjPcpUFm-Yoo-x9TeWUf1cUq5KcHq3LxVoxMwqt77Y2Uw0q3yq1x8fos3g; _gcl_au=1.1.495372604.1731249655; _pxvid=db73e21f-a108-11ef-9891-e978ab184f46; sailthru_hid=cb3fdc7373c486492690a8a8aa0873b96253c27a9c829dacdd03c6d3bee280cc4ef282cd7d93d6d999d1f12b; _sasource=; pxcts=fef82f87-c2ed-11ef-95de-0e6a59091b66; __hssrc=1; user_id=56320586; user_nick=bingxu.ct00; user_devices=1; u_voc=; sapu=12; user_remember_token=da85c3420514283a94246cc42bd80e614b4124a1; _gcl_aw=GCL.1735618658.CjwKCAiApsm7BhBZEiwAvIu2Xzxp4a5Zlwk82CPQQvIEOU1SLTPOC-PNdKhq-GWbDYW0mixPgUwn1hoCiJMQAvD_BwE; _gcl_gs=2.1.k1$i1735618655$u145688118; marketplace_author_slugs=chris-demuth-jr%2Cdavid-alton-clark%2Ccory-cramer%2Cseeking-alpha%2Csteven-cress-quant-team%2Cseeking-alpha-videos; _ig=56320586; _clck=103dq6t%7C2%7Cfs9%7C0%7C1451; session_id=2b0f23b7-a1d0-43e0-a9c3-7acadc5d9c39; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1735862713056.1735872705090.693; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1735862713056.1735872705090.693; user_cookie_key=1u32f5m; gk_user_access=1*premium.archived.mp_1156.chat.mpb_1156.mpf_1156.mp_1262.chat.mpb_1262.mpf_1262.mpb_206.mpf_206.mpb_1409.mpf_1409*1735873001; gk_user_access_sign=fa0eafce228e133ba907bd38441edf9917747403; sailthru_pageviews=3; _ga_KGRFF2R2C5=GS1.1.1735872698.926.1.1735874530.58.0.0; sailthru_content=3c5f57e561d9315a8a9a86be8720511e99ece8c70fd051753d58ab8d1fe73db4f8c90eb0aba765bf27aee3924e3982bf80185dd9118e58237f431db7ace4d8281fffa0c9e2eed6c14d4776bf9ec1d37699d4ee9c59caa19844d9b474633e63b19364eeecd36a0f6f0836f88e5d2a29a58ab900d14653d1244520ef010dc6fe160024a75f302d6af06d1d7e50162cf1c7ac720cfc757568327378d53ad23d54359eada221935b05aaed9d45ccc5957509be92222109ba1a4f6f53b8d9da6e02f8ff34e3ad8e575d483eb03e9d7b07791a5cc25f95acc6e5313215a0f2dc9e3ca887fec167e27465e3b276dfb64053e88ffea1d66c3fe2b9912878795f808ae003; sailthru_visitor=ead8cdb4-fb30-4250-a4dc-7d6ea9d70232; __hssc=234155329.3.1735872705090; _uetsid=79e42d50c90b11ef8a0ce1992982a406; _uetvid=3663e3b0822b11ee8bdd99a23ab714fa; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Fsymbol%2FCUZ%2Fratings%2Fquant-ratings%22%2C%22fromMpArticle%22%3Afalse%2C%22pageKey%22%3A%22a1c771f7-2c25-4f67-8ea5-0d0fa7073293%22%7D; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%225016027438574%22%2C%22machineCookieSessionId%22%3A%225016027438574%261735872696748%22%2C%22sessionStart%22%3A1735872696748%2C%22sessionEnd%22%3A1735876332257%2C%22firstSessionPageKey%22%3A%220922b3d6-3979-4f8d-94a8-25e94991844f%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1735874532257%7D%7D; _clsk=1vur4gd%7C1735874532295%7C9%7C0%7Ci.clarity.ms%2Fcollect; _px3=2669297b8569a33413a4f1ce3c0254f41eb1d74d09b9b7a082b20cbbb70b1925:nFkPT1Sqz3w2szQuFIpo+46Y6PfLayVIRRsnraEKD7Pu6BDUI6SGBagxck8/H7CURjd14Dq8P+zNLCDM1gStzg==:1000:lqEdk3DtgtKJDfb4f8MykVV7/8w5wI3XOIakG+MHD8YYQ1yGcGFDqYEvhT318yjuhaC02v3/0w5q1rEhQh/W/8Lzy5NeFlxV5j3iJnx99ydgrQqYFGz2hDtaZJ0VMNwLH8Glz5AdOzsn+D60sTEdRViUqmo+jkbMXFjwAA2p5+D750n9lcpLTNsDZ3B+LIqISE6e1YD3n+6OMWfHjAh0Vmm2wrEiycrNlnAIGxgIJBM=; _pxde=ad52832cad111f26570edfe54a52ca89cceaf4687c6912b6d6e8e025b48eb9b8:eyJ0aW1lc3RhbXAiOjE3MzU4NzQ1MzM5MzMsImZfa2IiOjB9",
#"machine_cookie=5016027438574; _ga=GA1.1.1590743390.1699883260; _pcid=%7B%22browserId%22%3A%22lowylqs8upq8hlkq%22%7D; _hjSessionUser_65666=eyJpZCI6IjA5MTZiMDQ1LTJiYjQtNWIxOC1iYWVmLWZjODJiYTFkY2VhOCIsImNyZWF0ZWQiOjE2OTk4ODMyNjAzOTEsImV4aXN0aW5nIjp0cnVlfQ==; hubspotutk=02ce2ff7585376b471a16cbd73ded066; __stripe_mid=a927a35d-026a-4dfe-8611-364643fa9f5d742dba; user_id=56320586; user_nick=bingxu.ct00; user_devices=1; u_voc=; has_paid_subscription=true; ever_pro=1; sapu=12; user_remember_token=b66ee10e22eb232b3664cfdf67f2510c3e2bd360; sailthru_hid=cb3fdc7373c486492690a8a8aa0873b96253c27a9c829dacdd03c6d3bee280cc4ef282cd7d93d6d999d1f12b; _pctx=%7Bu%7DN4IgrgzgpgThIC5QFYBsBmATABmQDlUVAAcYoAzASwA9EQQAaEAFwE9io6A1ADRAF9%2BTSLADKzAIbNIdMgHNKEZrCgATRiAiVlASXUIAdmAA2x-kA; _px2=eyJ1IjoiODAxZWRiNDAtZTZlMS0xMWVlLTkwNGUtYjMzYTY2ODliYzhlIiwidiI6IjM3N2U0ZGRkLTgyMmItMTFlZS1hMjFkLTg5Y2VkMGE3ZTlmYyIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiJmNjIyNDdjYWM3N2E4MTU3ODE5NmY1ODgzMTYwMTM2NjU5MTE1YTQ5OTdlZjA1OTc2NTliOWU5ZGNkNjdjOTk1In0=; _px=KAc6s9i7jAT4WE+XnefacR1LUrIPqiJR7nQPmeg34cPO36MzI7W19ZdioCnph2GsSbqUDt8DIsP5qARdN0OChQ==:1000:EOO9NfYHXZhu82qKpTEYOA7IHJg10BT9EgA/P9cPBO/lk6R674bNO/b+h05YZMlpV7ZxgdBn5s9QI6Ow7oe5gVbbNOOODvkTRCBhCFLY7RehLrSuctxQaM4kBPh4X0NIU1p6sLRL1aZsC1tgcW62YzllzkbIAxN7M1GJbUGEPTm4+z7eBxeyQ3DBJNrtsftGfSJzfkOBTFa4bYyhL4n6lmTnfNiYDMqgUoNhwa3EQ7CeEmAjhUiw/ROxBKBZ8Icvc/JZnv0EYFWUz2Dofw1YkA==; _pxhd=6d70a2b6f78b1cfd2110e137d19cfa4d06ebdd1d6688314ec21c9d85b8a4d293:d02fc780-65bc-11e9-b971-bb43e5539738; _hjDonePolls=1020340; _fbp=fb.1.1723468223457.42214377748168201; __tae=1728393722064; OptanonAlertBoxClosed=2024-10-08T13:23:22.449Z; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+08+2024+09%3A23%3A22+GMT-0400+(Eastern+Daylight+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0007%3A1&hosts=H40%3A1%2CH17%3A1%2CH13%3A1%2CH36%3A1%2CH55%3A1%2CH69%3A1%2CH45%3A1%2CH14%3A1%2CH15%3A1%2CH19%3A1%2CH47%3A1&genVendors=V12%3A1%2CV5%3A1%2CV7%3A1%2CV8%3A1%2CV13%3A1%2CV15%3A1%2CV3%3A1%2CV2%3A1%2CV6%3A1%2CV14%3A1%2CV1%3A1%2CV4%3A1%2CV9%3A1%2C; __tbc=%7Bkpex%7DeqkOP5pNjdf_J7DGsMNgG6cC3rqChOEKzp3ADvN311f-auC-3o7NSlys9XEInXHK; xbc=%7Bkpex%7D1EfXBMFRRIHcqHSj-bCC5_nSp69gvqSXT6vBBf3xZqr7W5KNXQiQMi3AGsp3Vv055FrEDGjbH3TR6wfEF31JTnJdR5G7eYCf_GBzKCAPbyMkbY-uFVQ0T3Ge0t1lKgFEdazhwFOsj_xFaHvEFcUZ4ocyzQlIp-3ZT28KfxaIOeaXPx4WCUXDn-12ajVuyRzPU1f_lWmqeNXkqdyf1kIIehU2LQ8G_P1e1E1Is4Onsa4Fk2cqQxIYt0Au3mgcq5A5MFlFR8Y8a7csrbtd5gC1mkcisyy7cLOxd-1pewEhhD44CAoEJoj1iOz2a2bjKS7DKFE-d2Eyuk5-YmAL5Ev5BAwGAjFdxjPcpUFm-Yoo-x9TeWUf1cUq5KcHq3LxVoxMwqt77Y2Uw0q3yq1x8fos3g; marketplace_author_slugs=david-alton-clark%2Ccory-cramer%2Cseeking-alpha%2Csteven-cress-quant-team%2Cseeking-alpha-videos; _gcl_aw=GCL.1730591672.Cj0KCQjwm5e5BhCWARIsANwm06hyN58PibaoAGid2-h7UFX_0wwdR8w-OymRmzYt0YY5z72s33mV_CkaAqpdEALw_wcB; _gcl_gs=2.1.k1$i1730591670$u84919963; _gcl_au=1.1.495372604.1731249655; _pxvid=db73e21f-a108-11ef-9891-e978ab184f46; _sasource=; __hssrc=1; pxcts=60b218be-a30f-11ef-ab3f-5ddabb82a5be; user_cookie_key=1e5z4py; gk_user_access=1*premium.archived.mp_1156.chat.mpb_1156.mpf_1156.mp_1262.chat.mpb_1262.mpf_1262.mpb_1409.mpf_1409*1731706812; gk_user_access_sign=727e974049844e261846888d92c815ce1d8fddef; _clck=103dq6t%7C2%7Cfqx%7C0%7C1451; _ig=56320586; session_id=0365665b-4a36-4ec9-9e13-584b8852e5f7; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1731717788249.1731766341607.539; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1731717788249.1731766341607.539; sailthru_pageviews=2; _ga_KGRFF2R2C5=GS1.1.1731766338.722.1.1731767847.60.0.0; sailthru_content=fea1d66c3fe2b9912878795f808ae003b820909b231e2039bd83a462b379f5a5f3302cc0da6d66f72921902840264d5805065878eb61b7b47d631e5af3ac6db2886fcd61ad113efa8c0750fd69046d959eada221935b05aaed9d45ccc59575091bbda4ad1a69f39f107ad1af35c4b49bbc2b00879e5119dc695ed544a4dc5b6aa744e5fc10220c745a2e254b25e6827029a3e7d86ad09991210f1e411ab7503a298ce4fd3534b6a524ff34050405fe8dadd6c7551d912079a6957c80b21652d98f3288391b6e81ab1f63ae4464f8c635b52624f3aa6be2d685985686dd2b45806d033c8f7ec9accb700e427f77fffe9e3c5f57e561d9315a8a9a86be8720511e; sailthru_visitor=ead8cdb4-fb30-4250-a4dc-7d6ea9d70232; _uetsid=467959409e1711efb682f515cc53ebd4; _uetvid=3663e3b0822b11ee8bdd99a23ab714fa; _clsk=1q3xij%7C1731767848104%7C13%7C0%7Cr.clarity.ms%2Fcollect; __hssc=234155329.2.1731766341607; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Fsymbol%2FNVDA%2Fratings%2Fquant-ratings%22%2C%22pageKey%22%3A%220a6a14ed-a773-46c2-bc71-83a22e609ae6%22%7D; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%225016027438574%22%2C%22machineCookieSessionId%22%3A%225016027438574%261731766308580%22%2C%22sessionStart%22%3A1731766308580%2C%22sessionEnd%22%3A1731769852752%2C%22firstSessionPageKey%22%3A%22b57d274c-5efe-46a2-98e9-32122dbe0233%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1731768052752%7D%7D; _px3=ad8bec8084c73519763ab29314c1b3ec26b7cc3c605cdeba484f4152f1877cdc:gFC+TApARSUB6Sp8zqHaOsYAUwwbk2ka8e84GtbkpbB/bNL5bkAEE4rHA8w0BdjzZFes3DQ44h8dw//jT6Mr4g==:1000:oi43AnChelHM9MrDr/LPCUtQXWK6ivnKzPHzLqmkiTOXbfuBfHdcYVIyzc4RD52eYnoO5TAEr5VVLdlx3hRswdxsKF4ebd97uRiO+llQEf0N8G1z3xKC32FeuyuhfAv8btMfrWvwfeYWXjUQt/lgdNtt8G1Ifiww6cf6ifmtPcpyhoKv1QyGBJQFrvQr0pEwZeYvlTecW/OImDyBuj774CoLVe6D1yiWc1L5pwJLF4E=; _pxde=5b14016424894bf19470b656e2b71f843296ec309f309f9df3dc11e65801fa2f:eyJ0aW1lc3RhbXAiOjE3MzE3NjgyNTIxMzEsImZfa2IiOjB9",
"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
"Content-Type": "application/json"


          }
      )


      #self.session.headers.update(
      #    {"cookie":
      #    "machine_cookie=5016027438574; _ga=GA1.1.1590743390.1699883260; _pcid=%7B%22browserId%22%3A%22lowylqs8upq8hlkq%22%7D; _hjSessionUser_65666=eyJpZCI6IjA5MTZiMDQ1LTJiYjQtNWIxOC1iYWVmLWZjODJiYTFkY2VhOCIsImNyZWF0ZWQiOjE2OTk4ODMyNjAzOTEsImV4aXN0aW5nIjp0cnVlfQ==; hubspotutk=02ce2ff7585376b471a16cbd73ded066; __stripe_mid=a927a35d-026a-4dfe-8611-364643fa9f5d742dba; user_id=56320586; user_nick=bingxu.ct00; user_devices=1; u_voc=; has_paid_subscription=true; ever_pro=1; sapu=12; user_remember_token=b66ee10e22eb232b3664cfdf67f2510c3e2bd360; sailthru_hid=cb3fdc7373c486492690a8a8aa0873b96253c27a9c829dacdd03c6d3bee280cc4ef282cd7d93d6d999d1f12b; _pctx=%7Bu%7DN4IgrgzgpgThIC5QFYBsBmATABmQDlUVAAcYoAzASwA9EQQAaEAFwE9io6A1ADRAF9%2BTSLADKzAIbNIdMgHNKEZrCgATRiAiVlASXUIAdmAA2x-kA; _px2=eyJ1IjoiODAxZWRiNDAtZTZlMS0xMWVlLTkwNGUtYjMzYTY2ODliYzhlIiwidiI6IjM3N2U0ZGRkLTgyMmItMTFlZS1hMjFkLTg5Y2VkMGE3ZTlmYyIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiJmNjIyNDdjYWM3N2E4MTU3ODE5NmY1ODgzMTYwMTM2NjU5MTE1YTQ5OTdlZjA1OTc2NTliOWU5ZGNkNjdjOTk1In0=; _px=KAc6s9i7jAT4WE+XnefacR1LUrIPqiJR7nQPmeg34cPO36MzI7W19ZdioCnph2GsSbqUDt8DIsP5qARdN0OChQ==:1000:EOO9NfYHXZhu82qKpTEYOA7IHJg10BT9EgA/P9cPBO/lk6R674bNO/b+h05YZMlpV7ZxgdBn5s9QI6Ow7oe5gVbbNOOODvkTRCBhCFLY7RehLrSuctxQaM4kBPh4X0NIU1p6sLRL1aZsC1tgcW62YzllzkbIAxN7M1GJbUGEPTm4+z7eBxeyQ3DBJNrtsftGfSJzfkOBTFa4bYyhL4n6lmTnfNiYDMqgUoNhwa3EQ7CeEmAjhUiw/ROxBKBZ8Icvc/JZnv0EYFWUz2Dofw1YkA==; _pxhd=6d70a2b6f78b1cfd2110e137d19cfa4d06ebdd1d6688314ec21c9d85b8a4d293:d02fc780-65bc-11e9-b971-bb43e5539738; _hjDonePolls=1020340; _fbp=fb.1.1723468223457.42214377748168201; __tae=1728393722064; OptanonAlertBoxClosed=2024-10-08T13:23:22.449Z; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+08+2024+09%3A23%3A22+GMT-0400+(Eastern+Daylight+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0007%3A1&hosts=H40%3A1%2CH17%3A1%2CH13%3A1%2CH36%3A1%2CH55%3A1%2CH69%3A1%2CH45%3A1%2CH14%3A1%2CH15%3A1%2CH19%3A1%2CH47%3A1&genVendors=V12%3A1%2CV5%3A1%2CV7%3A1%2CV8%3A1%2CV13%3A1%2CV15%3A1%2CV3%3A1%2CV2%3A1%2CV6%3A1%2CV14%3A1%2CV1%3A1%2CV4%3A1%2CV9%3A1%2C; __tbc=%7Bkpex%7DeqkOP5pNjdf_J7DGsMNgG6cC3rqChOEKzp3ADvN311f-auC-3o7NSlys9XEInXHK; xbc=%7Bkpex%7D1EfXBMFRRIHcqHSj-bCC5_nSp69gvqSXT6vBBf3xZqr7W5KNXQiQMi3AGsp3Vv055FrEDGjbH3TR6wfEF31JTnJdR5G7eYCf_GBzKCAPbyMkbY-uFVQ0T3Ge0t1lKgFEdazhwFOsj_xFaHvEFcUZ4ocyzQlIp-3ZT28KfxaIOeaXPx4WCUXDn-12ajVuyRzPU1f_lWmqeNXkqdyf1kIIehU2LQ8G_P1e1E1Is4Onsa4Fk2cqQxIYt0Au3mgcq5A5MFlFR8Y8a7csrbtd5gC1mkcisyy7cLOxd-1pewEhhD44CAoEJoj1iOz2a2bjKS7DKFE-d2Eyuk5-YmAL5Ev5BAwGAjFdxjPcpUFm-Yoo-x9TeWUf1cUq5KcHq3LxVoxMwqt77Y2Uw0q3yq1x8fos3g; marketplace_author_slugs=david-alton-clark%2Ccory-cramer%2Cseeking-alpha%2Csteven-cress-quant-team%2Cseeking-alpha-videos; _gcl_aw=GCL.1730591672.Cj0KCQjwm5e5BhCWARIsANwm06hyN58PibaoAGid2-h7UFX_0wwdR8w-OymRmzYt0YY5z72s33mV_CkaAqpdEALw_wcB; _gcl_gs=2.1.k1$i1730591670$u84919963; _gcl_au=1.1.495372604.1731249655; _pxvid=db73e21f-a108-11ef-9891-e978ab184f46; user_cookie_key=lhys5k; gk_user_access=1*premium.archived.mp_1156.chat.mpb_1156.mpf_1156.mp_1262.chat.mpb_1262.mpf_1262.mpb_1409.mpf_1409*1732035467; gk_user_access_sign=0e57815ee8535d099281fceee8382ad1e4109812; session_id=7a490c02-31a8-4188-a797-0179e53b4ef6; _sasource=; _clck=103dq6t%7C2%7Cfr0%7C0%7C1451; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1731961721830.1732035469150.545; __hstc=234155329.02ce2ff7585376b471a16cbd73ded066.1699883261180.1731961721830.1732035469150.545; __hssrc=1; pxcts=68e5b81c-a697-11ef-bdaa-71c5e30e250d; _ig=56320586; sailthru_pageviews=4; sailthru_visitor=ead8cdb4-fb30-4250-a4dc-7d6ea9d70232; _uetsid=467959409e1711efb682f515cc53ebd4; _uetvid=3663e3b0822b11ee8bdd99a23ab714fa; __hssc=234155329.4.1732035469150; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Fsymbol%2FAGX%2Fratings%2Fquant-ratings%22%2C%22pageKey%22%3A%2293b15998-2bd5-43f5-a33c-f167e8253731%22%7D; sailthru_content=713a41189566e9dec57db689263bed0dafa037d960b3951e03c414bde24ee168e51436a5011a54ce9da55817359f148b3c7969efad3579a18d83ab6b7e17c5557155aec4082fbb3dede0110b1e687974af51c6300b1913e7b836d973db3162d75781eeac74ca64c6109bb017de60f04b9e58b504a4a372c56b76498ab7eb2fede636beb6ff5c863f82fcadd934a64c2af42d6b4eb018e17c04fbd5b4c76a006c6d033c8f7ec9accb700e427f77fffe9eb52624f3aa6be2d685985686dd2b45803c5f57e561d9315a8a9a86be8720511e9eada221935b05aaed9d45ccc5957509c6b1a67ab7b28f70768970e08fcc7b7d9bb49add79094a22347c6242e021f5a4; _clsk=1b1iyas%7C1732037369878%7C14%7C0%7Cr.clarity.ms%2Fcollect; _px3=dc54b79098ddfccd2394e4733ba235f8a99ba8e419dab9873ea277120be61ff6:mv+YvjTAL8DBA17U9qf+5M2QpBbDZfXnUKKYRWjkCtERhdG7u93neytdLtnaNXXMrnY8r4c0BYZK+4KLQOlsww==:1000:kVDFk4iwphlHGbuz0BgZsua4cNDldnd2w5HTIKgMmpIrU/2FG1J1j90oX2E1WvEEcgTC8f7s4UAziwshMHrLEuNZhU33TugpQLl1QI5yF0iwnPCe1XwfHvqw6gE+JGIqMrntMSUVqSUSCPwjsPU8NMkxqhT4rkKral5HcIhmks4YKAE1NFfdJ+vfAwA6W9rfTo6wabRoIA3Az3w3cqhJEobdcy7NqXbK7jMnzvyJrPw=; _pxde=12c3620bb59884ca16b01311c9ff28833f8e72d567985abc884c398c0f1d2606:eyJ0aW1lc3RhbXAiOjE3MzIwMzczNzExMDcsImZfa2IiOjB9; _ga_KGRFF2R2C5=GS1.1.1732035468.736.1.1732037370.50.0.0; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%225016027438574%22%2C%22machineCookieSessionId%22%3A%225016027438574%261732035467135%22%2C%22sessionStart%22%3A1732035467135%2C%22sessionEnd%22%3A1732039191248%2C%22firstSessionPageKey%22%3A%22dd3dd6d6-06e4-4b39-aa27-23f82e1de2ea%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1732037391249%7D%7D"})
  
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
  
  
  def getScreenedStocks(self, filter=None )->list:

    if ( filter is None):
      filter = {
        "quant_rating": {"gte": 4.9, "lte": 5  },
        "sell_side_rating": { "gte": 4,    "lte": 5    },
        "close": { "gte": 4    },
      }


    cont = True
    page = 1
    totPages = 1
    tickers = []
    while (cont):
        
      #querystring = {"page": f"{page}","per_page":"100","type":"stock"}
      querystring=None
      print (f"search page {page}")
      payload={'filter': filter,
          'page': page,
          'per_page' :  100,
          "type" : "stock",
          'total_count': True,
          'sort' : None,
          }
      rtn = self.post("screener_results", querystring, payload)
      if ( 'meta' in rtn):
        meta = rtn['meta']
        count = meta['count']
        totPages = math.ceil(count/100 )
        if ( page >= totPages ):
          cont = False
      else:
        cont = False
      if ( 'data' in rtn):
        data = rtn['data']
        for entry in data:
            ticker = entry['attributes']['name'].upper()
            tickers.append(ticker)
      page += 1
    return tickers
   
  def fetchRatingHist(self, ticker, page):

    params = {"page[number]":f"{page}"}
    #params={}
    nMaxTries = 3
    ticker = ticker.lower()
    delay = 0
    for t in range (0, nMaxTries): ## try three times
      try: 
        rtn=self.get(f"symbols/{ticker}/rating/histories", params)
        #print (rtn)
        data = rtn['data']
        #print (data)
        ratingHistList=[]
        for row in data:
          attr = row ['attributes']
          ratings = attr['ratings']
          rt0 = {
              'Ticker': ticker,
              'Date': attr['asDate'],
              'authorsRating': None, 
              "authorsCount":None,
              "quantRating":None,
              "sellSideRating":None,
              "divConsistencyCategoryGrade":None,
              "divGrowthCategoryGrade":None,
              "divSafetyCategoryGrade":None,
              "dividendYieldGrade":None,
              "dpsRevisionsGrade":None,
              "epsRevisionsGrade":None,
              "growthGrade":None,
              "momentumGrade":None,
              "profitabilityGrade":None,
              "valueGrade":None,
              "yieldOnCostGrade":None,
              }

          for key in ratings:
              rt0[key] = ratings[key]
              #print (key, ":", ratings[key])
          ratingHistList.append(rt0)
          #print (rt0)
        #time.sleep(random.randint(1,4)) ## to prevent the server to coke
        return ratingHistList
      except Exception as error:
        delay +=3*(t+1)
        if ( t == nMaxTries-1 ):
          raise( error)
        else:
          time.sleep(delay)

  def fetchGradeMatix(self, tickers):
     slugs:str=",".join(tickers)
     slugs= slugs.lower()
     matrix=['quant_rating', 'authors_rating', 'sell_side_rating']

     #matrix = ['growth_category', 'momentum_category','profitability_category', 'value_category']
     
     params={'filter[slugs]': slugs, 'filter[fields]': ','.join(matrix)}
     rtn = self.get('metrics', params)
     return rtn
  
  def convertDailyratingToDbEntry(self, ticker, rating):
    namePair={
       'growth_category': 'growthGrade', 
       'momentum_category':'momentumGrade',
       'profitability_category':'profitabilityGrade', 
       'value_category':'valueGrade',
       'eps_revisions_category':'epsRevisionsGrade',

       'quant_rating':'quantRating',
       'authors_rating':'authorsRating',
       'sell_side_rating':'sellSideRating',
       'indexGroup': 'indexGroup',
       'exchange': 'exchange'}
    entry={'Ticker': ticker,
            'Date': datetime.now().strftime("%Y-%m-%d"),
            'authorsRating': None, 
            "authorsCount":None,
            "quantRating":None,
            "sellSideRating":None,
            "divConsistencyCategoryGrade":None,
            "divGrowthCategoryGrade":None,
            "divSafetyCategoryGrade":None,
            "dividendYieldGrade":None,
            "dpsRevisionsGrade":None,
            "epsRevisionsGrade":None,
            "growthGrade":None,
            "momentumGrade":None,
            "profitabilityGrade":None,
            "valueGrade":None,
            "yieldOnCostGrade":None}
    for key in rating:
      name = namePair[key]
      entry[name] = rating[key]
    return entry

  
  ## for some reason, SeekingAlpha has a bug in their system. The matrix sometimes does not return complete data.
  ## That is probably the reason that SA could not display all the filtered reason in their Stock Screener page.
  ## The API call for screen_results always gives more complete data than using their webpage stock filters
  def fetchCategoryMatix(self, tickers):
     slugs:str=",".join(tickers)
     slugs= slugs.lower()

     #matrix = ['growth_category', 'momentum_category','profitability_category', 'value_category','eps_revisions_category',
     #          "div_growth_category","div_safety_category","div_yield_category","div_consistency_category"]
     matrix = ['growth_category', 'momentum_category','profitability_category', 'value_category','eps_revisions_category']
               
     #params={'filter[slugs]': slugs, 'filter[fields]': ','.join(matrix), 'filter[algos]':'main_quant,dividends'}
     params={'filter[slugs]': slugs, 'filter[fields]': ','.join(matrix), 'filter[algos]':'main_quant'}
     rtn = self.get('ticker_metric_grades', params)
     return rtn
  

  def _convertToRatings(self, rtn, ratings=None):

    tickers={}
    indexGroup={}
    exchange={}
    if ( ratings == None):
      ratings={}  ## ratings
    fields={}
  

    if ( rtn == None):
      return None
    
    if ('included' in rtn):
      included = rtn['included']
      
      for elem in included:
        type = elem['type']
        attr =  elem['attributes']
        if ( type == 'metric_type'):
          #print (elem['id'], attr['field'])
          fields[elem['id']] = attr['field']
        elif (type == 'ticker'):
          ticker = attr['name']
          tickers[elem['id']] = ticker
          #print (elem['id'], attr['name'], attr['exchange'], attr['indexGroup'])
          exchange[ticker] = attr['exchange']
          indexGroup[ticker]= attr['indexGroup']

    #print (len(tickers))
    if ( 'data' in rtn):
      data = rtn['data']
      for item in data:
        #print (json.dumps(item))
        try:
          ids = json.loads(item['id']) ## format is "[xxxx,xxxx]"
        except Exception as err:
          ids = item['id'].split(',') ## format is "xxxx,xxxx,xxxxx"
        ticker=tickers[f'{ids[0]}']
        fieldName = fields[f'{ids[1]}']
        attr =  item['attributes']
        if ( 'value' in attr): 
          val = attr['value']
        elif ( 'grade' in attr):
          val = attr['grade']
        
        if ( ticker in ratings ):
          rating = ratings[ticker]
        else:
          rating = {}
        rating[fieldName] = val
        rating['indexGroup'] = indexGroup[ticker]
        rating['exchange'] = exchange[ticker]
        ratings[ticker] = rating
        
      return ratings

      #print (f'{ticker} {fieldName} {val}')



  def getTodayRatings(self, tickers):

    pagesize = 100
    nSize = len(tickers)
    wholeRatings = {}
    for page in range(0, math.ceil(nSize/pagesize)):
      print (f"Search metrics page {page} ..............")
      last = page*pagesize+pagesize
      if ( last > nSize):
        last = nSize
      subTickers=tickers[page*pagesize:last]
    
      rtn = self.fetchGradeMatix(subTickers)
      ratings = self._convertToRatings (rtn)
      rtn = self.fetchCategoryMatix(subTickers)
      ratings = self._convertToRatings (rtn, ratings)
      wholeRatings |= ratings
      
      time.sleep(2) ## to prevent server from throttling
    ratings={}
    for t in wholeRatings:
      ratings[t] = self.convertDailyratingToDbEntry(t, wholeRatings[t])
    return ratings
  
class TradeStation:
  def __init__(self):
    self.keyFile = "C:\\Users\\jwdai\\AppData\\Roaming\\Trading\\Security\\Tradestation\\keys"
    self.security = self.__get_keys__(self.keyFile)
    self.conn = http.client.HTTPSConnection("api.tradestation.com",443)
      
  
  def __get_keys__(self, fname):
    with open(fname) as f_in:
        return json.load(f_in)

  def getQuotes(self, tickers):
    #print(self.security)
   
    length = len(tickers)
    loops = int(length/100)+1
    
    quotes=[]
    for loop in range(loops):
      symbols = []
      num = length % 100 if ( loop == loops-1) else 100
      tickerStr = None
      for i in range(num):
        symbols.append(tickers[loop*100+i])
          #shold be less than 100 tickers
        tickerStr=",".join(symbols)
      if ( tickerStr == None or len(tickerStr) == 0):
         return
      url = f'https://api.tradestation.com/v3/marketdata/quotes/{tickerStr}'
      #url = f'https://44.212.249.107/v3/marketdata/quotes/{tickerStr}'
      url = f'/v3/marketdata/quotes/{tickerStr}'
      #44.212.249.107
      print (url)
        #token = f''
      #r=requests.get(url, headers={"Connection":"keep-alive", "Authorization": f'Bearer {self.security['access_token']}'})

      
 
      self.conn.request("GET", url,  headers={"Connection":"keep-alive", "Authorization": f'Bearer {self.security['access_token']}'})

      r1 = self.conn.getresponse()
      r  = r1.read()
    
      if ( r):
        #quotesObj = r.json()
        quotesObj = json.loads(r)
        print (r)
        if ( "Quotes" in quotesObj):
          quotes += quotesObj["Quotes"]
    return quotes
          
   # print (r)
    

class MyTradierAPI(TradierAPI):
  def __init__(self):
    token = "XY9MWydqz7fG3ARufuTaapMRT3us" # os.environ["TRADIER_TOKEN"]
    account_id = "6YA60331" # os.environ["TRADIER_ACCOUNT_ID"]
    super().__init__(token=token, default_account_id=account_id, endpoint="https://api.tradier.com/v1/")

  def  getQuotes(self, symbols):
    s = ",".join (symbols)
    params={'symbols': s}
    data = self.get('markets/quotes', params=params)
    if ( not 'quotes' in data):
        return None
    quote = data['quotes']
    if ( 'quote' not in quote):
        return None
    
    rtn = quote['quote']
    if ( isinstance(rtn, list)):
      quotes = rtn
    else:
      quotes = [rtn]
    return quotes
  
  def getHistoricalDailyPrices(self, symbol, start_date, end_date):
      """
      Fetch historical daily prices for a symbol from Tradier.
      Returns a list of dicts with keys: date, open, close, high, low, volume.
      """
      params = {
          'symbol': symbol,
          'start': start_date,
          'end': end_date,
          'interval': 'daily',
          'session_filter': 'all'
      }
      data = self.get('markets/history', params=params)
      if not data or 'history' not in data or 'day' not in data['history']:
          return []
      return data['history']['day']
  
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
    return root+dateStr+sideStr+strikeStr

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
    
  def getScreenedStocks(self, condition=None )->list:

    if ( condition is None):
      payload = {
        "quant_rating": {"gte": 4.9, "lte": 5  },
        "sell_side_rating": { "gte": 4,    "lte": 5    },
        "close": { "gte": 4    },
      }
    else:
      payload = condition

    page = 1
    totPages = 1
    tickers = []
    cont = True
    while (cont):
        
      querystring = {"page": f"{page}","per_page":"100","type":"stock"}
      print (f"search page {page}")
      rtn = self.post("screeners/get-results", querystring, payload)
      if ( 'meta' in rtn):
        meta = rtn['meta']
        count = meta['count']
        totPages = math.ceil(count/100 )
        if ( page >= totPages ):
          cont = False
      else:
        cont = False
      if ( 'data' in rtn):
        data = rtn['data']
        for entry in data:
            ticker = entry['attributes']['name'].upper()
            tickers.append({ticker:entry['id']})
      page += 1
    return tickers
            
  def getStockRating(self, ticker)->list:
     return self.request('GET', 'symbols/get-ratings' , {'symbol':ticker})

  # print (rtn)


class FetchPolygenAPI:
    
    class TimeSpan:
       SECOND = 'second'
       MINUTE = 'minute'
       HOUR = 'hour'
       DAY = 'day'
       WEEK = 'week'
       MONTH = 'month'

    class Sort:
      ASC = 'asc'
      DESC = 'desc'
    def __init__(self):
        self.endpoint = "https://api.polygon.io/v2/" 
        self.session = requests.Session()

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
        params['apiKey']='8pUxvsizskbaE9fK5sZn0oHN4kk34VMH'
        return self.request("GET", path, params)
    
    def getDailyPrice(self, ticker, startDate, endDate):
        ticker = ticker.upper()
        url = f"aggs/ticker/{ticker}/range/1/day/{startDate}/{endDate}"
        #print (url)
        rts = self.get(url, { 'adjusted':'true', 'sort':'asc'})
        return rts
    def getTickerDetails(self, ticker):
        ticker = ticker.upper()
        url = f"reference/tickers/{ticker}"
        #print (url)
        rts = self.get(url,{})
        return rts

    def getBars(self, ticker, multiplier, timespan:TimeSpan, startDate:datetime, endDate:datetime, sort:Sort=Sort.ASC, limit = 5000):
        ticker = ticker.upper()
        ts1=int(startDate.timestamp()*1000)
        ts2=int(endDate.timestamp()*1000)
        url = f"aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{ts1}/{ts2}"
        #print (url)
        rts = self.get(url, { 'adjusted':'true', 'sort':sort, 'limit': limit})
        return rts

class alphaDb :

  class MyPosition:
    Ticker:str = None
    AssetType:str = None
    Symbol:str = None
    Qty:float = None
    AveragePrice:float = None
    CurrentPrice:float = None
    Expiration:datetime = None
    MaintenanceMargin:float=None
    CallPut:str = None
    Strike:float = None
    Tag:str = None
    Sector:str = None
    Industry:str = None
    Beta:float = None
    CurrentDayProfitLoss:float = None
    Category:str=None

  def __init__(self, TrustedConnection=False):
    self.conn = self._getSqlConn_(TrustedConnection)

  def _getSqlConn_(self, TrustedConnection=False):
    SERVER=r"JD_NEWHP\SQLEXPRESS"
    DATABASE="seekAlpha"
    USERNAME="jaydai"
    PASSWORD="Rosebud00!"
    if ( TrustedConnection):
      connectionString = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
    else:
      connectionString = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};"

    conn = pyodbc.connect(connectionString) 
    return conn

  def queryTopTickers(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  Ticker, UpdatedDate, CurrentRank, IsCurrent  FROM dbo.TopStocks "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"

    res = cursor.execute(qstr)
    return res
  
  def queryDbSql(self,sql, commit=False):
    cursor = self.conn.cursor()

    res = cursor.execute(sql)
    if ( commit):
      cursor.commit()
    return res
  
  def queryMyPositions(self, conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  *  FROM dbo.MyPositions "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"

    res = cursor.execute(qstr)
    return res
  
  def removeMyPositions(self):
    cursor = self.conn.cursor()
    qstr = r"DELETE from dbo.MyPositions"
    res = cursor.execute(qstr)
    res = cursor.commit()
    return res    
    

  
  def saveMyPosition(self, position:MyPosition):
    cursor = self.conn.cursor()

    qstr = (r"INSERT INTO dbo.MyPositions(Ticker, AssetType, Symbol, Qty, AveragePrice, "
            r"CurrentPrice, MaintenanceMargin, Expiration, CallPut, Strike, CurrentDayProfitLoss, Tag, Sector, Industry, Beta,Category) "
            r"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    
    
    res = cursor.execute(qstr, position.Ticker,position.AssetType, position.Symbol, position.Qty, position.AveragePrice,
                         position.CurrentPrice, position.MaintenanceMargin, position.Expiration.strftime("%Y-%m-%d") if position.Expiration !=None else None , position.CallPut, position.Strike, position.CurrentDayProfitLoss,
                          position.Tag, position.Sector, position.Industry,position.Beta , position.Category)
    res = cursor.commit()
    return res    
  
  def queryAllTickers(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  Ticker  FROM dbo.Tickers "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"
    qstr += ' Order by Rank'
    res = cursor.execute(qstr)
    tickers = []
    for row in  res:
      
      ticker =row.Ticker.rstrip()
      tickers.append(ticker)
    return tickers

  

  def _convertTs_(self, ts):
    try:
      timeObj = parser.parse(ts)
      est = pytz.timezone('US/Eastern')
      fmt = '%Y-%m-%d %H:%M:%S'
      ts2 = timeObj.astimezone(est).strftime(fmt)
      print (f"converting {ts} into {ts2} ..........")
      return ts2
    except Exception as error:
      print ("Error in converting timestamp ", ts)
      return None
    
  def clearCurrentFlag(self):
    qstr = "UPDATE dbo.TopStocks SET isCurrent=0 "
    cursor = self.conn.cursor()
    cursor.execute(qstr)
    cursor.commit()

  def updateRank(self, ticker, rank):
    qstr = "UPDATE dbo.TopStocks SET UpdatedDate= CURRENT_TIMESTAMP, CurrentRank=?, IsCurrent=1 WHERE Ticker=? "
    cursor = self.conn.cursor()
    cursor.execute(qstr, rank, ticker)
    cursor.commit()
  
  def createBaMsgEntry(self, id, date, msg):
    qstr = (
      'INSERT INTO dbo.BaMessages ' 
      '(Id, Date, Msg ) '
      "VALUES(?, ?, ? )" )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, id, date, msg) ):
      rtn = cursor.commit()
      return True
    else:
      return None
    
  def queryBaMessages(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  *  FROM dbo.BaMessages "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"

    res = cursor.execute(qstr)
    return res
  
  def queryBaAlerts(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  *  FROM dbo.BaAlerts "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"

    res = cursor.execute(qstr)

    return res
  
  def updateBaAlertAmount (self, MessageId, AmountSide,  Amount):
    cursor = self.conn.cursor()
    qstr = r"UPDATE dbo.BaAlerts SET AmountSide=?, Amount=? WHERE MessageId=?" 
 

    res = cursor.execute(qstr, AmountSide, Amount, MessageId)
    res = cursor.commit()
    return res

  def queryBaPositions(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  *  FROM dbo.BaPositions "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"

    res = cursor.execute(qstr)

    return res  

  def createPositionEntry(self, AlertId, OpenDate, Ticker, Asset, BuyPower, OpenAmt, Trader, AdjustCnt=0, AdjustAmt=0):
    qstr = (
      'INSERT INTO dbo.BaPositions ' 
      '(AlertId, OpenDate, Ticker, Asset, BuyPower, OpenAmt, Trader, AdjustCnt, AdjustAmt) '
      "VALUES(?, ?, ?, ?, ?, ?,?,? ,?)" )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, AlertId, OpenDate, Ticker, Asset, BuyPower, OpenAmt, Trader, AdjustCnt, AdjustAmt) ):
      rtn = cursor.commit()
      return True
    else:
      return None
  
  def updatePositionCloseEntry(self, AlertId, CloseDate, CloseAmt, ReturnPct):
    qstr = (
      'UPDATE dbo.BaPositions ' 
      'SET CloseDate=?,CloseAmt=?, ReturnPct=? WHERE AlertId=? '
      )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, CloseDate, CloseAmt, ReturnPct, AlertId ) ):
      rtn = cursor.commit()
      return True
    else:
      return None
  
  def updateAlertFlag(self, AlertId, Command=None):
    qstr = (
      'UPDATE dbo.BaAlerts ' 
        "SET Flag1 ='DONE' WHERE AlertId=?" )
    #print (qstr)
    if ( Command != None):
      qstr += ' AND Command=?'
    else:
      qstr += ' AND Command <>?'
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr,  AlertId, Command ) ):
      rtn = cursor.commit()
      return True
    else:
      return None
  

  def updatePositionAdjustEntry(self, AlertId, AdjustCnt, AdjustAmt):
    qstr = (
      'UPDATE dbo.BaPositions ' 
      'SET AdjustCnt=?, AdjustAmt=?  WHERE AlertId=? '
      )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, AdjustCnt, AdjustAmt, AlertId ) ):
      rtn = cursor.commit()
      return True
    else:
      return None

  def createAlertEntry(self, info:BaAlert):
    qstr = (
      'INSERT INTO dbo.BaAlerts ' 
      '(MessageId, AlertId, Command, Asset, Ticker, TradeSide, Ts, TradeDetails, AmountSide, Amount, BuyPower, ReturnPct,  Trader, Comments) '
      "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ? )" )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, info.MessageId, info.AlertId, info.Command, info.Asset, info.Ticker, info.TradeSide, info.Ts,
                        info.TradeDetails,  info.AmountSide, info.Amount, info.BuyPower, info.ReturnPct,  
                        info.Trader, info.Comments[:199]) ):
      rtn = cursor.commit()
      return True
    else:
      return None
  
  def createEntry(self, ticker, rank):
    print ("Inserting entry into database..........")
    qstr = (
      'INSERT INTO dbo.TopStocks ' 
      '(Ticker,  CurrentRank, UpdatedDate, IsCurrent ) '
      "VALUES(?, ?, CURRENT_TIMESTAMP,? )" )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, ticker, rank, 1) ):
      rtn = cursor.commit()
      return True
    else:
      return None
  
  def insertDailyPrice(self, ticker, dateStr, open, close, high, low, volume):
    qstr = (
        'INSERT INTO dbo.DailyPrice(Ticker, Date, O, C, H, L, V) '
        ' VALUES (?, ?, ?, ?, ?, ?, ?)' )
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, ticker, dateStr, open, close, high, low, volume) ):
      rtn = cursor.commit()
      return True
    else:
      return None  

  def createRateHistoryEntry(self, entry):
    #print ("Inserting entry into database..........")
    qstr = (
      'INSERT INTO dbo.Ratings ' 
      '(Ticker,  Date, authorsRating, authorsCount, quantRating,'
      'sellSideRating, divConsistencyCategoryGrade, divGrowthCategoryGrade,'
      'divSafetyCategoryGrade, dividendYieldGrade, dpsRevisionsGrade,'
      'epsRevisionsGrade, growthGrade, momentumGrade,'
      'profitabilityGrade, valueGrade, yieldOnCostGrade) '
      'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr,  entry['Ticker'], entry['Date'], entry['authorsRating'], 
                        entry['authorsCount'], entry['quantRating'], entry['sellSideRating'], 
                        entry['divConsistencyCategoryGrade'], entry['divGrowthCategoryGrade'], entry['divSafetyCategoryGrade'], 
                        entry['dividendYieldGrade'], entry['dpsRevisionsGrade'], entry['epsRevisionsGrade'], 
                        entry['growthGrade'], entry['momentumGrade'], entry['profitabilityGrade'], 
                        entry['valueGrade'], entry['yieldOnCostGrade']) ):
      rtn = cursor.commit()
      return True
    else:
      return None
    
  def updateRateHistoryEntry(self, entry):
    #print ("Inserting entry into database..........")
    qstr = (
      'UPDATE dbo.Ratings ' 
      'SET authorsRating=?, authorsCount=?, quantRating=?,'
      'sellSideRating=?, divConsistencyCategoryGrade=?, divGrowthCategoryGrade=?,'
      'divSafetyCategoryGrade=?, dividendYieldGrade=?, dpsRevisionsGrade=?,'
      'epsRevisionsGrade=?, growthGrade=?, momentumGrade=?,'
      'profitabilityGrade=?, valueGrade=?, yieldOnCostGrade=? '
      'WHERE Ticker=? AND Date=?'
    )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr,  entry['authorsRating'], 
                        entry['authorsCount'], entry['quantRating'], entry['sellSideRating'], 
                        entry['divConsistencyCategoryGrade'], entry['divGrowthCategoryGrade'], entry['divSafetyCategoryGrade'], 
                        entry['dividendYieldGrade'], entry['dpsRevisionsGrade'], entry['epsRevisionsGrade'], 
                        entry['growthGrade'], entry['momentumGrade'], entry['profitabilityGrade'], 
                        entry['valueGrade'], entry['yieldOnCostGrade'], entry['Ticker'], entry['Date']) ):
      rtn = cursor.commit()
      return True
    else:
      return None
    

    
  def createTickerEntry(self, ticker, rank, id):
    print ("Inserting entry into database..........")
    qstr = (
      'INSERT INTO dbo.Tickers ' 
      '(Ticker,  Rank, Id ) '
      "VALUES(?, ?, ? )" )
    #print (qstr)
    cursor = self.conn.cursor()
    if ( cursor.execute(qstr, ticker, rank, id) ):
      rtn = cursor.commit()
      return True
    else:
      return None  
    
  def queryDb(self):
    self.clearCurrentFlag()
    rows = self.queryTopTickers()
    tickers={}
    for row in rows:
      ticker =row.Ticker.rstrip()
      updatedDate = row.UpdatedDate
      rank= row.CurrentRank
      isCurrent = row.IsCurrent
      tickers[ticker] = [updatedDate, rank, isCurrent]

    return tickers  
  
  def updateDb(self, tickers):
  # Perform a query.
    dbTickers = self.queryDb()
    rates={}
    rank = 1
    for ticker in tickers:
      if ( ticker in dbTickers):
        print ("This ticker is in db", ticker, dbTickers[ticker])
        self.updateRank(ticker, rank)
      else:
         self.createEntry(ticker, rank)
      rank +=1

  def queryTickerRatings(self,conditionStr=None):
    cursor = self.conn.cursor()
    qstr = r"SELECT  *  FROM dbo.Ratings "
    if ( conditionStr ):
       qstr = qstr + f" WHERE {conditionStr}"
    res = cursor.execute(qstr)
    return res