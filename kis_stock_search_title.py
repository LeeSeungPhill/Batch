import kis_api_resp as resp
import requests
import json
import telegram
import psycopg2 as db
import sys
import math
from datetime import datetime
import asyncio

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"    

conn_string = "dbname='fund_risk_mng' host='192.168.50.80' port='5432' user='postgres' password='sktl2389!1'"

conn = db.connect(conn_string)

def auth(APP_KEY, APP_SECRET):

    # 인증처리
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

def inquire_search_result(access_token, app_key, app_secret, id, seq):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "HHKST03900400",
               "custtype": "P"}
    params = {
            'user_id': id,
            'seq': seq
    }
    PATH = "/uapi/domestic-stock/v1/quotations/psearch-result"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output2

def inquire_search_title(access_token, app_key, app_secret, id):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "HHKST03900300",
               "custtype": "P"}
    params = {
            'user_id': id
    }
    PATH = "/uapi/domestic-stock/v1/quotations/psearch-title"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    # ar.printAll()
    return ar.getBody().output2

cur01 = conn.cursor()
cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date from \"stockAccount_stock_account\" where nick_name = 'phills2'")
result_one = cur01.fetchone()
cur01.close()

acct_no = result_one[0]
access_token = result_one[1]
app_key = result_one[2]
app_secret = result_one[3]

YmdHMS = datetime.now()
validTokenDate = datetime.strptime(result_one[4], '%Y%m%d%H%M%S')
diff = YmdHMS - validTokenDate
# print("diff : " + str(diff.days))
if diff.days >= 1:  # 토큰 유효기간(1일) 만료 재발급
    access_token = auth(app_key, app_secret)
    token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
    print("new access_token : " + access_token)
    # 계정정보 토큰값 변경
    cur02 = conn.cursor()
    update_query = "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s"
    # update 인자값 설정
    record_to_update = ([access_token, token_publ_date, datetime.now(), acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur02.execute(update_query, record_to_update)
    conn.commit()
    cur02.close()

item_search = inquire_search_title(access_token, app_key, app_secret, 'phills2')  # 종목조건검색 목록조회
print(item_search)