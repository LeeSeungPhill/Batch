import kis_stock_search_api as search
import requests
import json
from datetime import datetime
import psycopg2 as db

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

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

cur01 = conn.cursor()
cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date from \"stockAccount_stock_account\" where nick_name = 'phills75'")
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

# 거래폭발 종목검색
#search.search(access_token, app_key, app_secret, '0', 'phills2')
# 단기추세 종목검색
#search.search(access_token, app_key, app_secret, '1', 'phills2')
# 투자혁명 종목검색
#search.search(access_token, app_key, app_secret, '2', 'phills2')

conn.close()

