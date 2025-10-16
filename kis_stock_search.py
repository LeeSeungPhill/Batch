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
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
time = datetime.now().strftime("%H%M")

# 조건검색명
if arguments[1] == '0':
    search_name = "거래폭발"
elif arguments[1] == '1':
    search_name = "단기추세"
elif arguments[1] == '2':
    search_name = "투자혁명"
elif arguments[1] == '3':
    search_name = "파워급등주"
elif arguments[1] == '4':
    search_name = "파워종목"        

cur001 = conn.cursor()
cur001.execute("select bot_token1 from \"stockAccount_stock_account\" where nick_name = '" + arguments[2] + "'")
result_001 = cur001.fetchone()
cur001.close()
token = result_001[0]

# 텔레그램 연동 토큰값 설정
bot = telegram.Bot(token)

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
    # ar.printAll()
    return ar.getBody().output2

async def main(telegram_text):
    chat_id = "2147256258"
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id, telegram_text)

cur01 = conn.cursor()
cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date from \"stockAccount_stock_account\" where nick_name = '" + arguments[2] + "'")
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

item_search = inquire_search_result(access_token, app_key, app_secret, 'phills2', arguments[1])  # 종목조건검색 조회

number = 0
cur03 = conn.cursor()
cur04 = conn.cursor()

for i in item_search:

    number = number + 1

    print("순위 : " + str(number))
    print("종목코드 : " + i['code'])
    print("종목명 : " + i['name'])
    print("현재가 : " + format(math.ceil(float(i['price'])), ',d')) 
    print("등락률 : " + str(round(float(i['chgrate']), 2)))
    print("거래량 : " + format(math.ceil(float(i['acml_vol'])), ',d'))
    print("전일대비 : " + str(round(float(i['chgrate2']), 2)))
    print("고가 : " + format(math.ceil(float(i['high'])), ',d')) 
    print("저가 : " + format(math.ceil(float(i['low'])), ',d'))     
    print("시가총액 : " + format(int(round(float(i['stotprice']))), ',d'))
 
    # DB 연결된 커서의 쿼리 수행
    cur03.execute("select code from stock_search_form where code = %s and search_day = %s and search_name = %s", (i['code'], today, search_name))
    # DB에서 하나의 결과값 가져오기
    result_two = cur03.fetchone()

    # DB 미존재시
    if result_two == None:
        # 텔레그램 메시지 전송
        telegram_text = "<" + search_name + "(" + str(number) + ")> " + i['name'] + "[" + i['code'] + "] 현재가 : " + format(math.ceil(float(i['price'])), ',d') + "원, 등략율 : " + str(round(float(i['chgrate']), 2)) + "%, 거래량 : " + format(math.ceil(float(i['acml_vol'])), ',d') + "주, 전일대비 : " + str(round(float(i['chgrate2']), 2)) + "%, 고가 : " + format(math.ceil(float(i['high'])), ',d') + "원, 저가 : " + format(math.ceil(float(i['low'])), ',d') + "원, 시가총액 : " + format(int(round(float(i['stotprice']))), ',d') + "억원"
        # 텔레그램 메시지 전송
        asyncio.run(main(telegram_text))
        # insert 쿼리
        insert_query = "insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # insert 인자값 설정
        record_to_insert = ([today, time, search_name, i['code'], i['name'], math.ceil(float(i['low'])), math.ceil(float(i['high'])), math.ceil(float(i['price'])), int(round(float(i['chgrate']), 2)), math.ceil(float(i['acml_vol'])), int(round(float(i['chgrate2']), 2)), int(round(float(i['stotprice']))), datetime.now()])
        # DB 연결된 커서의 쿼리 수행
        cur04.execute(insert_query, record_to_insert)
        conn.commit()
    else:
        print("already exists")

cur03.close()
cur04.close()
conn.close()