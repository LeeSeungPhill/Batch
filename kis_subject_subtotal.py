import kis_api_resp as resp
import requests
import json
import psycopg2 as db
import sys
import math
from datetime import datetime

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
#conn_string = "dbname='fund_risk_mng' host='192.168.1.8' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
time = datetime.now().strftime("%H%M")

# 시장 구분
if arguments[2] == '0001':
    market_name = "코스피"
else:
    market_name = "코스닥"
# 매수 매도 구분
if arguments[3] == '0':
    tr_gubun = "순매수"
else:
    tr_gubun = "순매도"
# 외국합 기관 구분
if arguments[4] == '1':
    main_gubun = "외국인"
else:
    main_gubun = "기관"

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

def inquire_subtotal(access_token, app_key, app_secret, market_name, tr_gubun, main_gubun):
    print("market_name : "+market_name)
    print("tr_gubun : " + tr_gubun)
    print("main_gubun : " + main_gubun)
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHPTJ04400000",
               "custtype": "P"}
    params = {
            'FID_COND_MRKT_DIV_CODE': "V",
            'FID_COND_SCR_DIV_CODE': '16449',
            'FID_INPUT_ISCD': market_name,             # 0000:전체, 0001:코스피, 1001:코스닥
            'FID_DIV_CLS_CODE': '0',            # 0: 수량정열, 1: 금액정열
            'FID_RANK_SORT_CLS_CODE': tr_gubun, # 0: 순매수상위, 1: 순매도상위
            'FID_ETC_CLS_CODE': main_gubun      # 0:전체 1:외국합 2:기관계 3:기타
    }
    PATH = "/uapi/domestic-stock/v1/quotations/foreign-institution-total"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

cur01 = conn.cursor()
cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day from \"stockAccount_stock_account\" where nick_name = '" + arguments[1] + "'")
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
if diff.days >= 1 or result_one[5] != today:  # 토큰 유효기간(1일) 만료 재발급
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

if time > '1003' and time < '1122':
    hms = '1003'
elif time > '1122' and time < '1322':
    hms = '1122'
elif time > '1322' and time < '1432':
    hms = '1322'
else:
    hms = '1432'

number = 0
cur03 = conn.cursor()
cur04 = conn.cursor()

# DB 연결된 커서의 쿼리 수행
cur03.execute("select code from subject_sub_total where tr_day = %s and tr_time = %s and tr_subject = %s and market_type = %s and tr_type = %s", (today, hms, main_gubun, market_name, tr_gubun))
# DB에서 하나의 결과값 가져오기
result_two = cur03.fetchone()

# DB 미존재시
if result_two == None:

    foreign_institution_total = inquire_subtotal(access_token, app_key, app_secret, arguments[2], arguments[3], arguments[4])  # 장중투자자별매매상위
    for i in foreign_institution_total:

        number = number + 1

        print("순위 : " + str(number))
        print("종목명 : " + i['hts_kor_isnm'])
        print("종목코드 : " + i['mksc_shrn_iscd'])
        print("순매도수량 : " + i['ntby_qty'])
        print("현재가 : " + format(int(i['stck_prpr']), ',d'))
        print("전일대비 : " + format(int(i['prdy_vrss']), ',d'))
        print("전일대비율 : " + i['prdy_ctrt'])
        #print("전일대비율 : " + format(math.ceil(float(i['prdy_ctrt'])), ',d'))
        print("거래량 : " + format(int(i['acml_vol']), ',d'))
        print("외국인순매도수량 : " + format(int(i['frgn_ntby_qty']), ',d'))
        print("기관순매도수량 : " + format(int(i['orgn_ntby_qty']), ',d'))

        # insert 쿼리
        insert_query = "insert into subject_sub_total(tr_day, tr_time, tr_subject, market_type, tr_type, tr_order, code, name, puri_volumn, cdate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # insert 인자값 설정
        record_to_insert = ([today, hms, main_gubun, market_name, tr_gubun, number, i['mksc_shrn_iscd'], i['hts_kor_isnm'], abs(int(i['ntby_qty'])), datetime.now()])
        # DB 연결된 커서의 쿼리 수행
        cur04.execute(insert_query, record_to_insert)
        conn.commit()
else:
    print("already exists")

cur03.close()
cur04.close()
conn.close()