import kis_api_resp as resp
import requests
import json
import psycopg2 as db
import sys
import math
from datetime import datetime
from itertools import product

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
time = datetime.now().strftime("%H%M")

# 조합 목록
MARKET_MAP  = {'0001': '코스피', '1001': '코스닥'}
TR_MAP      = {'0': '순매수', '1': '순매도'}
MAIN_MAP    = {'1': '외국인', '2': '기관'}

def auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    ACCESS_TOKEN = res.json()["access_token"]
    return ACCESS_TOKEN

def inquire_subtotal(access_token, app_key, app_secret, market_code, tr_code, main_code):
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHPTJ04400000",
               "custtype": "P"}
    params = {
        'FID_COND_MRKT_DIV_CODE': "V",
        'FID_COND_SCR_DIV_CODE': '16449',
        'FID_INPUT_ISCD': market_code,      # 0001:코스피, 1001:코스닥
        'FID_DIV_CLS_CODE': '0',            # 0: 수량정렬, 1: 금액정렬
        'FID_RANK_SORT_CLS_CODE': tr_code,  # 0: 순매수상위, 1: 순매도상위
        'FID_ETC_CLS_CODE': main_code       # 1:외국합, 2:기관계
    }
    PATH = "/uapi/domestic-stock/v1/quotations/foreign-institution-total"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    return ar.getBody().output

def process_combination(access_token, app_key, app_secret, market_code, tr_code, main_code, hms):
    market_name = MARKET_MAP[market_code]
    tr_gubun    = TR_MAP[tr_code]
    main_gubun  = MAIN_MAP[main_code]

    print(f"\n[{market_name}] {tr_gubun} / {main_gubun}")

    cur_chk = conn.cursor()
    cur_chk.execute(
        "select code from subject_sub_total where tr_day = %s and tr_time = %s "
        "and tr_subject = %s and market_type = %s and tr_type = %s",
        (today, hms, main_gubun, market_name, tr_gubun)
    )
    already = cur_chk.fetchone()
    cur_chk.close()

    if already is not None:
        print("already exists")
        return

    try:
        rows = inquire_subtotal(access_token, app_key, app_secret, market_code, tr_code, main_code)
    except Exception as e:
        print(f"  API 오류: {e}")
        return

    records = []
    for number, i in enumerate(rows, start=1):
        print(f"  순위:{number} {i['hts_kor_isnm']}({i['mksc_shrn_iscd']}) "
              f"현재가:{format(int(i['stck_prpr']),',d')} "
              f"순매수량:{format(abs(int(i['ntby_qty'])),',d')}")
        records.append((
            today, hms, main_gubun, market_name, tr_gubun,
            number, i['mksc_shrn_iscd'], i['hts_kor_isnm'],
            abs(int(i['ntby_qty'])), datetime.now()
        ))

    if records:
        cur_ins = conn.cursor()
        insert_query = (
            "insert into subject_sub_total"
            "(tr_day, tr_time, tr_subject, market_type, tr_type, tr_order, code, name, puri_volumn, cdate) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        cur_ins.executemany(insert_query, records)
        conn.commit()
        cur_ins.close()
        print(f"  → {len(records)}건 저장 완료")

# 계정 조회 및 토큰 갱신
cur01 = conn.cursor()
cur01.execute(
    "select acct_no, access_token, app_key, app_secret, token_publ_date, "
    "substr(token_publ_date, 0, 9) AS token_day "
    "from \"stockAccount_stock_account\" where nick_name = %s",
    (arguments[1],)
)
result_one = cur01.fetchone()
cur01.close()

acct_no      = result_one[0]
access_token = result_one[1]
app_key      = result_one[2]
app_secret   = result_one[3]

validTokenDate = datetime.strptime(result_one[4], '%Y%m%d%H%M%S')
if (datetime.now() - validTokenDate).days >= 1 or result_one[5] != today:
    access_token = auth(app_key, app_secret)
    token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
    print("new access_token : " + access_token)
    cur02 = conn.cursor()
    cur02.execute(
        "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s",
        [access_token, token_publ_date, datetime.now(), acct_no]
    )
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

# 시장(2) × 매수매도(2) × 외국기관(2) = 8가지 조합 일괄 처리
for market_code, tr_code, main_code in product(MARKET_MAP, TR_MAP, MAIN_MAP):
    try:
        process_combination(access_token, app_key, app_secret, market_code, tr_code, main_code, hms)
    except Exception as e:
        print(f"[{market_code}/{tr_code}/{main_code}] 오류: {e}")

conn.close()
