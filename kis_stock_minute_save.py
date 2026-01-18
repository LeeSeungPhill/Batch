import requests
import json
import kis_api_resp as resp
import pandas as pd
from datetime import datetime, timedelta
import time
import psycopg2 as db

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

# 인증처리
def auth(APP_KEY, APP_SECRET):
    # 인증처리
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

# 계정정보 조회
def account(nickname):
    cur01 = conn.cursor()
    cur01.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day = result_two
    validTokenDate = datetime.strptime(token_publ_date, '%Y%m%d%H%M%S')
    if (datetime.now() - validTokenDate).days >= 1 or token_day != today:
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime('%Y%m%d%H%M%S')
        cur02 = conn.cursor()
        cur02.execute("""
            UPDATE "stockAccount_stock_account"
            SET access_token = %s, token_publ_date = %s, last_chg_date = %s
            WHERE acct_no = %s
        """, (access_token, token_publ_date, datetime.now(), acct_no))
        conn.commit()
        cur02.close()

    return {
        'acct_no': acct_no,
        'access_token': access_token,
        'app_key': app_key,
        'app_secret': app_secret
    }

# 주식분봉 조회
def inquire_time_itemchartprice(access_token, app_key, app_secret, code, time):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST03010200",
               "custtype": "P"}
    params = {
        'FID_ETC_CLS_CODE': "",
        'FID_COND_MRKT_DIV_CODE': "J",  # 시장 분류 코드(J : 주식, ETF, ETN U: 업종)
        'FID_INPUT_ISCD': code,
        'FID_INPUT_HOUR_1': time,
        # 종목(J)일 경우, 조회 시작일자(HHMMSS)ex) "123000" 입력 시 12시 30분 이전부터 1분 간격으로 조회 업종(U)일 경우, 조회간격(초) (60 or 120 만 입력 가능) ex) "60" 입력 시 현재시간부터 1분간격으로 조회 "120" 입력 시 현재시간부터 2분간격으로 조회
        'FID_PW_DATA_INCU_YN': 'Y'}  # 과거 데이터 포함 여부(Y/N) * 업종(U) 조회시에만 동작하는 구분값 N : 당일데이터만 조회 Y : 이후데이터도 조회(조회시점이 083000(오전8:30)일 경우 전일자 업종 시세 데이터도 같이 조회됨)
    PATH = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    # ar.printAll()
    return ar.getBody().output2

# 종목 당일 현재시간까지 10분봉 저장(OHLCV)
def stock_minute_get(access_token, app_key, app_secret, code, company):
    # 현재일
    stock_day = datetime.now().strftime("%Y%m%d")

    if stock_day == '20241114':
        startHms = '100000'
        endHms = '163000'
        circle = 13

        if time.strftime('%H%M%S') > endHms:
            hms = '162900'
        elif time.strftime('%H%M%S') < startHms:
            hms = '162900'
        else:
            hms = time.strftime('%H%M%S')
    elif stock_day == '20250102':    
        startHms = '100000'
        endHms = '153000'
        circle = 11

        if time.strftime('%H%M%S') > endHms:
            hms = '152900'
        elif time.strftime('%H%M%S') < startHms:
            hms = '152900'
        else:
            hms = time.strftime('%H%M%S')
    else:    
        startHms = '090000'
        endHms = '153000'
        circle = 13

        if time.strftime('%H%M%S') > endHms:
            hms = '152900'
        elif time.strftime('%H%M%S') < startHms:
            hms = '152900'
        else:
            hms = time.strftime('%H%M%S')        

    low_price = 0
    high_price = 0
    open_price = 0
    close_price = 0
    accum_vol = 0
    
    cur1 = conn.cursor()

    for i in range(circle):
        if hms > startHms:
            a = pd.DataFrame(inquire_time_itemchartprice(access_token, app_key, app_secret, code, hms))

            for i, name in enumerate(a.index):

                trTime = (datetime.strptime(a['stck_cntg_hour'][i][:4], '%H%M') + timedelta(minutes=1)).strftime('%H%M')

                if low_price == 0:
                    low_price = int(a['stck_lwpr'][i])
                elif low_price > int(a['stck_lwpr'][i]):
                    low_price = int(a['stck_lwpr'][i])

                if high_price == 0:
                    high_price = int(a['stck_hgpr'][i])
                elif high_price < int(a['stck_hgpr'][i]):
                    high_price = int(a['stck_hgpr'][i])


                if trTime[2:] == '11' or trTime[2:] == '21' or trTime[2:] == '31' or trTime[2:] == '41' or trTime[2:] == '51' or trTime[2:] == '01':
                    
                    open_price = int(a['stck_oprc'][i])
                    accum_vol += int(a['cntg_vol'][i])

                    tenMinuteUnit = (datetime.strptime(trTime, '%H%M') + timedelta(minutes=9)).strftime('%H%M')

                    print(code + " : " + a['stck_bsop_date'][i] + str(tenMinuteUnit) + ", 현재가 : " + str(close_price) + ", 시가 : " + str(open_price) + ", 저가 : " + str(low_price) + ", 고가 : " + str(high_price) +", 체결 거래량 : " + str(accum_vol)) 
                    
                    insert_query1 = "with upsert as (update stock_minute_info set current_price = %s, open_price = %s, high_price = %s, low_price = %s, volumn = %s, last_chg_date = %s where dt = %s and code = %s returning * ) insert into stock_minute_info(dt, name, code, current_price, open_price, high_price, low_price, volumn, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
                    # insert 인자값 설정
                    record_to_insert1 = ([close_price, open_price, high_price, low_price, accum_vol, datetime.now(), a['stck_bsop_date'][i] + str(tenMinuteUnit), code,
                                        a['stck_bsop_date'][i] + str(tenMinuteUnit), company, code, close_price, open_price, high_price, low_price, accum_vol, datetime.now()])
                    # DB 연결된 커서의 쿼리 수행
                    cur1.execute(insert_query1, record_to_insert1)
                    conn.commit() 

                    low_price = 0
                    high_price = 0
                    accum_vol = 0

                elif trTime[2:] == '10' or trTime[2:] == '20' or trTime[2:] == '30' or trTime[2:] == '40' or trTime[2:] == '50' or trTime[2:] == '00':

                    close_price = int(a['stck_prpr'][i])                
                    accum_vol += int(a['cntg_vol'][i])
                    
                else:
                    accum_vol += int(a['cntg_vol'][i])

            hms = (datetime.strptime((stock_day + hms), '%Y%m%d%H%M%S') - timedelta(minutes=30)).strftime('%H%M%S')

    cur1.close()

cur0 = conn.cursor()
cur0.execute("select name from stock_holiday where holiday = '"+today+"'")
result_one = cur0.fetchone()
cur0.close()

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong']

if result_one == None:

    for nick in nickname_list:
        try:
            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 잔고정보 및 관심종목 조회
            cur100 = conn.cursor()
            cur100.execute("select code, name from \"stockBalance_stock_balance\" where acct_no = '" + str(acct_no) + "' and proc_yn = 'Y' UNION ALL select code, name from \"interestItem_interest_item\" where acct_no = '" + str(acct_no) + "' and length(code) > 4")
            result_one00 = cur100.fetchall()
            cur100.close()

            for i in result_one00:
                time.sleep(3)
                code = i[0]
                name = i[1]

                # 종목 당일 현재시간까지 10분봉 저장(OHLCV)
                stock_minute_get(access_token, app_key, app_secret, code, name)

        except Exception as ex:
            print('종목별 분봉 저장 처리 에러 : ', ex)

    conn.close()

else:
    conn.close()    
    print("Today is Holiday")
