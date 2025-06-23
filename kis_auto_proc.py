import psycopg2 as db
from datetime import datetime
import kis_api_resp as resp
import requests
import json
import telegram
import asyncio
import math
import pandas as pd

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
time = datetime.now().strftime("%H%M")
second = datetime.now().strftime("%H%M%S")

# 인증처리
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

# 주식현재가 시세
def inquire_price(access_token, app_key, app_secret, code):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST01010100"}
    params = {
            'FID_COND_MRKT_DIV_CODE': "J",
            'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output

# 국내주식업종기간별시세
def inquire_daily_indexchartprice(access_token, app_key, app_secret, market, stock_day):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKUP03500100",
               "custtype": "P"}
    params = {
        'FID_COND_MRKT_DIV_CODE': "U",  # 시장 분류 코드(J : 주식, ETF, ETN U: 업종)
        'FID_INPUT_ISCD': market,
        'FID_INPUT_DATE_1': stock_day,
        'FID_INPUT_DATE_2': stock_day,
        'FID_PERIOD_DIV_CODE': 'D'}
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output1

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):
    
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}    # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
    params = {
                "CANO": acct_no,
                'ACNT_PRDT_CD': '01',
                'AFHR_FLPR_YN': 'N',
                'FNCG_AMT_AUTO_RDPT_YN': 'N',
                'FUND_STTL_ICLD_YN': 'N',
                'INQR_DVSN': '01',
                'OFL_YN': 'N',
                'PRCS_DVSN': '01',
                'UNPR_DVSN': '01',
                'CTX_AREA_FK100': '',
                'CTX_AREA_NK100': ''
            }
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    
    if rtFlag == "all" and ar.isOK():
        output = ar.getBody().output2
    else:    
        output = ar.getBody().output1

    return pd.DataFrame(output)

# 주식당일분봉조회
def inquire_time_itemchartprice(access_token, app_key, app_secret, code, req_minute):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST03010200",
               "custtype": "P"}
    params = {
                'FID_COND_MRKT_DIV_CODE': "J",      # J:KRX, NX:NXT, UN:통합
                'FID_INPUT_ISCD': code,
                'FID_INPUT_HOUR_1': req_minute,     # 입력시간 현재시간이전(123000):12시30분 이전부터 1분 간격 최대 30건, 현재시간이후(123000):현재시간(120000)으로 조회, 60:현재시간부터 1분 간격, 600:현재시간부터 10분 간격, 3600:현재시간부터 1시간 간격
                'FID_PW_DATA_INCU_YN': 'N',         # 과거 데이터 포함 여부 N:당일데이터만 조회, Y:과거데이터 포함 조회
                'FID_ETC_CLS_CODE': ""
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)

    return ar.getBody().output2

async def main(telegram_text):
    chat_id = "2147256258"
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=telegram_text, parse_mode='HTML')

# 휴일정보 조회
cur0 = conn.cursor()
cur0.execute("select name from stock_holiday where holiday = '"+today+"'")
result_one = cur0.fetchone()
cur0.close()

nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15']

# 휴일이 아닌 경우
if result_one == None:

    for nick in nickname_list:
        try:
            # 텔레그램봇 사용할 token
            if nick == 'chichipa':
                token = "6353758449:AAG6LVdzgSRDSspoSzSJZVGnGw1SGHlAgi4"
            elif nick == 'phills13':
                token = "5721274603:AAHiwtuara7M-I-MIzcrt3E8TZBCRUpBUB4"
            elif nick == 'phills15':
                token = "6376313566:AAFPYOKj5_yyZ5jZJJ4JXJPqpyZXXo3fZ4M"
            elif nick == 'phills2':
                token = "5458112774:AAGwNnfjuC75WdK2ZYm_mttmXajzkhyvaHc"
            elif nick == 'phills75':
                token = "7242807146:AAH9fbu34tKKNaDDtJ2ew6zYPhzXkVvc9KA"
            elif nick == 'yh480825':
                token = "8143915544:AAEF-wVvqg9XZFKkVF4zUjm5LYC648OSWOg"    
            else:
                token = "6008784254:AAEcJaePafd6Bh0riGL57OjhZ_ZoFxe6Fw0"  

            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']

            # 매매자동처리 조회
            cur31 = conn.cursor()

            cur31.execute("select id, name, code, base_dtm, trade_tp, signal_cd, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum from trade_auto_proc where base_day = '" + today + "' and proc_yn = 'Y' and acct_no = '" + str(acct_no) + "'")
            result_three_one = cur31.fetchall()
            cur31.close()

            # 매매자동처리 거래량과 주식당일분봉조회의 최대 거래량 비교
            for i in result_three_one:
                print("종목명 : " + i[1])

                # 주식당일분봉조회
                minute_info = inquire_time_itemchartprice(access_token, app_key, app_secret, i[2], second)
                minute_list = []
                for item in minute_info:
                    minute_list.append({
                        '체결시간': item['stck_cntg_hour'],
                        '종가': item['stck_prpr'],
                        '시가': item['stck_oprc'],
                        '고가': item['stck_hgpr'],
                        '저가': item['stck_lwpr'],
                        '거래량': item['cntg_vol']
                    })

                df = pd.DataFrame(minute_list)
                df['체결시간'] = pd.to_datetime(df['체결시간'], format='%H%M%S')
                df = df.sort_values('체결시간').reset_index(drop=True)
            
                df.rename(columns={
                    '종가': 'close',
                    '시가': 'open',
                    '고가': 'high',
                    '저가': 'low',
                    '거래량': 'volume',
                    '체결시간': 'timestamp'
                }, inplace=True)

                df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                df['body'] = (df['close'] - df['open']).abs()

                # 기준봉: 가장 거래량 많은 봉
                idx = df['volume'].idxmax()
                기준봉 = df.loc[idx]

                while True:
                    이후_봉들 = df[df['timestamp'] > 기준봉['timestamp']]
                    candidates = 이후_봉들[
                        (이후_봉들['volume'] >= 기준봉['volume']) 
                    ]
                    if candidates.empty:
                        break
                    기준봉 = candidates.iloc[0]

                # 매매자동처리 정보의 거래량보다 기준봉 거래량이 큰 경우 매매자동처리 생성 및 기존 매매자동처리 변경(proc_yn = 'N')
                if 기준봉['volume'] > i[10]:
                    avg_body = df['body'].rolling(20).mean().iloc[-1] if len(df) >= 20 else df['body'].mean()

                    # 몸통 유형 구분
                    body_value = 기준봉['body']
                    if body_value > avg_body * 1.5:
                        candle_body = "L"   # 장봉
                    elif body_value < avg_body * 0.5:
                        candle_body = "S"   # 단봉
                    else:
                        candle_body = "M"   # 보통
                    
                    # 매매자동처리 insert
                    cur500 = conn.cursor()
                    insert_query = """
                        INSERT INTO trade_auto_proc (
                            acct_no, name, code, base_day, base_dtm, trade_tp, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum, proc_yn, regr_id, reg_date, chgr_id, chg_date
                        )       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (acct_no, code, base_day, base_dtm, trade_tp) DO NOTHING
                    """
                    # insert 인자값 설정
                    cur500.execute(insert_query, (
                        acct_no, i[1], i[2], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), "S", 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, '100', 'Y', 'AUTO_SELL', datetime.now(), 'AUTO_SELL', datetime.now()
                    ))

                    was_inserted = cur500.rowcount == 1

                    conn.commit()
                    cur500.close()

                    if was_inserted:
                        # 매매자동처리 update
                        cur501 = conn.cursor()
                        update_query = """
                            UPDATE trade_auto_proc
                            SET
                                proc_yn = 'N'
                                , chgr_id = 'AUTO_UP_SELL'
                                , chg_date = %s
                            WHERE acct_no = %s
                            AND code = %s
                            AND base_day = %s
                            AND base_dtm <> %s
                            AND trade_tp = 'S'
                            AND proc_yn = 'Y'
                        """

                        # update 인자값 설정
                        cur501.execute(update_query, (
                            datetime.now(), str(acct_no), i[2], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S")
                        ))

                        conn.commit()
                        cur501.close()

            # 매매자동처리 조회
            cur32 = conn.cursor()

            cur32.execute("select id, name, code, base_dtm, trade_tp, signal_cd, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum from trade_auto_proc where base_day = '" + today + "' and proc_yn = 'Y' and acct_no = '" + str(acct_no) + "'")
            result_three_two = cur32.fetchall()
            cur32.close()

            # 매매자동처리 고가, 저가, 종가, 시가, 거래량, 캔들형태를 각각 실시간 종목시세의 최고가와 최저가 비교
            for i in result_three_two:
                print("종목명 : " + i[1])

                trail_signal_code = ""
                trail_signal_name = ""
                vol_appear = 0
                candle_type = ""

                if i[11] == 'L': 
                    candle_type  = "[장봉] "
                elif i[11] == 'S': 
                    candle_type  = "[단봉] "

                # 주식현재가 시세
                a = inquire_price(access_token, app_key, app_secret, i[2])

                # 매수 대상
                if i[4] == 'B':

                    n_buy_sum = 0
                    n_buy_amount = 0
                    loss_price = 0
                    item_loss_sum = 0

                    if int(a['stck_prpr']) > i[7]:
                        print("돌파가 돌파")
                        signal_cd = "01"
                        signal_cd_name = format(int(i[7]), ',d') + "원 {돌파가 돌파}"
                        # 매수금액
                        n_buy_sum = int(i[12])
                        # 매수량 = round(매수금액 / 현재가)
                        n_buy_amount = round(n_buy_sum / int(a['stck_prpr']))
                        # 손절가
                        loss_price = i[8]
                        # 손절금액 = (현재가 - 손절가) * 매수량
                        item_loss_sum = (int(a['stck_prpr']) - int(loss_price)) * n_buy_amount

                        buy_command = f"/InterestBuy_{i[2]}_{a['stck_prpr']}"

                        telegram_text = (f"[자동매수]{i[1]}[<code>{i[2]}</code>] : {candle_type}{trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                        # 텔레그램 메시지 전송
                        asyncio.run(main(telegram_text))

                        cur400 = conn.cursor()
                        # UPDATE
                        cur400.execute("""
                            UPDATE trade_auto_proc
                            SET
                                signal_cd = %s,
                                proc_yn = 'N',
                                chgr_id = 'AUTO_PROC_BAT',
                                chg_date = now()
                            WHERE acct_no = %s 
                            AND proc_yn = 'Y' 
                            AND base_day = %s 
                            AND code = %s
                            AND trade_tp = 'B'
                        """
                        , (
                            signal_cd, 
                            str(acct_no),
                            today,
                            i[2]
                        ))    

                        conn.commit()
                        cur400.close()

                # 매도 대상
                elif i[4] == 'S':

                    if int(a['stck_prpr']) < i[8]:
                        print("이탈가 이탈")
                        
                        # 계좌종목 조회
                        c = stock_balance(access_token, app_key, app_secret, acct_no, "")

                        balance_list = []

                        for j, name in enumerate(c.index):
                            J_code = c['pdno'][j]
                            j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                            sell_rate = 0
                            sell_amount = 0
                            sell_sum = 0

                            # 잔고정보의 매매자동처리 종목이 존재할 경우
                            if J_code == i[2]:
                                signal_cd = "02"
                                signal_cd_name = format(int(i[8]), ',d') + "원 {이탈가 이탈}"

                                # 주문가능수량 존재시
                                if j_ord_psbl_qty > 0:
                                    # 매도비율(%)
                                    sell_rate = int(i[12])
                                    # 매도량 = round((주문가능수량 / 매도비율 )* 100)
                                    sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                                    # 매도금액 = 매도량 * 현재가
                                    sell_sum = sell_amount * int(a['stck_prpr'])

                                    sell_command = f"/HoldingSell_{i[2]}_{sell_amount}"

                                    telegram_text = (f"[자동매도]{i[1]}[<code>{i[2]}</code>] : {candle_type}{signal_cd_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {format(sell_amount, ',d')}주, 매도금액 : {format(sell_sum, ',d')}원 => {sell_command}")
                                    # 텔레그램 메시지 전송
                                    asyncio.run(main(telegram_text))

                                    cur400 = conn.cursor()
                                    # UPDATE
                                    cur400.execute("""
                                        UPDATE trade_auto_proc
                                        SET
                                            signal_cd = %s,
                                            proc_yn = 'N',
                                            chgr_id = 'AUTO_PROC_BAT',
                                            chg_date = now()
                                        WHERE acct_no = %s 
                                        AND proc_yn = 'Y' 
                                        AND base_day = %s 
                                        AND code = %s
                                        AND trade_tp = 'S'
                                    """
                                    , (
                                        signal_cd, 
                                        str(acct_no),
                                        today,
                                        i[2]
                                    ))    

                                    conn.commit()
                                    cur400.close()

        except Exception as e:
            print(f"[{nick}] Error kis_auto_proc : {e}")      
    
    conn.close()

else:
    conn.close()
    print("Today is Holiday")