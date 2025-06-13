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
            cur03 = conn.cursor()

            cur03.execute("select id, name, code, base_dtm, trade_tp, signal_cd, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum from trade_auto_proc where base_day = '" + today + "' and proc_yn = 'Y' and acct_no = '" + str(acct_no) + "'")
            result_three = cur03.fetchall()
            cur03.close()

            # 매매자동처리 고가, 저가, 종가, 시가, 거래량, 캔들형태를 각각 실시간 종목시세의 최고가와 최저가 비교
            for i in result_three:
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