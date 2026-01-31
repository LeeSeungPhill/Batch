from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import requests
import pandas as pd
import psycopg2 as db
import json
from datetime import time
import sys
import kis_api_resp as resp
from telegram import Bot
from telegram.ext import Updater
import traceback

BASE_URL = "https://openapi.koreainvestment.com:9443"

arguments = sys.argv

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

bot = None
chat_id = None

# 인증처리
def auth(APP_KEY, APP_SECRET):

    # 인증처리
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

def account(nickname):
    cur01 = conn.cursor()
    cur01.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bot_token1, bot_token2, chat_id = result_two
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
        'app_secret': app_secret,
        'bot_token1': bot_token1,
        'bot_token2': bot_token2,
        'chat_id': chat_id
    }

# 일별주문체결조회
def get_my_complete(access_token, app_key, app_secret, acct_no, code, order_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",                            # (3개월이내) TTTC0081R, (3개월이전) CTSC9215R
               "custtype": "P"}
    params = {
            'CANO': acct_no,                                    # 종합계좌번호 계좌번호 체계(8-2)의 앞 8자리
            'ACNT_PRDT_CD':"01",                                # 계좌상품코드 계좌번호 체계(8-2)의 뒤 2자리
            'SORT_DVSN': "01",                                  # 00: 최근 순, 01: 과거 순, 02: 최근 순
            'INQR_STRT_DT': datetime.now().strftime('%Y%m%d'),  # 조회시작일(8자리) 
            'INQR_END_DT': datetime.now().strftime('%Y%m%d'),   # 조회종료일(8자리)
            # 'INQR_STRT_DT': "20250522",  # 조회시작일(8자리) 
            # 'INQR_END_DT': "20250522",   # 조회종료일(8자리)
            'SLL_BUY_DVSN_CD': "00",                            # 매도매수구분코드 00 : 전체 / 01 : 매도 / 02 : 매수
            'PDNO': code,                                       # 종목번호(6자리) ""공란입력 시, 전체
            'ORD_GNO_BRNO': "",                                 # 주문채번지점번호 ""공란입력 시, 전체
            'ODNO': order_no,                                   # 주문번호 ""공란입력 시, 전체
            'CCLD_DVSN': "00",                                  # 체결구분 00 전체, 01 체결, 02 미체결
            'INQR_DVSN': "01",                                  # 조회구분 00 역순, 01 정순
            'INQR_DVSN_1': "",                                  # 조회구분1 없음: 전체, 1: ELW, 2: 프리보드
            'INQR_DVSN_3': "00",                                # 조회구분3 00 전체, 01 현금, 02 신용, 03 담보, 04 대주, 05 대여, 06 자기융자신규/상환, 07 유통융자신규/상환
            'EXCG_ID_DVSN_CD': "ALL",                           # 거래소ID구분코드 KRX : KRX, NXT : NXT, SOR (Smart Order Routing) : SOR, ALL : 전체
            'CTX_AREA_NK100': "",
            'CTX_AREA_FK100': "" 
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{BASE_URL}/{PATH}"

    try:
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        return body.output1 if hasattr(body, 'output1') else []

    except Exception as e:
        print("일별주문체결조회 중 오류 발생:", e)
        return []

# 주식주문(정정취소)
def order_cancel_revice(access_token, app_key, app_secret, acct_no, cncl_dv, order_no, order_qty, order_price):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0013U",            # TTTC0013U[실전투자], VTTC0013U[모의투자]
               "custtype": "P"
    }
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "KRX_FWDG_ORD_ORGNO": "06010",
               "ORGN_ODNO": order_no,
               "ORD_DVSN": "00" if int(order_price) > 0 else "01",  # 지정가 : 00, 시장가 : 01
               "RVSE_CNCL_DVSN_CD": cncl_dv,    # 정정 : 01, 취소 : 02
               "ORD_QTY": str(order_qty),
               "ORD_UNPR": str(order_price),
               "QTY_ALL_ORD_YN": "Y"            # 전량 : Y, 일부 : N
    }
    PATH = "uapi/domestic-stock/v1/trading/order-rvsecncl"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 매도 주문정보 존재시 취소 처리
def sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code):
    
    result_msgs = []

    try:
        # 일별주문체결 조회
        output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, '')

        if len(output1) > 0:
        
            tdf = pd.DataFrame(output1)
            tdf.set_index('odno')
            d = tdf[['pdno', 'odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]
            order_no = 0

            for i, name in enumerate(d.index):

                # 매도주문 잔여수량 존재시
                if d['sll_buy_dvsn_cd'][i] == "01": 
                    
                    if int(d['rmn_qty'][i]) > 0: 
                        order_no = int(d['odno'][i])

                        # 주문취소
                        c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0")
                        if c['ODNO'] != "":
                            print("매도주문취소 완료")

                        else:
                            print("매도주문취소 실패")
                            msg = f"[{d['prdt_name'][i]}] 매도주문취소 실패"
                            result_msgs.append(msg)
            
    except Exception as e:
        print('매도주문취소 오류.', e)
        msg = f"[{code}] 매도주문취소 오류 - {str(e)}"
        result_msgs.append(msg)

    final_message = result_msgs if result_msgs else "success"
    
    return final_message   

# 주식주문(현금)
def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price, cndt_price=None):

    if buy_flag:
        tr_id = "TTTC0012U"                     #buy : TTTC0012U[실전투자], VTTC0012U[모의투자]
    else:
        tr_id = "TTTC0011U"                     #sell : TTTC0011U[실전투자], VTTC0011U[모의투자]

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": tr_id,
               "custtype": "P"
    }
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": stock_code,
               "ORD_DVSN": ord_dvsn,            # 00 : 지정가, 01 : 시장가, 22 : 스톱지정가
               "ORD_QTY": order_qty,
               "ORD_UNPR": order_price          # 시장가 등 주문시, "0"으로 입력
    }
    # 스톱지정가일 때만 조건가격 추가
    if ord_dvsn == "22":
        params["CNDT_PRIC"] = str(cndt_price)

    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):
   
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}            # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
    params = {
                "CANO": acct_no,
                'ACNT_PRDT_CD': '01',
                'AFHR_FLPR_YN': 'N',
                'OFL_YN': '',                   # 오프라인여부 : 공란(Default)
                'INQR_DVSN': '02',              # 조회구분 : 01 대출일별, 02 종목별
                'UNPR_DVSN': '01',              # 단가구분 : 01 기본값
                'FUND_STTL_ICLD_YN': 'N',       # 펀드결제분포함여부 : Y 포함, N 포함하지 않음
                'FNCG_AMT_AUTO_RDPT_YN': 'N',   # 융자금액자동상환여부 : N 기본값
                'PRCS_DVSN': '01',              # 처리구분 : 00 전일매매포함, 01 전일매매미포함
                'CTX_AREA_FK100': '',
                'CTX_AREA_NK100': ''
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
   
    if rtFlag == "all" and ar.isOK():
        output = ar.getBody().output2
    else:    
        output = ar.getBody().output1

    if isinstance(output, list):
        return pd.DataFrame(output)
    else:
        return pd.DataFrame([])

def get_kis_daily_chart(
        stock_code: str,
        trade_date: str,
        access_token: str,
        app_key: str,
        app_secret: str,
        market_code: str = "J",           # J:KRX, NX:NXT, UN:통합
        period: str = "D",                # D:최근30거래일, W:최근30주, M:최근30개월
        adjust_price: str = "1",          # 0:수정주가미반영, 1:수정주가반영
        verbose: bool = True              # 출력 제어 옵션
    ):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010400",
        "custtype": "P"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": market_code,
        "FID_INPUT_ISCD": stock_code,
        "FID_PERIOD_DIV_CODE": period,
        "FID_ORG_ADJ_PRC": adjust_price,
    }

    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if "output" not in data or not data["output"]:
        if verbose:
            print(f"⛔ 일봉 데이터 없음")
        return None

    df = pd.DataFrame(data["output"])
    if df.empty:
        return None

    # 날짜 필터 (YYYYMMDD)
    day_df = df[df["stck_bsop_date"] == trade_date]

    if day_df.empty:
        if verbose:
            print(f"⛔ {trade_date} 일봉 없음")
        return None

    # trade_date 저가
    return int(day_df.iloc[0]["stck_lwpr"])

def get_kis_1min_dailychart(
    stock_code: str,
    trade_date: str,
    trade_time: str,
    access_token: str,
    app_key: str,
    app_secret: str,
    market_code: str = "J",           # J:KRX, NX:NXT, UN:통합
    include_past: str = "Y",          # 과거 데이터 포함
    include_fake_tick: str = "N" ,    # 허봉 제외
    verbose: bool = True              # 출력 제어 옵션
):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST03010230",
        "custtype": "P"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": market_code,
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_DATE_1": trade_date,
        "FID_INPUT_HOUR_1": trade_time,
        "FID_PW_DATA_INCU_YN": include_past,
        "FID_FAKE_TICK_INCU_YN": include_fake_tick
    }

    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if "output2" not in data or not data["output2"]:
        if verbose:
            print(f"⛔ 데이터 없음 ({trade_date} {trade_time})")
        return pd.DataFrame()

    df = pd.DataFrame(data["output2"])
    if df.empty:
        return df

    df = df.rename(columns={
        "stck_bsop_date": "일자",
        "stck_cntg_hour": "시간",
        "stck_oprc": "시가",
        "stck_hgpr": "고가",
        "stck_lwpr": "저가",
        "stck_prpr": "종가",
        "cntg_vol": "거래량"
    })

    df["시간"] = df["시간"].str[:2] + ":" + df["시간"].str[2:4]
    df = df.sort_values(["일자", "시간"])

    return df[["일자", "시간", "시가", "고가", "저가", "종가", "거래량"]]

def get_10min_key(dt: datetime):
    return dt.replace(minute=(dt.minute // 10) * 10, second=0)

def get_completed_10min_key(dt: datetime):
    """
    현재 dt 시점에서 '이미 완성된' 가장 최근 10분봉 시작 시각
    """
    base_minute = (dt.minute // 10) * 10
    return dt.replace(minute=base_minute, second=0, microsecond=0)

def get_next_completed_10min_dt(dt: datetime) -> datetime:
    """
    dt가 속한 10분봉이 끝난 직후 시각 반환
    """
    base_minute = (dt.minute // 10) * 10
    base = dt.replace(minute=base_minute, second=0, microsecond=0)
    return base + timedelta(minutes=10)

def get_previous_business_day(day):
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def is_business_day(check_date: datetime) -> bool:
    """
    DB 기준 영업일 여부 확인
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT is_business_day(%s)",
        (check_date,)
    )
    result = cur.fetchone()
    cur.close()

    return bool(result[0])

def get_prev_day_low(stock_code, trade_date, access_token, app_key, app_secret):
    prev_date = get_previous_business_day((datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"))

    return get_kis_daily_chart(
        stock_code=stock_code,
        trade_date=prev_date,
        access_token=access_token,
        app_key=app_key,
        app_secret=app_secret
    )

def update_long_exit_trading_mng(udt_proc_yn, acct_no, code, trade_tp, start_date, proc_dtm):
    cur03 = conn.cursor()
    cur03.execute("""
        UPDATE public.tradng_simulation SET 
            proc_yn = %s
            , proc_dtm = %s
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trade_tp = %s
        AND trade_day <= %s
        AND proc_yn = 'L'
    """, (udt_proc_yn, proc_dtm, datetime.now(), acct_no, code, trade_tp, start_date))
    conn.commit()
    cur03.close()    

def update_exit_trading_mng(udt_proc_yn, acct_no, code, trade_tp, start_date, proc_dtm):
    cur03 = conn.cursor()
    cur03.execute("""
        UPDATE public.tradng_simulation SET 
            proc_yn = %s
            , proc_dtm = %s                  
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trade_tp = %s
        AND trade_day <= %s
        AND proc_yn != 'Y'
    """, (udt_proc_yn, proc_dtm, datetime.now(), acct_no, code, trade_tp, start_date))
    conn.commit()
    cur03.close()

def update_safe_trading_mng(udt_proc_yn, acct_no, code, trade_tp, start_date, proc_dtm):
    cur03 = conn.cursor()
    cur03.execute("""
        UPDATE public.tradng_simulation SET 
            proc_yn = %s
            , proc_dtm = %s 
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trade_tp = %s
        AND trade_day <= %s
        AND proc_yn IN ('N', 'C')
    """, (udt_proc_yn, proc_dtm, datetime.now(), acct_no, code, trade_tp, start_date))
    conn.commit()
    cur03.close()

def update_stop_price_trading_mng(loss_price, profit_price, acct_no, code, trade_tp, start_date, proc_dtm):
    cur03 = conn.cursor()
    cur03.execute("""
        UPDATE public.tradng_simulation SET 
            loss_price = %s
            , profit_price = %s
            , proc_dtm = %s 
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trade_tp = %s
        AND trade_day <= %s
        AND proc_yn != 'Y'
    """, (loss_price, profit_price, proc_dtm, datetime.now(), acct_no, code, trade_tp, start_date))
    conn.commit()
    cur03.close()


def update_trading_daily_close(trail_price, trail_qty, trail_amt, trail_rate, trail_plan, basic_qty, basic_amt, acct_no, access_token, app_key, app_secret, code, name, trail_day, trail_dtm, trail_tp, proc_min):
    
    trail_qty = trail_rate * 0.01
    
    cur04 = conn.cursor()
    cur04.execute("""
        UPDATE public.trading_trail SET 
            trail_price = %s
            , trail_qty = %s
            , trail_amt = %s      
            , trail_rate = %s      
            , trail_plan = %s
            , trail_tp = %s
            , proc_min = %s
            , basic_qty = %s
            , basic_amt = %s
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trail_day = %s
        AND trail_dtm = %s
        AND trail_tp = 'L'                  
    """, (trail_price, trail_qty, trail_amt, trail_rate, trail_plan, trail_tp, proc_min, basic_qty, basic_amt, datetime.now(), acct_no, code, trail_day, trail_dtm))
    conn.commit()
    cur04.close()

    # 매도 주문정보 존재시 취소 처리
    if sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code) == 'success':

        result_msgs = []
        try:
            # 매도 : 지정가 주문
            c = order_cash(False, access_token, app_key, app_secret, str(acct_no), code, "00", str(trail_qty), str(trail_price))

            if c['ODNO'] != "":
                # 일별주문체결 조회
                output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, c['ODNO'])
                tdf = pd.DataFrame(output1)
                tdf.set_index('odno')
                d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                for k, name in enumerate(d.index):
                    d_order_no = int(d['odno'][k])
                    d_order_type = d['sll_buy_dvsn_cd_name'][k]
                    d_order_dt = d['ord_dt'][k]
                    d_order_tmd = d['ord_tmd'][k]
                    d_name = d['prdt_name'][k]
                    d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                    d_order_amount = d['ord_qty'][k]
                    d_total_complete_qty = d['tot_ccld_qty'][k]
                    d_remain_qty = d['rmn_qty'][k]
                    d_total_complete_amt = d['tot_ccld_amt'][k]

                    print("매도주문 완료")
                    msg = f"[전일 저가 이탈 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                    result_msgs.append(msg)

            else:
                print("매도주문 실패")
                msg = f"[전일 저가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 매도주문 실패"
                result_msgs.append(msg)

        except Exception as e:
            print('매도주문 오류.', e)
            msg = f"[전일 저가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 [매도주문 오류] - {str(e)}"
            result_msgs.append(msg)

        message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다." 
        print(message)
        bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode='HTML'
        )       

def update_trading_close(trail_price, trail_qty, trail_amt, trail_rate, trail_plan, basic_qty, basic_amt, acct_no, access_token, app_key, app_secret, code, name, trail_day, trail_dtm, trail_tp, proc_min):
    cur04 = conn.cursor()
    cur04.execute("""
        UPDATE public.trading_trail SET 
            trail_price = %s
            , trail_qty = %s
            , trail_amt = %s 
            , trail_rate = %s      
            , trail_plan = %s
            , trail_tp = %s
            , proc_min = %s
            , basic_qty = %s
            , basic_amt = %s
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trail_day = %s
        AND trail_dtm = %s
        AND trail_tp <> 'L'                  
    """, (trail_price, trail_qty, trail_amt, trail_rate, trail_plan, trail_tp, proc_min, basic_qty, basic_amt, datetime.now(), acct_no, code, trail_day, trail_dtm))
    conn.commit()
    cur04.close()

    # 매도 주문정보 존재시 취소 처리
    if sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code) == 'success':

        result_msgs = []
        try:
            # 매도 : 지정가 주문
            c = order_cash(False, access_token, app_key, app_secret, str(acct_no), code, "00", str(trail_qty), str(trail_price))

            if c['ODNO'] != "":
                # 일별주문체결 조회
                output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, c['ODNO'])
                tdf = pd.DataFrame(output1)
                tdf.set_index('odno')
                d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                for k, name in enumerate(d.index):
                    d_order_no = int(d['odno'][k])
                    d_order_type = d['sll_buy_dvsn_cd_name'][k]
                    d_order_dt = d['ord_dt'][k]
                    d_order_tmd = d['ord_tmd'][k]
                    d_name = d['prdt_name'][k]
                    d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                    d_order_amount = d['ord_qty'][k]
                    d_total_complete_qty = d['tot_ccld_qty'][k]
                    d_remain_qty = d['rmn_qty'][k]
                    d_total_complete_amt = d['tot_ccld_amt'][k]

                    print("매도주문 완료")
                    msg = f"[이탈가 이탈 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                    result_msgs.append(msg)

            else:
                print("매도주문 실패")
                msg = f"[이탈가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 매도주문 실패"
                result_msgs.append(msg)

        except Exception as e:
            print('매도주문 오류.', e)
            msg = f"[이탈가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 [매도주문 오류] - {str(e)}"
            result_msgs.append(msg)

        message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다." 
        print(message)
        bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode='HTML'
        )        

def update_trading_trail(stop_price, target_price, volumn, acct_no, code, trail_day, trail_dtm, trail_tp, proc_min):
    cur04 = conn.cursor()
    cur04.execute("""
        UPDATE public.trading_trail SET 
            stop_price = %s      
            , target_price = %s
            , volumn = %s      
            , trail_tp = %s
            , proc_min = %s
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trail_day = %s
        AND trail_dtm = %s
        AND trail_tp <> 'L'
    """, (stop_price, target_price, volumn, trail_tp, proc_min, datetime.now(), acct_no, code, trail_day, trail_dtm))
    conn.commit()
    cur04.close()    

def get_kis_1min_full_day(
    stock_code,
    trade_date,
    start_time,
    access_token,
    app_key,
    app_secret,
    verbose=False
):
    all_df = []
    current_time = start_time
    prev_oldest_dt = None

    while True:
        df = get_kis_1min_dailychart(
            stock_code=stock_code,
            trade_date=trade_date,
            trade_time=current_time,
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            verbose=verbose
        )

        if df.empty:
            break

        # 시간 오름차순 보장
        df = df.sort_values("시간")

        oldest_time = df.iloc[0]["시간"].replace(":", "")
        oldest_dt = datetime.strptime(trade_date + oldest_time, "%Y%m%d%H%M")

        # 이전과 동일하면 탈출 (무한루프 방지)
        if prev_oldest_dt is not None and oldest_dt >= prev_oldest_dt:
            if verbose:
                print(f"⚠️ 더 이상 과거 분봉 없음 ({oldest_time})")
            break

        prev_oldest_dt = oldest_dt
        all_df.append(df)

        # 장 시작 도달 시 종료 : 1월 2일 10시 시작
        if trade_date.endswith("0102"): 
            if oldest_time <= "100000":
                break
        else:
            if oldest_time <= "090000":
                break

        # 다음 조회는 1분 이전
        dt = oldest_dt - timedelta(minutes=1)
        current_time = dt.strftime("%H%M%S")

        # 120건 미만이면 종료
        if len(df) < 120:
            break

    if not all_df:
        return pd.DataFrame()

    df_all = pd.concat(all_df, ignore_index=True)

    # 중복 제거 + 시간 정렬
    df_all["dt"] = pd.to_datetime(
        df_all["일자"] + df_all["시간"].str.replace(":", ""),
        format="%Y%m%d%H%M"
    )

    return (
        df_all
        .drop_duplicates("dt")
        .sort_values("dt")
        .reset_index(drop=True)
    )

def get_kis_1min_from_datetime(
    stock_code: str,
    stock_name: str,
    start_date: str,
    start_time: str,
    target_price: int,
    stop_price: int, 
    basic_price: int,
    basic_qty:int,
    trail_tp: str,
    trail_plan: str,
    proc_min: str,
    volumn: int,
    access_token: str,
    app_key: str,
    app_secret: str,
    breakout_type: str = "high",        # high / close
    breakdown_type: str = "low",        # low / close
    verbose: bool = True
):
    updater = Updater(token=token, use_context=True)
    bot = updater.bot
    start_dt = datetime.strptime(start_date + start_time, "%Y%m%d%H%M%S")
    # start_time 기준 다음 완성 10분봉 시각
    loop_start_dt = get_next_completed_10min_dt(start_dt)
    trade_date = start_dt.strftime("%Y%m%d")
    signals = []

    if verbose:
        print(f"[{stock_name}-{stock_code}] {trade_date} {datetime.now().strftime('%H%M%S')} 1분봉 생성 중")

    if trail_tp == 'L':
        prev_low = get_prev_day_low(
            stock_code,
            trade_date,
            access_token,
            app_key,
            app_secret
        )

        df = get_kis_1min_full_day(
            stock_code=stock_code,
            trade_date=trade_date,
            start_time="153000",
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            verbose=False
        )

        # 입력 시간 기준 10분 이후부터만 허용
        df = df[df["dt"] >= loop_start_dt]

        # 날짜별 시작 시간 설정 : 1월 2일 10시 시작
        if trade_date.endswith("0102"):
            start_t = time(10, 0)  
        else:
            start_t = time(9, 0) 

        # 시간 필터
        df = df[(df["dt"].dt.time >= start_t) & (df["dt"].dt.time <= time(15, 30))]

        # 시간 오름차순 정렬 (필수)
        df = df.sort_values("dt").reset_index(drop=True)

        for _, row in df.iterrows():

            if int(proc_min) < int(row['시간'].replace(':', '')+'00'):
                high_price = int(row["고가"])
                low_price = int(row["저가"])
                close_price = int(row["종가"])

                breakout_check = high_price if breakout_type == "high" else close_price
                breakdown_check = low_price if breakdown_type == "low" else close_price

                # ===============================
                # 09:10 이전 미처리
                # ===============================
                if row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                    continue

                # 현재 분봉 시간
                current_time = row["시간"].replace(":", "")

                # ===============================
                # 1️⃣ 15:10 이후 일봉 이탈 감시
                # ===============================
                if current_time >= "151000" and prev_low is not None:
                    if close_price < prev_low :
                        if verbose:
                            message = (
                                f"[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] 전일 저가 : {prev_low:,}원 이탈"
                            )
                            print(message)
                            bot.send_message(
                                chat_id=chat_id,
                                text=message,
                                parse_mode='HTML'
                            )

                        update_long_exit_trading_mng("Y", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                        
                        trail_rate = round((100 - (close_price / basic_price) * 100) * -1, 2)
                        i_trail_plan = trail_plan if trail_plan is not None else "100"
                        trail_qty = basic_qty * int(i_trail_plan) * 0.01
                        trail_amt = close_price * trail_qty
                        u_basic_qty = basic_qty - trail_qty
                        u_basic_amt = basic_price * u_basic_qty

                        update_trading_daily_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "4", row['시간'].replace(':', '')+'00')

                        signals.append({
                            "signal_type": "DAILY_BREAKDOWN_AFTER_1510",
                            "종목코드": stock_code,
                            "발생일자": row["일자"],
                            "발생시간": row["시간"],
                            "이탈가격": close_price,
                            "전일저가": prev_low,
                        })
                        return signals

    else:

        if trail_tp == '2':
            tenmin_state = {
                "base_low": int(stop_price) if stop_price else 0,
                "base_high": int(target_price) if target_price else 0,
                "base_vol": int(volumn) if volumn else 0,
            }
        else:
            tenmin_state = {
                "base_low": None,         # 기준봉 저가
                "base_high": None,        # 기준봉 고가
                "base_vol": None          # 기준봉 거래량
            }

        df = get_kis_1min_full_day(
            stock_code=stock_code,
            trade_date=trade_date,
            start_time="153000",
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            verbose=False
        )

        # 입력 시간 기준 10분 이후부터만 허용
        df = df[df["dt"] >= loop_start_dt]

        # 날짜별 시작 시간 설정 : 1월 2일 10시 시작
        if trade_date.endswith("0102"):
            start_t = time(10, 0)  
        else:
            start_t = time(9, 0) 

        # 시간 필터
        df = df[(df["dt"].dt.time >= start_t) & (df["dt"].dt.time <= time(15, 30))]

        # 시간 오름차순 정렬 (필수)
        df = df.sort_values("dt").reset_index(drop=True)

        for _, row in df.iterrows():

            if int(proc_min) < int(row['시간'].replace(':', '')+'00'):
                high_price = int(row["고가"])
                low_price = int(row["저가"])
                close_price = int(row["종가"])

                breakout_check = high_price if breakout_type == "high" else close_price
                breakdown_check = low_price if breakdown_type == "low" else close_price

                if high_price > low_price:
                    # ===============================
                    # 09:10 이전 미처리
                    # ===============================
                    if row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                        continue
                    
                    # ===============================
                    # 기준봉 미생성 상태 → 목표가 돌파 시 기준봉 생성
                    # ===============================
                    if tenmin_state["base_low"] is None:
                        # 돌파 이전 이탈 → 즉시 종료
                        if breakdown_check <= stop_price:
                            if trail_tp == '1':
                                if verbose:
                                    message = (
                                        f"[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] 돌파 전 이탈가 : {stop_price:,}원 이탈"
                                    )
                                    print(message)
                                    bot.send_message(
                                        chat_id=chat_id,
                                        text=message,
                                        parse_mode='HTML'
                                    )

                                update_exit_trading_mng("Y", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))

                                trail_rate = round((100 - (close_price / basic_price) * 100) * -1, 2)
                                i_trail_plan = trail_plan if trail_plan is not None else "100"
                                trail_qty = basic_qty * int(i_trail_plan) * 0.01
                                trail_amt = close_price * trail_qty
                                u_basic_qty = basic_qty - trail_qty
                                u_basic_amt = basic_price * u_basic_qty

                                update_trading_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "4", row['시간'].replace(':', '')+'00')

                                signals.append({
                                    "signal_type": "BREAKDOWN_BEFORE_BREAKOUT",
                                    "종목명": stock_name,
                                    "종목코드": stock_code,
                                    "발생일자": row["일자"],
                                    "발생시간": row["시간"],
                                    "이탈가격": breakdown_check
                                })
                                return signals

                        # 목표가 돌파
                        if breakout_check >= target_price:
                            if trail_tp == '1':
                                base_key = get_completed_10min_key(row["dt"])
                                base_10min = df[df["dt"].apply(get_10min_key) == base_key]

                                if base_10min.empty:
                                    continue

                                tenmin_state.update({
                                    "base_low": base_10min["저가"].astype(int).min(),
                                    "base_high": base_10min["고가"].astype(int).max(),
                                    "base_vol": base_10min["거래량"].astype(int).sum()
                                })

                                if verbose:
                                    message = (
                                        f"[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] 목표가 {target_price:,}원 돌파 기준봉 설정, 고가 : {tenmin_state['base_high']:,}원, 저가 : {tenmin_state['base_low']:,}원 "
                                    )
                                    print(message)
                                    bot.send_message(
                                        chat_id=chat_id,
                                        text=message,
                                        parse_mode='HTML'
                                    )

                                update_safe_trading_mng("C", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                
                                update_trading_trail(int(tenmin_state['base_low']), int(tenmin_state['base_high']), int(tenmin_state['base_vol']), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00')    

                                signals.append({
                                    "signal_type": "BREAKOUT",
                                    "종목명": stock_name,
                                    "종목코드": stock_code,
                                    "기준가격": target_price,
                                    "발생일자": row["일자"],
                                    "발생시간": row["시간"],
                                    "돌파가격": breakout_check
                                })
                                continue

                    # ===============================
                    # 기준봉 존재 → 저가 이탈 체크
                    # ===============================
                    else:
                        # 기준봉 저가 이탈 → 즉시 종료
                        if low_price < tenmin_state["base_low"]:
                            if verbose:
                                message = (
                                    f"[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] 목표가 돌파 후 10분 기준봉 저가 : {tenmin_state['base_low']:,}원 이탈"
                                )
                                print(message)
                                bot.send_message(
                                    chat_id=chat_id,
                                    text=message,
                                    parse_mode='HTML'
                                )

                            trail_rate = round((100 - (close_price / basic_price) * 100) * -1, 2)
                            i_trail_plan = trail_plan if trail_plan is not None else "50"
                            trail_qty = basic_qty * int(i_trail_plan) * 0.01
                            trail_amt = close_price * trail_qty
                            u_basic_qty = basic_qty - trail_qty
                            u_basic_amt = basic_price * u_basic_qty

                            if basic_qty == trail_qty:
                                update_exit_trading_mng("Y", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                update_trading_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "4", row['시간'].replace(':', '')+'00')
                            else:    
                                update_safe_trading_mng("L", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                update_trading_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "3", row['시간'].replace(':', '')+'00')

                            signals.append({
                                "signal_type": "BASE_10MIN_LOW_BREAK",
                                "종목명": stock_name,
                                "종목코드": stock_code,
                                "발생일자": row["일자"],
                                "발생시간": row["시간"],
                                "기준봉저가": tenmin_state["base_low"],
                                "10분봉 저가": row["저가"]
                            })
                            return signals

                        # ===============================
                        # 10분봉 완성 시 기준봉 갱신
                        # ===============================
                        completed_key = get_completed_10min_key(row["dt"])
                        tenmin_df = df[df["dt"].apply(get_completed_10min_key) == completed_key]

                        if not tenmin_df.empty and row["dt"] == tenmin_df["dt"].max():
                            new_high = tenmin_df["고가"].astype(int).max()
                            new_low = tenmin_df["저가"].astype(int).min()
                            new_vol = tenmin_df["거래량"].astype(int).sum()

                            if new_high > new_low:
                                if new_high > tenmin_state["base_high"] or new_vol > tenmin_state["base_vol"]:
                                    tenmin_state.update({
                                        "base_low": new_low,
                                        "base_high": new_high,
                                        "base_vol": new_vol
                                    })

                                    if verbose:
                                        reason = "고가 돌파" if new_high > tenmin_state["base_high"] else "거래량 돌파"
                                        message = (
                                            f"[{completed_key.strftime('%Y%m%d %H:%M')}]{stock_name}[<code>{stock_code}</code>] {reason} 기준봉 갱신 고가 : {new_high:,}원,  저가 : {new_low:,}원, 거래량 : {new_vol:,}주"
                                        )
                                        print(message)
                                        # bot.send_message(
                                        #     chat_id=chat_id,
                                        #     text=message,
                                        #     parse_mode='HTML'
                                        # )
                                    
                                    if trail_plan is not None:
                                        update_stop_price_trading_mng(int(new_low), int(new_high), acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                    else:
                                        update_safe_trading_mng("C", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))

                                    update_trading_trail(int(new_low), int(new_high), int(new_vol), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00')    

    return signals

if __name__ == "__main__":

    if is_business_day(today):
        
        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']
        token = ac['bot_token2']
        chat_id = ac['chat_id']

        # 계좌잔고 조회
        c = stock_balance(access_token, app_key, app_secret, acct_no, "")
            
        cur199 = conn.cursor()

        # 일별 매매 잔고 현행화
        for i in range(len(c)):
            insert_query199 = """
                INSERT INTO dly_trading_balance (
                    acct_no,
                    code,
                    name,
                    balance_day,
                    balance_price,
                    balance_qty,
                    balance_amt,
                    value_rate,
                    value_amt,
                    buy_qty,
                    sell_qty,
                    mod_dt
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (acct_no, code, balance_day)
                DO UPDATE SET
                    balance_price = EXCLUDED.balance_price,
                    balance_qty   = EXCLUDED.balance_qty,
                    balance_amt   = EXCLUDED.balance_amt,
                    value_rate    = EXCLUDED.value_rate,
                    value_amt     = EXCLUDED.value_amt,
                    buy_qty       = EXCLUDED.buy_qty,
                    sell_qty      = EXCLUDED.sell_qty,
                    mod_dt        = EXCLUDED.mod_dt;
            """
            record_to_insert199 = (
                acct_no,
                c['pdno'][i],
                c['prdt_name'][i],
                today,
                float(c['pchs_avg_pric'][i]),
                int(c['hldg_qty'][i]),
                int(c['pchs_amt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                float(c['evlu_pfls_rt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                int(c['evlu_pfls_amt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                int(c['thdt_buyqty'][i]) if int(c['thdt_buyqty'][i]) > 0 else 0,
                int(c['thdt_sll_qty'][i]) if int(c['thdt_sll_qty'][i]) > 0 else 0,
                datetime.now()
            )
            cur199.execute(insert_query199, record_to_insert199)
            conn.commit()

        cur199.close()        

        # 매매추적 조회
        cur200 = conn.cursor()
        cur200.execute("select code, name, trail_day, trail_dtm, target_price, stop_price, basic_price, COALESCE(basic_qty, 0), CASE WHEN trail_tp = 'L' THEN 'L' ELSE trail_tp END, trail_plan, proc_min, volumn from public.trading_trail where acct_no = '" + str(acct_no) + "' and trail_tp in ('1', '2', 'L') and trail_day = '" + today + "' and to_char(to_timestamp(proc_min, 'HH24MISS') + interval '5 minutes', 'HH24MISS') <= to_char(now(), 'HH24MISS') order by code, proc_min, mod_dt")
        result_two00 = cur200.fetchall()
        cur200.close()

        if len(result_two00) > 0:
            
            for i in result_two00:

                signal = get_kis_1min_from_datetime(
                    stock_code=i[0],
                    stock_name=i[1], 
                    start_date=i[2],
                    start_time=i[3],
                    target_price=int(i[4]),
                    stop_price=int(i[5]),
                    basic_price=int(i[6]),
                    basic_qty=int(i[7]),
                    trail_tp=i[8],
                    trail_plan=i[9],
                    proc_min=i[10],
                    volumn=i[11],
                    access_token=ac['access_token'],
                    app_key=ac['app_key'],
                    app_secret=ac['app_secret'],
                    breakout_type="high",
                    verbose=True
                )

                if signal:
                    print("\n📌 신호 결과")
                    print(signal)
                else:
                    print("\n📌 아직 신호 없음")
                    
