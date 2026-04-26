from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import requests
import pandas as pd
import psycopg2 as db
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import time as dt_time
import kis_api_resp as resp
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater

BASE_URL = "https://openapi.koreainvestment.com:9443"

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

today = datetime.now().strftime("%Y%m%d")
# today = '20260227'

# 일봉 데이터 캐시(장중 불변 데이터 - 종목코드 기준)
_daily_cache_lock = threading.Lock()
_daily_chart_full_cache = {}    # {stock_code: [daily_data]}
_prev_day_info_cache = {}       # {(stock_code, trade_date): {low_price, hight_price, ...}}

# 인증처리
def auth(APP_KEY, APP_SECRET):

    # 인증처리
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    ACCESS_TOKEN = res.json()["access_token"]

    return ACCESS_TOKEN

def account(nickname, conn):
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

def get_excg_id():
    """정규시장(09:00~15:30)이면 KRX, 그 외 시간이면 NXT 반환"""
    t = datetime.now().strftime('%H%M')
    return "KRX" if '0900' <= t < '1530' else "NXT"

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
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)

        # 응답에 output1이 있는지 확인
        body = ar.getBody()
        return body.output1 if hasattr(body, 'output1') else []

    except Exception as e:
        print("일별주문체결조회 중 오류 발생:", e)
        return []

# 주식주문(정정취소)
def order_cancel_revice(access_token, app_key, app_secret, acct_no, cncl_dv, order_no, order_qty, order_price, excg_id=None):

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
               "QTY_ALL_ORD_YN": "Y",           # 전량 : Y, 일부 : N
               "EXCG_ID_DVSN_CD": excg_id if excg_id is not None else get_excg_id()   # 한국거래소 : KRX, 대체거래소 (넥스트레이드) : NXT, SOR (Smart Order Routing) : SOR
    }
    PATH = "uapi/domestic-stock/v1/trading/order-rvsecncl"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    if ar.isOK():
        return ar.getBody().output
    else:
        ar.printError()
        return None

# 매도 주문정보 존재시 취소 처리
def sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code):

    result_msgs = []

    try:
        # 일별주문체결 조회
        output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, '')

        if len(output1) > 0:

            tdf = pd.DataFrame(output1)
            tdf.set_index('odno')
            d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
            order_no = 0

            for i, name in enumerate(d.index):

                # 매도주문 잔여수량 존재시
                if d['sll_buy_dvsn_cd'][i] == "01":

                    # 잔량 존재 AND cncl_yn != 'Y' (Y=취소, 그 외 공백/N 모두 취소 아님으로 처리)
                    if int(d['rmn_qty'][i]) > 0 and d['cncl_yn'][i] != 'Y':
                        order_no = int(d['odno'][i])
                        ord_excg_id = d['excg_id_dvsn_cd'][i] if 'excg_id_dvsn_cd' in d.columns else None

                        # 주문취소
                        c = order_cancel_revice(access_token, app_key, app_secret, acct_no, "02", str(order_no), "0", "0", ord_excg_id)
                        if c is not None and c['ODNO'] != "":
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
def order_cash(buy_flag, access_token, app_key, app_secret, acct_no, stock_code, ord_dvsn, order_qty, order_price, cndt_price=None, excg_id=None):

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
               "ORD_UNPR": order_price,         # 시장가 등 주문시, "0"으로 입력
               "EXCG_ID_DVSN_CD": excg_id if excg_id is not None else get_excg_id()   # 한국거래소 : KRX, 대체거래소 (넥스트레이드) : NXT, SOR (Smart Order Routing) : SOR
    }
    # 스톱지정가일 때만 조건가격 추가
    if ord_dvsn == "22":
        params["CNDT_PRIC"] = str(cndt_price)

    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{BASE_URL}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    if ar.isOK():
        return ar.getBody().output
    else:
        ar.printError()
        return None

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):

    t = datetime.now().strftime('%H%M')
    
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}            # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
    params = {
                "CANO": acct_no,
                'ACNT_PRDT_CD': '01',
                'AFHR_FLPR_YN': 'N' if '0900' <= t < '1530' else 'X',            # N : 기본값, Y : 시간외단일가, X : NXT 정규장 (프리마켓, 메인, 애프터마켓) NXT 거래종목만 시세 등 정보가 NXT 기준으로 변동됩니다. KRX 종목들은 그대로 유지
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
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

    for attempt in range(3):
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            data = res.json()
            if data.get('rt_cd') == '1' and attempt < 2:
                time.sleep(1)
                continue
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
            else:    
                if verbose:
                    print(f"\n⚠️ 일봉 조회 실패 ({stock_code}): {e}")
                return None

    if "output" not in data or not data["output"]:
        if verbose:
            print(f"⛔ 일봉 데이터 없음 ({stock_code}) rt_cd={data.get('rt_cd')}, msg={data.get('msg1', '')}")
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

    result = {
        "low_price": int(day_df.iloc[0]["stck_lwpr"]),
        "high_price": int(day_df.iloc[0]["stck_hgpr"]),
        "close_price": int(day_df.iloc[0]["stck_clpr"]),
        "volume": int(day_df.iloc[0]["acml_vol"]),
    }

    return result

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

    for attempt in range(3):
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            data = res.json()
            if data.get('rt_cd') == '1' and attempt < 2:
                time.sleep(1)
                continue
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
            else:    
                if verbose:
                    print(f"\n⚠️ 분봉 조회 실패 ({stock_code}, {trade_time}): {e}")
                return pd.DataFrame()    

    if "output2" not in data or not data["output2"]:
        if verbose:
            print(f"⛔ 데이터 없음 ({trade_date} {trade_time})")
        return pd.DataFrame()

    df = pd.DataFrame(data["output2"])
    acml_vol = data.get("output1", {}).get("acml_vol", "0")
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

    # 누적 거래량 컬럼 추가
    df["누적거래량"] = acml_vol

    df["시간"] = df["시간"].str[:2] + ":" + df["시간"].str[2:4]
    df = df.sort_values(["일자", "시간"])

    return df[["일자", "시간", "시가", "고가", "저가", "종가", "거래량", "누적거래량"]]

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

def get_previous_business_day(day, conn):
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def is_business_day(check_date: datetime, conn) -> bool:
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

def get_prev_day_info(stock_code, trade_date, access_token, app_key, app_secret, conn):
    cache_key = (stock_code, trade_date)
    with _daily_cache_lock:
        if cache_key in _prev_day_info_cache:
            return _prev_day_info_cache[cache_key]
        
    prev_date = get_previous_business_day((datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"), conn)

    result = get_kis_daily_chart(
        stock_code=stock_code,
        trade_date=prev_date,
        access_token=access_token,
        app_key=app_key,
        app_secret=app_secret
    )

    if result is not None:
        with _daily_cache_lock:
            _prev_day_info_cache[cache_key] = result

    return result            

def get_kis_daily_chart_full(stock_code, access_token, app_key, app_secret):
    """최근 30거래일 일봉 데이터 전체 조회 (ATR 계산용)"""
    with _daily_cache_lock:
        if stock_code in _daily_chart_full_cache:
            return _daily_chart_full_cache[stock_code]
        
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
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "1",
    }
    try:
        for attempt in range(3):
            try:
                res = requests.get(url, headers=headers, params=params, timeout=10)
                data = res.json()
                if data.get('rt_cd') == '1' and attempt < 2:
                    time.sleep(1)
                    continue
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
                else:    
                    raise
        if "output" not in data or not data["output"]:
            print(f"⛔ 일봉 전체 데이터 없음 ({stock_code}) rt_cd={data.get('rt_cd')}, msg={data.get('msg1', '')}")
            return []
        result = []
        for item in data["output"]:
            if item.get("stck_bsop_date") and item.get("acml_vol") and int(item["acml_vol"]) > 0:
                result.append({
                    "date": item["stck_bsop_date"],
                    "high_price": int(item["stck_hgpr"]),
                    "low_price": int(item["stck_lwpr"]),
                    "close_price": int(item["stck_clpr"]),
                    "volume": int(item["acml_vol"]),
                })
        sorted_return = sorted(result, key=lambda x: x["date"])
        if sorted_return:
            with _daily_cache_lock:
                _daily_chart_full_cache[stock_code] = sorted_return

        return sorted_return
    except Exception as e:
        print(f"일봉 전체 조회 오류: {e}")
        return []

def calculate_atr(daily_data, period=14):
    """ATR(Average True Range) 계산 - 일봉 데이터 기반"""
    if len(daily_data) < period + 1:
        print(f"ATR 계산 불가: 일봉 데이터 {len(daily_data)}건 (최소 {period + 1}건 필요)")
        return None
    trs = []
    for i in range(1, len(daily_data)):
        h = daily_data[i]['high_price']
        l = daily_data[i]['low_price']
        prev_c = daily_data[i-1]['close_price']
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
    return int(sum(trs[-period:]) / period)

def update_trading_daily_close(nick, trail_price, trail_qty, trail_amt, trail_rate, trail_plan, basic_qty, basic_amt, acct_no, access_token, app_key, app_secret, code, name, trail_day, trail_dtm, trail_tp, proc_min, trade_result, conn, bot, chat_id):

    d_order_price = 0
    d_order_amount = 0

    try:
        # 매도 주문정보 존재시 취소 처리
        if sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code) == 'success':

            result_msgs = []
            try:
                # 매도 : 지정가 주문
                c = order_cash(False, access_token, app_key, app_secret, str(acct_no), code, "00", str(int(trail_qty)), str(int(trail_price)))

                if c is not None and c['ODNO'] != "":
                    # 일별주문체결 조회
                    output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, c['ODNO'])
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]

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
                        msg = f"-{nick}-[전일 저가 이탈 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                        result_msgs.append(msg)

                else:
                    print("매도주문 실패")
                    msg = f"-{nick}-[전일 저가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 매도주문 실패"
                    result_msgs.append(msg)

            except Exception as e:
                print('매도주문 오류.', e)
                msg = f"-{nick}-[전일 저가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 [매도주문 오류] - {str(e)}"
                result_msgs.append(msg)

            try:
                message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."
                print(message)
                bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='HTML'
                )
            except Exception as te:
                print(f"텔레그램 발송 실패: {te}")

        # 주문가와 주문수량이 존재하는 경우
        if int(d_order_price) > 0 and int(d_order_amount) > 0:
            cur04 = conn.cursor()
            cur04.execute("""
                UPDATE public.trading_trail SET
                    order_no = %s
                    , order_type = %s
                    , order_dt = %s
                    , order_tmd = %s
                    , order_price = %s
                    , order_amount = %s
                    , complete_qty = %s
                    , remain_qty = %s
                    , trail_price = %s
                    , trail_qty = %s
                    , trail_amt = %s
                    , trail_rate = %s
                    , trail_plan = %s
                    , trail_tp = %s
                    , proc_min = %s
                    , basic_qty = %s
                    , basic_amt = %s
                    , trade_result = %s
                    , mod_dt = %s
                WHERE acct_no = %s
                AND code = %s
                AND trail_day = %s
                AND trail_dtm = %s
                AND trail_tp = 'L'
            """, (str(d_order_no), d_order_type, d_order_dt, d_order_tmd, int(d_order_price), int(d_order_amount), int(d_total_complete_qty), int(d_remain_qty), trail_price, trail_qty, trail_amt, trail_rate, trail_plan, trail_tp, proc_min, basic_qty, basic_amt, trade_result, datetime.now(), acct_no, code, trail_day, trail_dtm))
            conn.commit()
            cur04.close()

    except Exception as total_e:
        # DB 접속이나 아주 기초적인 로직 에러 시 여기서 잡힘
        print(f"CRITICAL: update_trading_daily_close 함수 전체 에러: {total_e}")
        # 에러가 나도 상위로 raise 하지 않고 리턴함
        return False

    return True

def update_trading_close(nick, trail_price, trail_qty, trail_amt, trail_rate, trail_plan, basic_qty, basic_amt, acct_no, access_token, app_key, app_secret, code, name, trail_day, trail_dtm, trail_tp, proc_min, trade_result, conn, bot, chat_id):
    d_order_price = 0
    d_order_amount = 0

    try:
        # 매도 주문정보 존재시 취소 처리
        if sell_order_cancel_proc(access_token, app_key, app_secret, acct_no, code) == 'success':

            result_msgs = []
            try:
                # 매도 : 지정가 주문
                c = order_cash(False, access_token, app_key, app_secret, str(acct_no), code, "00", str(int(trail_qty)), str(int(trail_price)))

                if c is not None and c['ODNO'] != "":
                    # 일별주문체결 조회
                    output1 = get_my_complete(access_token, app_key, app_secret, acct_no, code, c['ODNO'])
                    tdf = pd.DataFrame(output1)
                    tdf.set_index('odno')
                    d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]

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
                        msg = f"-{nick}-[이탈가 이탈 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도체결량 : {int(d_total_complete_qty):,}주, 매도체결금액 : {int(d_total_complete_amt):,}원 주문 완료, 주문번호 : <code>{d_order_no}</code>"
                        result_msgs.append(msg)

                else:
                    print("매도주문 실패")
                    msg = f"-{nick}-[이탈가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 매도주문 실패"
                    result_msgs.append(msg)

            except Exception as e:
                print('매도주문 오류.', e)
                msg = f"-{nick}-[이탈가 이탈 매도-{name}] 매도가 : {int(trail_price):,}원, 매도량 : {int(trail_qty):,}주 [매도주문 오류] - {str(e)}"
                result_msgs.append(msg)

            try:
                message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."
                print(message)
                bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='HTML'
                )
            except Exception as te:
                print(f"텔레그램 발송 실패: {te}")

        # 주문가와 주문수량이 존재하는 경우
        if int(d_order_price) > 0 and int(d_order_amount) > 0:
            cur04 = conn.cursor()
            cur04.execute("""
                UPDATE public.trading_trail SET
                    order_no = %s
                    , order_type = %s
                    , order_dt = %s
                    , order_tmd = %s
                    , order_price = %s
                    , order_amount = %s
                    , complete_qty = %s
                    , remain_qty = %s
                    , trail_price = %s
                    , trail_qty = %s
                    , trail_amt = %s
                    , trail_rate = %s
                    , trail_plan = %s
                    , trail_tp = %s
                    , proc_min = %s
                    , basic_qty = %s
                    , basic_amt = %s
                    , trade_result = %s
                    , mod_dt = %s
                WHERE acct_no = %s
                AND code = %s
                AND trail_day = %s
                AND trail_dtm = %s
                AND trail_tp IN ('1', '2')
            """, (str(d_order_no), d_order_type, d_order_dt, d_order_tmd, int(d_order_price), int(d_order_amount), int(d_total_complete_qty), int(d_remain_qty), trail_price, trail_qty, trail_amt, trail_rate, trail_plan, trail_tp, proc_min, basic_qty, basic_amt, trade_result, datetime.now(), acct_no, code, trail_day, trail_dtm))
            conn.commit()
            cur04.close()

    except Exception as total_e:
        # DB 접속이나 아주 기초적인 로직 에러 시 여기서 잡힘
        print(f"CRITICAL: update_trading_close 함수 전체 에러: {total_e}")
        # 에러가 나도 상위로 raise 하지 않고 리턴함
        return False

    return True

def update_trading_trail(stop_price, target_price, volumn, acct_no, code, trail_day, trail_dtm, trail_tp, proc_min, conn):
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
        AND trail_tp IN ('1', '2')
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

        # 장 시작 도달 시 종료 : 1월 2일 / 11월 19일 10시 시작
        if trade_date.endswith("0102") or trade_date.endswith("1119"):
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

# ── 봇 토큰당 단일 콜백 리스너 (레지스트리 패턴) ──────────────────────────
# 문제: 같은 봇에서 여러 종목이 동시에 _listen_prevlow_sell 스레드를 띄우면
#       각 스레드가 bot.get_updates()를 동시에 호출해 업데이트를 서로 뺏어감.
# 해결: 봇 토큰당 리스너 스레드 1개만 유지하고, 종목별 핸들러를 레지스트리에 등록.
_prevlow_registry_lock = threading.Lock()
_prevlow_registry: dict[str, dict] = {}   # token → {callback_data: handler_fn}
_prevlow_listeners: dict[str, threading.Thread] = {}  # token → Thread
_prevlow_offsets: dict[str, int] = {}     # token → last offset


def _bot_callback_listener(token: str, bot):
    """봇 토큰당 단일 get_updates 폴링 — 레지스트리 핸들러 실행 후 항목 제거."""
    import time as _time
    while True:
        with _prevlow_registry_lock:
            if not _prevlow_registry.get(token):
                _prevlow_registry.pop(token, None)
                break
        try:
            offset = _prevlow_offsets.get(token)
            updates = bot.get_updates(
                offset=offset, timeout=10,
                allowed_updates=["callback_query"]
            )
            for upd in updates:
                _prevlow_offsets[token] = upd.update_id + 1
                cq = upd.callback_query
                if cq is None:
                    continue
                with _prevlow_registry_lock:
                    handler = _prevlow_registry.get(token, {}).pop(cq.data, None)
                if handler:
                    threading.Thread(target=handler, args=(cq,), daemon=True).start()
        except Exception as e:
            print(f"[callback_listener:{token[:8]}…] 폴링 오류: {e}")
            _time.sleep(2)


def _register_prevlow_sell(token: str, bot, chat_id,
                            access_token, app_key, app_secret, acct_no,
                            stock_code, basic_qty, prev_low, callback_data: str):
    """
    종목별 전량매도 핸들러를 레지스트리에 등록하고
    봇 토큰당 단일 리스너 스레드를 보장한다.
    """
    def handler(cq):
        try:
            cq.answer("매도 주문 처리 중...")
        except Exception:
            pass
        result = order_cash(
            False, access_token, app_key, app_secret, str(acct_no),
            stock_code, "00", str(basic_qty), str(prev_low)
        )
        msg = (
            f"✅ 전량매도 완료\n종목: {stock_code} | {basic_qty:,}주 | {prev_low:,}원"
            if result else f"❌ 전량매도 실패: {stock_code}"
        )
        try:
            cq.edit_message_reply_markup(reply_markup=None)
            bot.send_message(chat_id=chat_id, text=msg)
        except Exception:
            pass

    with _prevlow_registry_lock:
        if token not in _prevlow_registry:
            _prevlow_registry[token] = {}
        _prevlow_registry[token][callback_data] = handler

        alive = (token in _prevlow_listeners and _prevlow_listeners[token].is_alive())
        if not alive:
            t = threading.Thread(
                target=_bot_callback_listener,
                args=(token, bot),
                daemon=True
            )
            _prevlow_listeners[token] = t
            t.start()
# ── 레지스트리 패턴 끝 ────────────────────────────────────────────────────


def volume_rate_chk(current_time, vol_ratio, trade_date=""):
    # ===============================
    # 시간대별 거래량 조건 설정
    # 1월 2일 / 11월 19일: 장 시작 10시
    # 11월 19일: 장 종료 16:30 / 1월 2일: 장 종료 15:30
    # ===============================
    is_volume_satisfied = False
    late_open  = trade_date.endswith("0102") or trade_date.endswith("1119")
    late_close = trade_date.endswith("1119")   # 16:30 장종료

    if late_open:
        # 10시 시작 특수일 시간대
        # 1. 10:00 ~ 10:20 사이: 20% 이상
        if 1000 <= int(current_time) <= 1020:
            if vol_ratio >= 20:
                is_volume_satisfied = True
        # 2. 10:21 ~ 10:30 사이: 25% 이상
        elif 1021 <= int(current_time) <= 1030:
            if vol_ratio >= 25:
                is_volume_satisfied = True
        # 3. 11:00 이전 거래량이 전일 대비 50% 이상
        elif int(current_time) < 1100 and vol_ratio >= 50:
            is_volume_satisfied = True
        # 4. 장마감 전 30분: 25% 이상 (1119→16:00~16:30 / 0102→15:00~15:30)
        elif late_close and 1600 <= int(current_time) <= 1630:
            if vol_ratio >= 25:
                is_volume_satisfied = True
        elif not late_close and 1500 <= int(current_time) <= 1530:
            if vol_ratio >= 25:
                is_volume_satisfied = True
        else:
            is_volume_satisfied = True
    else:
        # 일반 거래일 시간대
        # 1. 09:00 ~ 09:20 사이: 20% 이상
        if 900 <= int(current_time) <= 920:
            if vol_ratio >= 20:
                is_volume_satisfied = True
        # 2. 09:21 ~ 09:30 사이: 25% 이상
        elif 921 <= int(current_time) <= 930:
            if vol_ratio >= 25:
                is_volume_satisfied = True
        # 3. 10:00 이전 거래량이 전일 대비 50% 이상 (최우선 특이 케이스)
        elif int(current_time) < 1000 and vol_ratio >= 50:
            is_volume_satisfied = True
        # 4. 15:00 ~ 15:30 사이: 25% 이상
        elif 1500 <= int(current_time) <= 1530:
            if vol_ratio >= 25:
                is_volume_satisfied = True
        else:
            is_volume_satisfied = True

    return is_volume_satisfied

def get_valid_sell_price(price: int) -> int:
    """KIS 호가단위 기준으로 price를 유효한 매도 호가로 내림처리."""
    if price < 1000:
        tick = 1
    elif price < 5000:
        tick = 5
    elif price < 10000:
        tick = 10
    elif price < 50000:
        tick = 50
    elif price < 100000:
        tick = 100
    elif price < 500000:
        tick = 500
    else:
        tick = 1000
    return (price // tick) * tick

def get_kis_1min_from_datetime(
    nick: str,
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
    trade_tp: str,
    exit_price: int,
    access_token: str,
    app_key: str,
    app_secret: str,
    acct_no: str,
    conn,
    bot,
    chat_id,
    breakout_type: str = "high",        # high / close
    breakdown_type: str = "low",        # low / close
    verbose: bool = True
):
    start_dt = datetime.strptime(start_date + start_time, "%Y%m%d%H%M%S")
    # start_time 기준 다음 완성 10분봉 시각
    loop_start_dt = get_next_completed_10min_dt(start_dt)
    trade_date = start_dt.strftime("%Y%m%d")
    signals = []

    if verbose:
        print(f"-{nick}-[{stock_name}-{stock_code}] {trade_date} {datetime.now().strftime('%H%M%S')} 1분봉 생성 중")

    prev_day_info = get_prev_day_info(
        stock_code,
        trade_date,
        access_token,
        app_key,
        app_secret,
        conn
    )

    if prev_day_info is None:
        print(f"[{stock_name}-{stock_code}] 전일 일봉 데이터 미존재")
        return signals

    prev_low = prev_day_info['low_price']
    prev_volume = prev_day_info['volume']

    if trail_tp == 'L':
        _start_time = "163000" if trade_date.endswith("1119") else "153000"

        df = get_kis_1min_full_day(
            stock_code=stock_code,
            trade_date=trade_date,
            start_time=_start_time,
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            verbose=False
        )

        if df.empty:
            print(f"\n⚠️ [{stock_name}-{stock_code}] 분봉 데이터 없음 (건너뜀)")
            return signals

        # 10분봉 거래량 집계 (전체 당일 데이터 기준 — 이전 20봉 평균 계산용)
        _df_tv = df.copy()
        _df_tv["거래량"] = _df_tv["거래량"].astype(int)
        _df_tv["_tk"] = _df_tv["dt"].apply(get_10min_key)
        tenmin_vol_ser = (
            _df_tv.groupby("_tk")["거래량"].sum()
            .reset_index()
            .rename(columns={"_tk": "tenmin_key", "거래량": "tenmin_vol"})
            .sort_values("tenmin_key")
            .reset_index(drop=True)
        )

        def _is_tenmin_vol_surge(key, n=20, mult=2.0):
            """이탈 발생 10분봉 거래량이 직전 n개 10분봉 평균의 mult배 이상인지 확인
            반환: (충족여부: bool, 이탈봉거래량: int, 직전평균거래량: int)
            """
            rows = tenmin_vol_ser[tenmin_vol_ser["tenmin_key"] == key]
            if rows.empty:
                return False, 0, 0
            idx = rows.index[0]
            prev = tenmin_vol_ser.iloc[max(0, idx - n):idx]["tenmin_vol"]
            if prev.empty:
                return False, 0, 0
            cur_vol = int(rows.iloc[0]["tenmin_vol"])
            avg_vol = int(prev.mean())
            return cur_vol >= avg_vol * mult, cur_vol, avg_vol

        # 입력 시간 기준 10분 이후부터만 허용
        df = df[df["dt"] >= loop_start_dt]

        # 날짜별 시작 시간 설정 : 1월 2일 10시 시작
        if trade_date.endswith("0102"):
            start_t = dt_time(10, 0)
        else:
            start_t = dt_time(9, 0)

        # 시간 필터
        df = df[(df["dt"].dt.time >= start_t) & (df["dt"].dt.time <= dt_time(15, 30))]

        # 시간 오름차순 정렬 (필수)
        df = df.sort_values("dt").reset_index(drop=True)

        # ATR 기반 동적 트레일링 스탑 초기화
        daily_data = get_kis_daily_chart_full(stock_code, access_token, app_key, app_secret)
        atr_value = calculate_atr(daily_data, period=14)
        if atr_value is None or atr_value < int(basic_price * 0.01):
            print(f"ATR fallback 적용: 원래값={atr_value}, 매수가={basic_price:,}, fallback={int(basic_price * 0.03):,}")
            atr_value = int(basic_price * 0.03)
        day_high_close = basic_price  # 보유 기간 중 최고 종가 추적

        # 이탈가 이탈 후 10분봉 저가 확인 대기 상태
        breakdown_wait = {
            "active": False,          # 이탈 감시 활성화 여부
            "tenmin_key": None,       # 이탈 발생 10분봉 키
            "tenmin_low": None,       # 이탈 발생 10분봉의 저가
            "tenmin_vol_ok": None,    # 이탈 발생 10분봉 거래량 조건 충족 여부 (완성 후 확정)
            "tenmin_vol": 0,          # 이탈 발생 10분봉 거래량
            "tenmin_avg_vol": 0,      # 직전 20개 10분봉 평균 거래량
            "reason": "",             # 매도 사유 (트리거 시점 저장)
            "signal_type": "",        # 매도 신호 타입
            "effective_stop": 0,      # 트리거 시점 스탑 가격
        }
        # 15:00 전일저가 이탈 사전 경고 알림 발송 여부 (중복 방지)
        prevlow_warn_last_key = None  # 전일저가 이탈 경고 마지막 전송 10분봉 키
        # 이탈 감지 메시지 10분봉 중복 전송 방지
        breakdown_notify_last_key = None  # 이탈 감지 메시지 마지막 전송 10분봉 키

        for _, row in df.iterrows():

            if int(proc_min) < int(row['시간'].replace(':', '')+'00'):
                # ── 매 분봉 시작마다 sell_trigger 초기화 (이전 루프 잔존값 방지) ──
                sell_trigger     = False
                sell_reason      = ""
                sell_signal_type = ""

                high_price = int(row["고가"])
                low_price = int(row["저가"])
                close_price = int(row["종가"])
                acml_vol = int(row["누적거래량"])

                breakout_check = high_price if breakout_type == "high" else close_price
                breakdown_check = low_price if breakdown_type == "low" else close_price

                # ===============================
                # 09:10 또는 10:10 이전 미처리
                # ===============================
                _late_open = trade_date.endswith("0102") or trade_date.endswith("1119")
                if _late_open and row["dt"].time() < datetime.strptime("10:10", "%H:%M").time():
                    continue
                elif not _late_open and row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                    continue

                # 현재 분봉 시간
                current_time = row["시간"].replace(":", "")
                vol_ratio = (acml_vol / prev_volume) * 100 if prev_volume > 0 else 0

                # 일중 최고 종가 갱신 (20%+ 구간 동적 트레일링용)
                day_high_close = max(day_high_close, close_price)

                gain_pct = ((close_price - basic_price) / basic_price) * 100 if basic_price > 0 else 0

                # ===============================
                # 10분봉 저가 이탈 확인 대기 중인 경우
                # ===============================
                current_10min_key = get_10min_key(row["dt"])
                if breakdown_wait["active"]:
                    if current_10min_key != breakdown_wait["tenmin_key"]:
                        # 이탈 발생 10분봉이 완성됨 → 저가·거래량 확정
                        if breakdown_wait["tenmin_low"] is None:
                            trigger_key = breakdown_wait["tenmin_key"]
                            trigger_bars = df[df["dt"].apply(get_10min_key) == trigger_key]
                            if not trigger_bars.empty:
                                breakdown_wait["tenmin_low"] = trigger_bars["저가"].astype(int).min()
                                # 이탈 발생 10분봉 거래량이 직전 20개 10분봉 평균의 2배 이상인지 확인
                                vol_ok, cur_vol, avg_vol = _is_tenmin_vol_surge(trigger_key)
                                breakdown_wait["tenmin_vol_ok"] = vol_ok
                                breakdown_wait["tenmin_vol"] = cur_vol
                                breakdown_wait["tenmin_avg_vol"] = avg_vol
                                if not vol_ok:
                                    # 거래량 미충족 → 잔존 상태 방지를 위해 전체 리셋
                                    breakdown_wait.update({
                                        "active": False,
                                        "tenmin_key": None,
                                        "tenmin_low": None,
                                        "tenmin_vol_ok": None,
                                        "tenmin_vol": 0,
                                        "tenmin_avg_vol": 0,
                                        "reason": "",
                                        "signal_type": "",
                                        "effective_stop": 0,
                                    })

                    if breakdown_wait["active"] and breakdown_wait["tenmin_low"] is not None:
                        # 거래량 조건 충족 확정 + 현재 저가가 이탈 발생 10분봉 저가 이탈 시 매도
                        if low_price < breakdown_wait["tenmin_low"]:
                            sell_trigger = True
                            sell_reason = breakdown_wait["reason"] + f" → 10분봉저가({breakdown_wait['tenmin_low']:,}) 이탈 확정 (이탈봉:{breakdown_wait['tenmin_vol']:,}주/직전20봉평균:{breakdown_wait['tenmin_avg_vol']:,}주)"
                            sell_signal_type = breakdown_wait["signal_type"]
                            effective_stop = breakdown_wait["effective_stop"]
                        else:
                            # 저가 이탈 없으면 → 대기 유지
                            pass
                    else:
                        sell_trigger = False

                if not breakdown_wait["active"]:
                    sell_trigger = False
                    sell_reason = ""
                    sell_signal_type = ""

                    if gain_pct >= 20:
                        # ===============================
                        # 20%+ 고수익 구간: 하이브리드 ATR 동적 트레일링
                        # 최소 15% 수익 보호 + ATR×1.5 트레일
                        # ===============================
                        effective_stop = max(int(basic_price * 1.15), day_high_close - int(atr_value * 1.5))

                        if close_price <= effective_stop and volume_rate_chk(current_time, vol_ratio, trade_date):
                            # 이탈 발생 → 10분봉 저가·거래량 이탈 대기 등록
                            breakdown_wait.update({
                                "active": True,
                                "tenmin_key": current_10min_key,
                                "tenmin_low": None,
                                "tenmin_vol_ok": None,
                                "reason": f"동적스탑({effective_stop:,})원 이탈 [수익률:{gain_pct:.1f}%, ATR:{atr_value:,}]",
                                "signal_type": "DYNAMIC_TRAIL_STOP",
                                "effective_stop": effective_stop,
                            })
                            if verbose and current_10min_key != breakdown_notify_last_key:
                                breakdown_notify_last_key = current_10min_key
                                try:
                                    msg_wait = (
                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                                        f" 동적스탑({effective_stop:,}) 이탈 감지 → 10분봉 저가 이탈 대기"
                                    )
                                    print(msg_wait)
                                    bot.send_message(chat_id=chat_id, text=msg_wait, parse_mode='HTML')
                                except Exception as te:
                                    print(f"텔레그램 발송 실패: {te}")

                    else:
                        # ===============================
                        # 20% 미만: 기존 고정 이탈가 로직
                        # ===============================
                        if trade_tp == 'S':
                            fixed_stop = int(exit_price) if exit_price else int(stop_price)
                        else:
                            fixed_stop = int(stop_price)

                        if fixed_stop > 0 and close_price <= fixed_stop and volume_rate_chk(current_time, vol_ratio, trade_date):
                            # 이탈 발생 → 10분봉 저가·거래량 이탈 대기 등록
                            breakdown_wait.update({
                                "active": True,
                                "tenmin_key": current_10min_key,
                                "tenmin_low": None,
                                "tenmin_vol_ok": None,
                                "reason": f"이탈가({fixed_stop:,})원 이탈 [수익률:{gain_pct:.1f}%]",
                                "signal_type": "FIXED_STOP",
                                "effective_stop": fixed_stop,
                            })
                            if verbose and current_10min_key != breakdown_notify_last_key:
                                breakdown_notify_last_key = current_10min_key
                                try:
                                    msg_wait = (
                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                                        f" 이탈가({fixed_stop:,}) 이탈 감지 → 10분봉 저가 이탈 대기"
                                    )
                                    print(msg_wait)
                                    bot.send_message(chat_id=chat_id, text=msg_wait, parse_mode='HTML')
                                except Exception as te:
                                    print(f"텔레그램 발송 실패: {te}")

                # ===============================
                # 전일저가 이탈 사전 경고 알림 (gain_pct 무관)
                # ===============================
                _prevlow_start  = "101000" if (trade_date.endswith("0102") or trade_date.endswith("1119")) else "091000"
                _prevlow_warn_end = "161000" if trade_date.endswith("1119") else "151000"
                if current_time >= _prevlow_start and current_time < _prevlow_warn_end and prev_low is not None:
                    if close_price < prev_low and int(prev_volume/2) < acml_vol:
                        if current_10min_key != prevlow_warn_last_key:
                            prevlow_warn_last_key = current_10min_key
                            gain_pct_warn = ((close_price - basic_price) / basic_price) * 100 if basic_price > 0 else 0
                            try:
                                msg_warn = (
                                    f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                                    f" [사전경고] 전일저가 이탈 감시"
                                    f" | 전일저가:{prev_low:,}원 | 현재가:{close_price:,}원"
                                    f" | 수익률:{gain_pct_warn:+.1f}%"
                                )
                                _cb_data = f"prevlow_sell:{nick}:{stock_code}:{basic_qty}:{prev_low}"
                                sell_btn = InlineKeyboardButton(
                                    text=f"전량매도 {prev_low:,}원",
                                    callback_data=_cb_data
                                )
                                markup = InlineKeyboardMarkup([[sell_btn]])
                                print(msg_warn)
                                bot.send_message(chat_id=chat_id, text=msg_warn, parse_mode='HTML', reply_markup=markup)
                                # 버튼 클릭 수신 → 자체 매도 실행 (레지스트리 패턴)
                                _register_prevlow_sell(
                                    token=bot.token,
                                    bot=bot, chat_id=chat_id,
                                    access_token=access_token, app_key=app_key, app_secret=app_secret, acct_no=acct_no,
                                    stock_code=stock_code, basic_qty=basic_qty, prev_low=prev_low,
                                    callback_data=_cb_data
                                )
                            except Exception as te:
                                print(f"텔레그램 발송 실패: {te}")
                            # 전일저가 이탈 사전경고 → trail_tp 'P' 변경
                            try:
                                cur_p = conn.cursor()
                                cur_p.execute("""
                                    UPDATE public.trading_trail
                                    SET trail_tp = 'P', mod_dt = %s
                                    WHERE acct_no = %s
                                        AND code = %s
                                        AND trail_day = %s
                                        AND trail_tp NOT IN ('3', '4')
                                """, (datetime.now(), acct_no, stock_code, trade_date))
                                conn.commit()
                                cur_p.close()
                                print(f"  [{stock_name}-{stock_code}] trail_tp → P 변경 완료")
                            except Exception as de:
                                print(f"  [{stock_name}-{stock_code}] trail_tp P 변경 실패: {de}")

                # ===============================
                # 15:10(또는 11월 19일 16:10) 이후 전일저가 이탈 감시 (gain_pct 무관)
                # ===============================
                _prevlow_cutoff = "161000" if trade_date.endswith("1119") else "151000"
                if not breakdown_wait["active"] and not sell_trigger and current_time >= _prevlow_cutoff and prev_low is not None:
                    if close_price < prev_low and int(prev_volume/2) < acml_vol:
                        sell_trigger = True
                        sell_reason = '금일종가 전일저가 이탈'
                        sell_signal_type = "DAILY_BREAKDOWN_AFTER_1510"

                # ===============================
                # 매도 실행 (공통)
                # ===============================
                if sell_trigger:
                    # breakdown_wait 초기화
                    breakdown_wait["active"] = False

                    trail_rate = round((100 - (close_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                    i_trail_plan = trail_plan if trail_plan else "100"
                    trail_qty = basic_qty * int(i_trail_plan) * 0.01
                    trail_amt = close_price * trail_qty
                    u_basic_qty = basic_qty - trail_qty
                    u_basic_amt = basic_price * u_basic_qty

                    try:
                        result = update_trading_daily_close(nick, close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "4", row['시간'].replace(':', '')+'00', sell_reason, conn, bot, chat_id)
                        if result and verbose:
                            try:
                                if sell_signal_type == "DYNAMIC_TRAIL_STOP":
                                    message = (
                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] {sell_reason}, 최고종가:{day_high_close:,}원, 현재가:{close_price:,}원"
                                    )
                                elif sell_signal_type == "DAILY_BREAKDOWN_AFTER_1510":
                                    message = (
                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] 전일 저가 : {prev_low:,}원 이탈 및 전일 거래량 대비 50% : {int(prev_volume/2):,}주 돌파"
                                    )
                                else:
                                    message = (
                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] {sell_reason}, 현재가:{close_price:,}원"
                                    )
                                print(message)
                                bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                            except Exception as te:
                                print(f"텔레그램 발송 실패: {te}")
                    except Exception as e:
                        print(f"상위 호출부: 매도 함수 호출 중 예외 발생(무시됨): {e}")

                    if sell_signal_type == "DYNAMIC_TRAIL_STOP":
                        signals.append({
                            "signal_type": sell_signal_type,
                            "종목명": stock_name,
                            "종목코드": stock_code,
                            "발생일자": row["일자"],
                            "발생시간": row["시간"],
                            "이탈가격": close_price,
                            "동적스탑": effective_stop,
                            "수익률": gain_pct,
                            "ATR": atr_value,
                        })
                    elif sell_signal_type == "DAILY_BREAKDOWN_AFTER_1510":
                        signals.append({
                            "signal_type": sell_signal_type,
                            "종목코드": stock_code,
                            "발생일자": row["일자"],
                            "발생시간": row["시간"],
                            "이탈가격": close_price,
                            "전일저가": prev_low,
                            "전일거래량 대비 50%": int(prev_volume/2),
                        })
                    else:
                        signals.append({
                            "signal_type": sell_signal_type,
                            "종목코드": stock_code,
                            "발생일자": row["일자"],
                            "발생시간": row["시간"],
                            "이탈가격": close_price,
                            "수익률": gain_pct,
                        })
                    return signals

    else:

        if trail_tp == '2':
            tenmin_state = {
                "base_low": int(stop_price) if stop_price else 0,
                "base_high": int(target_price) if target_price else 0,
                "base_vol": int(volumn) if volumn else 0,
                "peak_high": int(target_price) if target_price else 0,  # 기준봉 갱신 이력 최고 고가
                "base_key": None,         # 돌파 발생 10분봉 키 (DB 복원 시 None → 스킵 없음)
            }
        else:
            tenmin_state = {
                "base_low": None,         # 기준봉 저가
                "base_high": None,        # 기준봉 고가
                "base_vol": None,         # 기준봉 거래량
                "peak_high": 0,           # 기준봉 갱신 이력 최고 고가
                "base_key": None,         # 돌파 발생 10분봉 키
            }

        _start_time = "163000" if trade_date.endswith("1119") else "153000"

        df = get_kis_1min_full_day(
            stock_code=stock_code,
            trade_date=trade_date,
            start_time=_start_time,
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            verbose=False
        )

        if df.empty:
            print(f"\n⚠️ [{stock_name}-{stock_code}] 분봉 데이터 없음 (건너뜀)")
            return signals

        # 입력 시간 기준 10분 이후부터만 허용
        df = df[df["dt"] >= loop_start_dt]

        # 날짜별 시작 시간 설정 : 1월 2일 10시 시작
        if trade_date.endswith("0102"):
            start_t = dt_time(10, 0)
        else:
            start_t = dt_time(9, 0)

        # 시간 필터
        df = df[(df["dt"].dt.time >= start_t) & (df["dt"].dt.time <= dt_time(15, 30))]

        # 시간 오름차순 정렬 (필수)
        df = df.sort_values("dt").reset_index(drop=True)

        # trail_tp='1' 이탈가 이탈 후 10분봉 저가·거래량 대기 상태
        if trail_tp == '1':
            _df_tv_1 = df.copy()
            _df_tv_1["거래량"] = _df_tv_1["거래량"].astype(int)
            _df_tv_1["_tk"] = _df_tv_1["dt"].apply(get_10min_key)
            tenmin_vol_ser_1 = (
                _df_tv_1.groupby("_tk")["거래량"].sum()
                .reset_index()
                .rename(columns={"_tk": "tenmin_key", "거래량": "tenmin_vol"})
                .sort_values("tenmin_key")
                .reset_index(drop=True)
            )

            def _is_tenmin_vol_surge_1(key, n=20, mult=2.0):
                """이탈 발생 10분봉 거래량이 직전 n개 10분봉 평균의 mult배 이상인지 확인
                반환: (충족여부: bool, 이탈봉거래량: int, 직전평균거래량: int)
                """
                rows = tenmin_vol_ser_1[tenmin_vol_ser_1["tenmin_key"] == key]
                if rows.empty:
                    return False, 0, 0
                idx = rows.index[0]
                prev = tenmin_vol_ser_1.iloc[max(0, idx - n):idx]["tenmin_vol"]
                if prev.empty:
                    return False, 0, 0
                cur_vol = int(rows.iloc[0]["tenmin_vol"])
                avg_vol = int(prev.mean())
                return cur_vol >= avg_vol * mult, cur_vol, avg_vol

            breakdown_wait_1 = {
                "active": False,        # 이탈 감시 활성화 여부
                "tenmin_key": None,     # 이탈 발생 10분봉 키
                "tenmin_low": None,     # 이탈 발생 10분봉 저가 (완성 후 확정)
                "tenmin_vol_ok": None,  # 거래량 조건 충족 여부 (완성 후 확정)
                "tenmin_vol": 0,        # 이탈 발생 10분봉 거래량
                "tenmin_avg_vol": 0,    # 직전 20개 10분봉 평균 거래량
                "sell_label": "",       # 매도 사유 ('손절매도' / '이탈매도')
            }

        # 현재 형성 중인 10분봉 키 — 이 키와 같거나 이후 봉은 미완성이므로 스킵
        current_10min_key = get_completed_10min_key(datetime.now())
        for _, row in df.iterrows():

            if int(proc_min) < int(row['시간'].replace(':', '')+'00'):
                # ── 매 분봉 시작마다 sell_trigger 초기화 (이전 루프 잔존값 방지) ──
                sell_trigger     = False
                sell_reason      = ""
                sell_signal_type = ""

                high_price = int(row["고가"])
                low_price = int(row["저가"])
                close_price = int(row["종가"])
                acml_vol = int(row["누적거래량"])

                breakout_check = high_price if breakout_type == "high" else close_price
                breakdown_check = low_price if breakdown_type == "low" else close_price

                current_time = row["dt"].time()

                if high_price > low_price:
                    if trade_date.endswith("0102") or trade_date.endswith("1119"):
                        pre_market = current_time < datetime.strptime("10:10", "%H:%M").time()
                    else:
                        pre_market = current_time < datetime.strptime("09:10", "%H:%M").time()

                    # ===============================
                    # 기준봉 미생성 상태 → 목표가 돌파 시 기준봉 생성 (09:10 또는 10:10 이전에도 수행)
                    # ===============================
                    if tenmin_state["base_low"] is None:
                        chk_vol = volumn if volumn else 0
                        if trail_tp == '1':
                            # ── breakdown_wait_1 대기 중: 10분봉 완성 후 저가·거래량 확인 ──
                            if breakdown_wait_1["active"]:
                                current_10min_key_1 = get_10min_key(row["dt"])
                                if current_10min_key_1 != breakdown_wait_1["tenmin_key"]:
                                    # 이탈 발생 10분봉 완성 → 저가·거래량 확정
                                    if breakdown_wait_1["tenmin_low"] is None:
                                        trigger_key_1 = breakdown_wait_1["tenmin_key"]
                                        trigger_bars_1 = df[df["dt"].apply(get_10min_key) == trigger_key_1]
                                        if not trigger_bars_1.empty:
                                            breakdown_wait_1["tenmin_low"] = trigger_bars_1["저가"].astype(int).min()
                                            vol_ok_1, cur_vol_1, avg_vol_1 = _is_tenmin_vol_surge_1(trigger_key_1)
                                            breakdown_wait_1["tenmin_vol_ok"] = vol_ok_1
                                            breakdown_wait_1["tenmin_vol"] = cur_vol_1
                                            breakdown_wait_1["tenmin_avg_vol"] = avg_vol_1
                                            if not vol_ok_1:
                                                # 거래량 미충족 → 잔존 상태 방지를 위해 전체 리셋
                                                breakdown_wait_1.update({
                                                    "active": False,
                                                    "tenmin_key": None,
                                                    "tenmin_low": None,
                                                    "tenmin_vol_ok": None,
                                                    "tenmin_vol": 0,
                                                    "tenmin_avg_vol": 0,
                                                    "sell_label": "",
                                                })

                                if breakdown_wait_1["active"] and breakdown_wait_1["tenmin_low"] is not None:
                                    if low_price < breakdown_wait_1["tenmin_low"]:
                                        # 10분봉 저가 이탈 확정 → 매도 실행
                                        trail_rate = round((100 - (close_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                                        i_trail_plan = trail_plan if trail_plan else "100"
                                        trail_qty = basic_qty * int(i_trail_plan) * 0.01
                                        trail_amt = close_price * trail_qty
                                        u_basic_qty = basic_qty - trail_qty
                                        u_basic_amt = basic_price * u_basic_qty
                                        sell_label = breakdown_wait_1["sell_label"]
                                        try:
                                            result = update_trading_close(nick, close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "4", row['시간'].replace(':', '')+'00', sell_label, conn, bot, chat_id)
                                            if result and verbose:
                                                try:
                                                    message = (
                                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                                                        f" 돌파 전 이탈가 이탈 10분봉저가({breakdown_wait_1['tenmin_low']:,}) 이탈 확정 → {sell_label}"
                                                        f" (이탈봉:{breakdown_wait_1['tenmin_vol']:,}주/직전20봉평균:{breakdown_wait_1['tenmin_avg_vol']:,}주)"
                                                    )
                                                    print(message)
                                                    bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                                                except Exception as te:
                                                    print(f"텔레그램 발송 실패: {te}")
                                        except Exception as e:
                                            print(f"상위 호출부: 매도 함수 호출 중 예외 발생(무시됨): {e}")
                                        signals.append({
                                            "signal_type": "BREAKDOWN_BEFORE_BREAKOUT",
                                            "종목명": stock_name,
                                            "종목코드": stock_code,
                                            "발생일자": row["일자"],
                                            "발생시간": row["시간"],
                                            "이탈가격": breakdown_check
                                        })
                                        return signals

                            # ── 이탈 감지: 즉시 매도 대신 10분봉 저가·거래량 대기 등록 ──
                            if not breakdown_wait_1["active"]:
                                # 손절매수 대상: 최종이탈가(exit_price) 이탈
                                if not pre_market and trade_tp is not None and trade_tp == 'S' and breakdown_check <= exit_price and acml_vol > chk_vol:
                                    current_10min_key_1 = get_10min_key(row["dt"])
                                    breakdown_wait_1.update({
                                        "active": True,
                                        "tenmin_key": current_10min_key_1,
                                        "tenmin_low": None,
                                        "tenmin_vol_ok": None,
                                        "sell_label": "손절매도",
                                    })
                                    try:
                                        message = (
                                            f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                                            f" 손절매수 대상: 최종이탈가({exit_price:,})원 이탈 대기"
                                        )
                                        print(message)
                                        bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                                    except Exception as te:
                                        print(f"텔레그램 발송 실패: {te}")

                                # 매수금액 대상: 손절가(stop_price) 이탈
                                elif not pre_market and trade_tp is not None and trade_tp == 'M' and breakdown_check <= stop_price and acml_vol > chk_vol:
                                    current_10min_key_1 = get_10min_key(row["dt"])
                                    breakdown_wait_1.update({
                                        "active": True,
                                        "tenmin_key": current_10min_key_1,
                                        "tenmin_low": None,
                                        "tenmin_vol_ok": None,
                                        "sell_label": "이탈매도",
                                    })
                                    try:
                                        message = (
                                            f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>]"
                                            f" 매수금액 대상: 손절가({stop_price:,})원 이탈 대기"
                                        )
                                        print(message)
                                        bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                                    except Exception as te:
                                        print(f"텔레그램 발송 실패: {te}")

                            # 목표가 돌파
                            if breakout_check >= target_price:
                                base_key = get_completed_10min_key(row["dt"])
                                base_10min = df[df["dt"].apply(get_10min_key) == base_key]

                                if base_10min.empty:
                                    continue

                                tenmin_state.update({
                                    "base_low": base_10min["저가"].astype(int).min(),
                                    "base_high": base_10min["고가"].astype(int).max(),
                                    "base_vol": base_10min["거래량"].astype(int).sum(),
                                    "base_key": base_key,   # 돌파 발생 10분봉 → 이 봉 완성 시점은 스킵
                                })

                                if verbose:
                                    try:
                                        message = (
                                            f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] 목표가 {target_price:,}원 돌파 기준봉 설정, 고가 : {tenmin_state['base_high']:,}원, 저가 : {tenmin_state['base_low']:,}원, 거래량 : {tenmin_state['base_vol']:,}주 "
                                        )
                                        print(message)
                                        bot.send_message(
                                            chat_id=chat_id,
                                            text=message,
                                            parse_mode='HTML'
                                        )
                                    except Exception as te:
                                        print(f"텔레그램 발송 실패: {te}")

                                update_trading_trail(int(tenmin_state['base_low']), int(tenmin_state['base_high']), int(tenmin_state['base_vol']), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00', conn)

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
                    # 기준봉 존재 → 10분봉 완성 시점에서 저가 이탈 체크 및 기준봉 갱신
                    # ===============================
                    else:

                        # ===============================
                        # 10분봉 완성 시 기준봉 갱신
                        # ===============================
                        completed_key = get_completed_10min_key(row["dt"])
                        tenmin_df = df[df["dt"].apply(get_completed_10min_key) == completed_key]

                        # 10분봉의 마지막 1분봉일 때만 처리 (10분봉 완성 시점)
                        # 돌파 발생 10분봉 자체는 스킵 → 다음 완성 10분봉부터 매도/갱신 체크
                        if not tenmin_df.empty and row["dt"] == tenmin_df["dt"].max():
                            if completed_key == tenmin_state["base_key"]:
                                continue
                            if completed_key >= current_10min_key:
                                continue
                            tenmin_low = tenmin_df["저가"].astype(int).min()          # 기준봉 갱신용
                            tenmin_high = tenmin_df["고가"].astype(int).max()
                            tenmin_vol = tenmin_df["거래량"].astype(int).sum()
                            tenmin_close = close_price                                 # 이탈 판단: 10분봉 종가(마지막 1분봉 종가)
                            sell_price = close_price

                             # ── 매도 조건 판단 (이탈 기준: 10분봉 종가) ─────────────────
                            sell_trigger = False
                            sell_reason = ""
                            safety_margin = int(basic_price + basic_price * 0.05)
                            PEAK_RETRACEMENT_RATE = 0.5  # 고점~안전마진 구간 중 허용 되돌림 비율 (50%)

                            # 조건 A: 기준봉 저가를 종가로 이탈 + 안전마진 이하 → 즉시 매도 (손절)
                            # 매도가 하한: basic_price (보유단가 이상 체결 시도), 호가단위 내림
                            if not sell_trigger and tenmin_close < tenmin_state["base_low"] and tenmin_close <= safety_margin:
                                sell_trigger = True
                                sell_price = get_valid_sell_price(max(tenmin_close, basic_price))
                                sell_reason = f"안전마진({safety_margin:,})원 이하 기준봉 저가({tenmin_state['base_low']:,})원 종가 이탈 (매도가:{sell_price:,})"

                            # 조건 B: 고점 대비 되돌림 → 수익 구간 동적 청산 (peak retracement)
                            # 활성화 조건: safety_margin 초과(수익 확보) + 최소 2% 이상 상승폭
                            # 14:30 이후 수익 보호 강화: 되돌림 허용 비율 50% → 30%
                            # 매도가 하한: safety_margin × 1.02 (안전마진 + 2% 이상), 호가단위 내림
                            peak_to_safety = tenmin_state["peak_high"] - safety_margin
                            effective_retracement_rate = 0.3 if current_time >= dt_time(14, 30) else PEAK_RETRACEMENT_RATE
                            if not sell_trigger and tenmin_state["peak_high"] > safety_margin and peak_to_safety >= int(basic_price * 0.02):
                                peak_sell_threshold = tenmin_state["peak_high"] - int(peak_to_safety * effective_retracement_rate)
                                if tenmin_close < peak_sell_threshold:
                                    sell_trigger = True
                                    sell_price = get_valid_sell_price(max(tenmin_close, int(safety_margin * 1.02)))
                                    sell_reason = f"고점({tenmin_state['peak_high']:,})원 되돌림 임계({peak_sell_threshold:,})원 종가 이탈 (매도가:{sell_price:,})"

                            # 조건 C: 기준봉 저가를 종가로 이탈 + 안전마진 이상 → 연속 이탈 판단
                            # 거래량 초과 OR 연속 이탈 시 매도 (저거래량 지속 하락 방어)
                            # 14:30 이후는 연속 이탈 1회만으로 매도 (장후반 모멘텀 소진 방어)
                            # 매도가 하한: safety_margin (안전마진 이상), 호가단위 내림
                            prev_key = completed_key - timedelta(minutes=10)
                            prev_tenmin_df = df[df["dt"].apply(get_completed_10min_key) == prev_key]
                            if not prev_tenmin_df.empty:
                                prev_close = int(prev_tenmin_df.loc[prev_tenmin_df["dt"].idxmax(), "종가"])
                                consecutive_breaks = (1 if tenmin_close < tenmin_state["base_low"] else 0) + \
                                                     (1 if prev_close < tenmin_state["base_low"] else 0)
                            else:
                                consecutive_breaks = 1 if tenmin_close < tenmin_state["base_low"] else 0
                            late_day_consec_threshold = 1 if current_time >= dt_time(14, 30) else 2
                            if not sell_trigger and tenmin_close < tenmin_state["base_low"] and tenmin_close > safety_margin and (tenmin_vol > tenmin_state["base_vol"] or consecutive_breaks >= late_day_consec_threshold):
                                sell_trigger = True
                                sell_price = get_valid_sell_price(max(tenmin_close, safety_margin))
                                sell_reason = f"기준봉 저가({tenmin_state['base_low']:,})원 종가 이탈 (매도가:{sell_price:,})"

                            if sell_trigger:
                                trail_rate = round((100 - (sell_price / basic_price) * 100) * -1, 2) if basic_price > 0 else 0
                                i_trail_plan = trail_plan if trail_plan else "50"
                                trail_qty = basic_qty * int(i_trail_plan) * 0.01
                                trail_amt = sell_price * trail_qty
                                u_basic_qty = basic_qty - trail_qty
                                u_basic_amt = basic_price * u_basic_qty

                                if basic_qty == trail_qty:
                                    try:
                                        result = update_trading_close(nick, sell_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "4", row['시간'].replace(':', '')+'00', '수익완료', conn, bot, chat_id)
                                        if result:
                                            if verbose:
                                                try:
                                                    message = (
                                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] {sell_reason} (10분봉 종가:{tenmin_close:,}원, 저가:{tenmin_low:,}원), 거래량:{tenmin_vol:,}주"
                                                    )
                                                    print(message)
                                                    bot.send_message(
                                                        chat_id=chat_id,
                                                        text=message,
                                                        parse_mode='HTML'
                                                    )
                                                except Exception as te:
                                                    print(f"텔레그램 발송 실패: {te}")
                                    except Exception as e:
                                        print(f"상위 호출부: 매도 함수 호출 중 예외 발생(무시됨): {e}")

                                else:
                                    try:
                                        result = update_trading_close(nick, sell_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, access_token, app_key, app_secret, stock_code, stock_name, start_date, start_time, "3", row['시간'].replace(':', '')+'00', '안전마진', conn, bot, chat_id)
                                        if result:
                                            if verbose:
                                                try:
                                                    message = (
                                                        f"-{nick}-[{row['일자']}-{row['시간']}]{stock_name}[<code>{stock_code}</code>] {sell_reason} (10분봉 종가:{tenmin_close:,}원, 저가:{tenmin_low:,}원), 거래량:{tenmin_vol:,}주"
                                                    )
                                                    print(message)
                                                    bot.send_message(
                                                        chat_id=chat_id,
                                                        text=message,
                                                        parse_mode='HTML'
                                                    )
                                                except Exception as te:
                                                    print(f"텔레그램 발송 실패: {te}")

                                    except Exception as e:
                                        print(f"상위 호출부: 매도 함수 호출 중 예외 발생(무시됨): {e}")

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

                            # 10분봉 완성 시 기준봉 갱신
                            base_updated = False
                            if tenmin_high > tenmin_low:
                                if tenmin_high > tenmin_state["base_high"] or tenmin_vol > tenmin_state["base_vol"]:
                                    tenmin_state.update({
                                        "base_low": max(tenmin_low, tenmin_state["base_low"]),  # 트레일링 스탑은 위로만 이동
                                        "base_high": tenmin_high,
                                        "base_vol": tenmin_vol,
                                        "peak_high": max(tenmin_state["peak_high"], tenmin_high),  # 고점 갱신
                                    })
                                    base_updated = True

                                    if verbose:
                                        reason = "고가 돌파" if tenmin_high > tenmin_state["base_high"] else "거래량 돌파"
                                        message = (
                                            f"-{nick}-[{completed_key.strftime('%Y%m%d %H:%M')}]{stock_name}[<code>{stock_code}</code>] {reason} 기준봉 갱신 고가 : {tenmin_high:,}원,  저가 : {tenmin_low:,}원, 거래량 : {tenmin_vol:,}주"
                                        )
                                        print(message)

                                    update_trading_trail(int(tenmin_low), int(tenmin_high), int(tenmin_vol), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00', conn)

                            # → 다음 1분 실행 시 이미 처리한 봉 재처리 방지
                            if not base_updated:
                                update_trading_trail(int(tenmin_state["base_low"]), int(tenmin_state["base_high"]), int(tenmin_state["base_vol"]), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00', conn)

    return signals


def process_stock(stock_info, nick, ac, bot, chat_id):
    """종목별 독립 DB 연결로 병렬 처리"""
    conn_stock = db.connect(conn_string)
    try:
        signal = get_kis_1min_from_datetime(
            nick=nick,
            stock_code=stock_info[0],
            stock_name=stock_info[1],
            start_date=stock_info[2],
            start_time=stock_info[3],
            target_price=int(stock_info[4]),
            stop_price=int(stock_info[5]),
            basic_price=int(stock_info[6]),
            basic_qty=int(stock_info[7]),
            trail_tp=stock_info[8],
            trail_plan=stock_info[9],
            proc_min=stock_info[10],
            volumn=stock_info[11],
            trade_tp=stock_info[12],
            exit_price=int(stock_info[13]),
            access_token=ac['access_token'],
            app_key=ac['app_key'],
            app_secret=ac['app_secret'],
            acct_no=ac['acct_no'],
            conn=conn_stock,
            bot=bot,
            chat_id=chat_id,
            breakout_type="high",
            verbose=True
        )

        if signal:
            print(f"\n📌 신호 결과-{nick}")
            print(signal)
        else:
            print(f"\n📌 아직 신호 없음-{nick}")

    except Exception as e:
        print(f"\n⚠️ [{stock_info[1]}-{stock_info[0]}] 처리 중 오류 (건너뜀): {e}")
    finally:
        conn_stock.close()


def process_account(nick):
    """계좌별 독립 DB 연결 및 Bot 인스턴스로 병렬 처리"""
    conn_acct = db.connect(conn_string)
    _bot = None
    _chat_id = None
    try:
        ac = account(nick, conn_acct)
        acct_no = ac['acct_no']
        token = ac['bot_token2']
        chat_id = ac['chat_id']
        _chat_id = chat_id

        # Bot 인스턴스를 계좌당 1회만 생성
        updater = Updater(token=token, use_context=True)
        bot = updater.bot
        _bot = bot

        # 계좌잔고 조회
        c = stock_balance(ac['access_token'], ac['app_key'], ac['app_secret'], acct_no, "")

        # 일별 매매 잔고 현행화 (배치 처리)
        if len(c) > 0:
            insert_query199 = """
                INSERT INTO dly_trading_balance (
                    acct_no, code, name, balance_day,
                    balance_price, balance_qty, balance_amt,
                    value_rate, value_amt, buy_qty, sell_qty, mod_dt
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            records199 = [
                (
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
                for i in range(len(c))
            ]
            cur199 = conn_acct.cursor()
            cur199.executemany(insert_query199, records199)
            conn_acct.commit()
            cur199.close()

        # 매매추적 동일종목 중복건 정리 및 잔고 현행화
        # 1) 동일 종목 중복건 중 trail_dtm 최종건을 제외한 나머지 trail_tp = 'Y' 처리
        cur_dedup = conn_acct.cursor()
        cur_dedup.execute("""
            UPDATE public.trading_trail tt
            SET trail_tp = 'Y', mod_dt = %s
            FROM (
                SELECT acct_no, code, trail_day, trail_dtm,
                       row_number() OVER (PARTITION BY code ORDER BY trail_dtm DESC) AS rn
                FROM public.trading_trail
                WHERE acct_no = %s AND trail_day = %s AND trail_tp IN ('1', '2', 'L')
            ) sub
            WHERE tt.acct_no = sub.acct_no
            AND tt.code = sub.code
            AND tt.trail_day = sub.trail_day
            AND tt.trail_dtm = sub.trail_dtm
            AND tt.trail_tp IN ('1', '2', 'L')
            AND sub.rn > 1
        """, (datetime.now(), acct_no, today))
        conn_acct.commit()
        cur_dedup.close()

        # 2) 잔고 데이터로 매매추적 최종건 basic_price, basic_qty, basic_amt 현행화 (배치 처리)
        if len(c) > 0:
            update_balance_query = """
                UPDATE public.trading_trail
                SET basic_price = %s, basic_qty = %s, basic_amt = %s, mod_dt = %s
                WHERE acct_no = %s AND code = %s AND trail_day = %s AND trail_tp IN ('1', '2', 'L')
            """
            balance_records = [
                (
                    float(c['pchs_avg_pric'][i]),
                    int(c['hldg_qty'][i]),
                    int(c['pchs_amt'][i]) if int(c['hldg_qty'][i]) > 0 else 0,
                    datetime.now(),
                    acct_no,
                    c['pdno'][i],
                    today
                )
                for i in range(len(c))
            ]
            cur_balance = conn_acct.cursor()
            cur_balance.executemany(update_balance_query, balance_records)
            conn_acct.commit()
            cur_balance.close()

        # 매매추적 조회
        cur200 = conn_acct.cursor()
        cur200.execute("select code, name, trail_day, trail_dtm, target_price, stop_price, basic_price, COALESCE(basic_qty, 0), CASE WHEN trail_tp = 'L' THEN 'L' ELSE trail_tp END, trail_plan, proc_min, volumn, trade_tp, exit_price from public.trading_trail where acct_no = '" + str(acct_no) + "' and trail_tp in ('1', '2', 'L') and trail_day = '" + today + "' and to_char(to_timestamp(proc_min, 'HH24MISS') + interval '5 minutes', 'HH24MISS') <= to_char(now(), 'HH24MISS') order by code, proc_min, mod_dt")
        # cur200.execute("select code, name, trail_day, trail_dtm, target_price, stop_price, basic_price, COALESCE(basic_qty, 0), CASE WHEN trail_tp = 'L' THEN 'L' ELSE trail_tp END, trail_plan, proc_min, volumn, trade_tp, exit_price from public.trading_trail where acct_no = '" + str(acct_no) + "' and trail_tp in ('1', '2', 'L') and trail_day = '" + today + "' order by code, proc_min, mod_dt")
        result_two00 = cur200.fetchall()
        cur200.close()

        if result_two00:
            # 일봉 데이터 사전 조회 (캐시 워밍업 - 순차 처리로 rate limit 방지)
            unique_codes = list(set(row[0] for row in result_two00))
            trade_date = today
            for code in unique_codes:
                get_prev_day_info(code, trade_date, ac['access_token'], ac['app_key'], ac['app_secret'], conn_acct)
                time.sleep(0.1)
                get_kis_daily_chart_full(code, ac['access_token'], ac['app_key'], ac['app_secret'])
                time.sleep(0.1)

            # 종목별 병렬 처리 (동일 app_key 공유 → max_workers=6로 API rate limit 제어)
            max_stock_workers = min(len(result_two00), 6)
            with ThreadPoolExecutor(max_workers=max_stock_workers) as stock_executor:
                futures = {
                    stock_executor.submit(process_stock, stock_info, nick, ac, bot, chat_id): stock_info
                    for stock_info in result_two00
                }
                for future in as_completed(futures):
                    stock_info = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"⚠️ [{stock_info[1]}-{stock_info[0]}] 종목 처리 오류: {e}")

    except Exception as e:
        print(f"[{nick}] 계좌 처리 오류: {e}")
        if _bot and _chat_id:
            try:
                _bot.send_message(chat_id=_chat_id, text=f"⚠️ [{nick}] 계좌 처리 오류\n{e}")
            except Exception:
                pass
    finally:
        conn_acct.close()


if __name__ == "__main__":

    # 영업일 확인용 임시 연결 (스레드 진입 전 단일 사용)
    _conn_check = db.connect(conn_string)
    try:
        _is_business = is_business_day(today, _conn_check)
    finally:
        _conn_check.close()

    if _is_business:

        nickname_list = ['phills2', 'phills13', 'phills15', 'yh480825', 'mamalong', 'worry106']

        # 6개 계좌 병렬 처리
        with ThreadPoolExecutor(max_workers=len(nickname_list)) as account_executor:
            account_futures = {
                account_executor.submit(process_account, nick): nick
                for nick in nickname_list
            }
            for future in as_completed(account_futures):
                nick = account_futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[{nick}] 계좌 최종 오류: {e}")
