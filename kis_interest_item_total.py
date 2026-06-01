import psycopg2 as db
from datetime import datetime, timedelta
import kis_api_resp as resp
import requests
import json
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import math
import pandas as pd
import time
from telegram.ext import Updater
from concurrent.futures import ThreadPoolExecutor, as_completed

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"

today = datetime.now().strftime("%Y%m%d")

# 인증처리
def auth(APP_KEY, APP_SECRET):

    # 인증처리
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
            "appkey":APP_KEY,
            "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
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

# 주식현재가 시세
def inquire_price(access_token, app_key, app_secret, code):

    t = datetime.now().strftime('%H%M')

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "FHKST01010100"}
    params = {
            'FID_COND_MRKT_DIV_CODE': "J" if '0900' <= t < '1530' else "NX",  # J:KRX, NX:NXT, UN:통합
            'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)

    return ar.getBody().output1

# 계좌잔고 조회
def stock_balance(access_token, app_key, app_secret, acct_no, rtFlag):

    t = datetime.now().strftime('%H%M')
    
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8434R"}    # tr_id : TTTC8434R[실전투자], VTTC8434R[모의투자]
    params = {
                "CANO": acct_no,
                'ACNT_PRDT_CD': '01',
                'AFHR_FLPR_YN': 'N' if '0900' <= t < '1530' else 'X',            # N : 기본값, Y : 시간외단일가, X : NXT 정규장 (프리마켓, 메인, 애프터마켓) NXT 거래종목만 시세 등 정보가 NXT 기준으로 변동됩니다. KRX 종목들은 그대로 유지
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
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    
    if rtFlag == "all" and ar.isOK():
        output = ar.getBody().output2
    else:    
        output = ar.getBody().output1

    return pd.DataFrame(output)

def fetch_candles_with_base(access_token, app_key, app_secret, code, base_dtm):
    """
    1분봉을 조회한 뒤 10분봉으로 리샘플링하여 base_dtm 포함 여부 확인.
    필요하면 과거 데이터를 추가 조회.
    최종 반환값은 원본 API 결과(dict 리스트).
    """

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST03010200",
        "custtype": "P"
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    URL = f"{URL_BASE}/{PATH}"

    def request_candles(start_time):
        """API 호출 → 원본 dict 리스트 반환"""
        params = {
            'FID_COND_MRKT_DIV_CODE': "J",
            'FID_INPUT_ISCD': code,
            'FID_INPUT_HOUR_1': start_time,  # 기준 시각
            'FID_PW_DATA_INCU_YN': 'N',
            'FID_ETC_CLS_CODE': ""
        }
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)
        return ar.getBody().output2  # 원본 dict 리스트

    _today_str = datetime.now().strftime("%Y%m%d")

    def convert_to_df(candle_list):
        """dict 리스트 → DataFrame(1분봉)"""
        minute_list = []
        for item in candle_list:
            minute_list.append({
                'timestamp': pd.to_datetime(_today_str + item['stck_cntg_hour'], format='%Y%m%d%H%M%S'),
                'open': float(item['stck_oprc']),
                'high': float(item['stck_hgpr']),
                'low': float(item['stck_lwpr']),
                'close': float(item['stck_prpr']),
                'volume': float(item['cntg_vol'])
            })
        return pd.DataFrame(minute_list).sort_values('timestamp').reset_index(drop=True)

    def resample_to_10min(df):
        """1분봉 → 10분봉 변환"""
        df = df.set_index('timestamp')
        df_10 = df.resample('10min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()
        return df_10

    # 1차 조회
    cur_time = datetime.now().strftime("%H%M%S")
    candle_list = request_candles(cur_time)
    df_1m = convert_to_df(candle_list)
    df_10m = resample_to_10min(df_1m)

    # base_dtm이 속한 봉 시작시간
    base_candle_start = base_dtm.replace(minute=(base_dtm.minute // 10) * 10, second=0)

    included = any(df_10m['timestamp'] == base_candle_start)

    # 포함 안 되면 과거 조회 반복
    while not included:
        oldest_time = df_1m['timestamp'].min() - timedelta(minutes=1)
        start_time_str = oldest_time.strftime("%H%M%S")

        extra_candles = request_candles(start_time_str)
        if not extra_candles:  # 더 이상 데이터 없음
            break

        extra_df = convert_to_df(extra_candles)
        df_1m = pd.concat([extra_df, df_1m]).drop_duplicates().sort_values('timestamp').reset_index(drop=True)
        df_10m = resample_to_10min(df_1m)

        # 원본도 합쳐줌
        candle_list = extra_candles + candle_list

        included = any(df_10m['timestamp'] == base_candle_start)

    return candle_list

def compute_market_ratio(
    kospi_short, kospi_mid, kospi_long,
    kosdak_short, kosdak_mid, kosdak_long,
):
    """6개 timeframe 신호를 종합한 시장 승률 (0~100)"""
    rules = [
        (kospi_short,  '01', '02', 5),
        (kospi_mid,    '03', '04', 8),
        (kospi_long,   '05', '06', 12),
        (kosdak_short, '01', '02', 5),
        (kosdak_mid,   '03', '04', 8),
        (kosdak_long,  '05', '06', 12),
    ]
    score = 0
    for signal, bull, bear, weight in rules:
        if signal == bull:
            score += weight
        elif signal == bear:
            score -= weight
    return max(0, min(100, 50 + score))


def fetch_market_state(conn, asset_num, acct_no):
    """현재 DB에 저장된 6개 시간프레임 신호 조회"""
    cur = conn.cursor()
    cur.execute("""
        SELECT kospi_short, kospi_mid, kospi_long,
               kosdak_short, kosdak_mid, kosdak_long
          FROM "stockFundMng_stock_fund_mng"
         WHERE asset_num = %s AND acct_no = %s
    """, (asset_num, acct_no))
    row = cur.fetchone()
    cur.close()
    return dict(zip(
        ('kospi_short','kospi_mid','kospi_long',
         'kosdak_short','kosdak_mid','kosdak_long'),
        row if row else (None,) * 6
    ))

# 자산정보 및 시장레벨정보 처리
def fund_marketLevel_proc(access_token, app_key, app_secret, acct_no, conn):
    # 계좌잔고 조회
    b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

    u_tot_evlu_amt = 0
    u_dnca_tot_amt = 0
    u_nass_amt = 0
    u_prvs_rcdl_excc_amt = 0
    u_scts_evlu_amt = 0
    u_asst_icdc_amt = 0

    for i, name in enumerate(b.index):
        u_tot_evlu_amt = int(b['tot_evlu_amt'][i])
        u_dnca_tot_amt = int(b['dnca_tot_amt'][i])
        u_nass_amt = int(b['nass_amt'][i])
        u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])
        u_scts_evlu_amt = int(b['scts_evlu_amt'][i])
        u_asst_icdc_amt = int(b['asst_icdc_amt'][i])

    # 자산정보 조회
    cur100 = conn.cursor()
    cur100.execute("""
        SELECT asset_num, cash_rate, tot_evlu_amt, prvs_rcdl_excc_amt
        FROM "stockFundMng_stock_fund_mng"
        WHERE acct_no = %s
    """, (str(acct_no),))
    result_one00 = cur100.fetchall()
    cur100.close()

    asset_num = 0 

    for i in result_one00:
        asset_num = i[0]

    # 자산정보 변경
    cur200 = conn.cursor()
    update_query200 = "update \"stockFundMng_stock_fund_mng\" set tot_evlu_amt = %s, dnca_tot_amt = %s, prvs_rcdl_excc_amt = %s, nass_amt = %s, scts_evlu_amt = %s, asset_icdc_amt = %s, last_chg_date = %s where asset_num = %s and acct_no = %s"
    # update 인자값 설정
    record_to_update200 = ([u_tot_evlu_amt, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_nass_amt, u_scts_evlu_amt, u_asst_icdc_amt, datetime.now(), asset_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur200.execute(update_query200, record_to_update200)
    conn.commit()
    cur200.close()

    # 시장레벨정보 조회
    cur300 = conn.cursor()
    cur300.execute("""
        SELECT asset_risk_num, market_level_num
        FROM "stockMarketMng_stock_market_mng"
        WHERE acct_no = %s AND aply_end_dt = '99991231'
    """, (str(acct_no),))
    result_one01 = cur300.fetchall()
    cur300.close()

    asset_risk_num = 0
    n_asset_sum = 0
    n_risk_rate = 0
    n_stock_num = 0

    for i in result_one01:

        asset_risk_num = i[0]
        # print("자산리스크번호 : " + str(asset_risk_num))   
        if i[1] == "1":   # 하락 지속 후, 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 30 * 0.01
            if n_asset_sum < 10000000:
                n_asset_sum = 10000000
                n_risk_rate = 2
                n_stock_num = 2
            elif n_asset_sum > 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 2
                n_stock_num = 4
            else:
                n_risk_rate = 1.8
                n_stock_num = 3
        elif i[1] == "2": # 단기 추세 전환 후, 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 30 * 0.01
            if n_asset_sum < 20000000:
                n_asset_sum = 20000000
                n_risk_rate = 3
                n_stock_num = 4
            elif n_asset_sum > 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 4
                n_stock_num = 6
            else:
                n_risk_rate = 3.5
                n_stock_num = 5
        elif i[1] == "3": # 패턴내에서 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 50 * 0.01
            if n_asset_sum < 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 4
                n_stock_num = 6
            elif n_asset_sum > 50000000:
                n_asset_sum = 50000000
                n_risk_rate = 4
                n_stock_num = 8
            else:
                n_risk_rate = 2.8
                n_stock_num = 5
        elif i[1] == "4": # 일봉상 추세 전환 후, 눌림구간에서 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 70 * 0.01
            if n_asset_sum < 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 5.5
                n_stock_num = 8
            elif n_asset_sum > 70000000:
                n_asset_sum = 70000000
                n_risk_rate = 3.5
                n_stock_num = 10
            else:
                n_risk_rate = 5
                n_stock_num = 10
        elif i[1] == "5": # 상승 지속 후, 패턴내에서 기술적 반등
            n_asset_sum = u_prvs_rcdl_excc_amt * 50 * 0.01
            if n_asset_sum < 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 4
                n_stock_num = 6
            elif n_asset_sum > 50000000:
                n_asset_sum = 50000000
                n_risk_rate = 4
                n_stock_num = 8
            else:
                n_risk_rate = 2.8
                n_stock_num = 5
        else:
            n_asset_sum = u_prvs_rcdl_excc_amt * 30 * 0.01
            if n_asset_sum < 10000000:
                n_asset_sum = 10000000
                n_risk_rate = 2
                n_stock_num = 2
            elif n_asset_sum > 30000000:
                n_asset_sum = 30000000
                n_risk_rate = 2
                n_stock_num = 4
            else:
                n_risk_rate = 1.8
                n_stock_num = 3

    n_risk_sum = n_asset_sum * n_risk_rate * 0.01

    # 시장레벨정보 변경
    cur400 = conn.cursor()
    update_query400 = "update \"stockMarketMng_stock_market_mng\" set total_asset = %s, risk_rate = %s, risk_sum = %s, item_number = %s where asset_risk_num = %s and acct_no = %s and aply_end_dt = '99991231'"
    # update 인자값 설정
    record_to_update400 = ([n_asset_sum, n_risk_rate, n_risk_sum, n_stock_num, asset_risk_num, acct_no])
    # DB 연결된 커서의 쿼리 수행
    cur400.execute(update_query400, record_to_update400)
    conn.commit()
    cur400.close()

def fundTrail_proc(acct_no, conn):
    # 관심종목 코스피, 코스닥 미존재시 생성
    cur100 = conn.cursor()
    insert_query100 = "with A as (select * from \"interestItem_interest_item\" where acct_no = %s and code = '0001') insert into \"interestItem_interest_item\"(acct_no, code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from A);"
    record_to_insert100 = ([acct_no, acct_no, '0001', '코스피', 0, 0, 0, 0, 0, 0, 0, datetime.now()])
    cur100.execute(insert_query100, record_to_insert100)
    conn.commit()
    cur100.close()

    cur200 = conn.cursor()
    insert_query200 = "with A as (select * from \"interestItem_interest_item\" where acct_no = %s and code = '1001') insert into \"interestItem_interest_item\"(acct_no, code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, buy_expect_sum, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from A);"
    record_to_insert200 = ([acct_no, acct_no, '1001', '코스닥', 0, 0, 0, 0, 0, 0, 0, datetime.now()])
    cur200.execute(insert_query200, record_to_insert200)
    conn.commit()
    cur200.close()

    # 추적신호 조회(코스피) : 현금비중·예정자금·시장승률 설정
    cur300 = conn.cursor()
    cur300.execute("""
        SELECT trail_signal_code, tot_evlu_amt, prvs_rcdl_excc_amt, asset_num
        FROM (SELECT row_number() OVER(PARTITION BY A.trail_signal_code ORDER BY A.trail_time DESC) AS num,
                    A.trail_signal_code, B.tot_evlu_amt, B.prvs_rcdl_excc_amt, B.asset_num
            FROM trail_signal_recent A, "stockFundMng_stock_fund_mng" B
            WHERE cast(A.acct_no AS INTEGER) = B.acct_no AND code = '0001' AND A.trail_day = prev_business_day_char(CURRENT_DATE) AND A.acct_no = %s) T
        WHERE num = 1
    """, (str(acct_no),))
    result_one100 = cur300.fetchall()
    cur300.close()

    for i in result_one100:
        trail_signal_result1 = i[0]
        tot_evlu_amt         = i[1]
        prvs_rcdl_excc_amt   = i[2]
        asset_num            = i[3]

        # ★ 루프마다 set_pairs 초기화 (이전 자산 데이터 오염 방지)
        set_pairs = [("last_chg_date", datetime.now())]

        kospi_ratio = ""
        cash_rate = 0
        cash_rate_amt = 0

        if trail_signal_result1 == '03':       # 저항가 돌파
            kospi_ratio = "MH"
            set_pairs.append(("kospi_mid", trail_signal_result1))
            cash_rate = 30
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)
        elif trail_signal_result1 == '04':     # 지지가 이탈
            kospi_ratio = "MD"
            set_pairs.append(("kospi_mid", trail_signal_result1))
            cash_rate = 70
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)
        elif trail_signal_result1 == '05':     # 추세상단가 돌파
            kospi_ratio = "LH"
            set_pairs.append(("kospi_long", trail_signal_result1))
            cash_rate = 10
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)
        elif trail_signal_result1 == '06':     # 추세하단가 이탈
            kospi_ratio = "LD"
            set_pairs.append(("kospi_long", trail_signal_result1))
            cash_rate = 90
            cash_rate_amt = round(tot_evlu_amt * cash_rate * 0.01, 0)
        elif trail_signal_result1 == '01':     # 돌파가 돌파
            kospi_ratio = "SH"
            set_pairs.append(("kospi_short", trail_signal_result1))
            remain_cash_rate = 30
            cash_rate_amt = round(prvs_rcdl_excc_amt * remain_cash_rate * 0.01, 0)
            cash_rate = 100 - (tot_evlu_amt / (tot_evlu_amt + prvs_rcdl_excc_amt - cash_rate_amt)) * 100
        elif trail_signal_result1 == '02':     # 이탈가 이탈
            kospi_ratio = "SD"
            set_pairs.append(("kospi_short", trail_signal_result1))
            remain_cash_rate = 70
            cash_rate_amt = round(prvs_rcdl_excc_amt * remain_cash_rate * 0.01, 0)
            cash_rate = 100 - (tot_evlu_amt / (tot_evlu_amt + prvs_rcdl_excc_amt - cash_rate_amt)) * 100

        sell_plan_amt = max(0, cash_rate_amt - prvs_rcdl_excc_amt)
        buy_plan_amt  = max(0, prvs_rcdl_excc_amt - cash_rate_amt)

        # ★ 재할당(=) 아닌 extend로 추가 (기존 kospi_* 컬럼 보존)
        set_pairs.extend([
            ("cash_rate",     cash_rate),
            ("cash_rate_amt", cash_rate_amt),
            ("sell_plan_amt", sell_plan_amt),
            ("buy_plan_amt",  buy_plan_amt),
        ])

        # ★ market_ratio 계산 (KOSPI 변경분 반영)
        state = fetch_market_state(conn, asset_num, acct_no)
        for col, val in set_pairs:
            if col in state:
                state[col] = val
        market_ratio = compute_market_ratio(
            state['kospi_short'],  state['kospi_mid'],  state['kospi_long'],
            state['kosdak_short'], state['kosdak_mid'], state['kosdak_long'],
        )
        set_pairs.append(("market_ratio", market_ratio))

        # 자산정보 변경
        cur400 = conn.cursor()
        set_clause = ", ".join(f'"{col}" = %s' for col, _ in set_pairs)
        update_query100 = (
            f'update "stockFundMng_stock_fund_mng" set {set_clause} '
            f'where asset_num = %s and acct_no = %s'
        )
        params = [v for _, v in set_pairs] + [asset_num, acct_no]
        cur400.execute(update_query100, params)
        conn.commit()
        cur400.close()

    # 추적신호 조회(코스닥) : 시장 흐름 + 시장 승률 갱신
    cur500 = conn.cursor()
    cur500.execute("""
        SELECT trail_signal_code, asset_num
        FROM (SELECT row_number() OVER(PARTITION BY A.trail_signal_code ORDER BY A.trail_time DESC) AS num,
                    A.trail_signal_code, B.asset_num
            FROM trail_signal_recent A, "stockFundMng_stock_fund_mng" B
            WHERE cast(A.acct_no AS INTEGER) = B.acct_no AND code = '1001' AND A.trail_day = prev_business_day_char(CURRENT_DATE) AND A.acct_no = %s) T
        WHERE num = 1
    """, (str(acct_no),))
    result_one200 = cur500.fetchall()
    cur500.close()

    for i in result_one200:
        trail_signal_result2 = i[0]
        asset_num            = i[1]

        # ★ 루프마다 set_pairs 초기화
        set_pairs = [("last_chg_date", datetime.now())]

        kosdak_ratio = ""

        if trail_signal_result2 == '03':
            kosdak_ratio = "MH"
            set_pairs.append(("kosdak_mid", trail_signal_result2))
        elif trail_signal_result2 == '04':
            kosdak_ratio = "MD"
            set_pairs.append(("kosdak_mid", trail_signal_result2))
        elif trail_signal_result2 == '05':
            kosdak_ratio = "LH"
            set_pairs.append(("kosdak_long", trail_signal_result2))
        elif trail_signal_result2 == '06':
            kosdak_ratio = "LD"
            set_pairs.append(("kosdak_long", trail_signal_result2))
        elif trail_signal_result2 == '01':
            kosdak_ratio = "SH"
            set_pairs.append(("kosdak_short", trail_signal_result2))
        elif trail_signal_result2 == '02':
            kosdak_ratio = "SD"
            set_pairs.append(("kosdak_short", trail_signal_result2))

        # ★ market_ratio 계산 (KOSDAQ 변경분 반영; KOSPI는 위에서 이미 갱신됨)
        state = fetch_market_state(conn, asset_num, acct_no)
        for col, val in set_pairs:
            if col in state:
                state[col] = val
        market_ratio = compute_market_ratio(
            state['kospi_short'],  state['kospi_mid'],  state['kospi_long'],
            state['kosdak_short'], state['kosdak_mid'], state['kosdak_long'],
        )
        set_pairs.append(("market_ratio", market_ratio))

        # 자산정보 변경
        cur600 = conn.cursor()
        set_clause = ", ".join(f'"{col}" = %s' for col, _ in set_pairs)
        update_query200 = (
            f'update "stockFundMng_stock_fund_mng" set {set_clause} '
            f'where asset_num = %s and acct_no = %s'
        )
        params = [v for _, v in set_pairs] + [asset_num, acct_no]
        cur600.execute(update_query200, params)
        conn.commit()
        cur600.close()
        
        # 자산번호와 시장승률이 다를 경우 자산정보 변경 처리
        if str(asset_num)[0:2] != str(market_ratio):
            
            # 자산번호 생성
            new_asset_num = str(market_ratio) + today
            # print("신규 자산번호 : " + new_asset_num)

            if int(new_asset_num) != asset_num:

                # 자산정보 이력 생성
                cur601 = conn.cursor()
                insert_query001 = "insert into stockFundMngHist(asset_num, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, last_chg_date, market_ratio, kospi_short, kospi_mid, kospi_long, kosdak_short, kosdak_mid, kosdak_long) select asset_num, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, now(), market_ratio, kospi_short, kospi_mid, kospi_long, kosdak_short, kosdak_mid, kosdak_long from \"stockFundMng_stock_fund_mng\" where acct_no = %s and asset_num = %s"
                # insert 인자값 설정
                record_to_insert001 = ([acct_no, asset_num])
                # DB 연결된 커서의 쿼리 수행
                cur601.execute(insert_query001, record_to_insert001)
                conn.commit()
                cur601.close()

                # 자산정보 생성
                cur602 = conn.cursor()
                insert_query002 = "insert into \"stockFundMng_stock_fund_mng\"(asset_num, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, last_chg_date, market_ratio, kospi_short, kospi_mid, kospi_long, kosdak_short, kosdak_mid, kosdak_long) select %s, acct_no, cash_rate, tot_evlu_amt, cash_rate_amt, dnca_tot_amt, prvs_rcdl_excc_amt, nass_amt, scts_evlu_amt, asset_icdc_amt, sell_plan_amt, buy_plan_amt, now(), market_ratio, kospi_short, kospi_mid, kospi_long, kosdak_short, kosdak_mid, kosdak_long from \"stockFundMng_stock_fund_mng\" where acct_no = %s and asset_num = %s"
                # insert 인자값 설정
                record_to_insert002 = ([int(new_asset_num), acct_no, asset_num])
                # DB 연결된 커서의 쿼리 수행
                cur602.execute(insert_query002, record_to_insert002)
                conn.commit()
                cur602.close()

                # 자산정보 삭제
                cur603 = conn.cursor()
                delete_query001 = "delete from \"stockFundMng_stock_fund_mng\" where acct_no = %s and asset_num = %s"
                # insert 인자값 설정
                record_to_delete001 = ([acct_no, asset_num])
                # DB 연결된 커서의 쿼리 수행
                cur603.execute(delete_query001, record_to_delete001)
                conn.commit()
                cur603.close()

def process_account(nick):
    conn_acct = db.connect(conn_string)
    try:
        cur_time = datetime.now().strftime("%H%M")
        ac = account(nick, conn_acct)
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']
        token = ac['bot_token2']
        chat_id = ac['chat_id']

        updater = Updater(token=token, use_context=True)
        bot = updater.bot

        # 자산정보 및 시장레벨정보 처리
        # fund_marketLevel_proc(access_token, app_key, app_secret, acct_no)

        # 관심정보 및 종목손실금액 조회
        cur03 = conn_acct.cursor()
        cur03.execute("""
            SELECT code, name, through_price, leave_price, resist_price, support_price,
                   trend_high_price, trend_low_price,
                   ROUND(B.buy_avail_cash * C.risk_rate * 0.01 / C.item_number, 0) AS item_loss_sum
            FROM "interestItem_interest_item" A,
                 (SELECT row_number() OVER (ORDER BY id DESC) AS ROWNUM, prvs_rcdl_excc_amt, acct_no,
                         CASE WHEN cash_rate > COALESCE(market_ratio, 0)
                              THEN ROUND(cash_rate * prvs_rcdl_excc_amt * 0.01, 0)
                              ELSE ROUND(COALESCE(market_ratio, 0) * prvs_rcdl_excc_amt * 0.01, 0)
                         END AS buy_avail_cash
                  FROM "stockFundMng_stock_fund_mng" WHERE acct_no = %s) B,
                 (SELECT acct_no, risk_rate, item_number FROM "stockMarketMng_stock_market_mng"
                  WHERE acct_no = %s AND aply_end_dt = '99991231') C
            WHERE A.acct_no = B.acct_no AND A.acct_no = C.acct_no AND A.acct_no = %s AND B.rownum = 1 AND A.interest_day = TO_CHAR(now(), 'YYYYMMDD') AND A.proc_yn = 'Y'
        """, (str(acct_no), str(acct_no), str(acct_no)))
        result_three = cur03.fetchall()
        cur03.close()

        # 관심종목 이탈가, 돌파가, 지지가, 저항가, 추세하단가, 추세상단가를 각각 실시간 종목시세의 최고가와 최저가 비교
        for i in result_three:
            # print("종목명 : " + i[1])

            trail_signal_code = ""
            trail_signal_name = ""
            a = ""
            b = ""

            if len(i[0]) == 6:

                try:
                    time.sleep(0.3)  # 초당 3건 이하로 제한
                    a = inquire_price(access_token, app_key, app_secret, i[0])
                except Exception as ex:
                    print(f"현재가 시세 에러 : [{i[0]}] {ex}")
                if not a:
                    continue

                signals = []

                if int(a['stck_hgpr']) > i[2]:
                    ba = round(2000000 / int(a['stck_prpr']))
                    signals.append({'code': '01',
                                    'name': format(int(i[2]), ',d') + "원 {돌파가 돌파}",
                                    'buy_amount': ba,
                                    'buy_sum': int(a['stck_prpr']) * ba,
                                    'loss_price': int(i[3]),
                                    'item_loss_sum': i[8]})
                if int(a['stck_lwpr']) < i[3]:
                    signals.append({'code': '02',
                                    'name': format(int(i[3]), ',d') + "원 {이탈가 이탈}",
                                    'buy_amount': 0, 'buy_sum': 0,
                                    'loss_price': 0, 'item_loss_sum': 0})
                if int(a['stck_hgpr']) > i[4]:
                    ba = round(2000000 / int(a['stck_prpr']))
                    signals.append({'code': '03',
                                    'name': format(int(i[4]), ',d') + "원 {저항가 돌파}",
                                    'buy_amount': ba,
                                    'buy_sum': int(a['stck_prpr']) * ba,
                                    'loss_price': int(i[3]),
                                    'item_loss_sum': i[8]})
                if int(a['stck_lwpr']) < i[5]:
                    signals.append({'code': '04',
                                    'name': format(int(i[5]), ',d') + "원 {지지가 이탈}",
                                    'buy_amount': 0, 'buy_sum': 0,
                                    'loss_price': 0, 'item_loss_sum': 0})
                if int(a['stck_hgpr']) > i[6]:
                    ba = round(2000000 / int(a['stck_prpr']))
                    signals.append({'code': '05',
                                    'name': format(int(i[6]), ',d') + "원 {추세상단가 돌파}",
                                    'buy_amount': ba,
                                    'buy_sum': int(a['stck_prpr']) * ba,
                                    'loss_price': int(i[3]),
                                    'item_loss_sum': i[8]})
                if int(a['stck_lwpr']) < i[7]:
                    signals.append({'code': '06',
                                    'name': format(int(i[7]), ',d') + "원 {추세하단가 이탈}",
                                    'buy_amount': 0, 'buy_sum': 0,
                                    'loss_price': 0, 'item_loss_sum': 0})

                breakout_signals  = [s for s in signals if s['code'] in ('01', '03', '05')]
                breakdown_signals = [s for s in signals if s['code'] in ('02', '04', '06')]
                if len(breakout_signals) >= 2:
                    print(f"[다중돌파] {i[1]}: " + ", ".join(s['name'] for s in breakout_signals))
                if len(breakdown_signals) >= 2:
                    print(f"[다중이탈] {i[1]}: " + ", ".join(s['name'] for s in breakdown_signals))

                for sig in signals:
                    trail_signal_code = sig['code']
                    trail_signal_name = sig['name']
                    n_buy_amount      = sig['buy_amount']
                    n_buy_sum         = sig['buy_sum']
                    loss_price        = sig['loss_price']
                    item_loss_sum     = sig['item_loss_sum']

                    # 오늘 동일 신호코드가 이미 기록된 경우 스킵
                    cur04 = conn_acct.cursor()
                    cur04.execute("""
                        SELECT TS.trail_signal_code FROM trail_signal TS
                        WHERE TS.acct = %s AND TS.code = %s AND TS.trail_day = TO_CHAR(now(), 'YYYYMMDD')
                        AND TS.trail_signal_code = %s
                    """, (str(acct_no), i[0], trail_signal_code))
                    result_four = cur04.fetchall()
                    cur04.close()

                    if len(result_four) > 0:
                        continue

                    print("종목명 : " + i[1] + " 추적신호 : " + trail_signal_name)

                    cur041 = conn_acct.cursor()
                    cur041.execute("""
                        SELECT CASE WHEN market_ratio = 0 THEN 100 - cash_rate ELSE market_ratio END AS market_ratio
                        FROM (SELECT row_number() OVER (ORDER BY id DESC) AS ROWNUM,
                                        cash_rate, COALESCE(market_ratio, 0) AS market_ratio
                                FROM "stockFundMng_stock_fund_mng" WHERE acct_no = %s) A
                        WHERE A.ROWNUM = 1
                    """, (str(acct_no),))
                    result_fourone = cur041.fetchall()
                    cur041.close()

                    for k in result_fourone:
                        if k[0] >= 50:
                            if n_buy_amount > 0:
                                telegram_text = (f"[시장상승]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원")
                            else:
                                telegram_text = "[시장상승]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                        else:
                            if n_buy_amount > 0:
                                telegram_text = (f"[시장하락]{i[1]}[<code>{i[0]}</code>] : {trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(loss_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원")
                            else:
                                telegram_text = "[시장하락]" + i[1] + "[<code>" + i[0] + "</code>] : " + trail_signal_name + ", 고가 : " + format(int(a['stck_hgpr']), ',d') + "원, 저가 : " + format(int(a['stck_lwpr']), ',d') + "원, 거래량 : " + format(int(a['acml_vol']), ',d') + "주, 거래대비 : " + a['prdy_vrss_vol_rate'] + ", 현재가 : " + format(int(a['stck_prpr']), ',d') + "원"
                        try:
                            send_markup = InlineKeyboardMarkup([[InlineKeyboardButton(
                                "매수주문등록",
                                callback_data=f"menu,interest_trail_buy_{i[0]}_{int(a['stck_prpr'])}_{int(i[3])}_{int(n_buy_sum)}_{int(item_loss_sum or 0)}"
                            )]]) if n_buy_amount > 0 else None
                            bot.send_message(chat_id=chat_id, text=telegram_text, parse_mode='HTML', reply_markup=send_markup)
                        except Exception as te:
                            print(f"텔레그램 전송 오류: {te}")

                        cur20 = conn_acct.cursor()
                        insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, buy_plan_qty = %s, buy_plan_amt = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                        record_to_insert0 = ([cur_time, i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], int(round(n_buy_amount)), int(n_buy_sum), datetime.now(), str(acct_no), today, i[0], trail_signal_code, str(acct_no), today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                        cur20.execute(insert_query0, record_to_insert0)

                        cur2 = conn_acct.cursor()
                        insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, volumn_rate, cdate, buy_plan_qty, buy_plan_amt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        record_to_insert = ([acct_no, today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], int(a['stck_prpr']), int(a['stck_hgpr']), int(a['stck_lwpr']), int(a['acml_vol']), a['prdy_vrss_vol_rate'], datetime.now(), int(round(n_buy_amount)), int(n_buy_sum)])
                        cur2.execute(insert_query, record_to_insert)
                        conn_acct.commit()
                        cur20.close()
                        cur2.close()

            elif len(i[0]) == 4:

                oldest_time = datetime.now().strftime("%H%M%S")
                if today.endswith("0102") or today.endswith("1119"):
                    if oldest_time <= "100000":
                        continue
                else:
                    if oldest_time <= "090000":
                        continue

                b = inquire_daily_indexchartprice(access_token, app_key, app_secret, i[0], today)
                # print("현재포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_prpr']), ',f'))  # 현재포인트
                # print("최고포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_hgpr']), ',f'))  # 최고포인트
                # print("최저포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_lwpr']), ',f'))  # 최저포인트
                # print("누적거래량 : " + format(int(b['acml_vol']), ',d'))  # 누적거래량

                # 시장레벨정보 조회
                cur05 = conn_acct.cursor()
                cur05.execute("""
                    SELECT asset_risk_num, market_level_num FROM "stockMarketMng_stock_market_mng"
                    WHERE acct_no = %s AND aply_end_dt = '99991231'
                """, (str(acct_no),))
                result_five = cur05.fetchall()
                cur05.close()

                signals = []
                cur_prpr = math.ceil(float(b['bstp_nmix_prpr']))

                if cur_prpr > i[2]:
                    mln, rr, inum = "", 0, 0
                    if i[0] == "0001" and len(result_five) > 0:
                        for k in result_five:
                            if int(k[1]) < 2:
                                mln, rr, inum = "2", 3, 4
                    signals.append({'code': '01', 'name': format(int(i[2]), ',d') + " {돌파포인트 돌파}",
                                    'market_level_num': mln, 'risk_rate': rr, 'item_number': inum})

                if cur_prpr < i[3]:
                    mln, rr, inum = "", 0, 0
                    if i[0] == "0001" and len(result_five) > 0:
                        for k in result_five:
                            if int(k[1]) < 3:
                                mln, rr, inum = "1", 2, 2
                    signals.append({'code': '02', 'name': format(int(i[3]), ',d') + " {이탈포인트 이탈}",
                                    'market_level_num': mln, 'risk_rate': rr, 'item_number': inum})

                if cur_prpr > i[4]:
                    mln, rr, inum = "", 0, 0
                    if i[0] == "0001" and len(result_five) > 0:
                        for k in result_five:
                            if int(k[1]) < 4:
                                mln, rr, inum = "4", 5.5, 8
                    signals.append({'code': '03', 'name': format(int(i[4]), ',d') + " {저항포인트 돌파}",
                                    'market_level_num': mln, 'risk_rate': rr, 'item_number': inum})

                if cur_prpr < i[5]:
                    mln, rr, inum = "", 0, 0
                    if i[0] == "0001" and len(result_five) > 0:
                        for k in result_five:
                            if int(k[1]) < 5:
                                mln, rr, inum = "1", 2, 2
                    signals.append({'code': '04', 'name': format(int(i[5]), ',d') + " {지지포인트 이탈}",
                                    'market_level_num': mln, 'risk_rate': rr, 'item_number': inum})

                if cur_prpr > i[6]:
                    mln, rr, inum = "", 0, 0
                    if i[0] == "0001" and len(result_five) > 0:
                        mln, rr, inum = "5", 4, 6
                    signals.append({'code': '05', 'name': format(int(i[6]), ',d') + " {추세상단포인트 돌파}",
                                    'market_level_num': mln, 'risk_rate': rr, 'item_number': inum})

                if cur_prpr < i[7]:
                    mln, rr, inum = "", 0, 0
                    if i[0] == "0001" and len(result_five) > 0:
                        mln, rr, inum = "1", 2, 2
                    signals.append({'code': '06', 'name': format(int(i[7]), ',d') + " {추세하단포인트 이탈}",
                                    'market_level_num': mln, 'risk_rate': rr, 'item_number': inum})

                breakout_signals  = [s for s in signals if s['code'] in ('01', '03', '05')]
                breakdown_signals = [s for s in signals if s['code'] in ('02', '04', '06')]
                if len(breakout_signals) >= 2:
                    print(f"[다중돌파포인트] {i[1]}: " + ", ".join(s['name'] for s in breakout_signals))
                if len(breakdown_signals) >= 2:
                    print(f"[다중이탈포인트] {i[1]}: " + ", ".join(s['name'] for s in breakdown_signals))

                for sig in signals:
                    trail_signal_code = sig['code']
                    trail_signal_name = sig['name']
                    market_level_num  = sig['market_level_num']
                    risk_rate         = sig['risk_rate']
                    item_number       = sig['item_number']

                    # 오늘 동일 신호코드가 이미 기록된 경우 스킵
                    cur04 = conn_acct.cursor()
                    cur04.execute("""
                        SELECT TS.trail_signal_code FROM trail_signal TS
                        WHERE TS.acct = %s AND TS.code = %s AND TS.trail_day = TO_CHAR(now(), 'YYYYMMDD')
                        AND TS.trail_signal_code = %s
                    """, (str(acct_no), i[0], trail_signal_code))
                    result_four = cur04.fetchall()
                    cur04.close()

                    if len(result_four) > 0:
                        continue

                    print("시장 : " + i[1] + " 추적신호 : " + trail_signal_name)
                    telegram_text = i[1] + "[" + i[0] + "] : " + trail_signal_name + ", 최고포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_hgpr']), ',f') + ", 최저포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_lwpr']), ',f') + ", 현재포인트 : " + '{:0,.2f}'.format(float(b['bstp_nmix_prpr']), ',f') + ", 거래량 : " + format(int(b['acml_vol']), ',d') + "주"
                    try:
                        bot.send_message(chat_id=chat_id, text=telegram_text, parse_mode='HTML')
                    except Exception as te:
                        print(f"텔레그램 전송 오류: {te}")

                    cur20 = conn_acct.cursor()
                    insert_query0 = "with upsert as (update trail_signal set trail_time = %s, name = %s, current_price = %s, high_price = %s, low_price = %s, volumn = %s, cdate = %s where acct = %s and trail_day = %s and code = %s and trail_signal_code = %s returning * ) insert into trail_signal(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
                    record_to_insert0 = ([cur_time, i[1], cur_prpr, math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now(), str(acct_no), today, trail_signal_code, i[0], str(acct_no), today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], cur_prpr, math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now()])
                    cur20.execute(insert_query0, record_to_insert0)

                    cur2 = conn_acct.cursor()
                    insert_query = "insert into trail_signal_hist(acct, trail_day, trail_time, trail_signal_code, trail_signal_name, code, name, current_price, high_price, low_price, volumn, cdate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    record_to_insert = ([acct_no, today, cur_time, trail_signal_code, trail_signal_name, i[0], i[1], cur_prpr, math.ceil(float(b['bstp_nmix_hgpr'])), math.floor(float(b['bstp_nmix_lwpr'])), int(b['acml_vol']), datetime.now()])
                    cur2.execute(insert_query, record_to_insert)
                    conn_acct.commit()
                    cur20.close()
                    cur2.close()

                    fundTrail_proc(acct_no, conn_acct)

        time.sleep(0.3)

    except Exception as e:
        print(f"[{nick}] Error interest item : {e}")
    finally:
        conn_acct.close()

if __name__ == "__main__":
    _conn_check = db.connect(conn_string)
    try:
        _cur0 = _conn_check.cursor()
        _cur0.execute("SELECT name FROM stock_holiday WHERE holiday = %s", (today,))
        _holiday = _cur0.fetchone()
        _cur0.close()
    finally:
        _conn_check.close()

    nickname_list = ['chichipa', 'phills2', 'phills75', 'yh480825', 'phills13', 'phills15', 'mamalong', 'honeylong', 'worry106']

    if _holiday is None:
        with ThreadPoolExecutor(max_workers=len(nickname_list)) as executor:
            futures = {executor.submit(process_account, nick): nick for nick in nickname_list}
            for future in as_completed(futures):
                nick = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[{nick}] 계좌 최종 오류: {e}")
    else:
        print("Today is Holiday")
