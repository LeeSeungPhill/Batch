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

def update_trading_daily_close(trail_price, trail_qty, trail_amt, trail_rate, trail_plan, basic_qty, basic_amt, acct_no, code, trail_day, trail_dtm, trail_tp, proc_min):
    
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

def update_trading_close(trail_price, trail_qty, trail_amt, trail_rate, trail_plan, basic_qty, basic_amt, acct_no, code, trail_day, trail_dtm, trail_tp, proc_min):
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

def update_trading_trail(stop_price, target_price, acct_no, code, trail_day, trail_dtm, trail_tp, proc_min):
    cur04 = conn.cursor()
    cur04.execute("""
        UPDATE public.trading_trail SET 
            stop_price = %s      
            , target_price = %s
            , trail_tp = %s
            , proc_min = %s
            , mod_dt = %s
        WHERE acct_no = %s
        AND code = %s
        AND trail_day = %s
        AND trail_dtm = %s
        AND trail_tp <> 'L'
    """, (stop_price, target_price, trail_tp, proc_min, datetime.now(), acct_no, code, trail_day, trail_dtm))
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
    current = start_dt.date()
    signals = []

    tenmin_state = {
        "active": False,          # 목표가 돌파 후 활성화
        "base_key": None,
        "base_low": None,         # 기준봉 저가
        "base_high": None,        # 기준봉 고가
        "base_vol": None,         # 기준봉 거래량
        "base_end_dt": None,      # 기준봉 종료시각 (dt)
    }

    trade_date = current.strftime("%Y%m%d")

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
                # ===============================
                # 09:10 이전 미처리
                # ===============================
                if row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                    continue

                # ===============================
                # 시가 갭 하락 → 기준봉 무효화
                # ===============================
                if (row["시간"] == "09:00" and int(row["시가"]) < stop_price):
                    if verbose:
                        print(
                            f"🚫 [{row['일자']} 09:00] "
                            f"시가 {int(row['시가']):,} < 기준봉 저가 {stop_price:,} "
                            f"→ 기준봉 무효화"
                        )

                    tenmin_state.update({
                        "active": False,
                        "base_key": None,
                        "base_low": None,
                        "base_high": None,
                        "base_vol": None,
                        "base_end_dt": None,
                    })
                    continue

                # ===============================
                # 시가 갭 상승 → 기준봉 무효화
                # ===============================
                if (row["시간"] == "09:00" and int(row["시가"]) > target_price):
                    if verbose:
                        print(
                            f"🚫 [{row['일자']} 09:00] "
                            f"시가 {int(row['시가']):,} > 기준봉 고가 {target_price:,} "
                            f"→ 기준봉 무효화"
                        )

                    tenmin_state.update({
                        "active": False,
                        "base_key": None,
                        "base_low": None,
                        "base_high": None,
                        "base_vol": None,
                        "base_end_dt": None,
                    })
                    continue

                high_price = int(row["고가"])
                low_price = int(row["저가"])
                close_price = int(row["종가"])

                breakout_check = high_price if breakout_type == "high" else close_price
                breakdown_check = low_price if breakdown_type == "low" else close_price

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

                        update_trading_daily_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, stock_code, start_date, start_time, "4", row['시간'].replace(':', '')+'00')

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

        df = get_kis_1min_full_day(
            stock_code=stock_code,
            trade_date=trade_date,
            start_time="153000",
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            verbose=False
        )

        breakout_done = False

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

                # ===============================
                # 09:10 이전 미처리
                # ===============================
                if row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                    continue

                # ===============================
                # 시가 갭 하락 → 기준봉 무효화
                # ===============================
                if (row["시간"] == "09:00" and int(row["시가"]) < stop_price):
                    if verbose:
                        print(
                            f"🚫 [{row['일자']} 09:00] "
                            f"시가 {int(row['시가']):,} < 기준봉 저가 {stop_price:,} "
                            f"→ 기준봉 무효화"
                        )

                    tenmin_state.update({
                        "active": False,
                        "base_key": None,
                        "base_low": None,
                        "base_high": None,
                        "base_vol": None,
                        "base_end_dt": None,
                    })
                    breakout_done = False
                    continue

                # ===============================
                # 시가 갭 상승 → 기준봉 무효화
                # ===============================
                if (row["시간"] == "09:00" and int(row["시가"]) > target_price):
                    if verbose:
                        print(
                            f"🚫 [{row['일자']} 09:00] "
                            f"시가 {int(row['시가']):,} > 기준봉 고가 {target_price:,} "
                            f"→ 기준봉 무효화"
                        )

                    tenmin_state.update({
                        "active": False,
                        "base_key": None,
                        "base_low": None,
                        "base_high": None,
                        "base_vol": None,
                        "base_end_dt": None,
                    })
                    breakout_done = False
                    continue

                high_price = int(row["고가"])
                low_price = int(row["저가"])
                close_price = int(row["종가"])

                breakout_check = high_price if breakout_type == "high" else close_price
                breakdown_check = low_price if breakdown_type == "low" else close_price

                if high_price > low_price:
                    if not breakout_done:
                        # 돌파 이전 이탈 → 즉시 종료
                        if breakdown_check <= stop_price:
                            if trail_tp == '1' or (trail_tp == '2' and trail_plan is not None):
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

                                update_trading_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, stock_code, start_date, start_time, "4", row['시간'].replace(':', '')+'00')

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
                            breakout_done = True

                            base_key = get_completed_10min_key(row["dt"])
                            base_10min = df[df["dt"].apply(get_10min_key) == base_key]

                            if base_10min.empty:
                                continue

                            tenmin_state.update({
                                "active": True,
                                "base_key": base_key,
                                "base_low": base_10min["저가"].astype(int).min(),
                                "base_high": base_10min["고가"].astype(int).max(),
                                "base_vol": base_10min["거래량"].astype(int).sum(),
                                "base_end_dt": base_key + timedelta(minutes=10),
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
                            update_trading_trail(int(tenmin_state['base_low']), int(tenmin_state['base_high']), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00')    

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
                    # 돌파 이후
                    # ===============================
                    if breakout_done and tenmin_state["active"]:
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
                                update_safe_trading_mng("Y", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                update_trading_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, stock_code, start_date, start_time, "4", row['시간'].replace(':', '')+'00')
                            else:    
                                update_safe_trading_mng("L", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                update_trading_close(close_price, trail_qty, trail_amt, trail_rate, i_trail_plan, u_basic_qty, u_basic_amt, acct_no, stock_code, start_date, start_time, "3", row['시간'].replace(':', '')+'00')

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
                        completed_key = get_completed_10min_key(row["dt"])
                        tenmin_df = df[df["dt"].apply(get_completed_10min_key) == completed_key]

                        if not tenmin_df.empty and row["dt"] == tenmin_df["dt"].max():
                            new_high = tenmin_df["고가"].astype(int).max()
                            new_low = tenmin_df["저가"].astype(int).min()
                            new_vol = tenmin_df["거래량"].astype(int).sum()

                            if new_high > new_low:
                                if new_high > tenmin_state["base_high"] or new_vol > tenmin_state["base_vol"]:
                                    tenmin_state.update({
                                        "base_key": completed_key,
                                        "base_low": new_low,
                                        "base_high": new_high,
                                        "base_vol": new_vol,
                                        "base_end_dt": completed_key
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
                                    update_safe_trading_mng("C", acct_no, stock_code, "1", start_date, row['일자']+row['시간'].replace(':', ''))
                                    update_trading_trail(int(new_low), int(new_high), acct_no, stock_code, start_date, start_time, "2", row['시간'].replace(':', '')+'00')    

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
        cur200.execute("select code, name, trail_day, trail_dtm, target_price, stop_price, basic_price, COALESCE(basic_qty, 0), CASE WHEN trail_tp = 'L' THEN 'L' ELSE trail_tp END, trail_plan, proc_min from public.trading_trail where acct_no = '" + str(acct_no) + "' and trail_tp in ('1', '2', 'L') and trail_day = '" + today + "' and to_char(to_timestamp(proc_min, 'HH24MISS') + interval '1 minutes', 'HH24MISS') <= to_char(now(), 'HH24MISS') order by code, proc_min, mod_dt")
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
                    
