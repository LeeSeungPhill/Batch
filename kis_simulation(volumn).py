from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import requests
import pandas as pd
import psycopg2 as db
import json

BASE_URL = "https://openapi.koreainvestment.com:9443"

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

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
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1
        FROM "stockAccount_stock_account"
        WHERE nick_name = %s
    """, (nickname,))
    result_two = cur01.fetchone()
    cur01.close()

    acct_no, access_token, app_key, app_secret, token_publ_date, token_day, bot_token1 = result_two
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
        'bot_token1': bot_token1
    }

def get_prev_day_low(stock_code, trade_date, access_token, app_key, app_secret):
    prev_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")

    df = get_kis_1min_full_day(
        stock_code=stock_code,
        trade_date=prev_date,
        start_time="153000",
        access_token=access_token,
        app_key=app_key,
        app_secret=app_secret,
        verbose=False
    )

    if df.empty:
        return None

    return int(df["저가"].astype(int).min())

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
    verbose: bool = True      # 🔥 출력 제어 옵션
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

def get_kis_1min_full_day(
    stock_code,
    trade_date,
    start_time,
    access_token,
    app_key,
    app_secret,
    is_last_business_day=False, 
    verbose=False
):
    all_df = []
    current_time = start_time
    prev_oldest_time = None  # ✅ 추가

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

        # 최근 영업일이면 한 번만 조회하고 종료
        if is_last_business_day:
            all_df.append(df)
            break

        # 시간 오름차순 보장
        df = df.sort_values("시간")

        oldest_time = df.iloc[0]["시간"].replace(":", "")

        # ✅ 핵심 1: 이전과 동일하면 탈출 (무한루프 방지)
        if oldest_time == prev_oldest_time:
            if verbose:
                print(f"⚠️ 더 이상 과거 분봉 없음 ({oldest_time})")
            break

        prev_oldest_time = oldest_time
        all_df.append(df)

        # ✅ 핵심 2: 장 시작 도달 시 종료
        if trade_date.endswith("0102"):
            if oldest_time <= "100000":
                break
        else:
            if oldest_time <= "090000":
                break

        # 다음 조회는 1분 이전
        dt = datetime.strptime(trade_date + oldest_time, "%Y%m%d%H%M")
        dt -= timedelta(minutes=1)
        current_time = dt.strftime("%H%M%S")

        # ✅ 핵심 3: 120건 미만이면 종료
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

def get_10min_key(dt: datetime):
    return dt.replace(minute=(dt.minute // 10) * 10, second=0)

def get_completed_10min_key(dt: datetime):
    """
    현재 dt 시점에서 '이미 완성된' 가장 최근 10분봉 시작 시각
    """
    base_minute = (dt.minute // 10) * 10
    key = dt.replace(minute=base_minute, second=0)

    # 현재 10분봉이 아직 끝나지 않았다면 이전 10분봉으로 이동
    if dt < key + timedelta(minutes=10):
        key -= timedelta(minutes=10)

    return key

def get_previous_business_day():
    # 매매시뮬레이션 조회
    cur100 = conn.cursor()
    cur100.execute("select get_previous_business_day(CURRENT_DATE)")
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

def get_kis_1min_from_datetime(
    stock_code: str,
    stock_name: str,
    start_date: str,
    start_time: str,
    target_price: int,
    stop_price: int, 
    access_token: str,
    app_key: str,
    app_secret: str,
    breakout_type: str = "high",        # high / close
    breakdown_type: str = "low",        # low / close
    verbose: bool = True
):
    start_dt = datetime.strptime(start_date + start_time, "%Y%m%d%H%M%S")
    current = start_dt.date()
    signals = []
    process_done = False
    breakout_done = False

    tenmin_state = {
        "active": False,          # 목표가 돌파 후 활성화
        "base_key": None,
        "base_low": None,         # 기준봉 저가
        "base_high": None,        # 기준봉 고가
        "base_vol": None,         # 기준봉 거래량
        "base_end_dt": None,      # 기준봉 종료시각 (dt)
        "skip_until_0910": False
    }

    last_business_day = get_previous_business_day()

    while current <= last_business_day  and not process_done:        
        trade_date = current.strftime("%Y%m%d")

        if not is_business_day(current):
            if verbose:
                print(f"⏭️ [{stock_name}-{stock_code}] {trade_date} 비영업일 → 건너뜀")
            current += timedelta(days=1)
            continue

        if verbose:
            print(f"[{stock_name}-{stock_code}] {trade_date} 1분봉 생성 중")

        prev_low = get_prev_day_low(
            stock_code,
            trade_date,
            access_token,
            app_key,
            app_secret
        )

        today_daily_close = None  # 금일 종가 갱신용

        df = get_kis_1min_full_day(
            stock_code=stock_code,
            trade_date=trade_date,
            start_time="153000",
            access_token=access_token,
            app_key=app_key,
            app_secret=app_secret,
            is_last_business_day=(current == last_business_day),
            verbose=False
        )

        if df.empty:
            current += timedelta(days=1)
            continue

        df = df[df["일자"] == trade_date]
        
        # 첫날은 입력 시간 이후만 허용
        if trade_date == start_date:
            df = df[df["dt"] > start_dt]

        # 🔥 최근 영업일은 15:30까지만
        if trade_date == last_business_day.strftime("%Y%m%d"):
            df = df[df["dt"] <= datetime.combine(
                last_business_day,
                datetime.strptime("15:30", "%H:%M").time()
            )]    

        for _, row in df.iterrows():
            
            # ===============================
            # 장 시작 특례 (09:10 이전 기준봉 비활성)
            # ===============================
            if tenmin_state["active"] and tenmin_state["skip_until_0910"]:
                if row["dt"].time() < datetime.strptime("09:10", "%H:%M").time():
                    continue
                else:
                    tenmin_state["skip_until_0910"] = False

            # ===============================
            # 시가 갭 하락 → 기준봉 무효화
            # ===============================
            if (
                tenmin_state["active"]
                and row["시간"] == "09:00"
                and int(row["시가"]) < tenmin_state["base_low"]
            ):
                if verbose:
                    print(
                        f"🚫 [{row['일자']} 09:00] "
                        f"시가 {int(row['시가']):,} < 기준봉 저가 {tenmin_state['base_low']:,} "
                        f"→ 기준봉 무효화"
                    )

                tenmin_state.update({
                    "active": False,
                    "base_key": None,
                    "base_low": None,
                    "base_high": None,
                    "base_vol": None,
                    "base_end_dt": None,
                    "skip_until_0910": False
                })

                # ❗ 기준봉이 무효화되었으므로 다음 돌파를 다시 기다림
                breakout_done = False
                continue

            if tenmin_state["active"] and tenmin_state["base_end_dt"] is not None:
                if row["dt"].date() > tenmin_state["base_end_dt"].date():
                    # 다음 영업일 진입 → 기준봉은 비교 가능 상태로 유지하되
                    # 시간 조건을 무력화
                    tenmin_state["base_end_dt"] = row["dt"].replace(
                        hour=0, minute=0, second=0
                    )

            high_price = int(row["고가"])
            low_price = int(row["저가"])
            close_price = int(row["종가"])

            breakout_check = high_price if breakout_type == "high" else close_price
            breakdown_check = low_price if breakdown_type == "low" else close_price

            # ===============================
            # 1️⃣ 돌파 이전 구간
            # ===============================
            if not breakout_done:
                # 🚨 이탈이 먼저 발생 → 즉시 종료
                if breakdown_check <= stop_price:
                    if verbose:
                        print(f"🚨 [{row['일자']} {row['시간']}] 돌파 전 이탈가 {stop_price:,}원 이탈 → 종료")

                    signals.append({
                        "signal_type": "BREAKDOWN_FIRST",
                        "종목명": stock_name,
                        "종목코드": stock_code,
                        "기준가격": stop_price,
                        "발생일자": row["일자"],
                        "발생시간": row["시간"],
                        "발생가격": breakdown_check,
                        "1분봉정보": {
                            "시가": int(row["시가"]),
                            "고가": high_price,
                            "저가": low_price,
                            "종가": close_price,
                            "거래량": int(row["거래량"])
                        }
                    })
                    process_done = True
                    return signals

                # 🔥 목표가 돌파
                if not tenmin_state["active"] and breakout_check >= target_price:
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
                        "skip_until_0910": row["dt"].time() < datetime.strptime("09:10", "%H:%M").time()
                    })


                    if verbose:
                        print(f"🔥 [{row['일자']} {row['시간']}] 목표가 {int(target_price):,}원 돌파")

                    signals.append({
                        "signal_type": "BREAKOUT",
                        "종목명": stock_name,
                        "종목코드": stock_code,
                        "기준가격": target_price,
                        "발생일자": row["일자"],
                        "발생시간": row["시간"],
                        "발생가격": breakout_check,
                        "1분봉정보": {
                            "시가": int(row["시가"]),
                            "고가": high_price,
                            "저가": low_price,
                            "종가": close_price,
                            "거래량": int(row["거래량"])
                        }
                    })
                    continue  # 다음 봉으로 이동

            # ===============================
            # 2️⃣ 돌파 이후 구간 (이탈만 감시)
            # ===============================
            else:
                # completed_key = get_completed_10min_key(row["dt"])

                # if (tenmin_state["active"] and completed_key >= tenmin_state["base_end_dt"]):

                #     new_10min = df[df["dt"].apply(get_completed_10min_key) == completed_key]

                #     if new_10min.empty:
                #         continue

                #     new_vol = new_10min["거래량"].astype(int).sum()
                #     new_low = new_10min["저가"].astype(int).min()

                #     # 🔥 거래량 돌파 시 기준봉 재설정 (날짜 무관)
                #     if new_vol > tenmin_state["base_vol"]:
                #         tenmin_state.update({
                #             "base_key": completed_key,
                #             "base_low": new_low,
                #             "base_vol": new_vol,
                #             "base_end_dt": completed_key + timedelta(minutes=10)
                #         })

                #         if verbose:
                #             print(
                #                 f"🔁 기준봉 갱신 "
                #                 f"[{completed_key.strftime('%Y%m%d %H:%M')}] "
                #                 f"거래량 {new_vol:,}, 저가 {new_low:,}"
                #             )

                # ===============================
                # 기준봉 갱신 판단 (날짜 무관)
                # ===============================
                completed_key = get_completed_10min_key(row["dt"])

                if tenmin_state["active"]:
                    new_10min = df[df["dt"].apply(get_completed_10min_key) == completed_key]

                    # 10분봉 완성 시점에서만
                    if not new_10min.empty and row["dt"] == new_10min["dt"].max():
                        new_vol = new_10min["거래량"].astype(int).sum()
                        new_low = new_10min["저가"].astype(int).min()
                        new_high = new_10min["고가"].astype(int).max()

                        if (new_vol > tenmin_state["base_vol"] or new_high > tenmin_state["base_high"]):
                            tenmin_state.update({
                                "base_key": completed_key,
                                "base_low": new_low,
                                "base_high": new_high,
                                "base_vol": new_vol,
                                "base_end_dt": completed_key + timedelta(minutes=10)
                            })

                            if verbose:
                                reason = (
                                    "거래량 돌파"
                                    if new_vol > tenmin_state["base_vol"]
                                    else "고가 돌파"
                                )
                                print(
                                    f"🔁 기준봉 갱신 ({reason}) "
                                    f"[{completed_key.strftime('%Y%m%d %H:%M')}] "
                                    f"고가 {new_high:,}, 저가 {new_low:,}, 거래량 {new_vol:,}"
                                )

                    # 🚨 1️⃣ 기준봉 저가 이탈
                    if int(row["저가"]) < tenmin_state["base_low"]:
                        print(
                            f"🔥 [{row['일자']} {row['시간']}] "
                            f"목표가 돌파 후 10분봉 저가 {tenmin_state['base_low']:,}원 이탈"
                        )
                        signals.append({
                            "signal_type": "10MIN_LOW_BREAKDOWN_AFTER_BREAKOUT",
                            "종목명": stock_name,
                            "종목코드": stock_code,
                            "발생일자": row["일자"],
                            "발생시간": row["시간"],
                            "기준10분저가": tenmin_state["base_low"],
                            "10분봉 저가": row["저가"]
                        })
                        process_done = True
                        return signals

                    # 🔄 2️⃣ 새로운 10분봉 완성 감지
                    curr_10min_key  = get_10min_key(row["dt"])
                    curr_10min_end = curr_10min_key + timedelta(minutes=10)

                    # 🔥 기준봉 이후 완성되는 모든 10분봉 대상
                    if curr_10min_end > tenmin_state["base_end_dt"]:

                        new_10min_df = df[(df["dt"] >= curr_10min_key) & (df["dt"] < curr_10min_end)]

                        # 10분봉 완성 시점에서만 비교
                        if (not new_10min_df.empty and row["dt"] == new_10min_df["dt"].max()):
                            new_vol = new_10min_df["거래량"].astype(int).sum()
                            new_low = new_10min_df["저가"].astype(int).min()
                            new_high = new_10min_df["고가"].astype(int).max()

                            # 🔥 거래량 돌파 → 기준봉 재설정
                            if new_vol > tenmin_state["base_vol"] or new_high > tenmin_state["base_high"]:
                                tenmin_state.update({
                                    "base_low": new_low,
                                    "base_high": new_high,
                                    "base_vol": new_vol,
                                    "base_end_dt": curr_10min_end
                                })

                                if verbose:
                                    reason = (
                                        "거래량 돌파"
                                        if new_vol > tenmin_state["base_vol"]
                                        else "고가 돌파"
                                    )
                                    print(
                                        f"🔁 기준봉 갱신 ({reason}) "
                                        f"[{completed_key.strftime('%Y%m%d %H:%M')}] "
                                        f"고가 {new_high:,}, 저가 {new_low:,}, 거래량 {new_vol:,}"
                                    )    

        # ✅ 최근 영업일이면 오늘 df 처리 후 즉시 종료 → 다음 종목으로
        if current == last_business_day:
            if verbose:
                print(
                    f"⏭️ [{stock_name}-{stock_code}] 최근 영업일({trade_date}) 처리 완료 → 다음 종목"
                )
            return signals
        
        current += timedelta(days=1)

    return signals

if __name__ == "__main__":

    ac = account('yh480825')
    acct_no = ac['acct_no']
    access_token = ac['access_token']
    app_key = ac['app_key']
    app_secret = ac['app_secret']
    token = ac['bot_token1']

    # 매매시뮬레이션 조회
    cur200 = conn.cursor()
    cur200.execute("select code, name, trade_day, trade_dtm, profit_price, loss_price from tradng_simulation where acct_no = '" + str(acct_no) + "' and trade_tp = '1' and trade_day = '20251219'")
    result_two00 = cur200.fetchall()
    cur200.close()

    if len(result_two00) > 0:
        
        for i in result_two00:

            signal = get_kis_1min_from_datetime(
                stock_code=i[0],
                stock_name=i[1], 
                start_date=i[2],
                start_time=i[3]+'00',
                target_price=int(i[4]),
                stop_price=int(i[5]),
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
