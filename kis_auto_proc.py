import psycopg2 as db
from datetime import datetime, timedelta
import kis_api_resp as resp
import requests
import json
import telegram
import asyncio
import pandas as pd
import time

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")
cur_time = datetime.now().strftime("%H%M")
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
    URL = f"{URL_BASE}/{PATH}"
    #res = requests.get(URL, headers=headers, params=params, verify=False)
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

# 일별주문체결 조회
def daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8001R",                                # tr_id : TTTC8001R(실전투자 3개월이내), CTSC9215R(실전투자 3개월이전), VTTC0081R(모의투자 3개월이내), VTSC9215R(모의투자 3개월이전)
               "custtype": "P"
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "INQR_STRT_DT": datetime.now().strftime('%Y%m%d'),  # 조회시작일자 YYYYMMDD
                "INQR_END_DT": datetime.now().strftime('%Y%m%d'),   # 조회종료일자 YYYYMMDD
                "SLL_BUY_DVSN_CD": '00',                            # 매도매수구분코드 : 00 전체, 01 매도, 02 매수
                "PDNO": code,
                "ORD_GNO_BRNO": "",
                "ODNO": order_no,
                "CCLD_DVSN": "00",                                  # 체결구분 : 00 전체, 01 체결, 02 미체결
                "INQR_DVSN": '00',                                  # 조회구분 : 00 역순, 01 정순
                "INQR_DVSN_1": "",        
                "INQR_DVSN_3": "00",                                # 조회구분3 : 00 전체, 01 현금, 02 신용
                "EXCG_ID_DVSN_CD": "KRX",                           # 거래소ID구분코드 : KRX, NXT
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output1

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
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)
        return ar.getBody().output2  # 원본 dict 리스트

    def convert_to_df(candle_list):
        """dict 리스트 → DataFrame(1분봉)"""
        minute_list = []
        for item in candle_list:
            minute_list.append({
                'timestamp': pd.to_datetime(item['stck_cntg_hour'], format='%H%M%S'),
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
        df_10 = df.resample('10T').agg({
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

def main(telegram_text):
    chat_id = "2147256258"
    bot = telegram.Bot(token=token)
    bot.send_message(chat_id=chat_id, text=telegram_text, parse_mode='HTML')

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
            ac = account(nick)
            acct_no = ac['acct_no']
            access_token = ac['access_token']
            app_key = ac['app_key']
            app_secret = ac['app_secret']
            token = ac['bot_token1']

            # 매매자동처리 조회
            cur31 = conn.cursor()

            cur31.execute("select id, name, code, base_dtm, trade_tp, signal_cd, open_price, high_price, low_price, close_price, vol, candle_body, trade_sum from trade_auto_proc where base_day = '" + today + "' and proc_yn = 'Y' and acct_no = '" + str(acct_no) + "'")
            result_three_one = cur31.fetchall()
            cur31.close()

            # 매매자동처리 거래량과 주식당일분봉조회의 최대 거래량 비교
            for i in result_three_one:

                # base_dtm datetime 변환
                base_dtm = datetime.strptime(today + i[3], '%Y%m%d%H%M%S')
                # 10분봉 조회 (필요시 과거 조회 포함)
                candle_list = fetch_candles_with_base(access_token, app_key, app_secret, i[2], base_dtm)

                minute_list = []
                for item in candle_list:
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

                # base_dtm 10분봉 시작 시간
                base_candle_start = base_dtm.replace(minute=(base_dtm.minute // 10) * 10, second=0, microsecond=0)

                # 1분봉 df → 10분봉 리샘플링
                df_10m = df.resample('10T', on='timestamp', label='left', closed='left').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).reset_index()
                # 10분봉 몸통(body) 계산
                df_10m['body'] = (df_10m['close'] - df_10m['open']).abs()

                # base_dtm 10분봉 거래량
                base_candle_df = df_10m[df_10m['timestamp'].dt.strftime("%H%M%S")  == base_candle_start.strftime("%H%M%S")]
                base_volume = base_candle_df.iloc[0]['volume'] if not base_candle_df.empty else 0

                # base_dtm 10분봉 이후 최대 거래량 봉
                df_after_base = df_10m[df_10m['timestamp'].dt.strftime("%H%M%S")  >= base_candle_start.strftime("%H%M%S")]
                if df_after_base.empty:
                    continue
                기준봉 = df_after_base.loc[df_after_base['volume'].idxmax()]

                # 매매자동처리 정보의 거래량보다 기준봉 거래량이 큰 경우 매매자동처리 생성 및 기존 매매자동처리 변경(proc_yn = 'N')
                if 기준봉['volume'] > base_volume:
                    print("종목명 : " + i[1] + " 거래량 돌파 : " + format(int(기준봉['close']), ',d') + "원")
                    avg_body = df_10m['body'].rolling(20).mean().iloc[-1] if len(df_10m) >= 20 else df_10m['body'].mean()

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
                        acct_no, i[1], i[2], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), i[4], 기준봉['open'], 기준봉['high'], 기준봉['low'], 기준봉['close'], 기준봉['volume'], candle_body, i[12], 'Y', 'AUTO_PROC_BAT', datetime.now(), 'AUTO_PROC_BAT', datetime.now()
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
                                , chgr_id = 'AUTO_UP_PROC_BAT'
                                , chg_date = %s
                            WHERE acct_no = %s
                            AND code = %s
                            AND base_day = %s
                            AND base_dtm <> %s
                            AND trade_tp = %s
                            AND proc_yn = 'Y'
                        """

                        # update 인자값 설정
                        cur501.execute(update_query, (
                            datetime.now(), str(acct_no), i[2], datetime.now().strftime("%Y%m%d"), 기준봉['timestamp'].strftime("%H%M%S"), i[4]
                        ))

                        conn.commit()
                        cur501.close()

            # 매매자동처리 조회
            cur32 = conn.cursor()

            sql = """
                SELECT 
                    tap.id,
                    tap.name,
                    tap.code,
                    tap.base_dtm,
                    tap.trade_tp,
                    tap.signal_cd,
                    tap.open_price,
                    tap.high_price,
                    tap.low_price,
                    tap.close_price,
                    tap.vol,
                    tap.candle_body,
                    tap.trade_sum,
                    CASE 
                        WHEN sb.trading_plan IS NOT NULL THEN 
                            CASE 
                                WHEN sb.trading_plan = 'as' THEN 100
                                WHEN sb.trading_plan = '66s' THEN 66
                                WHEN sb.trading_plan = '50s' THEN 50
                                WHEN sb.trading_plan = '33s' THEN 33
                                WHEN sb.trading_plan = '25s' THEN 25
                                WHEN sb.trading_plan = '20s' THEN 20
                                ELSE 0
                            END
                        ELSE 0
                    END AS trading_value,
                    tap.regr_id
                FROM public.trade_auto_proc tap
                LEFT OUTER JOIN public."stockBalance_stock_balance" sb
                ON tap.acct_no = CAST(sb.acct_no AS varchar)
                AND tap.code = sb.code
                WHERE tap.base_day = %s
                AND (sb.trading_plan NOT IN ('i', 'h') OR sb.trading_plan IS NULL)
                AND tap.proc_yn = 'Y'
                AND sb.proc_yn = 'Y'
                AND tap.acct_no = %s
            """

            cur32.execute(sql, (today, str(acct_no)))
            result_three_two = cur32.fetchall()
            cur32.close()

            current_time = datetime.now()

            # 매매자동처리 고가, 저가, 종가, 시가, 거래량, 캔들형태를 각각 실시간 종목시세의 최고가와 최저가 비교
            for i in result_three_two:
                trail_signal_code = ""
                trail_signal_name = ""
                vol_appear = 0
                candle_type = ""
                a = ""

                base_dtm = datetime.strptime(today + i[3], '%Y%m%d%H%M%S')
                base_minute = base_dtm.minute
                base_hour = base_dtm.hour

                if 0 <= base_minute <= 7:
                    # 0~7분이면 → 같은 구간의 10분봉이 완성될 때까지 대기
                    next_minute_block = ((base_minute // 10) + 1) * 10

                elif 8 <= base_minute <= 9:
                    # 8~9분이면 → 다다음 구간의 20분봉 완성까지 대기
                    next_minute_block = ((base_minute // 10) + 2) * 10

                else:
                    # 그 외 일반 케이스 → 다음 정규 10분봉까지 대기
                    next_minute_block = ((base_minute // 10) + 1) * 10

                # 시 넘어가는 경우 처리
                if next_minute_block >= 60:
                    candle_complete_time = base_dtm.replace(
                        hour=base_hour, minute=0, second=0, microsecond=0
                    ) + timedelta(hours=1)
                else:
                    candle_complete_time = base_dtm.replace(
                        minute=next_minute_block, second=0, microsecond=0
                    )

                # 현재 시간이 봉 완성 이후인지 확인
                if current_time > candle_complete_time:
                    # 10분봉 완성 후 실행
                    high_price = i[7]
                    low_price = i[8]
                    close_price = i[9]

                    if i[11] == 'L': 
                        candle_type  = "[장봉] "
                    elif i[11] == 'S': 
                        candle_type  = "[단봉] "

                    try:
                        time.sleep(0.3)  # 초당 3건 이하로 제한
                        a = inquire_price(access_token, app_key, app_secret, i[2])

                        # 매수 대상
                        if i[4] == 'B':

                            n_buy_sum = 0
                            n_buy_amount = 0
                            item_loss_sum = 0

                            # 매매자동처리 정보의 고가 돌파시
                            if int(a['stck_prpr']) > high_price:
                                print("종목명 : " + i[1] + "돌파가 : " + format(int(high_price), ',d') + "원 돌파")
                                signal_cd = "01"
                                signal_cd_name = format(int(high_price), ',d') + "원 {돌파가 돌파}"
                                # 매수금액
                                n_buy_sum = int(i[12])
                                # 매수량 = round(매수금액 / 현재가)
                                n_buy_amount = round(n_buy_sum / int(a['stck_prpr']))
                                # 손절금액 = (현재가 - 손절가) * 매수량
                                item_loss_sum = (int(a['stck_prpr']) - int(low_price)) * n_buy_amount

                                buy_command = f"/InterestBuy_{i[2]}_{a['stck_prpr']}"

                                telegram_text = (f"[자동매수]{i[1]}[<code>{i[2]}</code>] : {candle_type}{trail_signal_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매수량 : {format(int(round(n_buy_amount)), ',d')}주, 매수금액 : {format(int(n_buy_sum), ',d')}원, 손절가 : {format(int(low_price), ',d')}, 손절금액 : {format(int(item_loss_sum), ',d')}원 => {buy_command}")
                                # 텔레그램 메시지 전송
                                main(telegram_text)

                                cur400 = conn.cursor()
                                # UPDATE
                                cur400.execute("""
                                    UPDATE trade_auto_proc
                                    SET
                                        signal_cd = %s,
                                        proc_yn = 'N',
                                        chgr_id = 'AUTO_UP_PROC_BAT',
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

                            # 텔레그램 자산정리 자동처리 대상 존재시
                            if i[14] == 'AUTO_FUND_UP_SELL':

                                # 매매자동처리 정보의 고가 돌파시 
                                if int(a['stck_prpr']) > high_price:

                                    result_msgs = []
                                    
                                    # 계좌종목 조회
                                    c = stock_balance(access_token, app_key, app_secret, acct_no, "")

                                    for j, name in enumerate(c.index):
                                        J_code = c['pdno'][j]
                                        j_hldg_qty = int(c['hldg_qty'][j])
                                        j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                                        sell_rate = 0
                                        sell_amount = 0
                                        sell_sum = 0
                                        sell_price = 0

                                        # 잔고정보의 매매자동처리 종목이 존재할 경우
                                        if J_code == i[2]:
                                            print("종목명 : " + i[1] + "저항가 : " + format(int(i[7]), ',d') + "원 돌파")
                                            signal_cd = "02"
                                            signal_cd_name = format(int(i[7]), ',d') + "원 {저항가 돌파}"
                                            sell_price = int(a['stck_prpr'])
                                            # 매도비율(%)
                                            sell_rate = int(i[13])
                                            # 매도량 = round((주문가능수량 / 매도비율 )* 100)
                                            sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                                            # 매도금액 = 매도량 * 현재가
                                            sell_sum = sell_amount * sell_price
                                    
                                            # 주문가능수량 존재시
                                            if j_ord_psbl_qty > 0:
                                                
                                                try:
                                                    
                                                    # 매도 : 지정가 주문
                                                    c = order_cash(False, access_token, app_key, app_secret, str(acct_no), J_code, "00", str(sell_amount), str(sell_price))
                                            
                                                    if c['ODNO'] != "":

                                                        # 일별주문체결 조회
                                                        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, J_code, c['ODNO'])
                                                        tdf = pd.DataFrame(output1)
                                                        tdf.set_index('odno')
                                                        d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty']]

                                                        for i, name in enumerate(d.index):
                                                            d_order_no = int(d['odno'][i])
                                                            d_order_type = d['sll_buy_dvsn_cd_name'][i]
                                                            d_order_dt = d['ord_dt'][i]
                                                            d_order_tmd = d['ord_tmd'][i]
                                                            d_name = d['prdt_name'][i]
                                                            d_order_price = d['avg_prvs'][i] if int(d['avg_prvs'][i]) > 0 else d['ord_unpr'][i]
                                                            d_order_amount = d['ord_qty'][i]
                                                            d_total_complete_qty = d['tot_ccld_qty'][i]
                                                            d_remain_qty = d['rmn_qty'][i]
                                                            d_total_complete_amt = d['tot_ccld_amt'][i]

                                                            print("매도주문 완료")
                                                            msg = f"[자동처리 매도-{d_name}] 매도가 : {int(d_order_price):,}원, 매도량 : {int(d_order_amount):,}주 매도주문 완료, 주문번호 : <code>{d_order_no}</code>"
                                                            result_msgs.append(msg)

                                                    else:
                                                        print("매도주문 실패")
                                                        msg = f"[자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(sell_amount):,}주 매도주문 실패"
                                                        result_msgs.append(msg)


                                                except Exception as e:
                                                    print('매도주문 오류.', e)
                                                    msg = f"[자동처리 매도-{i[1]}] 매도가 : {int(sell_price):,}원, 매도량 : {int(sell_amount):,}주 [매도주문 오류] - {str(e)}"
                                                    result_msgs.append(msg)

                                                final_message = "\n".join(result_msgs) if result_msgs else "대상이 존재하지 않습니다."

                                                # 텔레그램 메시지 전송
                                                main(final_message)

                                            if j_hldg_qty > 0:
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


                            # 매매자동처리 정보의 저가가 이탈시
                            elif int(a['stck_prpr']) < low_price:
                                
                                # 매매내역정보의 매매미처리 매수 대상 조회
                                # 안전마진 확보 매도 및 이탈가 아탈 매도
                                
                                # 잔고수량 존재하는 매매내역정보의 매매처리 매수 대상 조회
                                # 매수당시 이탈가 체크 및 최종변경된 이탈가 체크 : 최종 수익금 체크 매도 

                                # 계좌종목 조회
                                c = stock_balance(access_token, app_key, app_secret, acct_no, "")

                                for j, name in enumerate(c.index):
                                    J_code = c['pdno'][j]
                                    j_hldg_qty = int(c['hldg_qty'][j])
                                    j_ord_psbl_qty = int(c['ord_psbl_qty'][j])

                                    sell_rate = 0
                                    sell_amount = 0
                                    sell_sum = 0

                                    # 잔고정보의 매매자동처리 종목이 존재할 경우
                                    if J_code == i[2]:
                                        print("종목명 : " + i[1] + "이탈가 : " + format(int(i[8]), ',d') + "원 이탈")
                                        signal_cd = "02"
                                        signal_cd_name = format(int(i[8]), ',d') + "원 {이탈가 이탈}"
                                        # 매도비율(%)
                                        sell_rate = int(i[13])
                                        # 매도량 = round((주문가능수량 / 매도비율 )* 100)
                                        sell_amount = round(j_ord_psbl_qty * (sell_rate / 100))
                                        # 매도금액 = 매도량 * 현재가
                                        sell_sum = sell_amount * int(a['stck_prpr'])

                                        sell_command = f"/HoldingSell_{i[2]}_{sell_amount}"
                                        
                                        # 주문가능수량 존재시
                                        if j_ord_psbl_qty > 0:
                                            telegram_text = (f"[자동매도]{i[1]}[<code>{i[2]}</code>] : {candle_type}{signal_cd_name}, 고가 : {format(int(a['stck_hgpr']), ',d')}원, 저가 : {format(int(a['stck_lwpr']), ',d')}원, 현재가 : {format(int(a['stck_prpr']), ',d')}원, 거래량 : {format(int(a['acml_vol']), ',d')}주, 거래대비 : {a['prdy_vrss_vol_rate']}, 매도량 : {format(sell_amount, ',d')}주, 매도금액 : {format(sell_sum, ',d')}원 => {sell_command}")
                                            # 텔레그램 메시지 전송
                                            main(telegram_text)

                                        if j_hldg_qty > 0:
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

                    except Exception as ex:
                        print(f"현재가 시세 에러 : [{i[2]}] {ex}")  

                else:
                    # 아직 봉 완성 전
                    continue

            time.sleep(3)
        
        except Exception as e:
            print(f"[{nick}] Error kis_auto_proc : {e}")      
    
    conn.close()

else:
    conn.close()
    print("Today is Holiday")