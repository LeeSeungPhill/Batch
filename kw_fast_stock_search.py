import requests
import json
from telegram import Bot
import psycopg2 as db
import sys
import math
from datetime import datetime, timedelta
import asyncio
import websockets
from psycopg2.extras import execute_values
import html
import pandas as pd
import time

#URL_BASE = "https://mockapi.kiwoom.com"   # 모의투자서비스
URL_BASE = "https://api.kiwoom.com"
SOCKET_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket"  # 접속 URL
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"      # KIS 실전투자

# conn_string = "dbname='fund_risk_mng' host='192.168.50.80' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

conn = db.connect(conn_string)

CHAT_ID = "2147256258"

def safe_day_rate(raw):
    day_rate = 0.00
    try:
        raw = str(raw).strip()
        if raw:
            # 부호 처리
            sign = -1 if raw.startswith('-') else 1
            # 숫자만 추출
            digits = ''.join(ch for ch in raw if ch.isdigit())
            if digits:
                value = sign * (int(digits) / 1000)  # 3자리 소수점
                # numeric(8,2) 허용 범위 내로 clamp
                if value > 999999.99:
                    day_rate = 999999.99
                elif value < -999999.99:
                    day_rate = -999999.99
                else:
                    day_rate = round(value, 2)
    except Exception as e:
        print(f"등락율 변환 오류: {raw} → {e}")
        day_rate = 0.00
    return day_rate


# KIS OAuth2 토큰 발급
def kis_auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{KIS_BASE_URL}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False)
    return res.json()["access_token"]

# KIS 1분봉 조회
def get_kis_1min_dailychart(
    stock_code: str,
    trade_date: str,
    trade_time: str,
    access_token: str,
    app_key: str,
    app_secret: str,
    market_code: str = "J",
    include_past: str = "Y",
    include_fake_tick: str = "N",
    verbose: bool = False
):
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"

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
            print(f"데이터 없음 ({trade_date} {trade_time})")
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

def get_next_completed_10min_dt(dt: datetime) -> datetime:
    base_minute = (dt.minute // 10) * 10
    base = dt.replace(minute=base_minute, second=0, microsecond=0)
    return base + timedelta(minutes=10)

# 텔레그램 메시지 전송 함수
async def send_telegram_message(message_text: str, bot_token: str, parse_mode: str = 'HTML'):
    bot = Bot(token=bot_token)
    
    await asyncio.to_thread(
        bot.send_message,
        chat_id=CHAT_ID,
        text=message_text,
        parse_mode=parse_mode
    )

def auth(APP_KEY, APP_SECRET):

    params = {
		'grant_type': 'client_credentials',  # grant_type
		'appkey': APP_KEY,  # 앱키
		'secretkey': APP_SECRET,  # 시크릿키
	}

    # 인증처리
    PATH = 'oauth2/token'
    url = f"{URL_BASE}/{PATH}"

    headers = {
		'Content-Type': 'application/json;charset=UTF-8', # 컨텐츠타입
	}

	# 3. http POST 요청
    response  = requests.post(url, headers=headers, json=params)

    return response.json()["token"]

class WebSocketClient:
    def __init__(self, uri, access_token, bot_token, kis_access_token=None, kis_app_key=None, kis_app_secret=None):
        self.uri = uri
        self.access_token = access_token
        self.bot_token = bot_token
        self.kis_access_token = kis_access_token
        self.kis_app_key = kis_app_key
        self.kis_app_secret = kis_app_secret
        self.websocket = None
        self.connected = False
        self.keep_running = True
        self.condition_list = []  # 조건검색 목록 저장
        self.search_results = []  # 조건검색 결과 저장

    # WebSocket 서버에 연결합니다.
    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.uri)
            self.connected = True
            print("서버와 연결을 시도 중입니다.")

            # 로그인 패킷
            param = {
                'trnm': 'LOGIN',
                'token': self.access_token
            }

            print('실시간 시세 서버로 로그인 패킷을 전송합니다.')
            # 웹소켓 연결 시 로그인 정보 전달
            await self.send_message(message=param)

        except Exception as e:
            print(f'Connection error: {e}')
            self.connected = False

    # 서버에 메시지를 보냅니다. 연결이 없다면 자동으로 연결합니다.
    async def send_message(self, message):
        if not self.connected:
            await self.connect()  # 연결이 끊어졌다면 재연결
        if self.connected:
            # message가 문자열이 아니면 JSON으로 직렬화
            if not isinstance(message, str):
                message = json.dumps(message)

            await self.websocket.send(message)
            print(f'Message sent: {message}')

    # 서버에서 오는 메시지를 수신하여 출력합니다.
    async def receive_messages(self):
        while self.keep_running:
            try:
                message = await self.websocket.recv()
                if not message:
                    print('수신된 메시지가 없습니다. 연결이 종료되었을 수 있습니다.')
                    self.connected = False
                    await self.websocket.close()
                    break

                try:
                    response = json.loads(message)
                except json.JSONDecodeError:
                    print(f'JSON 디코딩 오류: {message}')
                    continue

                trnm = response.get('trnm')

                # 메시지 유형이 LOGIN일 경우 로그인 시도 결과 체크
                if trnm == 'LOGIN':
                    if response.get('return_code') != 0:
                        print('로그인 실패하였습니다. : ', response.get('return_msg'))
                        await self.disconnect()
                    else:
                        print('로그인 성공하였습니다.')
                        await self.send_message({'trnm': 'CNSRLST'})

                elif trnm == 'CNSRLST':
                    self.condition_list = response.get('data', [])
                    # print(f'조건검색 목록 수신: {self.condition_list}')
                    if self.condition_list:
                        # 다섯번째 조건검색식: 파워급등주
                        seq5 = self.condition_list[5][0]
                        self.power_rapid_name = self.condition_list[5][1]  # 파워급등주 이름 저장
                        await self.send_message({
                            'trnm': 'CNSRREQ',
                            'seq': seq5,
                            'search_type': '0',
                            'stex_tp': 'K',
                            'cont_yn': 'N',
                            'next_key': '',
                        })
                        # 여섯번째 조건검색식: 파워종목
                        # seq6 = self.condition_list[6][0]
                        # self.power_item_name = self.condition_list[6][1]  # 파워종목 이름 저장
                        # await self.send_message({
                        #     'trnm': 'CNSRREQ',
                        #     'seq': seq6,
                        #     'search_type': '0',
                        #     'stex_tp': 'K',
                        #     'cont_yn': 'N',
                        #     'next_key': '',
                        # })


                elif trnm == 'CNSRREQ':
                    self.search_results = response.get('data', [])
                    # print(f'조건검색 결과 수신: {self.search_results}')
                    seq = response.get('seq', '').strip()  # 시퀀스 번호로 구분

                    if seq == self.condition_list[5][0]:  # 파워급등주 결과
                        print(f'{self.power_rapid_name}')
                        # print(f'{self.power_rapid_name}-{self.search_results}')
                        for i in self.search_results:
                            code = i['9001'][1:] if i['9001'].startswith('A') else i['9001']
                            name = i['302']
                            current_price = math.ceil(float(i['10']))
                            rate = float(i['12']) / 1000
                            vol = math.ceil(float(i['13']))
                            high_price = math.ceil(float(i['17']))
                            low_price = math.ceil(float(i['18']))
                            print(f"{name} [{code}] 현재가: {format(current_price, ',d')}원, "
                                  f"거래량: {format(vol, ',d')}주, 고가: {format(high_price, ',d')}원, "
                                  f"저가: {format(low_price, ',d')}원, 등락율: {rate:.2f}%")
                        await self.save_to_db(self.power_rapid_name, self.search_results)
                    # elif seq == self.condition_list[6][0]:  # 파워종목 결과
                    #     print(f'{self.power_item_name}')
                    #     # print(f'{self.power_item_name}-{self.search_results}')
                    #     for i in self.search_results:
                    #         code = i['9001'][1:] if i['9001'].startswith('A') else i['9001']
                    #         name = i['302']
                    #         current_price = math.ceil(float(i['10']))
                    #         rate = float(i['12']) / 1000
                    #         vol = math.ceil(float(i['13']))
                    #         high_price = math.ceil(float(i['17']))
                    #         low_price = math.ceil(float(i['18']))
                    #         print(f"{name} [{code}] 현재가: {format(current_price, ',d')}원, "
                    #               f"거래량: {format(vol, ',d')}주, 고가: {format(high_price, ',d')}원, "
                    #               f"저가: {format(low_price, ',d')}원, 등락율: {rate:.2f}%")
                    #     await self.save_to_db(self.power_item_name, self.search_results)
                
                # 메시지 유형이 PING일 경우 10분봉 돌파 체크 후 종료
                elif response.get('trnm') == 'PING':
                    await self.check_10min_breakout()
                    await self.websocket.close()
                    self.keep_running = False
                    self.connected = False
                    sys.exit(0)

                else:
                    print(f'실시간 시세 서버 응답 수신: {response}')

            except websockets.ConnectionClosed:
                print('서버에 의해 연결이 종료되었습니다.')
                self.connected = False
                break
            except Exception as e:
                print(f'예외 발생: {e}')
                self.connected = False
                break

    async def save_to_db(self, search_name, items):
        today = datetime.now().strftime('%Y%m%d')
        now = datetime.now().strftime('%H%M')
        data = []
        # telegram_messages = []

        with conn.cursor() as cur:
            for i in items:
                code = i['9001'][1:] if i['9001'].startswith('A') else i['9001']

                # 기존 데이터 확인
                cur.execute("""
                    SELECT 1
                    FROM stock_search_form
                    WHERE code = %s AND search_day = %s AND search_name = %s
                    LIMIT 1;
                """, (code, today, search_name))

                if not cur.fetchone():  # 기존 데이터가 없으면
                    # 데이터 준비
                    row = (
                        today, now, search_name, code, i['302'],
                        math.ceil(float(i['18'])),  # 저가
                        math.ceil(float(i['17'])),  # 고가
                        math.ceil(float(i['10'])),  # 현재가
                        safe_day_rate(i.get('12')), # 등락률
                        math.ceil(float(i['13'])),  # 거래량
                        datetime.now()
                    )
                    data.append(row)

                    # safe_search_name = html.escape(search_name)

                    # # 텔레그램 메시지 준비
                    # telegram_text = (
                    #     f"&lt;{safe_search_name}&gt; {i['302']} [<code>{code}</code>] 현재가: {format(math.ceil(float(i['10'])), ',d')}원, "
                    #     f"거래량: {format(math.ceil(float(i['13'])), ',d')}주, 고가: {format(math.ceil(float(i['17'])), ',d')}원, "
                    #     f"저가: {format(math.ceil(float(i['18'])), ',d')}원, 등락율: {safe_day_rate(i.get('12'))}%"
                    # )
                    # telegram_messages.append((code, telegram_text))

            if data:
                # 삽입 쿼리
                insert_query = """
                    INSERT INTO stock_search_form (
                        search_day, search_time, search_name, code, name,
                        low_price, high_price, current_price, day_rate, volumn, cdate
                    )
                    VALUES %s
                    ON CONFLICT (search_day, search_name, code) DO NOTHING
                    RETURNING code;
                """

                # execute_values로 데이터 삽입
                execute_values(cur, insert_query, data)

                # 커밋
                conn.commit()

                # 삽입된 코드 추출
                inserted_codes = [row[0] for row in cur.fetchall()]

                # bot_token = self.bot_token

                # # 텔레그램 메시지 전송
                # for code, message in telegram_messages:
                #     if code in inserted_codes:
                #         await send_telegram_message(message, bot_token, parse_mode='HTML')

                print(f"{len(inserted_codes)}건의 데이터가 저장되고 텔레그램 알림이 전송되었습니다.")
            else:
                print("새로운 데이터가 없어 삽입 및 알림이 수행되지 않았습니다.")

    # 10분봉 고가 돌파 체크
    async def check_10min_breakout(self):
        if not self.kis_access_token:
            print("KIS API 자격증명 없음 - 10분봉 돌파 체크 생략")
            return

        today = datetime.now().strftime('%Y%m%d')
        now = datetime.now()

        with conn.cursor() as cur:
            # 오늘 종목 중 돌파 미알림 건 조회
            cur.execute("""
                SELECT code, name, search_time
                FROM stock_search_form
                WHERE search_day = %s
                AND (breakout_noti_yn IS NULL OR breakout_noti_yn = 'N')
            """, (today,))
            stocks = cur.fetchall()

        if not stocks:
            print("10분봉 돌파 체크 대상 없음")
            return

        print(f"10분봉 돌파 체크 대상: {len(stocks)}건")

        for code, name, search_time in stocks:
            try:
                # search_time(HHMM) → datetime 변환
                search_dt = datetime.strptime(today + search_time, "%Y%m%d%H%M")
                # 10분봉 완성 시각 계산
                next_10min_dt = get_next_completed_10min_dt(search_dt)

                # 아직 10분봉 미완성이면 skip
                if now < next_10min_dt:
                    continue

                # KIS API 1분봉 조회
                current_time = now.strftime('%H%M%S')
                df = get_kis_1min_dailychart(
                    stock_code=code,
                    trade_date=today,
                    trade_time=current_time,
                    access_token=self.kis_access_token,
                    app_key=self.kis_app_key,
                    app_secret=self.kis_app_secret
                )

                if df.empty:
                    time.sleep(0.5)
                    continue

                # datetime 컬럼 생성
                df["dt"] = pd.to_datetime(
                    df["일자"] + df["시간"].str.replace(":", ""),
                    format="%Y%m%d%H%M"
                )

                # search_time이 속한 10분봉 구간
                tenmin_start = get_10min_key(search_dt)
                tenmin_end = tenmin_start + timedelta(minutes=10)

                # 10분봉 구간 내 분봉 추출 → 고가 계산
                tenmin_df = df[(df["dt"] >= tenmin_start) & (df["dt"] < tenmin_end)]
                if tenmin_df.empty:
                    time.sleep(0.5)
                    continue

                tenmin_high = tenmin_df["고가"].astype(int).max()

                # 10분봉 이후 분봉에서 고가 돌파 확인
                after_df = df[df["dt"] >= tenmin_end]
                if after_df.empty:
                    time.sleep(0.5)
                    continue

                latest_high = after_df["고가"].astype(int).max()

                if latest_high > tenmin_high:
                    # 돌파 알림 전송
                    message = (
                        f"[10분봉 돌파] {name}[<code>{code}</code>] "
                        f"10분봉고가: {tenmin_high:,}원 돌파, 현재고가: {latest_high:,}원"
                    )
                    print(message)
                    await send_telegram_message(message, self.bot_token, parse_mode='HTML')

                    # 돌파 알림 완료 업데이트
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE stock_search_form
                            SET breakout_noti_yn = 'Y'
                            WHERE code = %s AND search_day = %s
                        """, (code, today))
                        conn.commit()

                time.sleep(0.5)  # API rate limit

            except Exception as e:
                print(f"10분봉 돌파 체크 오류 [{name}-{code}]: {e}")
                time.sleep(0.5)

    # WebSocket 실행
    async def run(self):
        await self.connect()
        await self.receive_messages()

    # WebSocket 연결 종료
    async def disconnect(self):
        self.keep_running = False
        if self.connected and self.websocket:
            await self.websocket.close()
            self.connected = False
            print('Disconnected from WebSocket server')

async def main():

    cur01 = conn.cursor()
    cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1 from \"stockAccount_stock_account\" where nick_name = 'kwphills75'")
    result_one = cur01.fetchone()
    cur01.close()

    acct_no = result_one[0]
    access_token = result_one[1]
    app_key = result_one[2]
    app_secret = result_one[3]
    bot_token = result_one[6]
    today = datetime.now().strftime("%Y%m%d")

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_one[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    # print("diff : " + str(diff.days))
    if diff.days >= 1 or result_one[5] != today:  # 토큰 유효기간(1일) 만료 재발급
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        print("new access_token : " + access_token)
        # 계정정보 토큰값 변경
        cur02 = conn.cursor()
        update_query = "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s"
        # update 인자값 설정
        record_to_update = ([access_token, token_publ_date, datetime.now(), acct_no])
        # DB 연결된 커서의 쿼리 수행
        cur02.execute(update_query, record_to_update)
        conn.commit()
        cur02.close()

    # KIS API 계정 로드 (phills2 - 1분봉 조회용)
    cur_kis = conn.cursor()
    cur_kis.execute("""
        SELECT acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day
        FROM "stockAccount_stock_account"
        WHERE nick_name = 'phills2'
    """)
    kis_result = cur_kis.fetchone()
    cur_kis.close()

    kis_access_token = kis_result[1]
    kis_app_key = kis_result[2]
    kis_app_secret = kis_result[3]

    kis_valid = datetime.strptime(kis_result[4], '%Y%m%d%H%M%S')
    if (datetime.now() - kis_valid).days >= 1 or kis_result[5] != today:
        kis_access_token = kis_auth(kis_app_key, kis_app_secret)
        kis_token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        print("new KIS access_token : " + kis_access_token)
        cur_kis2 = conn.cursor()
        cur_kis2.execute(
            "update \"stockAccount_stock_account\" set access_token = %s, token_publ_date = %s, last_chg_date = %s where acct_no = %s",
            (kis_access_token, kis_token_publ_date, datetime.now(), kis_result[0])
        )
        conn.commit()
        cur_kis2.close()

	# WebSocketClient 전역 변수 선언
    websocket_client = WebSocketClient(SOCKET_URL, access_token, bot_token, kis_access_token, kis_app_key, kis_app_secret)
    await websocket_client.run()

# asyncio로 프로그램을 실행합니다.
if __name__ == '__main__':
	asyncio.run(main())

