import requests
import json
import os
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
import psycopg2 as db
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

conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

conn = db.connect(conn_string)

today = datetime.now().strftime("%Y%m%d")

CHAT_ID = "2147256258"

# 실행 허용 시간대 (이 시간대 밖이면 기동하지 않음)
RUN_START_HHMMSS = '090000'
RUN_END_HHMMSS = '152000'

# 스크립트 디렉터리 (cron 이 파이프/exec 로 실행하면 __file__ 이 없을 수 있어 cwd 로 폴백)
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

# 중복 실행 방지용 PID 파일
PID_FILE = os.path.join(SCRIPT_DIR, "kw_fast_stock_search.pid")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def acquire_singleton_lock() -> bool:
    """이미 동일 스크립트가 실행 중이면 False, 아니면 PID 파일 생성 후 True"""
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE) as f:
                old = int((f.read().strip() or "0"))
            if old and old != os.getpid() and _pid_alive(old):
                return False
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        print(f"PID 락 처리 오류(무시하고 진행): {e}")
        return True


def release_singleton_lock():
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE) as f:
                owner = f.read().strip()
            if owner == str(os.getpid()):
                os.remove(PID_FILE)
    except Exception:
        pass

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
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    return res.json()["access_token"]

# KIS 당일 분봉 조회 (최대 30건 페이징 처리)
def get_kis_1min_chart(
    stock_code: str,
    search_time: str,       # HHMM - 이 시간 이후 분봉이 필요
    access_token: str,
    app_key: str,
    app_secret: str,
    market_code: str = "J",
    verbose: bool = False
):
    """당일분봉조회 /inquire-time-itemchartprice
    search_time(HHMM) 이후 ~ 현재까지 전체 1분봉 반환 (30건 초과 시 페이징).
    """
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST03010200",
        "custtype": "P"
    }

    # HHMM → HHMMSS (API 파라미터 형식)
    search_time_hhmmss = search_time + "00" if len(search_time) == 4 else search_time
    query_time = datetime.now().strftime('%H%M%S')  # 현재 시각부터 역방향 조회
    all_rows = []
    prdy_ctrt = "0.00"
    seen_times = set()  # 중복 방지

    while True:
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": query_time,
            "FID_PW_DATA_INCU_YN": "Y"
        }

        res = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()

        if "output2" not in data or not data["output2"]:
            break

        if not all_rows:
            # 첫 번째 호출에서만 현재 등락률 추출 (output1)
            prdy_ctrt = data.get("output1", {}).get("prdy_ctrt", "0.00")

        rows = data["output2"]
        new_added = False
        for row in rows:
            t = row["stck_cntg_hour"]
            if t not in seen_times:
                seen_times.add(t)
                all_rows.append(row)
                new_added = True

        if not new_added:
            break  # 중복만 반환 → 무한루프 방지

        # 이번 배치의 가장 오래된 시간 확인
        oldest_time = min(row["stck_cntg_hour"] for row in rows)

        if oldest_time <= search_time_hhmmss:
            break  # search_time 이전 데이터까지 확보 완료

        # 다음 페이지: 가장 오래된 시각으로 재조회
        query_time = oldest_time
        time.sleep(0.2)

    if not all_rows:
        if verbose:
            print(f"데이터 없음 ({stock_code})")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
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
    df["등락률"] = prdy_ctrt
    df = df.drop_duplicates(subset=["일자", "시간"])
    df = df.sort_values(["일자", "시간"]).reset_index(drop=True)

    return df[["일자", "시간", "시가", "고가", "저가", "종가", "거래량", "등락률"]]


# KIS 당일 분봉 - 지정 시각 직전 최대 ~30봉을 1콜로 조회 (페이징 없음)
def get_kis_1min_window(stock_code, end_dt, access_token, app_key, app_secret, market_code="J"):
    """inquire-time-itemchartprice: end_dt(datetime) 기준 직전 1분봉 묶음(최대 30건)을
    1회 호출로 반환. dt(datetime) 컬럼 포함 DataFrame. 실패 시 빈 DataFrame."""
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST03010200",
        "custtype": "P",
    }
    params = {
        "FID_ETC_CLS_CODE": "",
        "FID_COND_MRKT_DIV_CODE": market_code,
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_HOUR_1": end_dt.strftime("%H%M%S"),
        "FID_PW_DATA_INCU_YN": "Y",
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()
    except Exception as e:
        print(f"분봉 조회 오류 ({stock_code}): {e}")
        return pd.DataFrame()

    rows = data.get("output2") or []
    if not rows:
        return pd.DataFrame()
    prdy_ctrt = (data.get("output1") or {}).get("prdy_ctrt", "0.00")

    df = pd.DataFrame(rows).rename(columns={
        "stck_bsop_date": "일자",
        "stck_cntg_hour": "시간",
        "stck_oprc": "시가",
        "stck_hgpr": "고가",
        "stck_lwpr": "저가",
        "stck_prpr": "종가",
        "cntg_vol": "거래량",
    })
    df["dt"] = pd.to_datetime(df["일자"] + df["시간"], format="%Y%m%d%H%M%S", errors="coerce")
    df = df.dropna(subset=["dt"])
    df["등락률"] = prdy_ctrt
    df = df.drop_duplicates(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df[["일자", "시간", "시가", "고가", "저가", "종가", "거래량", "등락률", "dt"]]


def get_10min_key(dt: datetime):
    return dt.replace(minute=(dt.minute // 10) * 10, second=0)

def get_next_completed_10min_dt(dt: datetime) -> datetime:
    base_minute = (dt.minute // 10) * 10
    base = dt.replace(minute=base_minute, second=0, microsecond=0)
    return base + timedelta(minutes=10)

# 텔레그램 메시지 전송 함수
async def send_telegram_message(message_text: str, bot_token: str, parse_mode: str = 'HTML', reply_markup=None):
    bot = Bot(token=bot_token)
    await asyncio.to_thread(
        bot.send_message,
        chat_id=CHAT_ID,
        text=message_text,
        parse_mode=parse_mode,
        reply_markup=reply_markup
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
    response  = requests.post(url, headers=headers, json=params, timeout=10)

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
        self.power_rapid_name = ''  # 파워급등주 조건식 이름

    # WebSocket 서버에 연결합니다.
    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.uri)
            self.connected = True
            # print("서버와 연결을 시도 중입니다.")

            # 로그인 패킷
            param = {
                'trnm': 'LOGIN',
                'token': self.access_token
            }

            # print('실시간 시세 서버로 로그인 패킷을 전송합니다.')
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
            # print(f'Message sent: {message}')

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
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 로그인 실패하였습니다. : {response.get('return_msg')}")
                        await self.disconnect()
                    else:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 로그인 성공하였습니다.")
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

                    # 조건검색 저장 후 돌파 체크(REST) 1회 수행하고 종료
                    await self.check_10min_breakout()
                    await self.disconnect()
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
                
                # 메시지 유형이 PING일 경우 수신값 그대로 반송(PONG)
                elif trnm == 'PING':
                    await self.send_message(response)

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

                # 데이터 준비 (code, search_day, search_name 기준 upsert)
                row = (
                    today, now, search_name, code, i['302'],
                    math.ceil(float(i['18'])),  # 저가
                    math.ceil(float(i['17'])),  # 고가
                    math.ceil(float(i['10'])),  # 현재가
                    safe_day_rate(i.get('12')), # 등락률
                    math.ceil(float(i['13'])),  # 거래량
                    datetime.now(),
                    datetime.now()
                )
                data.append(row)

            if data:
                # upsert 쿼리: (search_day, search_name, code) 충돌 시 시세 정보 갱신
                insert_query = """
                    INSERT INTO stock_search_form (
                        search_day, search_time, search_name, code, name,
                        low_price, high_price, current_price, day_rate, volumn, crt_dt, mod_dt
                    )
                    VALUES %s
                    ON CONFLICT (search_day, search_name, code) DO UPDATE SET
                        search_time = EXCLUDED.search_time,
                        name = EXCLUDED.name,
                        low_price = EXCLUDED.low_price,
                        high_price = EXCLUDED.high_price,
                        current_price = EXCLUDED.current_price,
                        day_rate = EXCLUDED.day_rate,
                        volumn = EXCLUDED.volumn,
                        mod_dt = EXCLUDED.mod_dt
                    RETURNING code;
                """

                # execute_values로 데이터 upsert
                execute_values(cur, insert_query, data)

                # 커밋
                conn.commit()

                # upsert된 코드 추출
                upserted_codes = [row[0] for row in cur.fetchall()]

                print(f"{len(upserted_codes)}건의 데이터가 저장(upsert)되었습니다.")
            else:
                print("데이터가 없어 저장이 수행되지 않았습니다.")

    # ── 돌파 체크 (B: REST 폴링 + C: 롤링 10분 거래량) ────────────────────────────
    #  1분 단위 cron 호출 전제. 조건검색 저장 직후 1회 수행하고 프로세스를 종료한다.
    #  기준(reference) : search_time 이 속한 10분 구간 [ref_start, ref_end) 의 고가 / 거래량 합
    #  현재(rolling)   : 조회 시점 기준 최근 10분의 고가 / 거래량 합
    #  두 값을 모두 돌파하면 알림 → DB 갱신.
    BREAKOUT_CONCURRENCY = 5   # KIS API 동시 호출 제한

    async def check_10min_breakout(self):
        if not self.kis_access_token:
            print("KIS 자격증명 없음 - 돌파 체크 생략")
            return

        today = datetime.now().strftime('%Y%m%d')
        now = datetime.now()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT code, name, search_time
                FROM stock_search_form
                WHERE search_day = %s
                  AND (breakout_noti_yn IS NULL OR breakout_noti_yn = 'N')
            """, (today,))
            stocks = cur.fetchall()

        if not stocks:
            print("돌파 체크 대상 없음")
            return

        print(f"돌파 체크 대상: {len(stocks)}건")

        sem = asyncio.Semaphore(self.BREAKOUT_CONCURRENCY)

        async def _guarded(code, name, search_time):
            async with sem:
                try:
                    return await self._eval_breakout(code, name, search_time, today, now)
                except Exception as e:
                    print(f"돌파 체크 오류 [{name}-{code}]: {e}")
                    return None

        results = await asyncio.gather(*[_guarded(c, n, s) for c, n, s in stocks])

        # 돌파 확정 건만 순차 처리 (알림 + DB 갱신)
        for r in [x for x in results if x]:
            await self._fire_breakout(today, now, r)

    async def _eval_breakout(self, code, name, search_time, today, now):
        """단일 종목 돌파 판정. 돌파면 dict, 아니면 None."""
        search_dt = datetime.strptime(today + search_time, "%Y%m%d%H%M")
        ref_start = get_10min_key(search_dt)
        ref_end = ref_start + timedelta(minutes=10)

        # 기준 10분 구간이 아직 끝나지 않았으면 판정 불가
        if now < ref_end:
            return None

        # 최근 분봉 1콜 (현재시각 기준 직전 ~30봉)
        recent_df = await asyncio.to_thread(
            get_kis_1min_window, code, now,
            self.kis_access_token, self.kis_app_key, self.kis_app_secret,
        )
        if recent_df.empty:
            return None

        # 최근 분봉이 기준 구간까지 덮으면 재사용, 아니면 기준 구간 별도 조회
        if recent_df["dt"].min() <= ref_start:
            ref_df = recent_df
        else:
            ref_df = await asyncio.to_thread(
                get_kis_1min_window, code, ref_end,
                self.kis_access_token, self.kis_app_key, self.kis_app_secret,
            )
            if ref_df.empty:
                return None

        ref_win = ref_df[(ref_df["dt"] >= ref_start) & (ref_df["dt"] < ref_end)]
        if ref_win.empty:
            return None
        ref_high = int(ref_win["고가"].astype(float).max())
        ref_vol = int(ref_win["거래량"].astype(float).sum())

        # 최근 10분 롤링 구간
        roll_win = recent_df[recent_df["dt"] >= now - timedelta(minutes=10)]
        if roll_win.empty:
            return None
        roll_high = int(roll_win["고가"].astype(float).max())
        roll_vol = int(roll_win["거래량"].astype(float).sum())

        # 돌파 판정: 최근 10분 고가 > 기준 10분 고가  AND  최근 10분 거래량 > 기준 10분 거래량
        if roll_high <= ref_high or roll_vol <= ref_vol:
            return None

        last = recent_df.iloc[-1]
        return {
            'code': code,
            'name': name,
            'ref_high': ref_high,
            'ref_vol': ref_vol,
            'roll_high': roll_high,
            'roll_vol': roll_vol,
            'current_price': int(float(last["종가"])),
            'current_rate': last["등락률"],
        }

    async def _fire_breakout(self, today, now, r):
        """돌파 확정 - 텔레그램 알림 + DB 갱신"""
        code = r['code']
        safe_name = html.escape(r['name'].strip())
        message = (
            f"[{now.strftime('%H:%M')}] {safe_name}[<code>{code}</code>] "
            f"기준 10분 고가 : {r['ref_high']:,}원 돌파, 최근10분 고가 : {r['roll_high']:,}원, "
            f"현재가 : {r['current_price']:,}원, "
            f"최근10분 거래량 : {r['roll_vol']:,} (기준 {r['ref_vol']:,}), 등락율 : {r['current_rate']}%"
        )
        print(message)
        reg_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("관심종목 등록", callback_data=f"menu,interest_register_{code}")
        ]])
        try:
            await send_telegram_message(message, self.bot_token, parse_mode='HTML', reply_markup=reg_markup)
        except Exception as e:
            print(f"돌파 알림 전송 오류 [{code}]: {e}")

        # 돌파 알림 완료 업데이트 (signal_time: 돌파 시각, signal_price: 돌파가)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE stock_search_form
                       SET breakout_noti_yn = 'Y'
                         , signal_time = %s
                         , signal_price = %s
                         , mod_dt = %s
                     WHERE code = %s AND search_day = %s
                """, (now.strftime('%H%M'), r['current_price'], now, code, today))
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"돌파 DB 갱신 오류 [{code}]: {e}")

    # WebSocket 실행 (1회성: 조건검색 → 저장 → 돌파 체크 → 종료)
    async def run(self):
        await self.connect()
        if not self.connected:
            print('WebSocket 연결 실패 - 종료 (다음 cron 에서 재시도)')
            return
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
    try:
        await websocket_client.run()
    except Exception as e:
        print(f'main 실행 예외: {e}')
    finally:
        try:
            conn.close()
        except Exception:
            pass

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

# asyncio로 프로그램을 실행합니다. (cron 이 1분 단위로 호출하는 1회성 프로세스)
if __name__ == '__main__':
    # 영업일 확인용 임시 연결
    _conn_check = db.connect(conn_string)
    try:
        _is_business = is_business_day(today, _conn_check)
    finally:
        _conn_check.close()

    _now_hms = datetime.now().strftime('%H%M%S')

    if not _is_business:
        print('영업일이 아니어서 종료합니다.')
    elif not (RUN_START_HHMMSS <= _now_hms <= RUN_END_HHMMSS):
        print(f'실행 허용 시간대(09:00~15:20)가 아니어서 종료합니다. (현재 {_now_hms[:2]}:{_now_hms[2:4]})')
    elif not acquire_singleton_lock():
        print('이전 실행이 아직 진행 중이어서 이번 호출은 건너뜁니다.')
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print('사용자 중단(KeyboardInterrupt)')
        except Exception as e:
            print(f'비정상 종료: {e}')
        finally:
            release_singleton_lock()

