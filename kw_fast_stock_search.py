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
from collections import deque
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

# 중단 시 재가동 버튼 콜백 (fnguidePerformbot.py 의 callback_get 에서 처리)
RESTART_CALLBACK = "menu,kwfast_restart"

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


async def notify_fatal(bot_token: str, reason: str):
    """비정상 중단 알림 + 재가동 버튼 전송"""
    try:
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 프로세스 재가동", callback_data=RESTART_CALLBACK)
        ]])
        msg = (
            f"⚠️ [{datetime.now().strftime('%H:%M:%S')}] 실시간 돌파 감시 프로세스가 중단됐습니다.\n"
            f"사유: {html.escape(str(reason))}\n\n아래 버튼으로 재가동할 수 있습니다."
        )
        await send_telegram_message(msg, bot_token, parse_mode='HTML', reply_markup=markup)
    except Exception as e:
        print(f"중단 알림 전송 오류: {e}")


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
        # 실시간 돌파 감시 상태 (A: 키움 실시간체결 + C: 롤링 10분 거래량)
        self.watch = {}            # code -> 감시 상태 dict
        self.watch_started = False # 감시 최초 기동 여부
        self.reg_seq = 0           # 실시간 등록 그룹 번호 시퀀스
        self.code_group = {}       # code -> 등록 그룹 번호
        self._pending_reregister = False  # 재접속 후 재등록 필요 여부
        self.stop_reason = None    # 종료 사유 ('market_close' 면 정상, 그 외는 비정상)
        self.reconnect_fail = 0    # 연속 재접속 실패 횟수
        self._session_ok = False   # 이번 접속에서 로그인 성공 여부

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
                        self.stop_reason = f"로그인 실패: {response.get('return_msg')}"
                        await self.disconnect()
                    else:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 로그인 성공하였습니다.")
                        self._session_ok = True  # 정상 접속 - 재접속 실패 카운터 리셋 근거
                        # 재접속인 경우 감시 종목 실시간 재등록
                        await self._reregister_after_reconnect()
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
                        # 저장 직후 실시간 돌파 감시 기동
                        await self.start_breakout_watch()
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
                
                # 실시간 체결 데이터 → 돌파 감시
                elif trnm == 'REAL':
                    await self.on_real(response.get('data', []))

                # 실시간 등록 응답
                elif trnm == 'REG':
                    if response.get('return_code') not in (0, None):
                        print(f"실시간 등록 응답: {response.get('return_msg')}")

                # 메시지 유형이 PING일 경우 연결 유지 + 감시 갱신
                elif trnm == 'PING':
                    await self.send_message(response)  # 수신값 그대로 반송(PONG)
                    await self.refresh_breakout_watch()

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

    # ── 실시간 돌파 감시 (A: 키움 실시간체결 + C: 롤링 10분 거래량) ──────────────
    #
    #  · 기준(reference): search_time 이 속한 10분 구간 [ref_start, ref_end).
    #    이 구간의 고가/거래량 합을 KIS 당일분봉으로 1회만 산출해 캐시한다.
    #  · 감시: 해당 종목을 키움 실시간체결(0B) 구독. 체결 틱마다
    #      현재가 > 기준고가  AND  최근 10분 롤링 거래량 > 기준 10분 거래량
    #    을 만족하면 즉시 알림 → DB 갱신 → 실시간 해제.
    #  · 롤링 10분 거래량은 누적거래량(FID 13) 스냅샷의 차분으로 구한다.
    #    감시 10분 미만 구간은 경과분 비례(pace) 임계값으로 대체한다.

    WATCH_WARMUP_SEC = 120      # 감시 시작 후 거래량 판정 유예
    WATCH_ROLL_SEC = 600       # 롤링 거래량 창(10분)
    REG_GROUP_SIZE = 90       # 실시간 등록 그룹당 종목 수
    WATCH_END_HHMMSS = '153000'  # 이 시각 이후 감시 종료

    def _load_watch_codes(self):
        """오늘 미알림 종목 조회 (code, name, search_time)"""
        today = datetime.now().strftime('%Y%m%d')
        with conn.cursor() as cur:
            cur.execute("""
                SELECT code, name, search_time
                FROM stock_search_form
                WHERE search_day = %s
                  AND (breakout_noti_yn IS NULL OR breakout_noti_yn = 'N')
            """, (today,))
            return cur.fetchall()

    def _ensure_watch_entries(self):
        """DB 미알림 종목 중 감시 목록에 없는 건을 추가하고 신규 code 리스트 반환"""
        today = datetime.now().strftime('%Y%m%d')
        new_codes = []
        for code, name, search_time in self._load_watch_codes():
            if code in self.watch:
                continue
            search_dt = datetime.strptime(today + search_time, "%Y%m%d%H%M")
            self.watch[code] = {
                'code': code,
                'name': name,
                'search_time': search_time,
                'ref_start': get_10min_key(search_dt),                 # 기준 10분 구간 시작
                'ref_end': get_next_completed_10min_dt(search_dt),     # 기준 10분 구간 종료(=완성 시각)
                'ref_high': None,
                'ref_vol': None,
                'ref_ready': False,
                'ref_last_try': None,
                'cur_high': 0,
                'acc_samples': deque(),      # (dt, 누적거래량) - 롤링 10분 계산용
                'watch_start': None,         # 첫 실시간 체결 수신 시각
                'watch_start_acc': None,     # 첫 실시간 체결 시점 누적거래량
                'notified': False,
            }
            new_codes.append(code)
        return new_codes

    async def start_breakout_watch(self):
        """조건검색 저장 직후 1회 호출 - 감시 대상 등록 및 기준값 산출"""
        if self.watch_started:
            return
        self.watch_started = True
        if not self.kis_access_token:
            print("KIS 자격증명 없음 - 실시간 돌파 감시 생략")
            return

        new_codes = self._ensure_watch_entries()
        if not new_codes:
            print("실시간 돌파 감시 대상 없음")
            return

        print(f"실시간 돌파 감시 대상: {len(new_codes)}건")
        await self._build_references()
        await self._register_codes(new_codes)

    async def refresh_breakout_watch(self):
        """PING 주기마다 호출 - 종료 시각 체크 / 신규 종목 등록 / 기준값 보완"""
        if datetime.now().strftime('%H%M%S') >= self.WATCH_END_HHMMSS:
            print("장 마감 - 실시간 돌파 감시 종료")
            self.stop_reason = 'market_close'
            self.keep_running = False
            await self.disconnect()
            return

        if not self.watch_started:
            await self.start_breakout_watch()
            return

        new_codes = self._ensure_watch_entries()
        await self._build_references()
        if new_codes:
            print(f"실시간 돌파 감시 신규 등록: {len(new_codes)}건")
            await self._register_codes(new_codes)

    async def _build_references(self):
        """기준 10분 구간이 완성된 종목의 기준 고가/거래량을 KIS 분봉으로 산출"""
        now = datetime.now()
        for w in self.watch.values():
            if w['ref_ready'] or w['notified']:
                continue
            if now < w['ref_end']:
                continue  # 기준 10분 구간 아직 미완성
            if w['ref_last_try'] and (now - w['ref_last_try']).total_seconds() < 30:
                continue  # 재시도 과다 방지
            w['ref_last_try'] = now
            try:
                ok = await asyncio.to_thread(self._build_reference_sync, w)
                if ok:
                    print(f"기준 산출 [{w['name']}-{w['code']}] "
                          f"고가 {w['ref_high']:,} / 10분거래량 {w['ref_vol']:,}")
            except Exception as e:
                print(f"기준 산출 오류 [{w['name']}-{w['code']}]: {e}")
            await asyncio.sleep(0.3)

    def _build_reference_sync(self, w):
        """(블로킹) KIS 당일분봉으로 기준 10분 구간 고가/거래량 합 계산"""
        df = get_kis_1min_chart(
            stock_code=w['code'],
            search_time=w['ref_start'].strftime('%H%M'),
            access_token=self.kis_access_token,
            app_key=self.kis_app_key,
            app_secret=self.kis_app_secret,
        )
        if df.empty:
            return False
        df['dt'] = pd.to_datetime(
            df['일자'] + df['시간'].str.replace(':', ''), format='%Y%m%d%H%M'
        )
        win = df[(df['dt'] >= w['ref_start']) & (df['dt'] < w['ref_end'])]
        if win.empty:
            return False
        w['ref_high'] = int(win['고가'].astype(float).max())
        w['ref_vol'] = int(win['거래량'].astype(float).sum())
        w['ref_ready'] = True
        return True

    async def _register_codes(self, codes):
        """키움 실시간체결(0B) 등록 - 그룹당 REG_GROUP_SIZE 종목"""
        if not codes:
            return
        for idx in range(0, len(codes), self.REG_GROUP_SIZE):
            chunk = codes[idx:idx + self.REG_GROUP_SIZE]
            self.reg_seq += 1
            grp = str(self.reg_seq)
            for c in chunk:
                self.code_group[c] = grp
            await self.send_message({
                'trnm': 'REG',
                'grp_no': grp,
                'refresh': '1',
                'data': [{'item': chunk, 'type': ['0B']}],
            })
            await asyncio.sleep(0.2)

    async def _unregister_code(self, code):
        """돌파 알림 완료 종목 실시간 해제 (best-effort)"""
        grp = self.code_group.pop(code, None)
        if not grp:
            return
        try:
            await self.send_message({
                'trnm': 'REMOVE',
                'grp_no': grp,
                'data': [{'item': [code], 'type': ['0B']}],
            })
        except Exception as e:
            print(f"실시간 해제 오류 [{code}]: {e}")

    def _rolling_volume(self, w, now, acc_now):
        """(현재 창 거래량, 비교 임계값) 반환. 판정 불가 시 (None, 0)."""
        if not w['watch_start']:
            return None, 0
        elapsed = (now - w['watch_start']).total_seconds()
        if elapsed < self.WATCH_WARMUP_SEC:
            return None, 0  # 워밍업 중 - 거래량 판정 보류
        if elapsed >= self.WATCH_ROLL_SEC:
            # 완전한 롤링 10분 합: 현재 누적 - 10분 전 누적(추정)
            cutoff = now - timedelta(seconds=self.WATCH_ROLL_SEC)
            base_acc = None
            for ts, acc in w['acc_samples']:
                if ts <= cutoff:
                    base_acc = acc
                else:
                    break
            if base_acc is None:
                base_acc = w['acc_samples'][0][1]
            return acc_now - base_acc, w['ref_vol']
        # 감시 10분 미만 → 경과분 비례(pace) 임계값
        return acc_now - w['watch_start_acc'], w['ref_vol'] * (elapsed / self.WATCH_ROLL_SEC)

    async def on_real(self, datalist):
        """실시간 체결(0B) 수신 → 롤링 돌파 판정"""
        now = datetime.now()
        for d in datalist or []:
            if d.get('type') != '0B':
                continue
            code = d.get('item', '') or ''
            code = code[1:] if code.startswith('A') else code
            w = self.watch.get(code)
            if not w or w['notified']:
                continue

            vals = d.get('values', {}) or {}
            try:
                price = abs(int(float(vals.get('10') or 0)))   # 현재가
                acc_vol = int(float(vals.get('13') or 0))      # 누적거래량
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue

            # 누적거래량 샘플 적재 (롤링 10분 계산용)
            if w['watch_start'] is None:
                w['watch_start'] = now
                w['watch_start_acc'] = acc_vol
            w['acc_samples'].append((now, acc_vol))
            while w['acc_samples'] and (now - w['acc_samples'][0][0]).total_seconds() > self.WATCH_ROLL_SEC + 120:
                w['acc_samples'].popleft()

            if not w['ref_ready']:
                continue
            if price > w['cur_high']:
                w['cur_high'] = price
            if price <= w['ref_high']:
                continue  # 가격 미돌파

            roll_vol, threshold = self._rolling_volume(w, now, acc_vol)
            if roll_vol is None or roll_vol <= threshold:
                continue  # 거래량 미돌파

            await self._fire_breakout(code, w, price, roll_vol, threshold, now, vals)

    async def _fire_breakout(self, code, w, price, roll_vol, threshold, now, vals):
        """돌파 확정 - 텔레그램 알림 + DB 갱신 + 실시간 해제"""
        w['notified'] = True
        today = datetime.now().strftime('%Y%m%d')
        rate = safe_day_rate(vals.get('12'))
        safe_name = html.escape(w['name'].strip())
        message = (
            f"[{now.strftime('%H:%M')}] {safe_name}[<code>{code}</code>] "
            f"기준 10분 고가 : {w['ref_high']:,}원 돌파, 현재가 : {price:,}원, "
            f"최근10분 거래량 : {int(roll_vol):,} (기준 {int(threshold):,}), 등락율 : {rate}%"
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
                """, (now.strftime('%H%M'), int(price), now, code, today))
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"돌파 DB 갱신 오류 [{code}]: {e}")

        await self._unregister_code(code)

    # WebSocket 실행 (장중 상주 - 끊기면 재접속 후 감시 종목 재등록)
    async def run(self):
        MAX_RECONNECT_FAIL = 5  # 연속 실패 시 포기하고 종료(→ 중단 알림)
        while self.keep_running:
            self._session_ok = False
            session_start = datetime.now()
            try:
                await self.connect()
                await self.receive_messages()
            except Exception as e:
                print(f'run 예외: {e}')
                if not self.stop_reason:
                    self.stop_reason = f'실행 예외: {e}'

            if not self.keep_running:
                break

            # 장 마감 시각 이후면 재접속하지 않고 정상 종료
            if datetime.now().strftime('%H%M%S') >= self.WATCH_END_HHMMSS:
                print('장 마감 - 재접속 중단 및 종료')
                self.stop_reason = 'market_close'
                self.keep_running = False
                break

            # 재접속 실패 카운트: 로그인 성공 + 세션이 2분 이상 유지됐으면 정상으로 보고 리셋
            session_dur = (datetime.now() - session_start).total_seconds()
            if self._session_ok and session_dur >= 120:
                self.reconnect_fail = 0
            else:
                self.reconnect_fail += 1
            if self.reconnect_fail >= MAX_RECONNECT_FAIL:
                print(f'재접속 {self.reconnect_fail}회 연속 실패 - 프로세스 종료')
                if not self.stop_reason:
                    self.stop_reason = f'재접속 {self.reconnect_fail}회 연속 실패'
                self.keep_running = False
                break

            # 비정상 종료 → 재접속 대기
            self.connected = False
            print(f'연결 끊김({self.reconnect_fail}/{MAX_RECONNECT_FAIL}) - 5초 후 재접속 시도')
            await asyncio.sleep(5)
            # 재접속 시 실시간 재등록을 위해 상태 초기화
            self.reg_seq = 0
            self.code_group = {}
            self.watch_started = False
            self._pending_reregister = True

    async def _reregister_after_reconnect(self):
        """재접속 직후 감시 목록 전체를 다시 실시간 등록"""
        if not getattr(self, '_pending_reregister', False):
            return
        self._pending_reregister = False
        codes = [c for c, w in self.watch.items() if not w['notified']]
        if codes:
            print(f"재접속 실시간 재등록: {len(codes)}건")
            self.watch_started = True
            await self._register_codes(codes)

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
        await notify_fatal(bot_token, f'실행 오류: {e}')
    else:
        # 정상 장마감(market_close) 외의 사유로 끝났으면 재가동 버튼 알림
        if websocket_client.stop_reason and websocket_client.stop_reason != 'market_close':
            await notify_fatal(bot_token, websocket_client.stop_reason)
    finally:
        release_singleton_lock()
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

# asyncio로 프로그램을 실행합니다.
if __name__ == '__main__':
    # 영업일 확인 + 재가동 알림용 봇 토큰 로드 (스레드 진입 전 단일 사용)
    _conn_check = db.connect(conn_string)
    try:
        _is_business = is_business_day(today, _conn_check)
        _bt_cur = _conn_check.cursor()
        _bt_cur.execute("select bot_token1 from \"stockAccount_stock_account\" where nick_name = 'kwphills75'")
        _boot_bot_token = _bt_cur.fetchone()[0]
        _bt_cur.close()
    finally:
        _conn_check.close()

    _now_hms = datetime.now().strftime('%H%M%S')

    if not _is_business:
        print('영업일이 아니어서 종료합니다.')
    elif not (RUN_START_HHMMSS <= _now_hms <= RUN_END_HHMMSS):
        print(f'실행 허용 시간대(09:00~15:20)가 아니어서 종료합니다. (현재 {_now_hms[:2]}:{_now_hms[2:4]})')
    elif not acquire_singleton_lock():
        print('이미 실행 중이어서 재가동을 건너뜁니다.')
        try:
            asyncio.run(send_telegram_message(
                f"ℹ️ [{datetime.now().strftime('%H:%M:%S')}] 실시간 돌파 감시가 이미 실행 중이라 재가동을 건너뛰었습니다.",
                _boot_bot_token))
        except Exception:
            pass
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print('사용자 중단(KeyboardInterrupt)')
            release_singleton_lock()
        except Exception as e:
            print(f'비정상 종료: {e}')
            release_singleton_lock()
            try:
                asyncio.run(notify_fatal(_boot_bot_token, f'기동 실패: {e}'))
            except Exception:
                pass

