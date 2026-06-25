import re
import pandas as pd
from telegram.ext import MessageHandler, Filters
import requests
from datetime import datetime, timedelta
import psycopg2 as db
import kis_api_resp as resp
import sys
import math
import json
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from dateutil.relativedelta import relativedelta
from psycopg2.extras import execute_values
import time
import threading

URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

arguments = sys.argv

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)

def get_conn():
    global conn
    try:
        conn.isolation_level
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        conn = db.connect(conn_string)
    return conn                

cur001 = conn.cursor()
cur001.execute("select bot_token2 from \"stockAccount_stock_account\" where nick_name = %s", (arguments[1],))
result_001 = cur001.fetchone()
cur001.close()
token = result_001[0]    

# 해당 링크는 한국거래소에서 상장법인목록을 엑셀로 다운로드하는 링크입니다.
# 다운로드와 동시에 Pandas에 excel 파일이 load가 되는 구조입니다.
krx_url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
# requests로 먼저 가져오기, 인코딩 지정
krx_res = requests.get(krx_url, timeout=10)
krx_res.encoding = 'EUC-KR'  # KRX는 EUC-KR로 인코딩됨
# pandas로 읽기
stock_code = pd.read_html(krx_res.text, header=0)[0]
# 필요한 것은 "회사명"과 "종목코드" 이므로 필요없는 column들은 제외
stock_code = stock_code[['회사명', '종목코드']]
# 한글 컬럼명을 영어로 변경
stock_code = stock_code.rename(columns={'회사명': 'company', '종목코드': 'code'})

# 맨 앞 문자만 제거 후 필터링 함수
def filter_code(code):
    code = str(code).strip()
    # 맨 앞이 문자이면 제거
    if code and code[0].isalpha():
        code = code[1:]
    # 제거 후 길이가 1 이상이면 통과
    return len(code) > 0

stock_code = stock_code[stock_code['code'].apply(filter_code)]

# 종목코드 6자리로 포맷
def normalize_code(code):
    code = str(code).strip()
    if code and code[0].isalpha():
        code = code[1:]
    # 길이 맞춤
    if len(code) < 6:
        code = code.zfill(6)
    elif len(code) > 6:
        code = code[-6:]
    return code

stock_code['code'] = stock_code['code'].apply(normalize_code)

# ETF 목록 추가 (KRX ETF 상장 종목 — 일반 상장법인 목록에 미포함)
try:
    etf_url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType=etk'
    etf_res = requests.get(etf_url, timeout=10)
    etf_res.encoding = 'EUC-KR'
    etf_df = pd.read_html(etf_res.text, header=0)[0]
    etf_df = etf_df[['회사명', '종목코드']].rename(columns={'회사명': 'company', '종목코드': 'code'})
    etf_df = etf_df[etf_df['code'].apply(filter_code)]
    etf_df['code'] = etf_df['code'].apply(normalize_code)
    stock_code = pd.concat([stock_code, etf_df], ignore_index=True).drop_duplicates('code')
except Exception as e:
    print(f"ETF 목록 로드 실패 (무시됨): {e}")

# 텔레그램봇 updater(토큰, 입력값)
updater = Updater(token=token, use_context=True)
dispatcher = updater.dispatcher

menuNum = "0"
chartReq = "1"
g_market_buy_company = ""
g_market_sell_company = ""
g_market_buy_code = ""
g_market_sell_code = ""
g_market_buy_amount = 0
g_market_sell_amount = 0
g_buy_code = ""
g_sell_code = ""
g_code = ""
g_company = ""
g_order_no = ""
g_dvsn_cd = ""
g_buy_price = 0
g_buy_amount = 0
g_sell_price = 0
g_sell_amount = 0
g_revise_price = 0
g_remain_qty = 0
g_loss_price = 0
g_risk_sum = 0
g_low_price = 0
g_selected_accounts = []  # 계좌 다중 선택 목록
_pending_register = {}   # {chat_id: 관심종목 등록 대기 데이터}
g_tp_pending = {}        # {chat_id: trail_plan 설정 대기 데이터 (acct_no, code, trail_day, trail_dtm, trail_tp)}
g_nxt_pending = {}       # {chat_id: trail_nxt 처리용 계정 정보 (acct_no, access_token, app_key, app_secret, trail_day)}

# 매수주문 미리보기 → 진행 콜백 공유 상태
g_buy21_code = ""
g_buy21_company = ""
g_buy21_buy_price = 0
g_buy21_loss_price = 0
g_buy21_buy_qty = 0
g_buy21_buy_amt = 0
g_buy21_loss_buy_qty = 0     # 손절금액 기준 매수량
g_buy21_loss_buy_amt = 0     # 손절금액 기준 매수금액
g_buy21_amt_buy_qty = 0      # 매수금액 기준 매수량
g_buy21_amt_buy_amt = 0      # 매수금액 기준 매수금액

# 보유종목 매도 수량
g_holding_sell_qty = 0

# 보유종목 수정 선택 상태
g_holding_edit_code = ""
g_holding_edit_name = ""
g_holding_edit_field = ""   # 수정할 필드명 (이탈가/최종이탈가/목표가/최종목표가)

# 추적정보 전일저가 이탈 전량매도 상태
g_trail_sell_code = ""
g_trail_sell_name = ""
g_trail_sell_qty  = 0

# 신호 전량매도 상태
g_signal_sell_code = ""
g_signal_sell_name = ""
g_signal_sell_qty  = 0

# 관심종목 수정/신규 상태
g_interest_edit_code  = ""
g_interest_edit_name  = ""
g_interest_edit_field = ""   # 수정할 필드명 (1차저항가/1차지지가/2차저항가/2차지지가/추세상한가/추세이탈가)

# 추적상태 재개/멈춤 버튼 선택 상태
g_trail_state_code = ""
g_trail_state_name = ""
g_trail_state_acct_no = ""
g_trail_state_accounts = []   # trail_resume_ 콜백에서 조회된 (acct_no, nick_name, name, stop, target, exit) 리스트

g_fibo_code = ""   # 피보나치매도 선택 종목코드
g_fibo_name = ""   # 피보나치매도 선택 종목명

g_sell3x_code = ""      # 매도주문 선택 종목코드
g_sell3x_company = ""   # 매도주문 선택 종목명

g_corr_code = ""        # 주문정정 선택 종목코드
g_corr_company = ""     # 주문정정 선택 종목명
g_corr_dvsn = ""        # 주문정정 매매구분 ("01"=매도, "02"=매수)

g_cncl_code = ""        # 주문취소 선택 종목코드
g_cncl_company = ""     # 주문취소 선택 종목명
g_cncl_dvsn = ""        # 주문취소 매매구분 ("01"=매도, "02"=매수)

g_rsv_sell_code = ""    # 예약매도 선택 종목코드
g_rsv_sell_name = ""    # 예약매도 선택 종목명

g_rsv_corr_code = ""    # 예약정정 선택 종목코드
g_rsv_corr_name = ""    # 예약정정 선택 종목명
g_rsv_corr_dvsn = ""    # 예약정정 매매구분 ("01"=매도, "02"=매수)

g_rsv_cncl_code = ""    # 예약취소 선택 종목코드
g_rsv_cncl_name = ""    # 예약취소 선택 종목명
g_rsv_cncl_dvsn = ""    # 예약취소 매매구분 ("01"=매도, "02"=매수)

# 코스피/코스닥 변경 상태
g_kk_code  = ""   # '0001':코스피, '1001':코스닥
g_kk_name  = ""
g_kk_field = ""   # 수정할 필드명

# 추적등록(손절금액) 미리보기 → 진행 콜백 공유 상태
g_trail71_code = ""
g_trail71_company = ""
g_trail71_buy_price = 0
g_trail71_loss_price = 0
g_trail71_item_loss_sum = ""
g_trail71_buy_qty = 0
g_trail71_buy_amt = 0
g_trail71_year_day = ""
g_trail71_hour_minute = ""
g_trail71_loss_buy_qty = 0   # 손절금액 기준 매수량
g_trail71_loss_buy_amt = 0   # 손절금액 기준 매수금액
g_trail71_amt_buy_qty = 0    # 매수금액 기준 매수량
g_trail71_amt_buy_amt = 0    # 매수금액 기준 매수금액
_trail71_from_signal = False  # 신호 버튼에서 진입 시 True → acc_71_confirm에서 입력 단계 생략

# SELECTABLE_ACCOUNTS = ['phills2', 'phills75', 'yh480825', 'mamalong', 'phills13', 'phills15', 'chichipa', 'honeylong', 'worry106']  # 선택 가능 계좌 목록
SELECTABLE_ACCOUNTS = ['phills2', 'phills75', 'yh480825', 'mamalong', 'phills13', 'phills15', 'worry106']  # 선택 가능 계좌 목록

def format_number(value):
    try:
        return f"{float(value):,.2f}" if isinstance(value, float) else f"{int(value):,}"
    except Exception:
        return str(value)
    
def build_date_buttons1(days=7):
    today = datetime.now().date()
    conn = get_conn()
    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"sell_trace_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def build_date_buttons2(days=7):
    today = datetime.now().date()
    conn = get_conn()
    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"trace_delete_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def build_date_buttons3(days=7):
    today = datetime.now().date()
    conn = get_conn()
    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"trading_signal_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def _show_holding_edit_keyboard(query, context_bot=None):
    """보유종목 수정 종목 선택 키보드 표시 (보유종목_수정 / 뒤로가기 공용)"""
    ac = account(arguments[1])
    try:
        query.edit_message_text(text="[보유종목 수정] 조회 중...")
        c = stock_balance(ac['access_token'], ac['app_key'], ac['app_secret'], ac['acct_no'], "")
        if len(c.index) == 0:
            query.edit_message_text(text="보유종목이 없습니다.")
            return
        edit_buttons = []
        for i, _ in enumerate(c.index):
            h_code = c['pdno'][i]
            h_name = c['prdt_name'][i]
            if int(c['hldg_qty'][i]) > 0:
                edit_buttons.append(InlineKeyboardButton(f"{h_name}({h_code})", callback_data=f"menu,holding_edit_{h_code}"))
        rows = [edit_buttons[i:i+2] for i in range(0, len(edit_buttons), 2)]
        query.edit_message_text(text="수정할 종목을 선택하세요:", reply_markup=InlineKeyboardMarkup(rows))
    except Exception as e:
        query.edit_message_text(text=f"[보유종목 수정] 오류: {str(e)}")


def _show_interest_edit_keyboard(query):
    """관심종목 변경 종목 선택 키보드 표시 (관심종목_변경 / 뒤로가기 공용)"""
    try:
        query.edit_message_text(text="[관심종목 변경] 조회 중...")
        with get_conn().cursor() as cur_ii:
            cur_ii.execute("""
                SELECT name, code
                FROM public."interestItem_interest_item"
                WHERE proc_yn = 'Y' AND interest_day >= prev_business_day_char(CURRENT_DATE) AND length(code) > 4
                ORDER BY name
            """)
            rows = cur_ii.fetchall()
        ii_buttons = [
            InlineKeyboardButton(f"{r[0]}({r[1]})", callback_data=f"menu,interest_edit_{r[1]}")
            for r in rows
        ]
        ii_buttons.append(InlineKeyboardButton("신규 등록", callback_data="menu,interest_new"))
        rows_kb = [ii_buttons[i:i+2] for i in range(0, len(ii_buttons), 2)]
        query.edit_message_text(text="변경할 관심종목을 선택하거나 신규 등록하세요:", reply_markup=InlineKeyboardMarkup(rows_kb))
    except Exception as e:
        query.edit_message_text(text=f"[관심종목 변경] 오류: {str(e)}")


def build_date_buttons4(days=7):
    today = datetime.now().date()
    conn = get_conn()
    cur00 = conn.cursor()
    cur00.execute("SELECT holiday FROM stock_holiday")
    holidays = {row[0] for row in cur00.fetchall()}
    cur00.close()

    buttons = []
    cnt = 0
    offset = 0

    while cnt < days:
        d = today - timedelta(days=offset)
        date_str = d.strftime("%Y%m%d")
        offset += 1

        # 주말 제외
        if d.weekday() >= 5:
            continue

        # 휴장일 제외
        if date_str in holidays:
            continue

        buttons.append(
            InlineKeyboardButton(
                text=d.strftime("%Y-%m-%d"),
                callback_data=f"trading_trail_date:{d.strftime('%Y-%m-%d')}"
            )
        )
        cnt += 1

    return InlineKeyboardMarkup(build_menu(buttons, 2))

def build_menu(buttons, n_cols, header_buttons=None, footer_buttons=None):
    menu = [buttons[i:i + n_cols] for i in range(0, len(buttons), n_cols)]
    if header_buttons:
        menu.insert(0, header_buttons)
    if footer_buttons:
        menu.append(footer_buttons)
    return menu

def build_button(text_list, callback_header = "") : # make button list
    button_list = []
    text_header = callback_header
    if callback_header != "" :
        text_header += ","

    for text in text_list :
        button_list.append(InlineKeyboardButton(text, callback_data=text_header + text))

    return button_list

def get_command(update, context) :
    main_buttons = build_button(["매수주문", "매도주문", "주문정정", "주문취소",
                                 "전체예약", "예약주문", "예약정정", "예약취소", 
                                 "전체주문", "종목관리", "추적준비", "추적삭제", 
                                 "추적등록", "추적변경", "추적상태", "매매추적",
                                 "매수손실금액", "피보나치매도", "코스피", "코스닥"], callback_header="menu")
    cancel_button = build_button(["취소"], callback_header="menu")
    show_markup = InlineKeyboardMarkup(build_menu(main_buttons, n_cols=4, footer_buttons=cancel_button))
    
    update.message.reply_text("메뉴를 선택하세요", reply_markup=show_markup) # reply text with markup

def start(update, context) :
    chat_id = update.effective_chat.id
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE \"stockAccount_stock_account\"
        SET chat_id = %s
        WHERE nick_name = %s
    """, (chat_id, arguments[1]))
    conn.commit()
    cur.close()

    context.bot.send_message(
        chat_id=chat_id,
        text="텔레그램 chat_id 등록이 완료되었습니다."
    )

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

# 계정정보 조회
def account(nickname=None):
    conn = get_conn()
    cur01 = conn.cursor()
    if nickname is None:
        cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id from \"stockAccount_stock_account\" where nick_name = %s", (arguments[1],))
    else:
        cur01.execute("select acct_no, access_token, app_key, app_secret, token_publ_date, substr(token_publ_date, 0, 9) AS token_day, bot_token1, bot_token2, chat_id from \"stockAccount_stock_account\" where nick_name = %s", (nickname,))        
    result_two = cur01.fetchone()
    cur01.close()

    acct_no = result_two[0]
    access_token = result_two[1]
    app_key = result_two[2]
    app_secret = result_two[3]
    bot_token1 = result_two[6]
    bot_token2 = result_two[7]
    chat_id = result_two[8] 
    today = datetime.now().strftime("%Y%m%d")

    YmdHMS = datetime.now()
    validTokenDate = datetime.strptime(result_two[4], '%Y%m%d%H%M%S')
    diff = YmdHMS - validTokenDate
    # print("diff : " + str(diff.days))
    if diff.days >= 1 or result_two[5] != today:  # 토큰 유효기간(1일) 만료 재발급
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

    account_rtn = {'acct_no':acct_no, 'access_token':access_token, 'app_key':app_key, 'app_secret':app_secret, 'bot_token1':bot_token1, 'bot_token2':bot_token2, 'chat_id':chat_id}

    return account_rtn

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

def is_nxt_able(access_token, app_key, app_secret, code):
    """해당 종목의 NXT 거래가능 여부 반환 (nxt_able_yn == 'Y')"""
    try:
        output = inquire_price(access_token, app_key, app_secret, code)
        return output is not None and output.get('nxt_able_yn', 'N') == 'Y'
    except Exception:
        return False

# 주식현재가 일자별
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
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price"

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

    res = requests.get(url, headers=headers, params=params, timeout=10)
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

    # trade_date 종가
    return int(day_df.iloc[0]["stck_clpr"])

def get_period_high_low(access_token, app_key, app_secret, stock_code, period="D", count=30):
    """KIS 일/주/월봉 API로 period 내 최고가·최저가 반환. period: D/W/M"""
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST01010400",
        "custtype": "P",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_PERIOD_DIV_CODE": period,
        "FID_ORG_ADJ_PRC": "1",
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    data = res.json()
    if "output" not in data or not data["output"]:
        return None, None
    df = pd.DataFrame(data["output"]).head(count)
    high = int(df["stck_hgpr"].astype(int).max())
    low  = int(df["stck_lwpr"].astype(int).min())
    return high, low

# 매수 가능(현금) 조회
def inquire_psbl_order(access_token, app_key, app_secret, acct_no):
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC8908R"             # tr_id : TTTC8908R[실전투자], VTTC8908R[모의투자]
    }            
    params = {
               "CANO": acct_no,
               "ACNT_PRDT_CD": "01",
               "PDNO": "",                      # 종목번호(6자리)
               "ORD_UNPR": "0",                 # 1주당 가격
               "ORD_DVSN": "02",                # 02 : 조건부지정가
               "CMA_EVLU_AMT_ICLD_YN": "Y",     # CMA평가금액포함여부
               "OVRS_ICLD_YN": "N"              # 해외포함여부
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)

    return ar.getBody().output['nrcvb_buy_amt']

# 주문정보 존재시 취소 처리
def order_cancel_proc(access_token, app_key, app_secret, acct_no, code, sell_buy_dvsn_cd):
    
    result_msgs = []

    try:
        # 일별주문체결 조회
        output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, code, '', sell_buy_dvsn_cd)

        if len(output1) > 0:
        
            tdf = pd.DataFrame(output1)
            d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
            order_no = 0

            for i, name in enumerate(d.index):

                # 매도매수구분코드 일치시
                if d['sll_buy_dvsn_cd'][i] == sell_buy_dvsn_cd:
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

def get_excg_id():
    """정규시장(09:00~15:30)이면 KRX, 그 외 시간이면 NXT 반환"""
    t = datetime.now().strftime('%H%M')
    return "KRX" if '0900' <= t < '1530' else "NXT"

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
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    if not ar.isOK():
        raise Exception(f"[{ar.getBody().msg_cd}] {ar.getBody().msg1}")
    return ar.getBody().output

# 일별주문체결 조회
def daily_order_complete(access_token, app_key, app_secret, acct_no, code, order_no, sell_buy_dvsn_cd):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "TTTC0081R",                                # (3개월이내) TTTC0081R, (3개월이전) CTSC9215R
               "custtype": "P"
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "INQR_STRT_DT": datetime.now().strftime('%Y%m%d'),  # 조회시작일자 YYYYMMDD
                "INQR_END_DT": datetime.now().strftime('%Y%m%d'),   # 조회종료일자 YYYYMMDD
                "SLL_BUY_DVSN_CD": sell_buy_dvsn_cd,                # 매도매수구분코드 : 00 전체, 01 매도, 02 매수
                "PDNO": code,
                "ORD_GNO_BRNO": "",
                "ODNO": order_no,
                "CCLD_DVSN": "00",                                  # 체결구분 : 00 전체, 01 체결, 02 미체결
                "INQR_DVSN": '00',                                  # 조회구분 : 00 역순, 01 정순
                "INQR_DVSN_1": "",        
                "INQR_DVSN_3": "00",                                # 조회구분3 : 00 전체, 01 현금, 02 신용
                "EXCG_ID_DVSN_CD": "ALL",                           # 거래소ID구분코드 KRX : KRX, NXT : NXT, SOR (Smart Order Routing) : SOR, ALL : 전체
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": ""
    }
    PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output1

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
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    if ar.isOK():
        return ar.getBody().output
    else:
        print(f"주문정정취소 API 오류: {ar.getErrorCode()} {ar.getErrorMessage()}")
        return None

# 주식예약주문 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve(access_token, app_key, app_secret, acct_no, code, ord_qty, ord_price, trade_cd, ord_dvsn_cd, reserve_end_dt):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0008U",                                # tr_id : CTSC0008U(국내예약매수입력/주문예약매도입력)
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PDNO": code,
                "ORD_QTY": ord_qty,                                 # 주문주식수
                "ORD_UNPR": ord_price,                              # 주문단가 : 시장가인 경우 0
                "SLL_BUY_DVSN_CD": trade_cd,                        # 매도매수구분코드 : 01 매도, 02 매수
                "ORD_DVSN_CD": ord_dvsn_cd,                         # 주문구분코드 : 00 지정가, 01 시장가, 02 조건부지정가, 05 장전 시간외
                "ORD_OBJT_CBLC_DVSN_CD":"10",                       # 주문대상잔고구분코드 : 10 현금
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자 : 현재일자 이후 8자리(YYYYMMDD), 미입력시 다음날 주문 처리되고 예약주문 종료, 익영업일부터 최대 30일까지 입력 가능
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    if not ar.isOK():
        raise Exception(f"{ar.getErrorCode()} {ar.getErrorMessage()}")
    return ar.getBody().output

# 주식예약주문정정취소 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve_cancel_revice(access_token, app_key, app_secret, acct_no, reserve_cd, code, ord_qty, ord_price, trade_cd, ord_dvsn_cd, reserve_end_dt, rsvn_ord_seq):

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0013U" if reserve_cd == "01" else "CTSC0009U", # tr_id : CTSC0013U 예약정정, CTSC0009U 예약최소
    }  
    params = {
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PDNO": code,
                "ORD_QTY": ord_qty,                                 # 주문주식수
                "ORD_UNPR": ord_price,                              # 주문단가 : 시장가인 경우 0
                "SLL_BUY_DVSN_CD": trade_cd,                        # 매도매수구분코드 : 01 매도, 02 매수
                "ORD_DVSN_CD": ord_dvsn_cd,                         # 주문구분코드 : 00 지정가, 01 시장가, 02 조건부지정가, 05 장전 시간외
                "ORD_OBJT_CBLC_DVSN_CD":"10",                       # 주문대상잔고구분코드 : 10 현금
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자 : 현재일자 이후 8자리(YYYYMMDD), 미입력시 다음날 주문 처리되고 예약주문 종료, 익영업일부터 최대 30일까지 입력 가능
                "RSVN_ORD_SEQ": rsvn_ord_seq                        # 예약주문순번
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv-rvsecncl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, data=json.dumps(params), headers=headers, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    if not ar.isOK():
        raise Exception(f"{ar.getErrorCode()} {ar.getErrorMessage()}")
    return ar.getBody().output

# 주식예약주문조회 : 15시 40분 ~ 다음 영업일 07시 30분까지 가능(23시 40분 ~ 0시 10분까지 서버초기화 작업시간 불가)
def order_reserve_complete(access_token, app_key, app_secret, reserve_strt_dt, reserve_end_dt, acct_no, code):

    # 현재 시간이 15:40 이후인지 체크
    now = datetime.now()
    cutoff = now.replace(hour=15, minute=40, second=0, microsecond=0)

    # reserve_strt_dt 문자열 → datetime 변환
    try:
        start_dt = datetime.strptime(reserve_strt_dt, "%Y%m%d")
    except ValueError:
        raise ValueError("reserve_strt_dt 는 YYYYMMDD 형식이어야 합니다.")

    # 현재 날짜와 reserve_strt_dt 날짜가 같고, 현재 시간이 15:40 이후라면 다음날로 변경
    if now.date() == start_dt.date() and now > cutoff:
        start_dt = start_dt + timedelta(days=1)
        reserve_strt_dt = start_dt.strftime("%Y%m%d")

    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {access_token}",
               "appKey": app_key,
               "appSecret": app_secret,
               "tr_id": "CTSC0004R",                                # tr_id : CTSC0004R
    }  
    params = {
                "RSVN_ORD_ORD_DT": reserve_strt_dt,                 # 예약주문시작일자
                "RSVN_ORD_END_DT": reserve_end_dt,                  # 예약주문종료일자
                "RSVN_ORD_SEQ": "",                                 # 예약주문순번
                "TMNL_MDIA_KIND_CD": "00",
                "CANO": acct_no,
                "ACNT_PRDT_CD": '01',
                "PRCS_DVSN_CD": "0",                                # 처리구분코드 : 전체 0, 처리내역 1, 미처리내역 2
                "CNCL_YN": "Y",                                     # 취소여부 : 'Y'
                "PDNO": code if code != "" else "",                 # 종목코드 : 공백 입력 시 전체 조회
                "SLL_BUY_DVSN_CD": "",                              # 매도매수구분코드 : 01 매도, 02 매수        
                "CTX_AREA_FK200": "",                               
                "CTX_AREA_NK200": "",                               
    }
    PATH = "uapi/domestic-stock/v1/trading/order-resv-ccnl"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    #ar.printAll()
    return ar.getBody().output

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
    URL = f"{URL_BASE}/{PATH}"

    # rate-limit / 일시오류 대응: isOK 체크 + 짧은 backoff 재시도 (최대 3회)
    # (rt_cd만 있고 output1/output2가 없는 응답에서 AttributeError 발생 방지)
    ar = None
    for attempt in range(3):
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        ar = resp.APIResp(res)
        if ar.isOK():
            body = ar.getBody()
            if rtFlag == "all":
                output = getattr(body, 'output2', [])
            else:
                output = getattr(body, 'output1', [])
            if isinstance(output, list):
                return pd.DataFrame(output)
            return pd.DataFrame([])
        time.sleep(0.3 * (attempt + 1))

    print(f"⚠️ stock_balance 응답 오류 (acct_no={acct_no}): {ar.getErrorCode()} {ar.getErrorMessage()}")
    return pd.DataFrame([])    

def is_positive_int(val: str) -> bool:
    """양수 정수만 허용 (1~100 범위)"""
    if val.isdigit():
        num = int(val)
        return 0 < num <= 100
    return False    

def post_business_day_char(business_day:str):
    conn = get_conn()
    cur100 = conn.cursor()
    cur100.execute("select post_business_day_char(%s::date)", (business_day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def get_previous_business_day(day):
    conn = get_conn()
    cur100 = conn.cursor()
    cur100.execute("select prev_business_day_char(%s)", (day,))
    result_one00 = cur100.fetchall()
    cur100.close()

    return result_one00[0][0]

def show_account_selection_keyboard(query, menu_num, send_new=False, chat_id=None, bot=None):
    """계좌 다중 선택 인라인 키보드를 표시한다. ✅/⬜ 토글 방식."""
    current_acc = arguments[1] if len(arguments) > 1 else ""
    extra = [current_acc] if current_acc and current_acc not in SELECTABLE_ACCOUNTS else []
    all_accounts = extra + SELECTABLE_ACCOUNTS
    all_checked = len(g_selected_accounts) == len(all_accounts) and all(a in g_selected_accounts for a in all_accounts)
    buttons = [[InlineKeyboardButton(
        f"{'✅' if all_checked else '⬜'} 전체선택",
        callback_data=f"acc_{menu_num}_all"
    )]]
    row = []
    for acc in all_accounts:
        check = "✅" if acc in g_selected_accounts else "⬜"
        row.append(InlineKeyboardButton(f"{check} {acc}", callback_data=f"acc_{menu_num}_t_{acc}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("✔ 확인", callback_data=f"acc_{menu_num}_confirm")])
    if send_new and bot and chat_id:
        bot.send_message(chat_id=chat_id, text="처리할 계좌를 선택하세요 (복수 선택 가능):", reply_markup=InlineKeyboardMarkup(buttons))
    else:
        query.edit_message_text(text="처리할 계좌를 선택하세요 (복수 선택 가능):", reply_markup=InlineKeyboardMarkup(buttons))

def _do_interest_register(chat_id, context, pending):
    try:
        c_reg = get_conn()
        now = datetime.now()
        interest_day = now.strftime('%Y%m%d')
        interest_dtm = now.strftime('%H%M%S')
        with c_reg.cursor() as cur_reg:
            cur_reg.execute("""
                UPDATE public."interestItem_interest_item"
                SET name             = %s,
                    through_price    = %s,
                    leave_price      = %s,
                    resist_price     = %s,
                    support_price    = %s,
                    trend_high_price = %s,
                    trend_low_price  = %s,
                    last_chg_date    = %s
                WHERE acct_no = %s AND code = %s AND interest_day = %s AND proc_yn = 'Y'
            """, (pending['name'],
                  pending['through_price'], pending['leave_price'],
                  pending['d20_high'], pending['d20_low'],
                  pending['y1_high'], pending['y1_low'],
                  now,
                  pending['acct_reg'], pending['code'], interest_day))
            if cur_reg.rowcount == 0:
                cur_reg.execute("""
                    INSERT INTO public."interestItem_interest_item"
                        (acct_no, code, name, through_price, leave_price, resist_price, support_price,
                         trend_high_price, trend_low_price, interest_day, interest_dtm, proc_yn, last_chg_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Y', %s)
                """, (pending['acct_reg'], pending['code'], pending['name'],
                      pending['through_price'], pending['leave_price'],
                      pending['d20_high'], pending['d20_low'],
                      pending['y1_high'], pending['y1_low'],
                      interest_day, interest_dtm, now))
        c_reg.commit()
        context.bot.send_message(
            chat_id=chat_id,
            text=(f"✅ [{pending['name']}(<code>{pending['code']}</code>)] 관심종목 등록 완료\n"
                  f"  1차저항가: {format(pending['through_price'], ',d')}원\n"
                  f"  1차지지가: {format(pending['leave_price'], ',d')}원\n"
                  f"  2차저항가(20일고가): {format(pending['d20_high'], ',d')}원\n"
                  f"  2차지지가(20일저가): {format(pending['d20_low'], ',d')}원\n"
                  f"  추세상한가(1년고가): {format(pending['y1_high'], ',d')}원\n"
                  f"  추세이탈가(1년저가): {format(pending['y1_low'], ',d')}원"),
            parse_mode='HTML'
        )
    except Exception as e:
        context.bot.send_message(chat_id=chat_id, text=f"[관심종목 등록] 오류: {str(e)}")

def callback_get(update, context) :
    data_selected = update.callback_query.data
    query = update.callback_query

    command = data_selected.split(",")[-1] if "," in data_selected else data_selected

    global menuNum
    global g_order_no
    global g_remain_qty
    global g_selected_accounts
    global g_holding_edit_field
    global g_interest_edit_field
    global g_trail_state_code, g_trail_state_name, g_trail_state_acct_no, g_trail_state_accounts
    global g_fibo_code, g_fibo_name
    global g_sell3x_code, g_sell3x_company
    global g_corr_code, g_corr_company, g_corr_dvsn
    global g_cncl_code, g_cncl_company, g_cncl_dvsn
    global g_rsv_sell_code, g_rsv_sell_name
    global g_rsv_corr_code, g_rsv_corr_name, g_rsv_corr_dvsn
    global g_rsv_cncl_code, g_rsv_cncl_name, g_rsv_cncl_dvsn

    print("command : ", command)
    if command.startswith("interest_confirm_"):
        # 현재값으로 등록
        chat_id = query.message.chat_id
        pending = _pending_register.pop(chat_id, None)
        try:
            query.answer()
        except Exception:
            pass
        if pending is None:
            context.bot.send_message(chat_id=chat_id, text="등록 정보가 만료됐습니다. 다시 시도해주세요.")
            return
        try:
            query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        _do_interest_register(chat_id, context, pending)

    elif command.startswith("interest_manual_"):
        # 직접입력 요청
        chat_id = query.message.chat_id
        pending = _pending_register.get(chat_id)
        try:
            query.answer()
        except Exception:
            pass
        if pending is None:
            context.bot.send_message(chat_id=chat_id, text="등록 정보가 만료됐습니다. 다시 시도해주세요.")
            return
        pending['waiting_input'] = True
        try:
            query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        context.bot.send_message(chat_id=chat_id, text="1차저항가(금일고가:0),1차지지가(금일저가:0)을 입력하세요")

    elif command.startswith("interest_trail_buy_"):
        # 돌파 신호 텔레그램 버튼 → 추적매수 미리보기(계좌선택 후 손절금액/매수금액 선택)
        tail = command[len("interest_trail_buy_"):]
        parts_tb = tail.split("_")
        if len(parts_tb) != 5:
            try: query.answer("파라미터 오류", show_alert=True)
            except Exception: pass
            return
        tb_code    = parts_tb[0]
        tb_price   = int(parts_tb[1])
        tb_loss    = int(parts_tb[2])
        tb_buy_amt = int(parts_tb[3])
        tb_item_loss = int(parts_tb[4])
        match_tb = stock_code[stock_code.code == tb_code]
        tb_company = match_tb.company.values[0].strip() if len(match_tb) > 0 else tb_code
        try: query.answer()
        except Exception: pass
        if tb_price <= tb_loss:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[{tb_company}] 매수가({format(tb_price,',d')})가 이탈가({format(tb_loss,',d')}) 이하 — 추적매수 불가"
            )
            return
        loss_buy_qty_tb = int(round(tb_item_loss / (tb_price - tb_loss)))
        loss_buy_amt_tb = tb_price * loss_buy_qty_tb
        amt_buy_qty_tb  = int(round(tb_buy_amt / tb_price)) if tb_price > 0 else 0
        amt_buy_amt_tb  = tb_price * amt_buy_qty_tb
        global g_trail71_code, g_trail71_company, g_trail71_buy_price, g_trail71_loss_price
        global g_trail71_item_loss_sum, g_trail71_buy_qty, g_trail71_buy_amt
        global g_trail71_year_day, g_trail71_hour_minute
        global g_trail71_loss_buy_qty, g_trail71_loss_buy_amt
        global g_trail71_amt_buy_qty, g_trail71_amt_buy_amt, _trail71_from_signal
        g_trail71_code          = tb_code
        g_trail71_company       = tb_company
        g_trail71_buy_price     = tb_price
        g_trail71_loss_price    = tb_loss
        g_trail71_item_loss_sum = tb_item_loss
        g_trail71_buy_qty       = 0
        g_trail71_buy_amt       = 0
        g_trail71_loss_buy_qty  = loss_buy_qty_tb
        g_trail71_loss_buy_amt  = loss_buy_amt_tb
        g_trail71_amt_buy_qty   = amt_buy_qty_tb
        g_trail71_amt_buy_amt   = amt_buy_amt_tb
        g_trail71_year_day      = datetime.now().strftime("%Y%m%d")
        g_trail71_hour_minute   = datetime.now().strftime('%H%M%S')
        _trail71_from_signal    = True
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "71")

    elif command == "취소":
        context.bot.edit_message_text(text="취소하였습니다.",
                                      chat_id=query.message.chat_id,
                                      message_id=query.message.message_id)
        return

    elif command.startswith("holding_") and not command.startswith("holding_edit_") and command != "holding_plan_toggle" and command != "holding_back":
        # 보유종목 종목 선택 → 기준계좌 잔고로 종목명 확인 후 계좌 선택 키보드 표시
        h_code = command[len("holding_"):]
        ac_h = account(arguments[1])
        try:
            c_h = stock_balance(ac_h['access_token'], ac_h['app_key'], ac_h['app_secret'], ac_h['acct_no'], "")
            matched = [(c_h['prdt_name'][i], int(c_h['hldg_qty'][i]), int(c_h['prpr'][i]))
                       for i, _ in enumerate(c_h.index) if c_h['pdno'][i] == h_code]
            h_name = matched[0][0] if matched else h_code
            h_price = matched[0][2] if matched else 0
            global g_holding_sell_code, g_holding_sell_name, g_holding_sell_price
            g_holding_sell_code = h_code
            g_holding_sell_name = h_name
            g_holding_sell_price = h_price
            g_selected_accounts.clear()
            show_account_selection_keyboard(query, "01", send_new=True, chat_id=query.message.chat_id, bot=context.bot)
        except Exception as e:
            query.edit_message_text(text=f"[보유종목 매도] 조회 오류: {str(e)}")

    elif command == "종목관리":
        sub_buttons = [
            InlineKeyboardButton("보유종목 조회", callback_data="menu,보유종목_조회"),
            InlineKeyboardButton("보유종목 수정", callback_data="menu,보유종목_수정"),
            InlineKeyboardButton("관심종목 조회", callback_data="menu,관심종목_조회"),
            InlineKeyboardButton("관심종목 변경", callback_data="menu,관심종목_변경"),
            InlineKeyboardButton("취소",          callback_data="menu,취소"),
        ]
        query.edit_message_text(
            text="종목관리 메뉴를 선택하세요:",
            reply_markup=InlineKeyboardMarkup(build_menu(sub_buttons, 2))
        )

    elif command == "보유종목_조회":

        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[보유종목 조회]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

            result_msgs = []
            # 계좌잔고 조회 (전체 요약)
            b = stock_balance(access_token, app_key, app_secret, acct_no, "all")

            # market_ratio 조회
            market_ratio = None
            try:
                with get_conn().cursor() as cur_mr:
                    cur_mr.execute(
                        'SELECT market_ratio FROM public."stockFundMng_stock_fund_mng" WHERE acct_no = %s',
                        (str(acct_no),)
                    )
                    row_mr = cur_mr.fetchone()
                    if row_mr:
                        market_ratio = float(row_mr[0])
            except Exception as mr_e:
                print(f"market_ratio 조회 오류: {mr_e}")

            # b에서 계좌 수준 값 추출 (가수도 정산금액, 전일증감은 계좌 전체 기준 유지)
            u_prvs_rcdl_excc_amt = 0
            u_asst_icdc_amt = 0
            for i, _ in enumerate(b.index):
                u_prvs_rcdl_excc_amt = int(b['prvs_rcdl_excc_amt'][i])
                u_asst_icdc_amt = int(b['asst_icdc_amt'][i])

            # 개별 종목 조회
            c = stock_balance(access_token, app_key, app_secret, acct_no, "")

            # stockBalance_stock_balance 신호가 조회 (code → 목표가/이탈가/최종목표가/최종이탈가/trading_plan)
            sb_map = {}
            try:
                with get_conn().cursor() as cur_sb:
                    cur_sb.execute(
                        """SELECT code, sign_resist_price, sign_support_price, end_target_price, end_loss_price, trading_plan
                           FROM public."stockBalance_stock_balance"
                           WHERE acct_no = %s AND proc_yn = 'Y'""",
                        (str(acct_no),)
                    )
                    for row in cur_sb.fetchall():
                        sb_map[row[0]] = (row[1], row[2], row[3], row[4], row[5])
            except Exception as sb_e:
                print(f"신호가 조회 오류: {sb_e}")

            # i/h 제외한 종목 평가금액 합산 (총평가금액, 잔고금액 기준)
            filtered_scts_evlu = sum(
                int(c['evlu_amt'][i])
                for i, _ in enumerate(c.index)
                if int(c['hldg_qty'][i]) > 0 and sb_map.get(c['pdno'][i], (None,)*5)[4] not in ('i', 'h')
            )
            filtered_tot_evlu = u_prvs_rcdl_excc_amt + filtered_scts_evlu

            # 시장비율 / 현재비율 계산
            current_ratio = 100 - (u_prvs_rcdl_excc_amt / filtered_tot_evlu * 100) if filtered_tot_evlu > 0 else 0.0
            need_sell = False
            sell_pct = 0.0
            if market_ratio is not None and current_ratio > market_ratio and filtered_scts_evlu > 0:
                sell_pct = (current_ratio / 100 - market_ratio / 100) * filtered_tot_evlu / filtered_scts_evlu * 100
                need_sell = True

            # 요약 메시지 (i/h 제외 기준)
            mr_str = ""
            if market_ratio is not None:
                mr_str = f", 시장비율:{market_ratio:.0f}%, 현재비율:{current_ratio:.1f}%"
            result_msgs.append(
                f"* 총 평가금액:{format(filtered_tot_evlu, ',d')}원, 잔고금액:{format(filtered_scts_evlu, ',d')}원, "
                f"가정산금:{format(u_prvs_rcdl_excc_amt, ',d')}원, 전일증감:{format(u_asst_icdc_amt, ',d')}원{mr_str}"
            )

            # 개별 종목 메시지
            for i, _ in enumerate(c.index):
                code = c['pdno'][i]
                name = c['prdt_name'][i]
                purchase_price = c['pchs_avg_pric'][i]
                purchase_amount = int(c['hldg_qty'][i])
                purchase_sum = int(c['pchs_amt'][i])
                current_price = int(c['prpr'][i])
                eval_sum = int(c['evlu_amt'][i])
                earnings_rate = c['evlu_pfls_rt'][i]
                valuation_sum = int(c['evlu_pfls_amt'][i])
                ord_psbl_qty = int(c['ord_psbl_qty'][i])

                sb = sb_map.get(code, (None, None, None, None, None))
                signal_str = ""
                if sb[0]: signal_str += f", 목표가:{format(int(sb[0]), ',d')}원"
                if sb[1]: signal_str += f", 이탈가:{format(int(sb[1]), ',d')}원"
                if sb[2]: signal_str += f", 최종목표가:{format(int(sb[2]), ',d')}원"
                if sb[3]: signal_str += f", 최종이탈가:{format(int(sb[3]), ',d')}원"
                if sb[4] == 'i':
                    name = f"(투자) {name}"
                elif sb[4] == 'h':
                    name = f"(홀딩) {name}"

                sell_qty_str = ""
                if need_sell and sb_map.get(code, (None,)*5)[4] not in ('i', 'h'):
                    sell_qty = min(round(purchase_amount * sell_pct / 100), ord_psbl_qty)
                    if sell_qty > 0:
                        sell_qty_str = f"(매도:{format(sell_qty, ',d')}주)"
                msg = (f"* {name}[<code>{code}</code>] 단가:{format(float(purchase_price), ',.2f')}원, "
                       f"보유량:{format(purchase_amount, ',d')}주{sell_qty_str}, 보유금액:{format(purchase_sum, ',d')}원, "
                       f"현재가:{format(current_price, ',d')}원, 평가금액:{format(eval_sum, ',d')}원, "
                       f"수익률:{str(earnings_rate)}%, 손수익금액:{format(valuation_sum, ',d')}원{signal_str}")
                result_msgs.append(msg)

            if result_msgs:
                # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                chunk_size = 10
                chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                for idx, chunk in enumerate(chunks):
                    final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용

                    if idx == 0:
                        # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                        context.bot.edit_message_text(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id,
                            message_id=query.message.message_id
                        )
                    else:
                        # 두 번째 묶음부터는 새로운 메시지로 전송
                        context.bot.send_message(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id
                        )

                # 보유종목 매도 선택 버튼 (종목별 1개씩)
                if len(c.index) > 0:
                    hold_buttons = []
                    for i, name in enumerate(c.index):
                        h_code = c['pdno'][i]
                        h_name = c['prdt_name'][i]
                        h_qty = int(c['hldg_qty'][i])
                        if h_qty > 0:
                            sb = sb_map.get(h_code, (None, None, None, None, None))
                            is_invest = sb[4] in ('i', 'h')
                            btn_label = f"{h_name}"
                            if need_sell and not is_invest:
                                btn_label += f" [{sell_pct:.0f}%]"
                            hold_buttons.append(
                                InlineKeyboardButton(btn_label, callback_data=f"menu,holding_{h_code}")
                            )
                    if hold_buttons:
                        hold_buttons.append(InlineKeyboardButton("취소", callback_data="취소"))
                        # 한 행에 2개씩
                        rows = [hold_buttons[i:i+2] for i in range(0, len(hold_buttons), 2)]
                        context.bot.send_message(
                            text="매도할 종목을 선택하세요:",
                            chat_id=query.message.chat_id,
                            reply_markup=InlineKeyboardMarkup(rows)
                        )
            else:
                context.bot.edit_message_text(
                    text="보유종목 조회 대상이 존재하지 않습니다.",
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )

        except Exception as e:
            print('보유종목 조회 오류.', e)
            context.bot.edit_message_text(text="[보유종목 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

    elif command in ("보유종목_수정", "holding_back"):
        initMenuNum()
        g_holding_edit_field = ""
        _show_holding_edit_keyboard(query)

    elif command == "관심종목_조회":
        try:
            query.edit_message_text(text="[관심종목 조회] 조회 중...")
            with get_conn().cursor() as cur_ii:
                cur_ii.execute("""
                    SELECT name, code, through_price, leave_price, resist_price, support_price,
                           trend_high_price, trend_low_price
                    FROM public."interestItem_interest_item"
                    WHERE proc_yn = 'Y' AND interest_day >= prev_business_day_char(CURRENT_DATE) AND length(code) > 4
                    ORDER BY name
                """)
                rows = cur_ii.fetchall()
            if not rows:
                query.edit_message_text(text="[관심종목 조회] 조회된 종목이 없습니다.")
            else:
                result_msgs = []
                for row in rows:
                    ii_name, ii_code, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price = row
                    parts = []
                    if through_price:   parts.append(f"1차저항가:{format(int(through_price), ',d')}원")
                    if leave_price:     parts.append(f"1차지지가:{format(int(leave_price), ',d')}원")
                    if resist_price:    parts.append(f"2차저항가:{format(int(resist_price), ',d')}원")
                    if support_price:   parts.append(f"2차지지가:{format(int(support_price), ',d')}원")
                    if trend_high_price: parts.append(f"추세상한가:{format(int(trend_high_price), ',d')}원")
                    if trend_low_price:  parts.append(f"추세이탈가:{format(int(trend_low_price), ',d')}원")
                    price_str = ", ".join(parts)
                    msg = f"* {ii_name}[<code>{ii_code}</code>]" + (f"\n  {price_str}" if price_str else "")
                    result_msgs.append(msg)
                chunk_size = 10
                chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]
                for idx, chunk in enumerate(chunks):
                    text = "\n\n".join(chunk)
                    if idx == 0:
                        context.bot.edit_message_text(
                            text=text, parse_mode='HTML',
                            chat_id=query.message.chat_id,
                            message_id=query.message.message_id
                        )
                    else:
                        context.bot.send_message(
                            text=text, parse_mode='HTML',
                            chat_id=query.message.chat_id
                        )

                # 관심종목 삭제 선택 버튼
                del_buttons = [
                    InlineKeyboardButton(f"{r[0]}({r[1]})", callback_data=f"interest_del_{r[1]}")
                    for r in rows
                ]
                del_buttons.append(InlineKeyboardButton("취소", callback_data="취소"))
                del_rows = [del_buttons[i:i+2] for i in range(0, len(del_buttons), 2)]
                context.bot.send_message(
                    text="삭제할 관심 종목을 선택하세요:",
                    chat_id=query.message.chat_id,
                    reply_markup=InlineKeyboardMarkup(del_rows)
                )
        except Exception as e:
            query.edit_message_text(text=f"[관심종목 조회] 오류: {str(e)}")

    elif command.startswith("interest_del_"):
        del_code = command[len("interest_del_"):]
        try:
            # 클릭된 버튼만 제거, 나머지 유지
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != command
            ]
            # 취소 버튼만 남은 경우도 메시지 삭제
            remaining_no_cancel = [b for b in remaining if b.callback_data != "취소"]
            if remaining_no_cancel:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup(
                        [remaining[i:i+2] for i in range(0, len(remaining), 2)]
                    )
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )

            with get_conn().cursor() as cur_del:
                cur_del.execute(
                    """SELECT name FROM public."interestItem_interest_item"
                       WHERE code = %s AND proc_yn = 'Y'""",
                    (del_code,)
                )
                row_del = cur_del.fetchone()
            del_name = row_del[0] if row_del else del_code

            with get_conn().cursor() as cur_upd:
                cur_upd.execute(
                    """UPDATE public."interestItem_interest_item"
                       SET proc_yn = 'N'
                       WHERE code = %s AND proc_yn = 'Y'""",
                    (del_code,)
                )
            get_conn().commit()
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[{del_name}({del_code})] 관심종목에서 삭제되었습니다."
            )
        except Exception as e:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[관심종목 삭제] 오류: {str(e)}"
            )

    elif command in ("관심종목_변경", "interest_edit_back"):
        initMenuNum()
        g_interest_edit_field = ""
        _show_interest_edit_keyboard(query)

    elif command.startswith("interest_edit_") and not command.startswith("interest_edit_field_"):
        ii_code = command[len("interest_edit_"):]
        try:
            with get_conn().cursor() as cur_ie:
                cur_ie.execute("""
                    SELECT name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price
                    FROM public."interestItem_interest_item"
                    WHERE code = %s AND proc_yn = 'Y' AND interest_day >= prev_business_day_char(CURRENT_DATE) AND length(code) > 4
                """, (ii_code,)
                )
                row_ie = cur_ie.fetchone()
            if row_ie:
                ii_name, ii_through, ii_leave, ii_resist, ii_support, ii_trend_high, ii_trend_low = row_ie
            else:
                ii_name = ii_code
                ii_through = ii_leave = ii_resist = ii_support = ii_trend_high = ii_trend_low = None
            global g_interest_edit_code, g_interest_edit_name
            g_interest_edit_code = ii_code
            g_interest_edit_name = ii_name
            def _ifmt(v):
                return format(int(v), ',d') if v is not None else '-'
            field_btns = [
                InlineKeyboardButton(f"1차저항가({_ifmt(ii_through)})",   callback_data="menu,interest_edit_field_1차저항가"),
                InlineKeyboardButton(f"1차지지가({_ifmt(ii_leave)})",     callback_data="menu,interest_edit_field_1차지지가"),
                InlineKeyboardButton(f"2차저항가({_ifmt(ii_resist)})",    callback_data="menu,interest_edit_field_2차저항가"),
                InlineKeyboardButton(f"2차지지가({_ifmt(ii_support)})",   callback_data="menu,interest_edit_field_2차지지가"),
                InlineKeyboardButton(f"추세상한가({_ifmt(ii_trend_high)})", callback_data="menu,interest_edit_field_추세상한가"),
                InlineKeyboardButton(f"추세이탈가({_ifmt(ii_trend_low)})", callback_data="menu,interest_edit_field_추세이탈가"),
                InlineKeyboardButton("관심제외",                           callback_data="menu,interest_edit_field_관심제외"),
                InlineKeyboardButton("뒤로가기",                           callback_data="menu,interest_edit_back"),
            ]
            rows_f = [field_btns[i:i+2] for i in range(0, len(field_btns), 2)]
            query.edit_message_text(
                text=f"[{ii_name}({ii_code})] 수정할 항목을 선택하세요:",
                reply_markup=InlineKeyboardMarkup(rows_f)
            )
        except Exception as e:
            query.edit_message_text(text=f"[관심종목 수정] 오류: {str(e)}")

    elif command.startswith("interest_edit_field_"):
        g_interest_edit_field = command[len("interest_edit_field_"):]
        if g_interest_edit_field == "관심제외":
            try:
                with get_conn().cursor() as cur_exc:
                    cur_exc.execute(
                        """UPDATE public."interestItem_interest_item"
                           SET proc_yn = 'N'
                           WHERE code = %s AND proc_yn = 'Y' AND interest_day >= prev_business_day_char(CURRENT_DATE)""",
                        (g_interest_edit_code,)
                    )
                get_conn().commit()
                query.edit_message_text(
                    text=f"[{g_interest_edit_name}({g_interest_edit_code})] 관심종목에서 제외되었습니다."
                )
            except Exception as e_exc:
                query.edit_message_text(
                    text=f"[관심제외] 오류: {str(e_exc)}"
                )
        else:
            menuNum = '04'
            try:
                current_markup = query.message.reply_markup
                remaining = [
                    btn
                    for row in current_markup.inline_keyboard
                    for btn in row
                    if btn.callback_data != data_selected
                ]
                if remaining:
                    context.bot.edit_message_reply_markup(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id,
                        reply_markup=InlineKeyboardMarkup([remaining[i:i+2] for i in range(0, len(remaining), 2)])
                    )
                else:
                    context.bot.delete_message(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )
            except Exception:
                pass
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[{g_interest_edit_name}({g_interest_edit_code})] {g_interest_edit_field} 값을 입력하세요. (숫자만 입력)"
            )

    elif command == "interest_new":
        menuNum = '05'
        query.edit_message_text(
            text="신규 관심종목 종목코드(종목명)을 입력하세요."
        )

    elif command.startswith("holding_edit_") and not command.startswith("holding_edit_field_"):
        h_code = command[len("holding_edit_"):]
        ac_h = account(arguments[1])
        try:
            c = stock_balance(ac_h['access_token'], ac_h['app_key'], ac_h['app_secret'], ac_h['acct_no'], "")
            matched = [(c['prdt_name'][i],) for i, _ in enumerate(c.index) if c['pdno'][i] == h_code]
            h_name = matched[0][0] if matched else h_code
            global g_holding_edit_code, g_holding_edit_name
            g_holding_edit_code = h_code
            g_holding_edit_name = h_name
            h_tp = None
            h_target = h_loss = h_end_target = h_end_loss = None
            try:
                with get_conn().cursor() as cur_tp:
                    cur_tp.execute(
                        'SELECT trading_plan, sign_resist_price, sign_support_price, end_target_price, end_loss_price '
                        'FROM public."stockBalance_stock_balance" WHERE acct_no = %s AND code = %s AND proc_yn = \'Y\'',
                        (ac_h['acct_no'], h_code)
                    )
                    tp_row = cur_tp.fetchone()
                    if tp_row:
                        h_tp, h_target, h_loss, h_end_target, h_end_loss = tp_row
            except Exception:
                pass
            def _hfmt(v):
                return format(int(v), ',d') if v is not None else '-'
            plan_btn_text = "매매계획(투자)" if h_tp == 'i' else ("매매계획(홀딩)" if h_tp == 'h' else "매매계획(일반)")
            field_buttons = [
                InlineKeyboardButton(f"목표가({_hfmt(h_target)})",      callback_data=f"menu,holding_edit_field_목표가"),
                InlineKeyboardButton(f"이탈가({_hfmt(h_loss)})",        callback_data=f"menu,holding_edit_field_이탈가"),
                InlineKeyboardButton(f"최종목표가({_hfmt(h_end_target)})", callback_data=f"menu,holding_edit_field_최종목표가"),
                InlineKeyboardButton(f"최종이탈가({_hfmt(h_end_loss)})", callback_data=f"menu,holding_edit_field_최종이탈가"),
                InlineKeyboardButton(plan_btn_text,                      callback_data="menu,holding_plan_toggle"),
                InlineKeyboardButton("뒤로가기",                         callback_data="menu,holding_back"),
            ]
            rows = [field_buttons[i:i+2] for i in range(0, len(field_buttons), 2)]
            query.edit_message_text(
                text=f"[{h_name}({h_code})] 수정할 항목을 선택하세요:",
                reply_markup=InlineKeyboardMarkup(rows)
            )
        except Exception as e:
            query.edit_message_text(text=f"[보유종목 수정] 오류: {str(e)}")

    elif command.startswith("holding_edit_field_"):
        field = command[len("holding_edit_field_"):]
        g_holding_edit_field = field
        menuNum = '02'
        try:
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != data_selected
            ]
            if remaining:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup([remaining[i:i+2] for i in range(0, len(remaining), 2)])
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
        except Exception:
            pass
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"[{g_holding_edit_name}({g_holding_edit_code})] {field} 값을 입력하세요. (숫자만 입력)"
        )

    elif command == "holding_plan_toggle":
        try:
            with get_conn().cursor() as cur_tp:
                cur_tp.execute(
                    'SELECT trading_plan FROM public."stockBalance_stock_balance" WHERE code = %s AND proc_yn = \'Y\'',
                    (g_holding_edit_code,)
                )
                tp_row = cur_tp.fetchone()
                cur_tp_val = tp_row[0] if tp_row else None
            if cur_tp_val == 'i':
                new_tp = 'h'
            elif cur_tp_val == 'h':
                new_tp = None
            else:
                new_tp = 'i'
            with get_conn().cursor() as cur_upd:
                if new_tp is None:
                    cur_upd.execute(
                        'UPDATE public."stockBalance_stock_balance" SET trading_plan = NULL WHERE code = %s AND proc_yn = \'Y\'',
                        (g_holding_edit_code,)
                    )
                else:
                    cur_upd.execute(
                        'UPDATE public."stockBalance_stock_balance" SET trading_plan = %s WHERE code = %s AND proc_yn = \'Y\'',
                        (new_tp, g_holding_edit_code)
                    )
                get_conn().commit()
                updated = cur_upd.rowcount
            plan_label = "투자" if new_tp == 'i' else ("홀딩" if new_tp == 'h' else "일반")
            query.edit_message_text(
                text=f"[{g_holding_edit_name}(<code>{g_holding_edit_code}</code>)] 매매계획 → {plan_label} ({updated}건 업데이트)",
                parse_mode='HTML'
            )
        except Exception as e:
            query.edit_message_text(text=f"[매매계획 변경] 오류: {str(e)}")

    elif command.startswith("prevlow_sell:"):
        # kis_trading_trail_vol_state.py 에서 전송한 전일저가 이탈 전량매도 대상 매도가 입력 처리
        parts = command.split(":")
        p_name  = parts[1]
        p_code  = parts[2]
        p_qty   = int(parts[3])
        global g_trail_sell_code, g_trail_sell_name, g_trail_sell_qty
        g_trail_sell_code = p_code
        g_trail_sell_name = p_name
        g_trail_sell_qty = p_qty
        menuNum = '07'
        query.edit_message_text(
            text=f"[{p_name}({p_code})] {format(p_qty, ',d')}주 전량매도\n매도가를 입력하세요. (현재가:0)"
        )

    elif command.startswith("signal_sell_"):
        # kis_holding_item_total.py 에서 전송한 전량매도 대상 매도가 입력 처리
        parts = command.split("_")
        sig_name = parts[2]
        sig_code = parts[3]
        sig_qty  = int(parts[4])
        # sig_name_match = stock_code[stock_code.code == sig_code]
        # sig_name = sig_name_match.company.values[0].strip() if len(sig_name_match) > 0 else sig_code
        global g_signal_sell_code, g_signal_sell_name, g_signal_sell_qty
        g_signal_sell_code = sig_code
        g_signal_sell_name = sig_name
        g_signal_sell_qty  = sig_qty
        menuNum = '03'
        query.edit_message_text(
            text=f"[{sig_name}({sig_code})] {format(sig_qty, ',d')}주 전량매도\n매도가를 입력하세요. (현재가:0)"
        )

    elif command == "전체주문":

        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[일별주문체결 조회]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

            # 일별주문체결 조회
            output1 = daily_order_complete(access_token, app_key, app_secret, acct_no, '', '', '00')

            if len(output1) > 0:
                tdf = pd.DataFrame(output1)
                d = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
                result_msgs = []

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
                    d_excg_dvsn = d['excg_id_dvsn_cd'][i]

                    msg = f"* [{d_excg_dvsn} {d_name} - {d_order_tmd[:2]}:{d_order_tmd[2:4]}:{d_order_tmd[4:]}] 주문번호:<code>{str(d_order_no)}</code>, {d_order_type}가:{format(int(d_order_price), ',d')}원, {d_order_type}량:{format(int(d_order_amount), ',d')}주, 체결량:{format(int(d_total_complete_qty), ',d')}주, 잔량:{format(int(d_remain_qty), ',d')}주, 체결금:{format(int(d_total_complete_amt), ',d')}원"
                    result_msgs.append(msg)

                if result_msgs:
                    # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                    chunk_size = 10
                    chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                    for idx, chunk in enumerate(chunks):
                        final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                        
                        if idx == 0:
                            # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                            context.bot.edit_message_text(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id
                            )
                        else:
                            # 두 번째 묶음부터는 새로운 메시지로 전송
                            context.bot.send_message(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id
                            )
                else:
                    context.bot.edit_message_text(
                        text="일별주문체결 조회 대상이 존재하지 않습니다.",
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )                                

            else:
                context.bot.send_message(text="일별주문체결 조회 미존재 : " + g_company,
                                            chat_id=query.message.chat_id)

        except Exception as e:
            print('일별주문체결 조회 오류.', e)
            context.bot.edit_message_text(text="[일별주문체결 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)
    
    elif command == "매수주문":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "21")

    elif command in ("손절금액", "매수금액") and "buy21" in data_selected:
        # 매수주문 미리보기 → 실제 주문 처리
        if command == "손절금액":
            g_buy21_buy_qty = g_buy21_loss_buy_qty
        else:
            g_buy21_buy_qty = g_buy21_amt_buy_qty

        cb_user_id = query.message.chat_id
        cb_bot = context.bot
        cb_code = g_buy21_code
        cb_company = g_buy21_company
        cb_buy_price = g_buy21_buy_price
        cb_buy_qty = g_buy21_buy_qty
        target_nicks = g_selected_accounts[:] if g_selected_accounts else [None]
        ac_default = account(arguments[1])

        def process_buy21(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                t_buy_price = cb_buy_price
                t_buy_qty = cb_buy_qty

                buy_expect_sum = t_buy_price * t_buy_qty
                b = inquire_psbl_order(t_access_token, t_app_key, t_app_secret, t_acct_no)

                if int(b) > int(buy_expect_sum):
                    try:
                        c_ord = order_cash(True, t_access_token, t_app_key, t_app_secret, str(t_acct_no), cb_code, "00", str(t_buy_qty), str(t_buy_price))
                        if c_ord is not None and c_ord['ODNO'] != "":
                            time.sleep(0.5)
                            output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, cb_code, c_ord['ODNO'], '02')
                            tdf = pd.DataFrame(output1)
                            d_ord = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
                            for i, _ in enumerate(d_ord.index):
                                d_order_price = d_ord['avg_prvs'][i] if int(d_ord['avg_prvs'][i]) > 0 else d_ord['ord_unpr'][i]
                                d_order_amount = d_ord['ord_qty'][i]
                                d_order_no = int(d_ord['odno'][i])
                                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : <code>" + str(d_order_no) + "</code>", parse_mode='HTML')
                        else:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(t_buy_price), ',d') + "원, 매수량 : " + format(int(t_buy_qty), ',d') + "주 매수주문 실패", parse_mode='HTML')
                    except Exception as e:
                        cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] [매수주문 오류] - " + str(e), parse_mode='HTML')
                else:
                    cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] 매수 가능(현금) : " + format(int(b) - int(buy_expect_sum), ',d') + "원 부족")

            except Exception as top_e:
                print(f"process_buy21 오류 [{nick}]: {top_e}")
                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] [매수주문 오류] " + str(top_e))

        query.edit_message_text(text="[" + cb_company + "] 주문 처리 중...")
        threads_b21 = []
        for nick in target_nicks:
            if nick is not None:
                ac_t = account(nick)
                t_acct_no = ac_t['acct_no']
                t_access_token = ac_t['access_token']
                t_app_key = ac_t['app_key']
                t_app_secret = ac_t['app_secret']
                t_nick = nick
            else:
                t_acct_no = ac_default['acct_no']
                t_access_token = ac_default['access_token']
                t_app_key = ac_default['app_key']
                t_app_secret = ac_default['app_secret']
                t_nick = arguments[1]
            t = threading.Thread(target=process_buy21, args=(t_nick, t_acct_no, t_access_token, t_app_key, t_app_secret))
            threads_b21.append(t)
            t.start()
        for t in threads_b21:
            t.join()

    elif command == "다시계산" and "buy21" in data_selected:
        menuNum = "21"
        selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
        query.edit_message_text(text="[선택계좌: " + selected_str + "]\n종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.")

    elif command == "매도주문":
        try:
            ac_sl = account(arguments[1])
            c_sl = stock_balance(ac_sl['access_token'], ac_sl['app_key'], ac_sl['app_secret'], ac_sl['acct_no'], "")
            sl_buttons = []
            for i in range(len(c_sl.index)):
                if int(c_sl['hldg_qty'][i]) <= 0:
                    continue
                sl_name     = c_sl['prdt_name'][i]
                sl_code_i   = c_sl['pdno'][i]
                sl_qty      = int(c_sl['hldg_qty'][i])
                try:
                    sl_rate = float(c_sl['evlu_pfls_rt'][i])
                    sl_evlu = int(c_sl['evlu_amt'][i])
                    sign    = "+" if sl_rate >= 0 else ""
                    btn_txt = f"{sl_name}({sl_code_i}) | {format(sl_qty, ',')}주 | {sign}{sl_rate:.2f}% | {format(sl_evlu, ',')}원"
                except Exception:
                    btn_txt = f"{sl_name}({sl_code_i}) | {format(sl_qty, ',')}주"
                sl_buttons.append(InlineKeyboardButton(btn_txt, callback_data=f"sell_ord_{sl_code_i}"))
            if sl_buttons:
                query.edit_message_text(
                    text="매도할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(sl_buttons, 1))
                )
            else:
                query.edit_message_text(text="보유종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[매도주문] 오류: {str(e)}")

    elif command.startswith("sell_ord_"):
        sl_code = command[len("sell_ord_"):]
        try:
            ac_sl2 = account(arguments[1])
            c_sl2 = stock_balance(ac_sl2['access_token'], ac_sl2['app_key'], ac_sl2['app_secret'], ac_sl2['acct_no'], "")
            sl_company = sl_code
            for i in range(len(c_sl2.index)):
                if c_sl2['pdno'][i] == sl_code:
                    sl_company = c_sl2['prdt_name'][i]
                    break
        except Exception:
            sl_company = sl_code
        g_sell3x_code = sl_code
        g_sell3x_company = sl_company
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "SL")

    elif command == "주문정정":
        btn_buy  = InlineKeyboardButton("매수주문정정", callback_data="corr_type_02")
        btn_sell = InlineKeyboardButton("매도주문정정", callback_data="corr_type_01")
        query.edit_message_text(
            text="정정할 주문 유형을 선택하세요:",
            reply_markup=InlineKeyboardMarkup([[btn_buy, btn_sell]])
        )

    elif command.startswith("corr_type_"):
        # corr_type_01=매도, corr_type_02=매수
        ct_dvsn = command[len("corr_type_"):]   # "01" or "02"
        ct_label = "매도" if ct_dvsn == "01" else "매수"
        try:
            ac_ct = account(arguments[1])
            output_ct = daily_order_complete(
                ac_ct['access_token'], ac_ct['app_key'], ac_ct['app_secret'],
                ac_ct['acct_no'], "", "", ct_dvsn
            )
            if not output_ct:
                query.edit_message_text(text=f"[{ct_label}] 주문정정 가능한 미체결 주문이 없습니다.")
                return
            tdf_ct = pd.DataFrame(output_ct)
            tdf_ct = tdf_ct[tdf_ct['rmn_qty'].astype(int) > 0]
            if tdf_ct.empty:
                query.edit_message_text(text=f"[{ct_label}] 미체결 주문이 없습니다.")
                return
            # 종목별로 그룹화 (동일 종목 복수 주문 시 첫 번째 기준)
            seen_codes = {}
            ct_buttons = []
            for _, row in tdf_ct.iterrows():
                ct_code = row['pdno']
                if ct_code in seen_codes:
                    continue
                seen_codes[ct_code] = True
                ct_name    = row['prdt_name']
                ct_price   = int(row['ord_unpr'])
                ct_rmn_qty = int(row['rmn_qty'])
                btn_txt = f"{ct_name}({ct_code}) | {format(ct_price, ',')}원 | {format(ct_rmn_qty, ',')}주"
                ct_buttons.append(InlineKeyboardButton(btn_txt, callback_data=f"corr_ord_{ct_dvsn}_{ct_code}"))
            if ct_buttons:
                query.edit_message_text(
                    text=f"[{ct_label}] 정정할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(ct_buttons, 1))
                )
            else:
                query.edit_message_text(text=f"[{ct_label}] 미체결 주문이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[주문정정] 오류: {str(e)}")

    elif command.startswith("corr_ord_"):
        # corr_ord_{dvsn}_{code}
        co_suffix = command[len("corr_ord_"):]   # "01_005930"
        co_dvsn   = co_suffix[:2]                # "01" or "02"
        co_code   = co_suffix[3:]                # "005930"
        try:
            ac_co = account(arguments[1])
            c_co = stock_balance(ac_co['access_token'], ac_co['app_key'], ac_co['app_secret'], ac_co['acct_no'], "")
            co_company = co_code
            for i in range(len(c_co.index)):
                if c_co['pdno'][i] == co_code:
                    co_company = c_co['prdt_name'][i]
                    break
            if co_company == co_code:
                output_co = daily_order_complete(
                    ac_co['access_token'], ac_co['app_key'], ac_co['app_secret'],
                    ac_co['acct_no'], co_code, "", co_dvsn
                )
                if output_co:
                    co_company = output_co[0].get('prdt_name', co_code)
        except Exception:
            co_company = co_code
        g_corr_code = co_code
        g_corr_company = co_company
        g_corr_dvsn = co_dvsn
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "51N")

    elif command == "주문취소":
        btn_buy  = InlineKeyboardButton("매수주문취소", callback_data="cncl_type_02")
        btn_sell = InlineKeyboardButton("매도주문취소", callback_data="cncl_type_01")
        query.edit_message_text(
            text="취소할 주문 유형을 선택하세요:",
            reply_markup=InlineKeyboardMarkup([[btn_buy, btn_sell]])
        )

    elif command.startswith("cncl_type_"):
        cn_dvsn  = command[len("cncl_type_"):]   # "01" or "02"
        cn_label = "매도" if cn_dvsn == "01" else "매수"
        try:
            ac_cn = account(arguments[1])
            output_cn = daily_order_complete(
                ac_cn['access_token'], ac_cn['app_key'], ac_cn['app_secret'],
                ac_cn['acct_no'], "", "", cn_dvsn
            )
            if not output_cn:
                query.edit_message_text(text=f"[{cn_label}] 주문취소 가능한 미체결 주문이 없습니다.")
                return
            tdf_cn = pd.DataFrame(output_cn)
            tdf_cn = tdf_cn[(tdf_cn['rmn_qty'].astype(int) > 0) & (tdf_cn['cncl_yn'] != 'Y')]
            if tdf_cn.empty:
                query.edit_message_text(text=f"[{cn_label}] 미체결 주문이 없습니다.")
                return
            seen_cn = {}
            cn_buttons = []
            for _, row in tdf_cn.iterrows():
                cn_code = row['pdno']
                if cn_code in seen_cn:
                    continue
                seen_cn[cn_code] = True
                cn_name    = row['prdt_name']
                cn_price   = int(row['ord_unpr'])
                cn_rmn_qty = int(row['rmn_qty'])
                btn_txt = f"{cn_name}({cn_code}) | {format(cn_price, ',')}원 | {format(cn_rmn_qty, ',')}주"
                cn_buttons.append(InlineKeyboardButton(btn_txt, callback_data=f"cncl_ord_{cn_dvsn}_{cn_code}"))
            if cn_buttons:
                query.edit_message_text(
                    text=f"[{cn_label}] 취소할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(cn_buttons, 1))
                )
            else:
                query.edit_message_text(text=f"[{cn_label}] 미체결 주문이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[주문취소] 오류: {str(e)}")

    elif command.startswith("cncl_ord_"):
        # cncl_ord_{dvsn}_{code}
        co2_suffix  = command[len("cncl_ord_"):]
        co2_dvsn    = co2_suffix[:2]
        co2_code    = co2_suffix[3:]
        try:
            ac_co2 = account(arguments[1])
            output_co2 = daily_order_complete(
                ac_co2['access_token'], ac_co2['app_key'], ac_co2['app_secret'],
                ac_co2['acct_no'], co2_code, "", co2_dvsn
            )
            co2_company = co2_code
            if output_co2:
                co2_company = output_co2[0].get('prdt_name', co2_code)
        except Exception:
            co2_company = co2_code
        g_cncl_code = co2_code
        g_cncl_company = co2_company
        g_cncl_dvsn = co2_dvsn
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "52N")
    
    elif command == "전체예약":
    
        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[전체예약 조회]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

            
            reserve_strt_dt = datetime.now().strftime("%Y%m%d")
            reserve_end_dt = (datetime.now() + relativedelta(months=1)).strftime("%Y%m%d")
            reserve_end_dt = get_previous_business_day(reserve_end_dt)
            # 전체예약 조회
            output = order_reserve_complete(access_token, app_key, app_secret, reserve_strt_dt, reserve_end_dt, str(acct_no), "")

            if len(output) > 0:
                tdf = pd.DataFrame(output)
                d = tdf[['rsvn_ord_seq', 'rsvn_ord_ord_dt', 'rsvn_ord_rcit_dt', 'pdno', 'ord_dvsn_cd', 'ord_rsvn_qty', 'tot_ccld_qty', 'cncl_ord_dt', 'ord_tmd', 'odno', 'rsvn_ord_rcit_tmd', 'kor_item_shtn_name', 'sll_buy_dvsn_cd', 'ord_rsvn_unpr', 'tot_ccld_amt', 'cncl_rcit_tmd', 'prcs_rslt', 'ord_dvsn_name', 'rsvn_end_dt']]
                result_msgs = []

                for i, name in enumerate(d.index):
                    d_rsvn_ord_seq = int(d['rsvn_ord_seq'][i])          # 예약주문 순번
                    d_rsvn_ord_ord_dt = d['rsvn_ord_ord_dt'][i]         # 예약주문주문일자
                    d_rsvn_ord_rcit_dt = d['rsvn_ord_rcit_dt'][i]       # 예약주문접수일자
                    d_code = d['pdno'][i]
                    d_ord_dvsn_cd = d['ord_dvsn_cd'][i]                 # 주문구분코드
                    d_ord_rsvn_qty = int(d['ord_rsvn_qty'][i])          # 주문예약수량
                    d_tot_ccld_qty = int(d['tot_ccld_qty'][i])          # 총체결수량
                    d_cncl_ord_dt = d['cncl_ord_dt'][i]                 # 취소주문일자
                    d_ord_tmd = d['ord_tmd'][i]                         # 주문시각
                    d_order_no = d['odno'][i]                           # 주문번호
                    d_rsvn_ord_rcit_tmd = d['rsvn_ord_rcit_tmd'][i]     # 예약주문접수시각
                    d_name = d['kor_item_shtn_name'][i]                 # 종목명
                    d_sll_buy_dvsn_cd = d['sll_buy_dvsn_cd'][i]         # 매도매수구분코드
                    d_ord_rsvn_unpr = int(d['ord_rsvn_unpr'][i])        # 주문예약단가
                    d_tot_ccld_amt = int(d['tot_ccld_amt'][i])          # 총체결금액
                    d_cncl_rcit_tmd = d['cncl_rcit_tmd'][i]             # 취소접수시각
                    d_prcs_rslt = d['prcs_rslt'][i]                     # 처리결과
                    d_ord_dvsn_name = d['ord_dvsn_name'][i]             # 주문구분명
                    d_rsvn_end_dt = d['rsvn_end_dt'][i]                 # 예약종료일자

                    msg1 = f"* {d_name}[<code>{d_code}</code>] {d_rsvn_ord_ord_dt[:4]}/{d_rsvn_ord_ord_dt[4:6]}/{d_rsvn_ord_ord_dt[6:]}~{d_rsvn_end_dt[:4]}/{d_rsvn_end_dt[4:6]}/{d_rsvn_end_dt[6:]} 예약번호:<code>{str(d_rsvn_ord_seq)}</code>, {d_ord_dvsn_name}:{format(d_ord_rsvn_unpr, ',d')}원, 예약수량:{format(d_ord_rsvn_qty, ',d')}주 {d_prcs_rslt}"
                    result_msgs.append(msg1)
                    
                    if d_cncl_ord_dt != "":
                        msg2 = f", 취소주문일자:{d_cncl_ord_dt[:4]}/{d_cncl_ord_dt[4:6]}/{d_cncl_ord_dt[6:]}"
                        result_msgs.append(msg2)
                    if str(d_order_no) != "":
                        msg3 = f", 주문번호:<code>{str(d_order_no)}</code>, 체결수량:{format(d_tot_ccld_qty, ',d')}주, 체결금액:{format(d_tot_ccld_amt, ',d')}원"
                        result_msgs.append(msg3)

                if result_msgs:
                    # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                    chunk_size = 10
                    chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                    for idx, chunk in enumerate(chunks):
                        final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                        
                        if idx == 0:
                            # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                            context.bot.edit_message_text(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id
                            )
                        else:
                            # 두 번째 묶음부터는 새로운 메시지로 전송
                            context.bot.send_message(
                                text=final_message,
                                parse_mode='HTML',
                                chat_id=query.message.chat_id
                            )
                else:
                    context.bot.edit_message_text(
                        text="전체예약 조회 대상이 존재하지 않습니다.",
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )                

            else:
                context.bot.send_message(text="전체예약 조회 미존재 : " + g_company,
                                            chat_id=query.message.chat_id)

        except Exception as e:
            print('전체예약 조회 오류.', e)
            context.bot.edit_message_text(text="[전체예약 조회] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

    elif command == "예약주문":
        btn_buy  = InlineKeyboardButton("매수예약", callback_data="rsv_type_buy")
        btn_sell = InlineKeyboardButton("매도예약", callback_data="rsv_type_sell")
        query.edit_message_text(
            text="예약 주문 유형을 선택하세요:",
            reply_markup=InlineKeyboardMarkup([[btn_buy, btn_sell]])
        )

    elif command == "rsv_type_buy":
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "61B")

    elif command == "rsv_type_sell":
        try:
            ac_rsv_sl = account(arguments[1])
            c_rsv_sl = stock_balance(ac_rsv_sl['access_token'], ac_rsv_sl['app_key'], ac_rsv_sl['app_secret'], ac_rsv_sl['acct_no'], "")
            rsv_sl_buttons = []
            for i in range(len(c_rsv_sl.index)):
                if int(c_rsv_sl['hldg_qty'][i]) <= 0:
                    continue
                rsv_sl_name  = c_rsv_sl['prdt_name'][i]
                rsv_sl_code  = c_rsv_sl['pdno'][i]
                rsv_sl_qty   = int(c_rsv_sl['hldg_qty'][i])
                try:
                    rsv_sl_avg = int(float(c_rsv_sl['pchs_avg_pric'][i]))
                    btn_txt = f"{rsv_sl_name}({rsv_sl_code}) | 단가:{format(rsv_sl_avg, ',')}원 | {format(rsv_sl_qty, ',')}주"
                except Exception:
                    btn_txt = f"{rsv_sl_name}({rsv_sl_code}) | {format(rsv_sl_qty, ',')}주"
                rsv_sl_buttons.append(InlineKeyboardButton(btn_txt, callback_data=f"rsv_sell_ord_{rsv_sl_code}"))
            if rsv_sl_buttons:
                query.edit_message_text(
                    text="예약매도할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(rsv_sl_buttons, 1))
                )
            else:
                query.edit_message_text(text="보유종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[예약매도] 오류: {str(e)}")

    elif command.startswith("rsv_sell_ord_"):
        rsv_sl_code2 = command[len("rsv_sell_ord_"):]
        try:
            ac_rsv_sl2 = account(arguments[1])
            c_rsv_sl2 = stock_balance(ac_rsv_sl2['access_token'], ac_rsv_sl2['app_key'], ac_rsv_sl2['app_secret'], ac_rsv_sl2['acct_no'], "")
            rsv_sl_company2 = rsv_sl_code2
            for i in range(len(c_rsv_sl2.index)):
                if c_rsv_sl2['pdno'][i] == rsv_sl_code2:
                    rsv_sl_company2 = c_rsv_sl2['prdt_name'][i]
                    break
        except Exception:
            rsv_sl_company2 = rsv_sl_code2
        g_rsv_sell_code = rsv_sl_code2
        g_rsv_sell_name = rsv_sl_company2
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "61S")
    
    elif command == "예약정정":
        btn_buy_rc  = InlineKeyboardButton("매수예약정정", callback_data="rsv_corr_type_02")
        btn_sell_rc = InlineKeyboardButton("매도예약정정", callback_data="rsv_corr_type_01")
        query.edit_message_text(
            text="정정할 예약 유형을 선택하세요:",
            reply_markup=InlineKeyboardMarkup([[btn_buy_rc, btn_sell_rc]])
        )

    elif command.startswith("rsv_corr_type_"):
        rc_dvsn = command[len("rsv_corr_type_"):]   # "01"=매도, "02"=매수
        rc_label = "매도" if rc_dvsn == "01" else "매수"
        try:
            ac_rc = account(arguments[1])
            now_rc = datetime.now()
            rc_start = (now_rc + timedelta(days=1)).strftime("%Y%m%d") if now_rc > now_rc.replace(hour=15, minute=40, second=0, microsecond=0) else now_rc.strftime("%Y%m%d")
            rc_end   = (now_rc + relativedelta(months=1)).strftime("%Y%m%d")
            rc_end   = get_previous_business_day(rc_end)
            output_rc = order_reserve_complete(ac_rc['access_token'], ac_rc['app_key'], ac_rc['app_secret'], rc_start, rc_end, str(ac_rc['acct_no']), "")
            if not output_rc:
                query.edit_message_text(text=f"{rc_label} 예약정정 가능한 주문이 없습니다.")
                return
            tdf_rc = pd.DataFrame(output_rc)
            # 해당 매매구분 + 미취소 건 필터
            filtered_rc = tdf_rc[(tdf_rc['sll_buy_dvsn_cd'] == rc_dvsn) & (tdf_rc['cncl_ord_dt'] == "")]
            if filtered_rc.empty:
                query.edit_message_text(text=f"{rc_label} 예약정정 가능한 주문이 없습니다.")
                return
            # 종목코드별 그룹화 (중복 제거, 대표 1건 버튼)
            seen_rc = {}
            rc_buttons = []
            for _, row in filtered_rc.iterrows():
                rc_code_i = row['pdno']
                if rc_code_i in seen_rc:
                    continue
                seen_rc[rc_code_i] = True
                rc_name_i    = row['kor_item_shtn_name']
                rc_price_i   = int(row['ord_rsvn_unpr'])
                rc_qty_i     = int(row['ord_rsvn_qty'])
                rc_end_dt_i  = row['rsvn_end_dt']
                end_fmt = f"{rc_end_dt_i[:4]}/{rc_end_dt_i[4:6]}/{rc_end_dt_i[6:]}" if len(rc_end_dt_i) == 8 else rc_end_dt_i
                btn_txt = f"{rc_name_i}({rc_code_i}) | {format(rc_price_i, ',')}원 | {format(rc_qty_i, ',')}주 | ~{end_fmt}"
                rc_buttons.append(InlineKeyboardButton(btn_txt, callback_data=f"rsv_corr_ord_{rc_dvsn}_{rc_code_i}"))
            if rc_buttons:
                query.edit_message_text(
                    text=f"{rc_label} 예약정정할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(rc_buttons, 1))
                )
            else:
                query.edit_message_text(text=f"{rc_label} 예약정정 가능한 종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[예약정정 조회] 오류: {str(e)}")

    elif command.startswith("rsv_corr_ord_"):
        parts_rco = command[len("rsv_corr_ord_"):].split("_", 1)
        rco_dvsn = parts_rco[0]  # "01" or "02"
        rco_code = parts_rco[1]
        try:
            ac_rco = account(arguments[1])
            now_rco = datetime.now()
            rco_start = (now_rco + timedelta(days=1)).strftime("%Y%m%d") if now_rco > now_rco.replace(hour=15, minute=40, second=0, microsecond=0) else now_rco.strftime("%Y%m%d")
            rco_end   = (now_rco + relativedelta(months=1)).strftime("%Y%m%d")
            rco_end   = get_previous_business_day(rco_end)
            out_rco = order_reserve_complete(ac_rco['access_token'], ac_rco['app_key'], ac_rco['app_secret'], rco_start, rco_end, str(ac_rco['acct_no']), "")
            rco_name = rco_code
            if out_rco:
                df_rco = pd.DataFrame(out_rco)
                matched_rco = df_rco[df_rco['pdno'] == rco_code]
                if not matched_rco.empty:
                    rco_name = matched_rco.iloc[0]['kor_item_shtn_name']
        except Exception:
            rco_name = rco_code
        g_rsv_corr_code = rco_code
        g_rsv_corr_name = rco_name
        g_rsv_corr_dvsn = rco_dvsn
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "62N")

    elif command == "예약취소":
        btn_buy_cn  = InlineKeyboardButton("매수예약취소", callback_data="rsv_cncl_type_02")
        btn_sell_cn = InlineKeyboardButton("매도예약취소", callback_data="rsv_cncl_type_01")
        query.edit_message_text(
            text="취소할 예약 유형을 선택하세요:",
            reply_markup=InlineKeyboardMarkup([[btn_buy_cn, btn_sell_cn]])
        )

    elif command.startswith("rsv_cncl_type_"):
        cn_dvsn = command[len("rsv_cncl_type_"):]   # "01"=매도, "02"=매수
        cn_label = "매도" if cn_dvsn == "01" else "매수"
        try:
            ac_cn = account(arguments[1])
            now_cn = datetime.now()
            cn_start = (now_cn + timedelta(days=1)).strftime("%Y%m%d") if now_cn > now_cn.replace(hour=15, minute=40, second=0, microsecond=0) else now_cn.strftime("%Y%m%d")
            cn_end   = (now_cn + relativedelta(months=1)).strftime("%Y%m%d")
            cn_end   = get_previous_business_day(cn_end)
            output_cn = order_reserve_complete(ac_cn['access_token'], ac_cn['app_key'], ac_cn['app_secret'], cn_start, cn_end, str(ac_cn['acct_no']), "")
            if not output_cn:
                query.edit_message_text(text=f"{cn_label} 예약취소 가능한 주문이 없습니다.")
                return
            tdf_cn = pd.DataFrame(output_cn)
            filtered_cn = tdf_cn[(tdf_cn['sll_buy_dvsn_cd'] == cn_dvsn) & (tdf_cn['cncl_ord_dt'] == "")]
            if filtered_cn.empty:
                query.edit_message_text(text=f"{cn_label} 예약취소 가능한 주문이 없습니다.")
                return
            seen_cn = {}
            cn_buttons = []
            for _, row in filtered_cn.iterrows():
                cn_code_i = row['pdno']
                if cn_code_i in seen_cn:
                    continue
                seen_cn[cn_code_i] = True
                cn_name_i   = row['kor_item_shtn_name']
                cn_price_i  = int(row['ord_rsvn_unpr'])
                cn_qty_i    = int(row['ord_rsvn_qty'])
                cn_end_dt_i = row['rsvn_end_dt']
                end_fmt = f"{cn_end_dt_i[:4]}/{cn_end_dt_i[4:6]}/{cn_end_dt_i[6:]}" if len(cn_end_dt_i) == 8 else cn_end_dt_i
                btn_txt = f"{cn_name_i}({cn_code_i}) | {format(cn_price_i, ',')}원 | {format(cn_qty_i, ',')}주 | ~{end_fmt}"
                cn_buttons.append(InlineKeyboardButton(btn_txt, callback_data=f"rsv_cncl_ord_{cn_dvsn}_{cn_code_i}"))
            if cn_buttons:
                query.edit_message_text(
                    text=f"{cn_label} 예약취소할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(cn_buttons, 1))
                )
            else:
                query.edit_message_text(text=f"{cn_label} 예약취소 가능한 종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[예약취소 조회] 오류: {str(e)}")

    elif command.startswith("rsv_cncl_ord_"):
        parts_rcn = command[len("rsv_cncl_ord_"):].split("_", 1)
        rcn_dvsn = parts_rcn[0]  # "01" or "02"
        rcn_code = parts_rcn[1]
        try:
            ac_rcn = account(arguments[1])
            now_rcn = datetime.now()
            rcn_start = (now_rcn + timedelta(days=1)).strftime("%Y%m%d") if now_rcn > now_rcn.replace(hour=15, minute=40, second=0, microsecond=0) else now_rcn.strftime("%Y%m%d")
            rcn_end   = (now_rcn + relativedelta(months=1)).strftime("%Y%m%d")
            rcn_end   = get_previous_business_day(rcn_end)
            out_rcn = order_reserve_complete(ac_rcn['access_token'], ac_rcn['app_key'], ac_rcn['app_secret'], rcn_start, rcn_end, str(ac_rcn['acct_no']), "")
            rcn_name = rcn_code
            if out_rcn:
                df_rcn = pd.DataFrame(out_rcn)
                matched_rcn = df_rcn[df_rcn['pdno'] == rcn_code]
                if not matched_rcn.empty:
                    rcn_name = matched_rcn.iloc[0]['kor_item_shtn_name']
        except Exception:
            rcn_name = rcn_code
        g_rsv_cncl_code = rcn_code
        g_rsv_cncl_name = rcn_name
        g_rsv_cncl_dvsn = rcn_dvsn
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "63N")

    elif command == "추적등록":
        button_list = build_button(["매수주문등록", "주문제외등록"], data_selected)
        show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list)))

        context.bot.edit_message_text(text="추적등록 방식을 선택해 주세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id,
                                        reply_markup=show_markup)

    elif command == "매수주문등록" and "추적등록" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "71")

    elif command == "주문제외등록" and "추적등록" in data_selected:
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "72")

    elif command in ("손절금액", "매수금액") and "trail71" in data_selected:
        # 손절금액/매수금액 선택에 따라 매수량·매수금액 설정
        if command == "손절금액":
            g_trail71_buy_qty = g_trail71_loss_buy_qty
        else:
            g_trail71_buy_qty = g_trail71_amt_buy_qty

        # 추적등록 미리보기 → 실제 주문 + 매매추적 처리
        cb_user_id = query.message.chat_id
        cb_bot = context.bot          # 스레드에서 안전하게 사용하기 위해 bot 참조 분리
        cb_code = g_trail71_code
        cb_company = g_trail71_company
        cb_buy_price = g_trail71_buy_price
        cb_loss_price = g_trail71_loss_price
        cb_buy_qty = g_trail71_buy_qty
        cb_year_day = g_trail71_year_day
        cb_hour_minute = g_trail71_hour_minute
        target_nicks = g_selected_accounts[:] if g_selected_accounts else [None]
        ac_default = account(arguments[1])

        def process_trail71(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                t_buy_price = cb_buy_price
                t_buy_qty = cb_buy_qty

                c_bal = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                hold_price = 0
                hldg_qty = 0
                hold_amt = 0
                for i, _ in enumerate(c_bal.index):
                    if cb_code == c_bal['pdno'][i]:
                        hold_price = float(c_bal['pchs_avg_pric'][i])
                        hldg_qty = int(c_bal['hldg_qty'][i])
                        hold_amt = int(c_bal['pchs_amt'][i])

                buy_expect_sum = t_buy_price * t_buy_qty
                b = inquire_psbl_order(t_access_token, t_app_key, t_app_secret, t_acct_no)
                d_order_no = None
                d_order_type = None
                d_order_dt = None
                d_order_tmd = None
                d_order_price = 0
                d_order_amount = 0
                d_order_complete_qty = 0
                d_order_remain_qty = 0

                if int(b) > int(buy_expect_sum):
                    try:
                        c_ord = order_cash(True, t_access_token, t_app_key, t_app_secret, str(t_acct_no), cb_code, "00", str(t_buy_qty), str(t_buy_price))
                        if c_ord is not None and c_ord['ODNO'] != "":
                            time.sleep(0.5)
                            output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret, t_acct_no, cb_code, c_ord['ODNO'], '02')
                            tdf = pd.DataFrame(output1)
                            d_ord = tdf[['odno', 'prdt_name', 'ord_dt', 'ord_tmd', 'orgn_odno', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn', 'tot_ccld_amt', 'tot_ccld_qty', 'rmn_qty', 'cncl_cfrm_qty', 'excg_id_dvsn_cd']]
                            for i, _ in enumerate(d_ord.index):
                                d_order_no = int(d_ord['odno'][i])
                                d_order_type = d_ord['sll_buy_dvsn_cd_name'][i]
                                d_order_dt = d_ord['ord_dt'][i]
                                d_order_tmd = d_ord['ord_tmd'][i]
                                d_order_price = d_ord['avg_prvs'][i] if int(d_ord['avg_prvs'][i]) > 0 else d_ord['ord_unpr'][i]
                                d_order_amount = d_ord['ord_qty'][i]
                                d_order_complete_qty = d_ord['tot_ccld_qty'][i]
                                d_order_remain_qty = d_ord['rmn_qty'][i]
                                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(d_order_price), ',d') + "원, 매수량 : " + format(int(d_order_amount), ',d') + "주 매수주문 완료, 주문번호 : <code>" + str(d_order_no) + "</code>", parse_mode='HTML')
                        else:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 매수가 : " + format(int(t_buy_price), ',d') + "원, 매수량 : " + format(int(t_buy_qty), ',d') + "주 매수주문 실패", parse_mode='HTML')
                    except Exception as e:
                        cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] [매수주문 오류] - " + str(e), parse_mode='HTML')
                else:
                    cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] 매수 가능(현금) : " + format(int(b) - int(buy_expect_sum), ',d') + "원 부족")

                if int(d_order_price) > 0 and int(d_order_amount) > 0:
                    t_buy_price = int(d_order_price)
                    t_buy_qty = int(d_order_amount)
                    t_buy_amt = t_buy_price * t_buy_qty
                    safe_margin_price = int(t_buy_price + t_buy_price * 0.05)
                    base_qty = hldg_qty
                    base_amt = hold_amt
                    sum_base_qty = base_qty + t_buy_qty
                    avg_base_price = int(round((base_amt + t_buy_amt) / sum_base_qty))
                    avg_safe_margin_price = int(avg_base_price + avg_base_price * 0.05)
                    loss_amt = int((avg_base_price - cb_loss_price) * sum_base_qty) if base_qty > 0 else int((t_buy_price - cb_loss_price) * t_buy_qty)

                    thread_conn = db.connect(conn_string)
                    try:
                        cur400 = thread_conn.cursor()
                        merge_query = """
                            WITH ins AS (
                                INSERT INTO trading_trail (
                                    order_no, order_type, order_dt, order_tmd,
                                    order_price, order_amount, complete_qty, remain_qty,
                                    acct_no, code, name, trail_day, trail_dtm, trail_tp,
                                    stop_price, target_price, basic_price, basic_qty, basic_amt,
                                    proc_min, trade_tp, exit_price, loss_amt, crt_dt, mod_dt
                                )
                                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                RETURNING 1 AS flag
                            )
                            SELECT flag FROM ins;
                        """
                        cur400.execute(merge_query, (
                            str(d_order_no), d_order_type, d_order_dt, d_order_tmd,
                            int(d_order_price), int(d_order_amount), int(d_order_complete_qty), int(d_order_remain_qty),
                            t_acct_no, cb_code, cb_company, cb_year_day, cb_hour_minute, "1",
                            cb_loss_price, avg_safe_margin_price if hold_price > 0 else safe_margin_price,
                            avg_base_price if hold_price > 0 else t_buy_price,
                            sum_base_qty if base_qty > 0 else t_buy_qty,
                            avg_base_price * sum_base_qty if base_qty > 0 else t_buy_amt,
                            cb_hour_minute, 'S' if command == "손절금액" else 'M', cb_loss_price, loss_amt, datetime.now(), datetime.now()
                        ))
                        was_updated = cur400.fetchone() is not None
                        thread_conn.commit()
                        cur400.close()
                        if was_updated:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "{<code>" + cb_code + "</code>}] 평균보유가 : " + format(avg_base_price if hold_price > 0 else t_buy_price, ',d') + "원, 총보유량 : " + format(sum_base_qty if base_qty > 0 else t_buy_qty, ',d') + "주, 이탈가 : " + format(cb_loss_price, ',d') + "원, 안전마진가 : " + format(avg_safe_margin_price if hold_price > 0 else safe_margin_price, ',d') + "원 매매추적 처리", parse_mode='HTML')
                        else:
                            cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] 매매추적 미처리")
                    finally:
                        thread_conn.close()

            except Exception as top_e:
                print(f"process_trail71 오류 [{nick}]: {top_e}")
                cb_bot.send_message(chat_id=cb_user_id, text="-" + nick + "-[" + cb_company + "] [추적등록 오류] " + str(top_e))

        query.edit_message_text(text="[" + cb_company + "] 주문 처리 중...")
        threads_t71 = []
        for nick in target_nicks:
            if nick is not None:
                ac_t = account(nick)
                t_acct_no = ac_t['acct_no']
                t_access_token = ac_t['access_token']
                t_app_key = ac_t['app_key']
                t_app_secret = ac_t['app_secret']
                t_nick = nick
            else:
                t_acct_no = ac_default['acct_no']
                t_access_token = ac_default['access_token']
                t_app_key = ac_default['app_key']
                t_app_secret = ac_default['app_secret']
                t_nick = arguments[1]
            t = threading.Thread(target=process_trail71, args=(t_nick, t_acct_no, t_access_token, t_app_key, t_app_secret))
            threads_t71.append(t)
            t.start()
        for t in threads_t71:
            t.join()

    elif command == "다시계산" and "trail71" in data_selected:
        menuNum = "71"
        selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
        query.edit_message_text(text="[선택계좌: " + selected_str + "]\n종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.")

    elif command == "추적변경":
        try:
            with get_conn().cursor() as cur_tc:
                cur_tc.execute("""
                    SELECT DISTINCT code, name FROM trading_trail
                    WHERE trail_tp IN ('1', '2', 'L')
                    AND trail_day = prev_business_day_char(CURRENT_DATE)
                    AND basic_qty > 0
                    ORDER BY name
                """)
                tc_rows = cur_tc.fetchall()
            if tc_rows:
                tc_buttons = [
                    InlineKeyboardButton(name, callback_data=f"trail_change_{code}")
                    for code, name in tc_rows
                ]
                query.edit_message_text(
                    text="변경할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(tc_buttons, 2))
                )
            else:
                query.edit_message_text(text="추적 중인 종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[추적변경] 오류: {str(e)}")

    elif command.startswith("acc_") and command.endswith("_all"):
        # 전체선택/해제: callback_data = "acc_{menu_num}_all"
        menu_num = command.split("_")[1]
        current_acc = arguments[1] if len(arguments) > 1 else ""
        extra = [current_acc] if current_acc and current_acc not in SELECTABLE_ACCOUNTS else []
        all_accounts = extra + SELECTABLE_ACCOUNTS
        all_checked = len(g_selected_accounts) == len(all_accounts) and all(a in g_selected_accounts for a in all_accounts)
        g_selected_accounts.clear()
        if not all_checked:
            g_selected_accounts.extend(all_accounts)
        show_account_selection_keyboard(query, menu_num)

    elif command.startswith("acc_") and "_t_" in command:
        # 계좌 토글: callback_data = "acc_{menu_num}_t_{account_name}"
        parts = command.split("_t_", 1)
        account_name = parts[1]
        menu_num = parts[0].split("_", 1)[1]
        if account_name in g_selected_accounts:
            g_selected_accounts.remove(account_name)
        else:
            g_selected_accounts.append(account_name)
        show_account_selection_keyboard(query, menu_num)

    elif command.startswith("acc_") and command.endswith("_confirm"):
        # 계좌 선택 확인: callback_data = "acc_{menu_num}_confirm"
        menu_num = command.split("_")[1]
        menuNum = menu_num
        # 신호 버튼 경유 시 종목 컨텍스트와 신호값 힌트를 포함한 텍스트 입력 프롬프트 표시
        if menu_num == "71" and _trail71_from_signal:
            _trail71_from_signal = False
            selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
            stock_prefix = f"{g_trail71_company}(<code>{g_trail71_code}</code>)" if g_trail71_company else ""
            hint = f"{g_trail71_code},{g_trail71_buy_price},{g_trail71_loss_price},{g_trail71_amt_buy_amt},{g_trail71_amt_buy_amt-g_trail71_item_loss_sum}"
            query.edit_message_text(
                text=f"[선택계좌: {selected_str}]\n{stock_prefix}, 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.\n참고: {hint}",
                parse_mode='HTML'
            )
            return
        prompt_texts = {
            "01": "매도가(현재가:0), 매도율(%)을 입력하세요.",
            "02": "수정할 가격 값을 입력하세요. (숫자만 입력)",
            "03": "매도가를 입력하세요. (현재가:0)",
            "21": "종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.",
            "71": "종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액을 입력하세요.",
            "72": "종목코드(종목명), 이탈가(현재가:0), 목표가(현재가5%:0), 최종이탈가(저가:0), 매도비율(취소:0)을 입력하세요.",
        }
        selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
        prompt = prompt_texts.get(menu_num, "입력하세요.")
        if menu_num == "52N":
            # 주문취소 — 입력 불필요, 즉시 실행
            cn_code    = g_cncl_code
            cn_company = g_cncl_company
            cn_dvsn    = g_cncl_dvsn
            cn_label   = "매도" if cn_dvsn == "01" else "매수"
            selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
            query.edit_message_text(text=f"[{cn_label}] [{cn_company}({cn_code})] 주문취소 처리 중...")
            target_nicks_cn = g_selected_accounts[:] if g_selected_accounts else [None]

            def process_nick_52n(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                t_nick_label = nick if nick else arguments[1]
                try:
                    output_52n = daily_order_complete(
                        t_access_token, t_app_key, t_app_secret, t_acct_no, cn_code, "", cn_dvsn
                    )
                    if not output_52n:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"-{t_nick_label}-[{cn_company}] {cn_label} 미체결 주문 없음")
                        return
                    tdf_52n = pd.DataFrame(output_52n)
                    tdf_52n = tdf_52n[(tdf_52n['rmn_qty'].astype(int) > 0) & (tdf_52n['cncl_yn'] != 'Y')]
                    if tdf_52n.empty:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"-{t_nick_label}-[{cn_company}] {cn_label} 미체결 주문 없음")
                        return
                    for _, row in tdf_52n.iterrows():
                        order_no     = row['odno']
                        ord_price    = int(row['ord_unpr'])
                        ord_qty      = int(row['ord_qty'])
                        ord_excg_id  = row.get('excg_id_dvsn_cd', None)
                        try:
                            c_52n = order_cancel_revice(
                                t_access_token, t_app_key, t_app_secret, t_acct_no,
                                "02", order_no, "0", "0", ord_excg_id
                            )
                            if c_52n is not None and c_52n['ODNO'] != "":
                                context.bot.send_message(
                                    chat_id=query.message.chat_id,
                                    text=(f"-{t_nick_label}-[{cn_company}(<code>{cn_code}</code>)] "
                                          f"{cn_label} 주문취소 완료 "
                                          f"{format(ord_price, ',')}원 {format(ord_qty, ',')}주 "
                                          f"주문번호:<code>{str(int(c_52n['ODNO']))}</code>"),
                                    parse_mode='HTML'
                                )
                                try:
                                    with get_conn().cursor() as cur_52n:
                                        cur_52n.execute("""
                                            UPDATE trading_trail SET trail_tp = %s, mod_dt = %s
                                            WHERE acct_no = %s AND code = %s
                                            AND trail_day = %s AND order_no = %s
                                        """, ("C", datetime.now(), t_acct_no, cn_code,
                                              datetime.now().strftime("%Y%m%d"), str(int(order_no))))
                                        get_conn().commit()
                                except Exception:
                                    pass
                            else:
                                context.bot.send_message(chat_id=query.message.chat_id,
                                    text=f"-{t_nick_label}-[{cn_company}] {cn_label} 주문취소 실패")
                        except Exception as e:
                            context.bot.send_message(chat_id=query.message.chat_id,
                                text=f"-{t_nick_label}-[{cn_company}] {cn_label} 주문취소 오류: {str(e)}")
                except Exception as e:
                    context.bot.send_message(chat_id=query.message.chat_id,
                        text=f"-{t_nick_label}-[{cn_company}] 주문체결 조회 오류: {str(e)}")

            threads_52n = []
            for nick in target_nicks_cn:
                if nick is not None:
                    try:
                        ac_52n = account(nick)
                    except Exception as e:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"-{nick}- 계좌조회 오류: {str(e)}")
                        continue
                    t_acct_52n = ac_52n['acct_no']
                    t_tok_52n  = ac_52n['access_token']
                    t_key_52n  = ac_52n['app_key']
                    t_sec_52n  = ac_52n['app_secret']
                else:
                    try:
                        ac_52n_def = account(arguments[1])
                    except Exception as e:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"계좌조회 오류: {str(e)}")
                        continue
                    t_acct_52n = ac_52n_def['acct_no']
                    t_tok_52n  = ac_52n_def['access_token']
                    t_key_52n  = ac_52n_def['app_key']
                    t_sec_52n  = ac_52n_def['app_secret']
                t = threading.Thread(target=process_nick_52n,
                                     args=(nick, t_acct_52n, t_tok_52n, t_key_52n, t_sec_52n))
                threads_52n.append(t)
                t.start()
                time.sleep(0.5)
            for t in threads_52n:
                t.join()
            menuNum = "0"
            return

        elif menu_num == "63N":
            # 예약취소 — 입력 불필요, 즉시 실행
            cn63_code  = g_rsv_cncl_code
            cn63_name  = g_rsv_cncl_name
            cn63_dvsn  = g_rsv_cncl_dvsn
            cn63_label = "매도" if cn63_dvsn == "01" else "매수"
            query.edit_message_text(text=f"[{cn63_label}예약] [{cn63_name}({cn63_code})] 예약취소 처리 중...")
            target_nicks_63n = g_selected_accounts[:] if g_selected_accounts else [None]

            def process_nick_63n(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                t_nick_label = nick if nick else arguments[1]
                now_63n = datetime.now()
                t_start_63n = (now_63n + timedelta(days=1)).strftime("%Y%m%d") if now_63n > now_63n.replace(hour=15, minute=40, second=0, microsecond=0) else now_63n.strftime("%Y%m%d")
                t_end_63n   = (now_63n + relativedelta(months=1)).strftime("%Y%m%d")
                t_end_63n   = get_previous_business_day(t_end_63n)
                try:
                    output_63n = order_reserve_complete(t_access_token, t_app_key, t_app_secret, t_start_63n, t_end_63n, str(t_acct_no), "")
                    if not output_63n:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"-{t_nick_label}-[{cn63_name}] {cn63_label} 예약정보 없음")
                        return
                    df_63n = pd.DataFrame(output_63n)
                    matched_63n = df_63n[(df_63n['pdno'] == cn63_code) & (df_63n['sll_buy_dvsn_cd'] == cn63_dvsn) & (df_63n['cncl_ord_dt'] == "")]
                    if matched_63n.empty:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"-{t_nick_label}-[{cn63_name}] {cn63_label} 취소 가능한 예약주문 없음")
                        return
                    for _, row in matched_63n.iterrows():
                        rsvn_seq_63n = str(int(row['rsvn_ord_seq']))
                        rsvn_qty_63n = int(row['ord_rsvn_qty'])
                        rsvn_price_63n = int(row['ord_rsvn_unpr'])
                        sll_buy_cd_63n = row['sll_buy_dvsn_cd']
                        rsvn_end_dt_63n = row['rsvn_end_dt']
                        try:
                            rsv_cncl_result = order_reserve_cancel_revice(
                                t_access_token, t_app_key, t_app_secret,
                                str(t_acct_no), "02",
                                cn63_code, str(rsvn_qty_63n), str(rsvn_price_63n),
                                sll_buy_cd_63n, "01" if rsvn_price_63n == 0 else "00",
                                rsvn_end_dt_63n, rsvn_seq_63n
                            )
                            if rsv_cncl_result and rsv_cncl_result.get('NRML_PRCS_YN', '') == 'Y':
                                context.bot.send_message(
                                    chat_id=query.message.chat_id,
                                    text=(f"-{t_nick_label}-[{cn63_name}(<code>{cn63_code}</code>)] "
                                          f"{cn63_label} 예약취소 완료 | 예약번호: {rsvn_seq_63n} | "
                                          f"{format(rsvn_price_63n, ',d')}원 | {format(rsvn_qty_63n, ',d')}주"),
                                    parse_mode='HTML'
                                )
                            else:
                                context.bot.send_message(chat_id=query.message.chat_id,
                                    text=f"-{t_nick_label}-[{cn63_name}] 예약번호:{rsvn_seq_63n} 예약취소 실패")
                        except Exception as e:
                            context.bot.send_message(chat_id=query.message.chat_id,
                                text=f"-{t_nick_label}-[{cn63_name}(<code>{cn63_code}</code>)] 예약취소 오류: {str(e)}",
                                parse_mode='HTML')
                except Exception as e:
                    context.bot.send_message(chat_id=query.message.chat_id,
                        text=f"-{t_nick_label}-[{cn63_name}] 예약조회 오류: {str(e)}")

            threads_63n = []
            for nick in target_nicks_63n:
                if nick is not None:
                    try:
                        ac_63n = account(nick)
                    except Exception as e:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"-{nick}- 계좌조회 오류: {str(e)}")
                        continue
                    t_acct_63n = ac_63n['acct_no']
                    t_tok_63n  = ac_63n['access_token']
                    t_key_63n  = ac_63n['app_key']
                    t_sec_63n  = ac_63n['app_secret']
                else:
                    try:
                        ac_63n_def = account(arguments[1])
                    except Exception as e:
                        context.bot.send_message(chat_id=query.message.chat_id,
                            text=f"계좌조회 오류: {str(e)}")
                        continue
                    t_acct_63n = ac_63n_def['acct_no']
                    t_tok_63n  = ac_63n_def['access_token']
                    t_key_63n  = ac_63n_def['app_key']
                    t_sec_63n  = ac_63n_def['app_secret']
                t = threading.Thread(target=process_nick_63n,
                                     args=(nick, t_acct_63n, t_tok_63n, t_key_63n, t_sec_63n))
                threads_63n.append(t)
                t.start()
                time.sleep(0.5)
            for t in threads_63n:
                t.join()
            menuNum = "0"
            return

        elif menu_num == "51N":
            menuNum = "51N"
            selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
            dvsn_label = "매도" if g_corr_dvsn == "01" else "매수"
            query.edit_message_text(
                text=(f"[선택계좌: {selected_str}]\n"
                      f"[{g_corr_company}({g_corr_code})] {dvsn_label} 정정가(현재가:0)를 입력하세요.")
            )
        elif menu_num == "SL":
            menuNum = "SL"
            selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "선택 없음(현재계좌)"
            query.edit_message_text(
                text=(f"[선택계좌: {selected_str}]\n"
                      f"[{g_sell3x_company}({g_sell3x_code})] 매도가(현재가:0), 매도비율(1~100)을 입력하세요.")
            )
        elif menu_num == "FB":
            menuNum = "FB"
            query.edit_message_text(
                text=(f"[선택계좌: {selected_str}]\n"
                      f"[{g_fibo_name}({g_fibo_code})] 고가(자동:0), 저가(자동:0), 매도비율(1~100)을 입력하세요.\n"
                      f"(0 입력 시 최근 1개월 일봉 고가/저가 자동 적용)")
            )
        elif menu_num == "61B":
            menuNum = "61B"
            query.edit_message_text(
                text=(f"[선택계좌: {selected_str}]\n"
                      f"종목코드, 매수가(현재가:0), 매수금액(원), 예약종료일(YYYYMMDD, 생략시 30일후)을 입력하세요.\n")
            )
        elif menu_num == "61S":
            menuNum = "61S"
            query.edit_message_text(
                text=(f"[선택계좌: {selected_str}]\n"
                      f"[{g_rsv_sell_name}({g_rsv_sell_code})] "
                      f"매도가(현재가:0), 매도비율(%), 예약종료일(YYYYMMDD, 생략시 30일후)을 입력하세요.\n")
            )
        elif menu_num == "62N":
            menuNum = "62N"
            rc_label = "매도" if g_rsv_corr_dvsn == "01" else "매수"
            query.edit_message_text(
                text=(f"[선택계좌: {selected_str}]\n"
                      f"[{g_rsv_corr_name}({g_rsv_corr_code})] {rc_label} 예약정정\n"
                      f"정정가(현재가:0), 예약종료일(YYYYMMDD, 생략시 30일후)을 입력하세요.\n")
            )
        elif menu_num == "01" and g_holding_sell_name:
            stock_prefix = f"[{g_holding_sell_name}(<code>{g_holding_sell_code}</code>)] "
            query.edit_message_text(
                text=f"[선택계좌: {selected_str}]\n{stock_prefix}{prompt}",
                parse_mode='HTML'
            )
        else:
            query.edit_message_text(text=f"[선택계좌: {selected_str}]\n{prompt}")

    elif command == "추적준비":
        query.edit_message_text(
            text="📅 추적준비 시작일을 선택하세요",
            reply_markup=build_date_buttons1(38)  # 최근 38일
        )

    elif command.startswith("sell_trace_date:"):
        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[추적 준비]",
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
            trail_day = post_business_day_char(business_day)
            prev_date = get_previous_business_day((datetime.strptime(business_day, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"))
            result_msgs = []

            # 계좌잔고 조회
            c = stock_balance(access_token, app_key, app_secret, acct_no, "")
            
            conn199 = get_conn()
            cur199 = conn199.cursor()
            balance_rows = []

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

            #  일별 매매 잔고 현행화
            for i in range(len(c)):
                record_to_insert199 = (
                    acct_no,
                    c['pdno'][i],
                    c['prdt_name'][i],
                    business_day.replace('-', ''),
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

                if int(c['hldg_qty'][i]) > 0:
                    balance_rows.append((
                        acct_no,
                        c['pdno'][i],                   # code
                        c['prdt_name'][i],              # name
                        float(c['pchs_avg_pric'][i]),   # purchase_price
                        int(c['hldg_qty'][i])           # purchase_qty
                    ))

            conn199.commit()
            cur199.close()

            if len(balance_rows) > 0:
                balance_sql_tmpl = """
                WITH balance(acct_no, code, name, purchase_price, purchase_qty) AS (
                    VALUES %%s
                ),
                sim AS (
                    SELECT *
                    FROM (
                        SELECT DISTINCT ON (code)
                            acct_no,
                            name,
                            code,
                            trail_day,
                            trail_dtm,
                            basic_price,
                            basic_qty,
                            stop_price,
                            target_price,
                            volumn,
                            trail_tp,
                            trade_tp,
                            exit_price,
                            loss_amt
                        FROM trading_trail
                        WHERE acct_no = %s
                        AND trail_day = %s
                        AND trail_tp IN ('1','2','3','L','P','C','U')
                        ORDER BY code, trail_dtm DESC                        
                    ) t
                )
                """

                insert_query_tmpl = """
                INSERT INTO trading_trail (
                    acct_no,
                    name,
                    code,
                    trail_day,
                    trail_dtm,
                    trail_tp,
                    basic_price,
                    basic_qty,
                    basic_amt,
                    volumn,
                    stop_price,
                    target_price,
                    proc_min,
                    trade_tp,
                    exit_price,
                    loss_amt,
                    crt_dt,
                    mod_dt
                )
                SELECT
                    BAL.acct_no,
                    BAL.name,
                    BAL.code,
                    %s AS trail_day,
                    '090000' AS trail_dtm,
                    CASE WHEN COALESCE(S.trail_tp, '1') IN ('3', 'L') THEN 'L' ELSE  CASE WHEN COALESCE(S.trail_tp, '1') IN ('P','C','U') THEN 'P' ELSE '1' END END AS trail_tp,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_price ELSE S.basic_price END AS basic_price,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_qty ELSE S.basic_qty END AS basic_qty,
                    CASE WHEN COALESCE(BAL.purchase_qty, 0) > 0 THEN BAL.purchase_price*BAL.purchase_qty ELSE S.basic_price*S.basic_qty END AS basic_amt,
                    COALESCE(S.volumn, 0) AS volumn,
                    COALESCE(S.stop_price, 0) AS stop_price,
                    COALESCE(S.target_price, 0) AS target_price,
                    '090000' AS proc_min,
                    COALESCE(S.trade_tp, 'M') AS trade_tp,
                    COALESCE(S.exit_price, 0) AS exit_price,
                    COALESCE(S.loss_amt, 0) AS loss_amt,
                    now(),
                    now()
                FROM balance BAL
                LEFT JOIN sim S ON S.acct_no = BAL.acct_no AND S.code = BAL.code
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM trading_trail T
                    WHERE T.acct_no = BAL.acct_no
                    AND T.code = BAL.code
                    AND T.trail_day = %s
                    AND T.trail_dtm >= CASE WHEN S.trail_day = %s THEN S.trail_dtm ELSE '090000' END
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM public.dly_stock_balance DSB
                    WHERE DSB.acct::int = BAL.acct_no
                    AND DSB.code = BAL.code
                    AND DSB.dt = %s
                    AND DSB.trading_plan IN ('i', 'h')
                );
                """

                conn200 = get_conn()
                cur200 = conn200.cursor()
                full_query = cur200.mogrify(
                    balance_sql_tmpl + insert_query_tmpl,
                    (int(acct_no), prev_date, trail_day, trail_day, trail_day, prev_date)
                ).decode()

                execute_values(
                    cur200,
                    full_query,
                    balance_rows,
                    template="(%s, %s, %s, %s, %s)"
                )

                countProc = cur200.rowcount

                conn200.commit()
                cur200.close()

                if countProc >= 1:
                    msg = f"* [{trail_day}]-추적준비 등록 {countProc}건 처리"
                    result_msgs.append(msg)
                else:
                    msg = f"* [{trail_day}]-추적준비 등록 미처리"
                    result_msgs.append(msg)
            
            final_message = "\n".join(result_msgs) if result_msgs else "추적준비 등록 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=query.message.chat_id,
                message_id=query.message.message_id
            )

        except Exception as e:
            print('추적준비 등록 오류.', e)
            context.bot.edit_message_text(text="[추적준비 등록] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)   

    elif command == "추적삭제":
        query.edit_message_text(
            text="📅 추적 삭제 시작일을 선택하세요",
            reply_markup=build_date_buttons2(38)  # 최근 38일
        )
            
    elif command.startswith("trace_delete_date:"):            
        ac = account(arguments[1])
        acct_no = ac['acct_no']

        try:
            context.bot.edit_message_text(text="[추적삭제]",
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
            trail_day = post_business_day_char(business_day)
            result_msgs = []
        
            # 추적 delete
            conn200 = get_conn()
            cur200 = conn200.cursor()
            delete_query = """
                DELETE FROM trading_trail WHERE acct_no = %s AND trail_day = %s
                """
            # delete 인자값 설정
            cur200.execute(delete_query, (acct_no, trail_day))

            countProc = cur200.rowcount

            conn200.commit()
            cur200.close()

            if countProc >= 1:
                msg = f"* [{trail_day}]-추적 삭제 {countProc}건 처리"
                result_msgs.append(msg)
            else:
                msg = f"* [{trail_day}]-추적 삭제 미처리"
                result_msgs.append(msg)

            final_message = "\n".join(result_msgs) if result_msgs else "추적 삭제 대상이 존재하지 않습니다."

            context.bot.edit_message_text(
                text=final_message,
                parse_mode='HTML',
                chat_id=query.message.chat_id,
                message_id=query.message.message_id
            )                

        except Exception as e:
            print('추적 삭제 오류.', e)
            context.bot.edit_message_text(text="[추적 삭제] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)          

    elif command == "추적상태":
        button_list = build_button(["재개", "멈춤"], data_selected)
        show_markup = InlineKeyboardMarkup(build_menu(button_list, len(button_list)))

        context.bot.edit_message_text(text="상태를 선택해 주세요.",
                                        chat_id=update.callback_query.message.chat_id,
                                        message_id=update.callback_query.message.message_id,
                                        reply_markup=show_markup)

    elif command == "재개" and "추적상태" in data_selected:
        try:
            with get_conn().cursor() as cur_rs:
                cur_rs.execute("""
                    SELECT DISTINCT code, name FROM trading_trail
                    WHERE trail_tp IN ('P', 'C', 'U')
                    AND trail_day = prev_business_day_char(CURRENT_DATE)
                    AND basic_qty > 0
                    ORDER BY name
                """)
                resume_rows = cur_rs.fetchall()
            if resume_rows:
                rs_buttons = [
                    InlineKeyboardButton(name, callback_data=f"trail_resume_{code}")
                    for code, name in resume_rows
                ]
                query.edit_message_text(
                    text="재개할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(rs_buttons, 2))
                )
            else:
                query.edit_message_text(text="재개할 종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[추적재개] 오류: {str(e)}")

    elif command == "멈춤" and "추적상태" in data_selected:
        try:
            with get_conn().cursor() as cur_st:
                cur_st.execute("""
                    SELECT DISTINCT code, name FROM trading_trail
                    WHERE trail_tp IN ('1', '2', 'L')
                    AND trail_day = prev_business_day_char(CURRENT_DATE)
                    AND basic_qty > 0
                    ORDER BY name
                """)
                stop_rows = cur_st.fetchall()
            if stop_rows:
                st_buttons = [
                    InlineKeyboardButton(name, callback_data=f"trail_stop_{code}")
                    for code, name in stop_rows
                ]
                query.edit_message_text(
                    text="멈출 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(st_buttons, 2))
                )
            else:
                query.edit_message_text(text="멈출 종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[추적멈춤] 오류: {str(e)}")

    elif command == "매매추적":
        query.edit_message_text(
            text="📅 매매 추적 시작일을 선택하세요",
            reply_markup=build_date_buttons4(38)  # 최근 38일
        )
            
    elif command.startswith("trading_trail_date:"):            
        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']

        try:
            context.bot.edit_message_text(text="[매매추적]",
                                chat_id=query.message.chat_id,
                                message_id=query.message.message_id)
            
            business_day = command.split(":")[1]
            trail_day = post_business_day_char(business_day)
            result_msgs = []
        
            # 매매추적 select
            cur200 = get_conn().cursor()
            select_query = """
                SELECT code, name, trail_day, trail_dtm, trail_tp, trail_price, trail_qty, trail_amt, trail_rate, basic_price, basic_qty, basic_amt, stop_price, target_price, proc_min, exit_price, trade_tp, trade_result FROM trading_trail WHERE acct_no = %s AND trail_day = %s ORDER BY trail_tp, proc_min DESC
                """
            # select 인자값 설정
            cur200.execute(select_query, (acct_no, trail_day))
            result_two00 = cur200.fetchall()
            cur200.close()

            after_1520 = datetime.now().strftime('%H%M') >= '1520'
            nxt_targets = []  # (code, name) for NXT buttons

            if len(result_two00) > 0:

                for row in result_two00:
                    # 각 값이 None이면 0으로, 아니면 원래 값 유지
                    r = [val if val is not None else 0 for val in row]
                    # 각 컬럼을 변수에 할당 (언패킹)
                    (code, name, trail_day, trail_dtm, trail_tp,
                    trail_price, trail_qty, trail_amt, trail_rate, basic_price, basic_qty, basic_amt,
                    stop_price, target_price, proc_min, exit_price, trade_tp, trade_result) = r

                    # 일자별 종가
                    stck_prpr = get_kis_daily_chart(
                        stock_code=code,
                        trade_date=trail_day,
                        access_token=access_token,
                        app_key=app_key,
                        app_secret=app_secret
                    )
                    if stck_prpr is None:
                        stck_prpr = 0
                    stck_rate = round((100-(stck_prpr/basic_price)*100)*-1,2) if basic_price > 0 else 0  # 수익률
                    trail = ""
                    if trail_price > 0:
                        trail_qty = int(round(trail_amt/trail_price)) if trail_qty == 0 else trail_qty  # 추적수량
                        basic_qty = trail_qty if basic_qty == 0 else basic_qty
                        trail = (f", 추적가:{trail_price:,}원({trail_qty:,}주), 추적율:{trail_rate}%, 추적금액:{trail_amt:,}원, {'손절' if trade_tp == 'S' else '정액'} 매매결과:{trade_result}")

                    msg = (f"[{trail_day}-{proc_min[:2]}:{proc_min[2:4]}]{name}[<code>{code}</code>] -{trail_tp}- "
                        f"보유가:{basic_price:,}원({basic_qty:,}주), 보유금액:{basic_price*basic_qty:,}원, 현재가:{stck_prpr:,}원, "
                        f"수익율:{str(stck_rate)}%, 손절가:{stop_price:,}원, 목표가:{target_price:,}원, 최종이탈가:{exit_price:,}원{trail}")

                    result_msgs.append(msg)

                    # 15:20 이후 trail_tp '1','2' 대상 NXT 버튼 수집
                    if after_1520 and trail_tp in ('1', '2'):
                        nxt_targets.append((code, name))

            if result_msgs:
                # 메시지를 10개씩 묶어서 보냅니다 (원하는 개수로 조정 가능)
                chunk_size = 10
                chunks = [result_msgs[i:i + chunk_size] for i in range(0, len(result_msgs), chunk_size)]

                for idx, chunk in enumerate(chunks):
                    final_message = "\n\n".join(chunk) # 가독성을 위해 두 줄 바꿈 사용
                    
                    if idx == 0:
                        # 첫 번째 묶음은 기존 메뉴 메시지를 수정해서 출력
                        context.bot.edit_message_text(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id,
                            message_id=query.message.message_id
                        )
                    else:
                        # 두 번째 묶음부터는 새로운 메시지로 전송
                        context.bot.send_message(
                            text=final_message,
                            parse_mode='HTML',
                            chat_id=query.message.chat_id
                        )
            else:
                context.bot.edit_message_text(
                    text="매매 추적 대상이 존재하지 않습니다.",
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )

            # NXT 버튼: 15:20 이후 trail_tp '1','2' 대상 중 NXT 거래가능 종목만
            nxt_targets = [
                (c, n) for c, n in nxt_targets
                if is_nxt_able(access_token, app_key, app_secret, c)
            ]
            if nxt_targets:
                global g_nxt_pending
                g_nxt_pending[query.message.chat_id] = {
                    'acct_no':      acct_no,
                    'access_token': access_token,
                    'app_key':      app_key,
                    'app_secret':   app_secret,
                    'trail_day':    trail_day,
                }
                nxt_buttons = [
                    InlineKeyboardButton(f"{name} NXT", callback_data=f"trail_nxt:{code}")
                    for code, name in nxt_targets
                ]
                nxt_markup = InlineKeyboardMarkup(build_menu(nxt_buttons, 2))
                context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="NXT 매매추적 등록",
                    reply_markup=nxt_markup
                )

        except Exception as e:
            print('매매 추적 오류.', e)
            context.bot.edit_message_text(text="[매매 추적] 오류 : "+str(e),
                                            chat_id=query.message.chat_id,
                                            message_id=query.message.message_id)

    elif command.startswith("trail_nxt:"):
        user_id = query.message.chat_id
        clicked_code = command.split(":")[1]

        # 클릭된 버튼만 제거, 나머지 버튼은 유지
        try:
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != command
            ]
            if remaining:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup(build_menu(remaining, 2))
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
        except Exception:
            pass

        try:
            pending = g_nxt_pending.get(query.message.chat_id)
            if pending is None:
                context.bot.send_message(chat_id=user_id, text="[매매추적 NXT] 세션 정보가 없습니다. 매매추적을 다시 조회해주세요.")
                return

            acct_no      = pending['acct_no']
            access_token = pending['access_token']
            app_key      = pending['app_key']
            app_secret   = pending['app_secret']
            trail_day    = pending['trail_day']

            c = stock_balance(access_token, app_key, app_secret, acct_no, "")

            balance_rows = []
            if c is not None:
                for i in range(len(c)):
                    if c['pdno'][i] == clicked_code and int(c['hldg_qty'][i]) > 0:
                        balance_rows.append((
                            acct_no,
                            c['pdno'][i],
                            c['prdt_name'][i],
                            float(c['pchs_avg_pric'][i]),
                            int(c['hldg_qty'][i])
                        ))

            if len(balance_rows) > 0:
                balance_sql_tmpl = """
                    WITH balance(acct_no, code, name, purchase_price, purchase_qty) AS (
                        VALUES %%s
                    ),
                    sim AS (
                        SELECT *
                        FROM (
                            SELECT
                                acct_no,
                                name,
                                code,
                                trail_day,
                                trail_dtm,
                                trail_tp,
                                basic_price,
                                basic_qty,
                                stop_price,
                                target_price,
                                trade_tp,
                                trail_plan,
                                exit_price
                            FROM trading_trail
                            WHERE acct_no = %s
                            AND trail_day = %s
                            AND trail_tp IN ('1','2')
                        ) t
                    )
                """

                insert_query_tmpl = """
                    INSERT INTO trading_trail_nxt (
                        acct_no,
                        name,
                        code,
                        trail_day,
                        trail_dtm,
                        trail_tp,
                        basic_price,
                        basic_qty,
                        basic_amt,
                        stop_price,
                        target_price,
                        proc_min,
                        trade_tp,
                        trail_plan,
                        exit_price,
                        loss_amt,
                        crt_dt,
                        mod_dt
                    )
                    SELECT
                        BAL.acct_no,
                        BAL.name,
                        BAL.code,
                        S.trail_day AS trail_day,
                        %s AS trail_dtm,
                        S.trail_tp AS trail_tp,
                        COALESCE(BAL.purchase_price, 0) AS basic_price,
                        COALESCE(BAL.purchase_qty, 0) AS basic_qty,
                        COALESCE(BAL.purchase_price*BAL.purchase_qty, 0) AS basic_amt,
                        COALESCE(S.stop_price, 0) AS stop_price,
                        COALESCE(S.target_price, 0) AS target_price,
                        %s AS proc_min,
                        S.trade_tp AS trade_tp,
                        S.trail_plan AS trail_plan,
                        COALESCE(S.exit_price, 0) AS exit_price,
                        COALESCE((BAL.purchase_price-S.exit_price)*BAL.purchase_qty, 0) AS loss_amt,
                        now(),
                        now()
                    FROM balance BAL
                    JOIN sim S ON S.acct_no = BAL.acct_no AND S.code = BAL.code
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM trading_trail_nxt T
                        WHERE T.acct_no = BAL.acct_no
                        AND T.code = BAL.code
                        AND T.trail_day = S.trail_day
                        AND T.trail_dtm >= S.trail_dtm
                    )
                """

                conn200 = get_conn()
                cur200 = conn200.cursor()

                # 실제 삽입될 종목명 사전 조회 (trading_trail 매칭 + NOT EXISTS 조건)
                balance_codes = [row[1] for row in balance_rows]
                cur200.execute("""
                    SELECT t.name
                    FROM trading_trail t
                    WHERE t.acct_no = %s
                    AND t.trail_day = %s
                    AND t.trail_tp IN ('1','2')
                    AND t.code = ANY(%s)
                    AND NOT EXISTS (
                        SELECT 1 FROM trading_trail_nxt n
                        WHERE n.acct_no = t.acct_no
                        AND n.code = t.code
                        AND n.trail_day = t.trail_day
                        AND n.trail_dtm >= t.trail_dtm
                    )
                """, (int(acct_no), trail_day, balance_codes))
                insert_names = [row[0] for row in cur200.fetchall()]

                full_query = cur200.mogrify(
                    balance_sql_tmpl + insert_query_tmpl,
                    (int(acct_no), trail_day, datetime.now().strftime('%H%M%S'), datetime.now().strftime('%H%M%S'))
                ).decode()

                execute_values(
                    cur200,
                    full_query,
                    balance_rows,
                    template="(%s, %s, %s, %s, %s)"
                )
                inserted = cur200.rowcount
                conn200.commit()
                cur200.close()

                if inserted > 0:
                    name_list = ", ".join(insert_names) if insert_names else f"{inserted}건"
                    context.bot.send_message(chat_id=user_id, text=f"[매매추적 NXT] {name_list} 등록 완료")
                else:
                    context.bot.send_message(chat_id=user_id, text="[매매추적 NXT] 등록 대상이 없습니다.")
            else:
                context.bot.send_message(chat_id=user_id, text="[매매추적 NXT] 잔고 조회 결과가 없습니다.")

        except Exception as e:
            print('매매추적 NXT 오류.', e)
            context.bot.send_message(chat_id=user_id, text="[매매추적 NXT] 오류 : " + str(e))

    elif command.startswith("trail_resume_"):
        ts_code = command[len("trail_resume_"):]

        # ── 유효성 검증: 현재도 재개 대상(trail_tp IN ('P','C','U'))인지 확인 ──
        try:
            with get_conn().cursor() as cur_chk:
                cur_chk.execute("""
                    SELECT DISTINCT name FROM trading_trail
                    WHERE code = %s
                      AND trail_day = prev_business_day_char(CURRENT_DATE)
                      AND trail_tp IN ('P', 'C', 'U')
                      AND basic_qty > 0
                    ORDER BY name
                """, (ts_code,))
                chk_row = cur_chk.fetchone()
            if not chk_row:
                try:
                    context.bot.delete_message(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )
                except Exception:
                    pass
                context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"[{ts_code}] 더 이상 재개 대상이 아닙니다. (이미 재개되었거나 과거 버튼)"
                )
                return
        except Exception as e_chk:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[추적재개] 유효성 검증 오류: {str(e_chk)}"
            )
            return

        try:
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != command
            ]
            if remaining:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup(build_menu(remaining, 2))
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
        except Exception:
            pass
        try:
            with get_conn().cursor() as cur_rn:
                cur_rn.execute(
                    "SELECT t.acct_no, s.nick_name, t.name, "
                    "min(t.stop_price), max(t.target_price), min(t.exit_price) "
                    "FROM trading_trail t "
                    "JOIN \"stockAccount_stock_account\" s ON s.acct_no = t.acct_no "
                    "WHERE t.code = %s AND t.trail_day = prev_business_day_char(CURRENT_DATE) "
                    "AND t.basic_qty > 0 AND t.trail_tp IN ('P', 'C', 'U') "
                    "GROUP BY t.acct_no, s.nick_name, t.name",
                    (ts_code,)
                )
                rn_rows = cur_rn.fetchall()
            # rn_rows: [(acct_no, nick_name, name, min_stop, max_target, min_exit), ...]
            ts_name = rn_rows[0][2] if rn_rows else ts_code
            ts_stop_price  = int(rn_rows[0][3]) if rn_rows and rn_rows[0][3] else 0
            ts_target_price = int(rn_rows[0][4]) if rn_rows and rn_rows[0][4] else 0
            ts_exit_price  = int(rn_rows[0][5]) if rn_rows and rn_rows[0][5] else 0
        except Exception:
            rn_rows = []
            ts_name = ts_code
            ts_stop_price = ts_target_price = ts_exit_price = 0
        g_trail_state_code = ts_code
        g_trail_state_name = ts_name
        g_trail_state_accounts = rn_rows
        menuNum = '41B'
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"[{ts_name}({ts_code})] 이탈가 {ts_stop_price:,}원(저가:0), 목표가 {ts_target_price:,}원(고가:0), 최종이탈가 {ts_exit_price:,}원(저가:0), 추적상태(L,1,2)를 입력하세요."
        )

    elif command.startswith("trail_stop_"):
        ts_code = command[len("trail_stop_"):]

        # ── 유효성 검증: 현재도 추적 대상(trail_tp IN ('1','2','L'))인지 확인 ──
        try:
            with get_conn().cursor() as cur_chk:
                cur_chk.execute("""
                    SELECT DISTINCT name FROM trading_trail
                    WHERE code = %s
                      AND trail_day = prev_business_day_char(CURRENT_DATE)
                      AND trail_tp IN ('1', '2', 'L')
                      AND basic_qty > 0
                    ORDER BY name
                """, (ts_code,))
                chk_row = cur_chk.fetchone()
            if not chk_row:
                try:
                    context.bot.delete_message(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )
                except Exception:
                    pass
                context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"[{ts_code}] 더 이상 추적 대상이 아닙니다. (이미 멈춤/매도 처리되었거나 과거 버튼)"
                )
                return
        except Exception as e_chk:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[추적멈춤] 유효성 검증 오류: {str(e_chk)}"
            )
            return

        try:
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != command
            ]
            if remaining:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup(build_menu(remaining, 2))
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
        except Exception:
            pass
        try:
            c_sp = get_conn()
            with c_sp.cursor() as cur_sn:
                cur_sn.execute(
                    "SELECT name, stop_price, target_price, exit_price FROM trading_trail WHERE code = %s AND trail_day = prev_business_day_char(CURRENT_DATE) AND basic_qty > 0 ORDER BY trail_dtm DESC LIMIT 1",
                    (ts_code,)
                )
                sn_row = cur_sn.fetchone()
            ts_name = sn_row[0] if sn_row else ts_code
            ts_stop_price = 0
            ts_target_price = 0
            ts_exit_price = 0
            if sn_row:
                ts_stop_price = int(sn_row[1])
                ts_target_price = int(sn_row[2])
                ts_exit_price = int(sn_row[3])
            with c_sp.cursor() as cur_sp:
                cur_sp.execute("""
                    UPDATE trading_trail SET trail_tp = 'P', mod_dt = now()
                    WHERE code = %s
                    AND trail_day = prev_business_day_char(CURRENT_DATE)
                    AND trail_tp IN ('1', '2', 'L')
                    AND basic_qty > 0
                """, (ts_code,))
                updated_sp = cur_sp.rowcount
            c_sp.commit()
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[{ts_name}({ts_code})] 이탈가 {ts_stop_price:,}원, 목표가 {ts_target_price:,}원, 최종이탈가 {ts_exit_price:,}원 추적멈춤 처리 ({updated_sp}건)"
            )
        except Exception as e:
            try: get_conn().rollback()
            except Exception: pass
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[추적멈춤] 오류: {str(e)}"
            )

    elif command.startswith("trail_change_"):
        ts_code = command[len("trail_change_"):]

        # ── 유효성 검증: 현재도 변경 대상(trail_tp IN ('1','2','L'))인지 확인 ──
        try:
            with get_conn().cursor() as cur_chk:
                cur_chk.execute("""
                    SELECT DISTINCT name FROM trading_trail
                    WHERE code = %s
                      AND trail_day = prev_business_day_char(CURRENT_DATE)
                      AND trail_tp IN ('1', '2', 'L')
                      AND basic_qty > 0
                    ORDER BY name
                """, (ts_code,))
                chk_row = cur_chk.fetchone()
            if not chk_row:
                try:
                    context.bot.delete_message(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id
                    )
                except Exception:
                    pass
                context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"[{ts_code}] 더 이상 변경 대상이 아닙니다. (이미 멈춤/매도 처리되었거나 과거 버튼)"
                )
                return
        except Exception as e_chk:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[추적변경] 유효성 검증 오류: {str(e_chk)}"
            )
            return

        try:
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != command
            ]
            if remaining:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup(build_menu(remaining, 2))
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
        except Exception:
            pass
        try:
            with get_conn().cursor() as cur_tc2:
                cur_tc2.execute(
                     "SELECT t.acct_no, s.nick_name, t.name, "
                    "min(t.stop_price), max(t.target_price), min(t.exit_price) "
                    "FROM trading_trail t "
                    "JOIN \"stockAccount_stock_account\" s ON s.acct_no = t.acct_no "
                    "WHERE t.code = %s AND t.trail_day = prev_business_day_char(CURRENT_DATE) "
                    "AND t.basic_qty > 0 AND t.trail_tp IN ('1', '2', 'L') "
                    "GROUP BY t.acct_no, s.nick_name, t.name",
                    (ts_code,)
                )
                tc2_row = cur_tc2.fetchall()
            # tc2_row: [(acct_no, nick_name, name, min_stop, max_target, min_exit), ...]
            ts_name = tc2_row[0][2] if tc2_row else ts_code
            ts_stop_price  = int(tc2_row[0][3]) if tc2_row and tc2_row[0][3] else 0
            ts_target_price = int(tc2_row[0][4]) if tc2_row and tc2_row[0][4] else 0
            ts_exit_price  = int(tc2_row[0][5]) if tc2_row and tc2_row[0][5] else 0
        except Exception:
            tc2_row = []
            ts_name = ts_code
            ts_stop_price = ts_target_price = ts_exit_price = 0
        g_trail_state_code = ts_code
        g_trail_state_name = ts_name
        g_trail_state_accounts = tc2_row
        menuNum = '81B'
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"[{ts_name}({ts_code})] 이탈가 {ts_stop_price:,}원(저가:0), 목표가 {ts_target_price:,}원(고가:0), 최종이탈가 {ts_exit_price:,}원(저가:0), 매도비율(1~100)을 입력하세요."
        )                

    elif command in ("코스피", "코스닥"):
        global g_kk_code, g_kk_name, g_kk_field
        g_kk_code = '0001' if command == "코스피" else '1001'
        g_kk_name = '코스피' if command == "코스피" else '코스닥'
        try:
            with get_conn().cursor() as cur_kk:
                cur_kk.execute(
                    'SELECT through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price '
                    'FROM public."interestItem_interest_item" WHERE code = %s',
                    (g_kk_code,)
                )
                kk_row = cur_kk.fetchone()
            def _fmt(v):
                return format(int(v), ',d') if v is not None else '-'
            through_v, leave_v, resist_v, support_v, trend_high_v, trend_low_v = kk_row if kk_row else (None,)*6
            kk_field_btns = [
                InlineKeyboardButton(f"돌파가({_fmt(through_v)})",       callback_data="menu,kk_field_돌파가"),
                InlineKeyboardButton(f"이탈가({_fmt(leave_v)})",         callback_data="menu,kk_field_이탈가"),
                InlineKeyboardButton(f"저항가({_fmt(resist_v)})",        callback_data="menu,kk_field_저항가"),
                InlineKeyboardButton(f"지지가({_fmt(support_v)})",       callback_data="menu,kk_field_지지가"),
                InlineKeyboardButton(f"추세상한가({_fmt(trend_high_v)})", callback_data="menu,kk_field_추세상한가"),
                InlineKeyboardButton(f"추세이탈가({_fmt(trend_low_v)})", callback_data="menu,kk_field_추세이탈가"),
                InlineKeyboardButton("취소",                              callback_data="menu,취소"),
            ]
            rows_kk = [kk_field_btns[i:i+2] for i in range(0, len(kk_field_btns), 2)]
            query.edit_message_text(
                text=f"[{g_kk_name}({g_kk_code})] 변경할 항목을 선택하세요:",
                reply_markup=InlineKeyboardMarkup(rows_kk)
            )
        except Exception as e:
            query.edit_message_text(text=f"[{g_kk_name} 변경] 오류: {str(e)}")

    elif command.startswith("kk_field_"):
        global g_kk_field
        g_kk_field = command[len("kk_field_"):]
        menuNum = '06'
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"[{g_kk_name}({g_kk_code})] {g_kk_field} 값을 입력하세요. (숫자만 입력)"
        )

    elif command == "매수손실금액":
        menuNum = "91"

        context.bot.edit_message_text(text="종목코드(종목명), 매수가(현재가:0), 이탈가(저가:0)를 입력하세요.",
                                        chat_id=query.message.chat_id,
                                        message_id=query.message.message_id)

    elif command == "피보나치매도":
        try:
            ac_fb = account(arguments[1])
            c_fb = stock_balance(ac_fb['access_token'], ac_fb['app_key'], ac_fb['app_secret'], ac_fb['acct_no'], "")
            fb_buttons = [
                InlineKeyboardButton(
                    f"{c_fb['prdt_name'][i]}({c_fb['pdno'][i]})",
                    callback_data=f"fibo_sell_{c_fb['pdno'][i]}"
                )
                for i in range(len(c_fb.index))
                if int(c_fb['hldg_qty'][i]) > 0
            ]
            if fb_buttons:
                query.edit_message_text(
                    text="피보나치 매도할 종목을 선택하세요:",
                    reply_markup=InlineKeyboardMarkup(build_menu(fb_buttons, 2))
                )
            else:
                query.edit_message_text(text="보유종목이 없습니다.")
        except Exception as e:
            query.edit_message_text(text=f"[피보나치매도] 오류: {str(e)}")

    elif command.startswith("fibo_sell_"):
        fb_code = command[len("fibo_sell_"):]
        try:
            current_markup = query.message.reply_markup
            remaining = [
                btn
                for row in current_markup.inline_keyboard
                for btn in row
                if btn.callback_data != command
            ]
            if remaining:
                context.bot.edit_message_reply_markup(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                    reply_markup=InlineKeyboardMarkup(build_menu(remaining, 2))
                )
            else:
                context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id
                )
        except Exception:
            pass
        try:
            ac_fb2 = account(arguments[1])
            c_fb2 = stock_balance(ac_fb2['access_token'], ac_fb2['app_key'], ac_fb2['app_secret'], ac_fb2['acct_no'], "")
            fb_name = fb_code
            for i in range(len(c_fb2.index)):
                if c_fb2['pdno'][i] == fb_code:
                    fb_name = c_fb2['prdt_name'][i]
                    break
        except Exception:
            fb_name = fb_code
        g_fibo_code = fb_code
        g_fibo_name = fb_name
        g_selected_accounts.clear()
        show_account_selection_keyboard(query, "FB", send_new=True, chat_id=query.message.chat_id, bot=context.bot)

    elif command.startswith("fibo_price:"):
        parts_fp = command.split(":")
        # parts_fp: ['fibo_price', code, price, qty]
        if len(parts_fp) < 4:
            query.answer("잘못된 요청입니다.")
            return
        fp_code  = parts_fp[1]
        fp_price = parts_fp[2]
        fp_qty   = parts_fp[3]
        fp_name  = g_fibo_name if g_fibo_code == fp_code else fp_code
        sell_buttons = [
            InlineKeyboardButton("예약매도", callback_data=f"fibo_rsv:{fp_code}:{fp_price}:{fp_qty}"),
            InlineKeyboardButton("매도주문", callback_data=f"fibo_ord:{fp_code}:{fp_price}:{fp_qty}"),
        ]
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"[{fp_name}({fp_code})] {format(int(fp_price), ',d')}원 × {fp_qty}주\n주문 유형을 선택하세요:",
            reply_markup=InlineKeyboardMarkup(build_menu(sell_buttons, 2))
        )

    elif command.startswith("fibo_rsv:"):
        parts_fr = command.split(":")
        if len(parts_fr) < 4:
            query.answer("잘못된 요청입니다.")
            return
        fr_code  = parts_fr[1]
        fr_price = parts_fr[2]
        fr_qty   = int(parts_fr[3])
        fr_name  = g_fibo_name if g_fibo_code == fr_code else fr_code
        try:
            context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except Exception:
            pass
        reserve_end_dt = (datetime.today() + timedelta(days=30)).strftime('%Y%m%d')
        reserve_end_dt = get_previous_business_day(reserve_end_dt)
        target_nicks_fr = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_fibo_rsv(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            nick_label = f"[{nick}]" if nick else ""
            try:
                e_fr2 = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                ord_psbl_qty_fr = 0
                for i in range(len(e_fr2.index)):
                    if e_fr2['pdno'][i] == fr_code:
                        ord_psbl_qty_fr = int(e_fr2['ord_psbl_qty'][i])
                        break
                if ord_psbl_qty_fr <= 0:
                    context.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=f"{nick_label}[{fr_name}({fr_code})] 주문가능수량이 없어 예약매도 불가"
                    )
                else:
                    sell_qty_fr = min(fr_qty, ord_psbl_qty_fr)
                    rsv_result = order_reserve(
                        t_access_token, t_app_key, t_app_secret,
                        str(t_acct_no), fr_code, str(sell_qty_fr), str(fr_price),
                        "01", "00", reserve_end_dt
                    )
                    if rsv_result.get('RSVN_ORD_SEQ', '') != '':
                        context.bot.send_message(
                            chat_id=query.message.chat_id,
                            text=(f"{nick_label}[{fr_name}(<code>{fr_code}</code>)] "
                                  f"예약주문번호: <code>{rsv_result['RSVN_ORD_SEQ']}</code>\n"
                                  f"예약매도주문: {format(int(fr_price), ',d')}원 × {sell_qty_fr}주"),
                            parse_mode='HTML'
                        )
                    else:
                        context.bot.send_message(
                            chat_id=query.message.chat_id,
                            text=f"{nick_label}[{fr_name}({fr_code})] 예약매도주문 실패"
                        )
            except Exception as e:
                context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"{nick_label}[피보나치 예약매도] 오류: {str(e)}"
                )

        threads_fr = []
        for nick in target_nicks_fr:
            try:
                ac_fr = account(nick) if nick else account()
            except Exception as e:
                nick_label = f"[{nick}]" if nick else ""
                context.bot.send_message(chat_id=query.message.chat_id, text=f"{nick_label}[계좌조회 오류] {str(e)}")
                continue
            t = threading.Thread(
                target=process_nick_fibo_rsv,
                args=(nick, ac_fr['acct_no'], ac_fr['access_token'], ac_fr['app_key'], ac_fr['app_secret'])
            )
            threads_fr.append(t)
            t.start()
        for t in threads_fr:
            t.join()

    elif command.startswith("fibo_ord:"):
        parts_fo = command.split(":")
        if len(parts_fo) < 4:
            query.answer("잘못된 요청입니다.")
            return
        fo_code  = parts_fo[1]
        fo_price = parts_fo[2]
        fo_qty   = int(parts_fo[3])
        fo_name  = g_fibo_name if g_fibo_code == fo_code else fo_code
        try:
            context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except Exception:
            pass
        target_nicks_fo = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_fibo_ord(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            nick_label = f"[{nick}]" if nick else ""
            try:
                # 주문정보 존재시 취소 처리
                if order_cancel_proc(t_access_token, t_app_key, t_app_secret, str(t_acct_no), fo_code, '01') != 'success':
                    return
                e_fo2 = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                ord_psbl_qty_fo = 0
                for i in range(len(e_fo2.index)):
                    if e_fo2['pdno'][i] == fo_code:
                        ord_psbl_qty_fo = int(e_fo2['ord_psbl_qty'][i])
                        break
                if ord_psbl_qty_fo <= 0:
                    context.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=f"{nick_label}[{fo_name}({fo_code})] 주문가능수량이 없어 매도 불가"
                    )
                else:
                    sell_qty_fo = min(fo_qty, ord_psbl_qty_fo)
                    ord_result = order_cash(
                        False, t_access_token, t_app_key, t_app_secret,
                        str(t_acct_no), fo_code, "00", str(sell_qty_fo), str(int(fo_price))
                    )
                    ord_no = ord_result.get('KRX_FWDG_ORD_ORGNO', '') + '-' + ord_result.get('ODNO', '')
                    context.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=(f"{nick_label}[{fo_name}(<code>{fo_code}</code>)] "
                              f"주문번호: <code>{ord_no}</code>\n"
                              f"매도주문: {format(int(fo_price), ',d')}원 × {sell_qty_fo}주"),
                        parse_mode='HTML'
                    )
            except Exception as e:
                context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"{nick_label}[피보나치 매도주문] 오류: {str(e)}"
                )

        threads_fo = []
        for nick in target_nicks_fo:
            try:
                ac_fo = account(nick) if nick else account()
            except Exception as e:
                nick_label = f"[{nick}]" if nick else ""
                context.bot.send_message(chat_id=query.message.chat_id, text=f"{nick_label}[계좌조회 오류] {str(e)}")
                continue
            t = threading.Thread(
                target=process_nick_fibo_ord,
                args=(nick, ac_fo['acct_no'], ac_fo['access_token'], ac_fo['app_key'], ac_fo['app_secret'])
            )
            threads_fo.append(t)
            t.start()
        for t in threads_fo:
            t.join()

    elif data_selected.startswith('tp:'):
        # kis_trading_set.py 에서 전송한 종목 교체 고려 대상 이탈가(stop_price), 목표가(target_price), 최종이탈가(exit_price), 매도비율(trail_plan) 입력 처리
        parts = data_selected.split(':')
        if len(parts) == 7:
            global g_tp_pending
            g_tp_pending[query.message.chat_id] = {
                'acct_no':   parts[1],
                'name':      parts[2],
                'code':      parts[3],
                'trail_day': parts[4],
                'trail_dtm': parts[5],
                'trail_tp':  parts[6],
            }
            menuNum = 'tp'
            # 버튼 메시지는 그대로 유지 — 새 메시지로 입력 요청
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"[{parts[2]}] 이탈가(현재가:0), 목표가(현재가5%:0), 최종이탈가(저가:0), 매도비율(취소:0)을 입력하세요."
            )

get_handler = CommandHandler('reserve', get_command)
updater.dispatcher.add_handler(get_handler)

updater.dispatcher.add_handler(CallbackQueryHandler(callback_get))

def is_signed_float_2dec(val: str) -> bool:
    """양수/음수 실수 허용, 소숫점 2자리까지"""
    pattern = r"^-?\d+(\.\d{1,2})?$"
    return re.match(pattern, val) is not None

def get_tick_size(price):
    """가격에 따른 국내 주식 호가단위 계산 (2025년 기준)"""
    if price < 2000:
        return 1
    elif price < 5000:
        return 5
    elif price < 20000:
        return 10
    elif price < 50000:
        return 50
    elif price < 200000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000

def round_to_valid_price(price, tick, direction='nearest'):
    """입력 가격을 호가단위에 맞게 조정"""
    if direction == 'up':
        return math.ceil(price / tick) * tick
    elif direction == 'down':
        return math.floor(price / tick) * tick
    else:
        return round(price / tick) * tick

# 날짜형식 변환(년월)
def get_date_str(s):
    r = re.search(r"\d{4}/\d{2}", s)
    return r.group().replace('/', '-') if r else ''

def initMenuNum():
    global menuNum
    global chartReq
    menuNum = "0"
    chartReq = "0"

def echo(update, context):
    user_id = update.effective_chat.id
    user_text = update.message.text
    global g_buy_amount
    global g_buy_price
    global g_sell_amount
    global g_sell_price
    global g_buy_code
    global g_sell_code
    global g_code
    global g_company
    global g_market_buy_company
    global g_market_sell_company
    global g_market_buy_code
    global g_market_sell_code
    global g_market_buy_amount
    global g_market_sell_amount
    global chartReq
    global g_order_no
    global g_revise_price
    global g_dvsn_cd
    global g_remain_qty
    global g_loss_price
    global g_risk_sum
    global g_low_price
    global g_selected_accounts
    global menuNum
    global g_sell3x_code, g_sell3x_company
    global g_corr_code, g_corr_company, g_corr_dvsn
    global g_cncl_code, g_cncl_company, g_cncl_dvsn
    global g_fibo_code, g_fibo_name
    global g_rsv_sell_code, g_rsv_sell_name
    global g_rsv_corr_code, g_rsv_corr_name, g_rsv_corr_dvsn
    global g_rsv_cncl_code, g_rsv_cncl_name, g_rsv_cncl_dvsn

    # 관심종목 가격 직접입력 대기 처리
    pending = _pending_register.get(user_id)
    if pending and pending.get('waiting_input'):
        parts = user_text.strip().replace(' ', '').split(',')
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            input_high = int(parts[0])
            input_low  = int(parts[1])
            # 0 입력시 금일고가/금일저가(자동조회값) 사용
            if input_high != 0:
                pending['through_price'] = input_high
            if input_low != 0:
                pending['leave_price'] = input_low
            del pending['waiting_input']
            _pending_register.pop(user_id, None)
            _do_interest_register(user_id, context, pending)
        else:
            context.bot.send_message(
                chat_id=user_id,
                text="입력 형식이 올바르지 않습니다. 쉼표로 구분된 숫자 두 개를 입력하세요.\n예) 75000,73000  (0 입력시 금일고가/저가 자동적용)"
            )
        return

    code = ""
    company = ""
    # 주식 현재가
    stck_prpr = ''
    # 전일 대비율
    prdy_ctrt = ''
    # 누적 거래량
    acml_vol = ''
    # 전일 대비 거래량 비율
    prdy_vrss_vol_rate = ''
    # HTS 시가총액
    hts_avls = ''
    # PBR
    pbr = ''
    # BPS
    bps = ''
    # 대표시장
    market_kor = ''
    # 업종
    industry_kor = ''

    chartReq = "1"

    # 보유종목 매도 — 종목코드는 버튼 선택 시 저장된 전역값 사용, 매도가+매도율만 입력
    if menuNum == '01':
        initMenuNum()
        global g_holding_sell_code, g_holding_sell_name, g_holding_sell_price
        parts01 = user_text.strip().split(',', 1)
        if (len(parts01) < 2
                or not parts01[0].strip().isdecimal()
                or not parts01[1].strip().isdecimal()
                or not (1 <= int(parts01[1].strip()) <= 100)):
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{g_holding_sell_name}] 매도가(현재가:0) 정수, 매도 비율은 1~100 사이 정수로 입력하세요."
            )
            return
        sell_price_01 = g_holding_sell_price if parts01[0].strip() == '0' else int(parts01[0].strip())
        sell_price_01 = round_to_valid_price(sell_price_01, get_tick_size(sell_price_01))
        sell_ratio_01 = int(parts01[1].strip())
        cb_code_01    = g_holding_sell_code
        cb_name_01    = g_holding_sell_name
        target_nicks_01 = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_01(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                c_bal = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                t_hldg_qty = 0
                for i, _ in enumerate(c_bal.index):
                    if c_bal['pdno'][i] == cb_code_01:
                        t_hldg_qty = int(c_bal['hldg_qty'][i])
                        break
                if t_hldg_qty == 0:
                    context.bot.send_message(chat_id=user_id,
                        text=f"-{nick}-[{cb_name_01}(<code>{cb_code_01}</code>)] 보유 수량 없음", parse_mode='HTML')
                    return
                t_sell_qty = max(1, int(t_hldg_qty * sell_ratio_01 / 100))
                c_ord = order_cash(False, t_access_token, t_app_key, t_app_secret, str(t_acct_no),
                                   cb_code_01, "00", str(t_sell_qty), str(sell_price_01))
                if c_ord is not None and c_ord['ODNO'] != "":
                    time.sleep(0.5)
                    output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret,
                                                   t_acct_no, cb_code_01, c_ord['ODNO'], '01')
                    tdf = pd.DataFrame(output1)
                    d = tdf[['odno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'tot_ccld_qty', 'tot_ccld_amt', 'rmn_qty']]
                    for k, _ in enumerate(d.index):
                        d_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                        d_qty   = d['ord_qty'][k]
                        d_no    = int(d['odno'][k])
                        context.bot.send_message(
                            chat_id=user_id,
                            text=f"-{nick}-[{cb_name_01}(<code>{cb_code_01}</code>)] "
                                 f"매도가:{format(int(d_price), ',d')}원 | 매도량:{format(int(d_qty), ',d')}주 "
                                 f"({sell_ratio_01}%) 매도주문 완료, 주문번호:<code>{d_no}</code>",
                            parse_mode='HTML'
                        )
                else:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"-{nick}-[{cb_name_01}(<code>{cb_code_01}</code>)] "
                             f"매도가:{format(sell_price_01, ',d')}원 | 매도량:{format(t_sell_qty, ',d')}주 매도주문 실패",
                        parse_mode='HTML'
                    )
            except Exception as e:
                context.bot.send_message(chat_id=user_id,
                    text=f"-{nick}-[{cb_name_01}(<code>{cb_code_01}</code>)] 매도주문 오류: {str(e)}", parse_mode='HTML')

        threads_01 = []
        for nick in target_nicks_01:
            if nick is not None:
                ac_01 = account(nick)
            else:
                ac_01 = account(arguments[1])
                nick  = arguments[1]
            t = threading.Thread(target=process_nick_01,
                                 args=(nick, ac_01['acct_no'], ac_01['access_token'],
                                       ac_01['app_key'], ac_01['app_secret']))
            threads_01.append(t)
            t.start()
        for t in threads_01:
            t.join()
        return

    # 보유종목 필드값 수정 — 입력값은 가격 숫자만, 종목코드 입력 없음
    if menuNum == '02':
        initMenuNum()
        global g_holding_edit_code, g_holding_edit_name, g_holding_edit_field
        field_col_map = {
            "이탈가":    "sign_support_price",
            "최종이탈가": "end_loss_price",
            "목표가":    "sign_resist_price",
            "최종목표가": "end_target_price",
        }
        col = field_col_map.get(g_holding_edit_field)
        if not col:
            context.bot.send_message(chat_id=user_id, text="수정할 항목이 선택되지 않았습니다.")
            return
        if not user_text.strip().lstrip('-').isdecimal():
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_holding_edit_name}({g_holding_edit_code})] {g_holding_edit_field}: 숫자만 입력하세요.")
            return
        new_val = int(user_text.strip())
        new_val = round_to_valid_price(new_val, get_tick_size(new_val))
        try:
            c02 = get_conn()
            with c02.cursor() as cur02:
                cur02.execute(
                    f'UPDATE public."stockBalance_stock_balance" SET {col} = %s '
                    f"WHERE code = %s AND proc_yn = 'Y'",
                    (new_val, g_holding_edit_code)
                )
                updated = cur02.rowcount
            c02.commit()
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{g_holding_edit_name}(<code>{g_holding_edit_code}</code>)] "
                     f"{g_holding_edit_field} → {format(new_val, ',d')}원 ({updated}건 업데이트)",
                parse_mode='HTML'
            )
        except Exception as e:
            try:
                get_conn().rollback()
            except Exception:
                pass
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_holding_edit_name}] {g_holding_edit_field} 업데이트 오류: {str(e)}")
        return

    # kis_holding_item_total.py 에서 전송한 전량매도 대상 매도가 입력 처리
    if menuNum == '03':
        initMenuNum()
        global g_signal_sell_code, g_signal_sell_name, g_signal_sell_qty
        if not user_text.strip().isdecimal():
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_signal_sell_name}] 매도가는 숫자(0=현재가)로 입력하세요.")
            return
        input_price = int(user_text.strip())
        input_price = round_to_valid_price(input_price, get_tick_size(input_price))
        ss_code = g_signal_sell_code
        ss_name = g_signal_sell_name
        ss_qty  = g_signal_sell_qty

        def process_signal_sell_94():
            try:
                ac_03 = account(arguments[1])
                sell_price_03 = input_price
                if sell_price_03 == 0:
                    ap = inquire_price(ac_03['access_token'], ac_03['app_key'], ac_03['app_secret'], ss_code)
                    sell_price_03 = int(ap['stck_prpr'])
                c_ord = order_cash(False, ac_03['access_token'], ac_03['app_key'], ac_03['app_secret'],
                                   str(ac_03['acct_no']), ss_code, "00", str(ss_qty), str(sell_price_03))
                if c_ord is not None and c_ord['ODNO'] != "":
                    time.sleep(0.5)
                    output1 = daily_order_complete(ac_03['access_token'], ac_03['app_key'], ac_03['app_secret'],
                                                   ac_03['acct_no'], ss_code, c_ord['ODNO'], '01')
                    tdf = pd.DataFrame(output1)
                    d = tdf[['odno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'tot_ccld_qty', 'tot_ccld_amt', 'rmn_qty']]
                    for k, _ in enumerate(d.index):
                        d_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                        d_qty  = d['ord_qty'][k]
                        d_ccld = d['tot_ccld_qty'][k]
                        d_amt  = d['tot_ccld_amt'][k]
                        d_rmn  = d['rmn_qty'][k]
                        d_no   = int(d['odno'][k])
                        context.bot.send_message(
                            chat_id=user_id,
                            text=f"[{ss_name}(<code>{ss_code}</code>)] "
                                 f"매도가:{format(int(d_price), ',d')}원 | 주문량:{format(int(d_qty), ',d')}주 | "
                                 f"체결량:{format(int(d_ccld), ',d')}주 | 체결금:{format(int(d_amt), ',d')}원 | "
                                 f"잔량:{format(int(d_rmn), ',d')}주 | 주문번호:<code>{d_no}</code>",
                            parse_mode='HTML'
                        )
                else:
                    context.bot.send_message(chat_id=user_id,
                        text=f"[{ss_name}({ss_code})] 전량매도 주문 실패")
            except Exception as e:
                context.bot.send_message(chat_id=user_id,
                    text=f"[{ss_name}({ss_code})] 전량매도 오류: {str(e)}")

        threading.Thread(target=process_signal_sell_94).start()
        return

    # 관심종목 필드값 수정 — 숫자만 입력
    if menuNum == '04':
        initMenuNum()
        global g_interest_edit_code, g_interest_edit_name, g_interest_edit_field
        ii_field_col_map = {
            "1차저항가":  "through_price",
            "1차지지가":  "leave_price",
            "2차저항가":  "resist_price",
            "2차지지가":  "support_price",
            "추세상한가": "trend_high_price",
            "추세이탈가": "trend_low_price",
        }
        col04 = ii_field_col_map.get(g_interest_edit_field)
        if not col04:
            context.bot.send_message(chat_id=user_id, text="수정할 항목이 선택되지 않았습니다.")
            return
        if not user_text.strip().lstrip('-').isdecimal():
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_interest_edit_name}({g_interest_edit_code})] {g_interest_edit_field}: 숫자만 입력하세요.")
            return
        new_val04 = int(user_text.strip())
        new_val04 = round_to_valid_price(new_val04, get_tick_size(new_val04))
        try:
            c04 = get_conn()
            # 현재 DB 값 조회 (연쇄 비교용)
            with c04.cursor() as cur_r:
                cur_r.execute(
                    'SELECT through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price '
                    'FROM public."interestItem_interest_item" '
                    "WHERE code = %s AND proc_yn = 'Y' AND interest_day >= prev_business_day_char(CURRENT_DATE) AND length(code) > 4",
                    (g_interest_edit_code,)
                )
                cur_row = cur_r.fetchone()
            if cur_row:
                _, _, db_resist, db_support, db_trend_high, db_trend_low = [
                    int(v) if v is not None else 0 for v in cur_row
                ]
            else:
                db_resist = db_support = db_trend_high = db_trend_low = 0

            # 변경할 컬럼/값 결정 (연쇄 규칙 적용)
            updates = {col04: new_val04}
            changed_labels = [g_interest_edit_field]

            if g_interest_edit_field == "1차저항가":
                if new_val04 > db_resist:
                    updates["resist_price"] = new_val04
                    changed_labels.append("2차저항가")
                if new_val04 > db_trend_high:
                    updates["trend_high_price"] = new_val04
                    changed_labels.append("추세상한가")
            elif g_interest_edit_field == "2차저항가":
                if new_val04 > db_trend_high:
                    updates["trend_high_price"] = new_val04
                    changed_labels.append("추세상한가")
            elif g_interest_edit_field == "1차지지가":
                if new_val04 < db_support:
                    updates["support_price"] = new_val04
                    changed_labels.append("2차지지가")
                if new_val04 < db_trend_low:
                    updates["trend_low_price"] = new_val04
                    changed_labels.append("추세이탈가")
            elif g_interest_edit_field == "2차지지가":
                if new_val04 < db_trend_low:
                    updates["trend_low_price"] = new_val04
                    changed_labels.append("추세이탈가")

            set_clause = ", ".join(f"{k} = %s" for k in updates)
            values = list(updates.values()) + [g_interest_edit_code]
            with c04.cursor() as cur04:
                cur04.execute(
                    f'UPDATE public."interestItem_interest_item" SET last_chg_date = now(), {set_clause} '
                    f"WHERE code = %s AND proc_yn = 'Y'",
                    values
                )
                updated04 = cur04.rowcount
            c04.commit()
            changed_str = ", ".join(changed_labels)
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{g_interest_edit_name}(<code>{g_interest_edit_code}</code>)] "
                     f"{changed_str} → {format(new_val04, ',d')}원 ({updated04}건 업데이트)",
                parse_mode='HTML'
            )
        except Exception as e:
            try: get_conn().rollback()
            except Exception: pass
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_interest_edit_name}] {g_interest_edit_field} 업데이트 오류: {str(e)}")
        return

    # 신규 관심종목 등록 — 종목코드(종목명) 입력 후 가격 조회 → 2단계 확인
    if menuNum == '05':
        initMenuNum()
        try:
            ac05 = account(arguments[1])
            input_stripped = user_text.strip()
            # 종목코드 또는 종목명으로 코드 확인
            if input_stripped[:6].isdecimal() and len(input_stripped) >= 6:
                ii_new_code = input_stripped[:6].zfill(6)
                match05 = stock_code[stock_code.code == ii_new_code]
                ii_new_name = match05.company.values[0].strip() if len(match05) > 0 else ii_new_code
            else:
                match05 = stock_code[stock_code.company.str.strip() == input_stripped]
                if len(match05) == 0:
                    context.bot.send_message(chat_id=user_id,
                        text=f"'{input_stripped}' 종목을 찾을 수 없습니다. 종목코드(6자리) 또는 정확한 종목명을 입력하세요.")
                    return
                ii_new_code = match05.code.values[0]
                ii_new_name = input_stripped
            # 금일 고가/저가
            ap05 = inquire_price(ac05['access_token'], ac05['app_key'], ac05['app_secret'], ii_new_code)
            today_high = int(ap05['stck_hgpr'])
            today_low  = int(ap05['stck_lwpr'])
            # 20일 최고가/최저가 (일봉 중 최근 20일)
            d20_high, d20_low = get_period_high_low(ac05['access_token'], ac05['app_key'], ac05['app_secret'],
                                                     ii_new_code, period="D", count=20)
            # 1년 최고가/최저가 (월봉 12개월)
            y1_high, y1_low = get_period_high_low(ac05['access_token'], ac05['app_key'], ac05['app_secret'],
                                                   ii_new_code, period="M", count=12)
            d20_high = d20_high if d20_high is not None else 0
            d20_low  = d20_low  if d20_low  is not None else 0
            y1_high  = y1_high  if y1_high  is not None else 0
            y1_low   = y1_low   if y1_low   is not None else 0
            _pending_register[user_id] = {
                'code': ii_new_code,
                'name': ii_new_name,
                'acct_reg': str(ac05['acct_no']),
                'through_price': today_high,
                'leave_price': today_low,
                'd20_high': d20_high,
                'd20_low': d20_low,
                'y1_high': y1_high,
                'y1_low': y1_low,
            }
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("현재값으로 등록", callback_data=f"menu,interest_confirm_{ii_new_code}"),
                InlineKeyboardButton("직접입력", callback_data=f"menu,interest_manual_{ii_new_code}"),
            ]])
            context.bot.send_message(
                chat_id=user_id,
                text=(f"[{ii_new_name}(<code>{ii_new_code}</code>)] 관심종목 등록\n"
                      f"  1차저항가(금일고가): {format(today_high, ',d')}원\n"
                      f"  1차지지가(금일저가): {format(today_low, ',d')}원\n"
                      f"  2차저항가(20일고가): {format(d20_high, ',d')}원\n"
                      f"  2차지지가(20일저가): {format(d20_low, ',d')}원\n"
                      f"  추세상한가(1년고가): {format(y1_high, ',d')}원\n"
                      f"  추세이탈가(1년저가): {format(y1_low, ',d')}원\n\n"
                      f"등록하시겠습니까?"),
                parse_mode='HTML',
                reply_markup=markup
            )
        except Exception as e:
            context.bot.send_message(chat_id=user_id,
                text=f"[관심종목 신규 등록] 오류: {str(e)}")
        return

    # 코스피/코스닥 필드값 변경 — 숫자만 입력
    if menuNum == '06':
        initMenuNum()
        global g_kk_code, g_kk_name, g_kk_field
        kk_field_col_map = {
            "돌파가":    "through_price",
            "이탈가":    "leave_price",
            "지지가":    "support_price",
            "저항가":    "resist_price",
            "추세상한가": "trend_high_price",
            "추세이탈가": "trend_low_price",
        }
        col06 = kk_field_col_map.get(g_kk_field)
        if not col06:
            context.bot.send_message(chat_id=user_id, text="변경할 항목이 선택되지 않았습니다.")
            return
        if not user_text.strip().lstrip('-').isdecimal():
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_kk_name}({g_kk_code})] {g_kk_field}: 숫자만 입력하세요.")
            return
        new_val06 = int(user_text.strip())
        try:
            c06 = get_conn()
            with c06.cursor() as cur06:
                cur06.execute(
                    f'UPDATE public."interestItem_interest_item" '
                    f'SET last_chg_date = now(), {col06} = %s '
                    f"WHERE code = %s",
                    (new_val06, g_kk_code)
                )
                updated06 = cur06.rowcount
            c06.commit()
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{g_kk_name}({g_kk_code})] {g_kk_field} → {format(new_val06, ',d')}원 ({updated06}건 업데이트)"
            )
        except Exception as e:
            try: get_conn().rollback()
            except Exception: pass
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_kk_name}] {g_kk_field} 업데이트 오류: {str(e)}")
        return

    # kis_trading_trail_vol_state.py 에서 전송한 전일저가 이탈 전량매도 대상 매도가 입력 처리
    if menuNum == '07':
        initMenuNum()
        global g_trail_sell_code, g_trail_sell_name, g_trail_sell_qty
        if not user_text.strip().isdecimal():
            context.bot.send_message(chat_id=user_id,
                text=f"[{g_signal_sell_name}] 매도가는 숫자(0=현재가)로 입력하세요.")
            return
        input_price = int(user_text.strip())
        input_price = round_to_valid_price(input_price, get_tick_size(input_price))
        ss_code = g_trail_sell_code
        ss_name = g_trail_sell_name
        ss_qty  = g_trail_sell_qty

        def process_trail_sell_07():
            try:
                ac_03 = account(arguments[1])
                sell_price_03 = input_price
                if sell_price_03 == 0:
                    ap = inquire_price(ac_03['access_token'], ac_03['app_key'], ac_03['app_secret'], ss_code)
                    sell_price_03 = int(ap['stck_prpr'])
                c_ord = order_cash(False, ac_03['access_token'], ac_03['app_key'], ac_03['app_secret'],
                                   str(ac_03['acct_no']), ss_code, "00", str(ss_qty), str(sell_price_03))
                if c_ord is not None and c_ord['ODNO'] != "":
                    time.sleep(0.5)
                    output1 = daily_order_complete(ac_03['access_token'], ac_03['app_key'], ac_03['app_secret'],
                                                   ac_03['acct_no'], ss_code, c_ord['ODNO'], '01')
                    tdf = pd.DataFrame(output1)
                    d = tdf[['odno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'tot_ccld_qty', 'tot_ccld_amt', 'rmn_qty']]
                    for k, _ in enumerate(d.index):
                        d_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                        d_qty  = d['ord_qty'][k]
                        d_ccld = d['tot_ccld_qty'][k]
                        d_amt  = d['tot_ccld_amt'][k]
                        d_rmn  = d['rmn_qty'][k]
                        d_no   = int(d['odno'][k])
                        context.bot.send_message(
                            chat_id=user_id,
                            text=f"[{ss_name}(<code>{ss_code}</code>)] "
                                 f"매도가:{format(int(d_price), ',d')}원 | 주문량:{format(int(d_qty), ',d')}주 | "
                                 f"체결량:{format(int(d_ccld), ',d')}주 | 체결금:{format(int(d_amt), ',d')}원 | "
                                 f"잔량:{format(int(d_rmn), ',d')}주 | 주문번호:<code>{d_no}</code>",
                            parse_mode='HTML'
                        )
                else:
                    context.bot.send_message(chat_id=user_id,
                        text=f"[{ss_name}({ss_code})] 전량매도 주문 실패")
            except Exception as e:
                context.bot.send_message(chat_id=user_id,
                    text=f"[{ss_name}({ss_code})] 전량매도 오류: {str(e)}")

        threading.Thread(target=process_trail_sell_07).start()
        return

    # 추적재개 — 버튼에서 종목 선택 후 이탈가, 목표가, 최종이탈가, 추적상태만 입력
    if menuNum == '41B':
        initMenuNum()
        ts_code = g_trail_state_code
        ts_name = g_trail_state_name
        parts41B = user_text.strip().split(',')
        if (len(parts41B) < 4
                or not parts41B[0].strip().isdecimal()
                or not parts41B[1].strip().isdecimal()
                or not parts41B[2].strip().isdecimal()
                or parts41B[3].strip() not in ('L', '1', '2')):
            context.bot.send_message(chat_id=user_id,
                text=f"[{ts_name}] 이탈가(저가:0), 목표가(고가:0), 최종이탈가(저가:0), 추적상태(L,1,2) 형식이 올바르지 않습니다.")
            menuNum = '41B'
            return

        def process_nick_41b(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                ap_41b = inquire_price(t_access_token, t_app_key, t_app_secret, ts_code)
                stck_lwpr_41b = int(ap_41b['stck_lwpr'])
                stck_hgpr_41b = int(ap_41b['stck_hgpr'])
            except Exception as e:
                context.bot.send_message(chat_id=user_id, text=f"[추적재개] 가격 조회 오류: {str(e)}")
                return
            stop_price_41b   = stck_lwpr_41b if int(parts41B[0].strip()) == 0 else int(parts41B[0].strip())
            stop_price_41b = round_to_valid_price(stop_price_41b, get_tick_size(stop_price_41b))
            target_price_41b = stck_hgpr_41b if int(parts41B[1].strip()) == 0 else int(parts41B[1].strip())
            target_price_41b = round_to_valid_price(target_price_41b, get_tick_size(target_price_41b))
            exit_price_41b   = stck_lwpr_41b if int(parts41B[2].strip()) == 0 else int(parts41B[2].strip())
            exit_price_41b = round_to_valid_price(exit_price_41b, get_tick_size(exit_price_41b))
            trail_tp_41b = parts41B[3].strip()
            hour_minute_41b = datetime.now().strftime('%H%M%S')
            try:
                c_bal = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                hold_price = 0
                hldg_qty = 0
                hold_amt = 0
                for i in range(len(c_bal)):
                    if c_bal['pdno'][i] == ts_code:
                        hold_price = float(c_bal['pchs_avg_pric'][i])
                        hldg_qty = int(c_bal['hldg_qty'][i])
                        hold_amt = int(c_bal['pchs_amt'][i])
                        break
            except Exception as e:
                context.bot.send_message(chat_id=user_id, text=f"-{nick}- [추적재개] 잔고 조회 오류: {str(e)}")
                return
            thread_conn = db.connect(conn_string)
            try:
                with thread_conn.cursor() as cur:
                    cur.execute("""
                        WITH target AS (
                            SELECT acct_no, code, trail_day, trail_dtm, trail_tp
                            FROM trading_trail
                            WHERE acct_no = %s AND code = %s
                            AND trail_day = prev_business_day_char(CURRENT_DATE)
                            AND trail_tp IN ('C', 'U', 'P')
                            AND basic_qty > 0
                            ORDER BY trail_dtm DESC
                            LIMIT 1
                        )
                        UPDATE trading_trail SET
                            trail_dtm = %s, trail_tp = %s, stop_price = %s, target_price = %s, exit_price = %s,
                            proc_min = %s, mod_dt = %s, basic_price = %s, basic_qty = %s, basic_amt = %s,
                            trail_plan = NULL, trail_price = NULL, trail_rate = NULL,
                            trail_qty = NULL, trail_amt = NULL, volumn = NULL
                        FROM target
                        WHERE trading_trail.acct_no = target.acct_no
                          AND trading_trail.code = target.code
                          AND trading_trail.trail_day = target.trail_day
                          AND trading_trail.trail_dtm = target.trail_dtm
                          AND trading_trail.trail_tp = target.trail_tp
                        RETURNING 1
                    """, (
                        t_acct_no, ts_code,
                        hour_minute_41b, trail_tp_41b,
                        stop_price_41b, target_price_41b, exit_price_41b,
                        hour_minute_41b, datetime.now(),
                        int(hold_price), hldg_qty, hold_amt,
                    ))
                    was_updated = cur.fetchone() is not None
                thread_conn.commit()
                if was_updated:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"-{nick}-[{ts_name}(<code>{ts_code}</code>)] 이탈가:{format(stop_price_41b, ',d')}원, 목표가:{format(target_price_41b, ',d')}원, 최종이탈가:{format(exit_price_41b, ',d')}원, 추적상태:{trail_tp_41b} 추적재개 처리",
                        parse_mode='HTML'
                    )
                else:
                    context.bot.send_message(chat_id=user_id,
                        text=f"-{nick}-[{ts_name}({ts_code})] 이탈가:{format(stop_price_41b, ',d')}원, 목표가:{format(target_price_41b, ',d')}원, 최종이탈가:{format(exit_price_41b, ',d')}원, 추적상태:{trail_tp_41b} 추적재개 미처리")
            except Exception as e:
                thread_conn.rollback()
                context.bot.send_message(chat_id=user_id, text=f"-{nick}- [추적재개] 오류: {str(e)}")
            finally:
                thread_conn.close()

        threads_41b = []
        rows_41b = g_trail_state_accounts if g_trail_state_accounts else []
        if not rows_41b:
            context.bot.send_message(chat_id=user_id, text=f"[{ts_name}({ts_code})] 추적재개 대상 계좌가 없습니다.")
            return
        for row in rows_41b:
            t_acct_no = row[0]
            nick = row[1]
            try:
                ac_row = account(nick)
                t_access_token = ac_row['access_token']
                t_app_key = ac_row['app_key']
                t_app_secret = ac_row['app_secret']
            except Exception as e:
                context.bot.send_message(chat_id=user_id, text=f"-{nick}- [추적재개] 계정 조회 오류: {str(e)}")
                continue
            t = threading.Thread(
                target=process_nick_41b,
                args=(nick, t_acct_no, t_access_token, t_app_key, t_app_secret)
            )
            threads_41b.append(t)
            t.start()
        for t in threads_41b:
            t.join()
        return

    if menuNum == '81B':
        initMenuNum()
        ts_code = g_trail_state_code
        ts_name = g_trail_state_name
        parts81B = user_text.strip().split(',')
        if (len(parts81B) < 4
                or not parts81B[0].strip().isdecimal()
                or not parts81B[1].strip().isdecimal()
                or not parts81B[2].strip().isdecimal()
                or not is_positive_int(parts81B[3].strip())):
            context.bot.send_message(chat_id=user_id,
                text=f"[{ts_name}] 이탈가(현재가:0), 목표가(고가:0), 최종이탈가(저가:0), 매도비율(1~100) 형식이 올바르지 않습니다.")
            menuNum = '81B'
            return

        def process_nick_81b(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            try:
                ap_81b = inquire_price(t_access_token, t_app_key, t_app_secret, ts_code)
                stck_lwpr_81b = int(ap_81b['stck_lwpr'])
                stck_hgpr_81b = int(ap_81b['stck_hgpr'])
            except Exception as e:
                context.bot.send_message(chat_id=user_id, text=f"[추적변경] 가격 조회 오류: {str(e)}")
                return
            stop_price_81b   = stck_lwpr_81b if int(parts81B[0].strip()) == 0 else int(parts81B[0].strip())
            stop_price_81b = round_to_valid_price(stop_price_81b, get_tick_size(stop_price_81b))
            target_price_81b = stck_hgpr_81b if int(parts81B[1].strip()) == 0 else int(parts81B[1].strip())
            target_price_81b = round_to_valid_price(target_price_81b, get_tick_size(target_price_81b))
            exit_price_81b   = stck_lwpr_81b if int(parts81B[2].strip()) == 0 else int(parts81B[2].strip())
            exit_price_81b = round_to_valid_price(exit_price_81b, get_tick_size(exit_price_81b))
            sell_rate_81b = int(parts81B[3].strip())
            hour_minute_81b = datetime.now().strftime('%H%M%S')
            try:
                c_bal = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")
                hold_price = 0
                hldg_qty = 0
                hold_amt = 0
                for i in range(len(c_bal)):
                    if c_bal['pdno'][i] == ts_code:
                        hold_price = float(c_bal['pchs_avg_pric'][i])
                        hldg_qty = int(c_bal['hldg_qty'][i])
                        hold_amt = int(c_bal['pchs_amt'][i])
                        break
            except Exception as e:
                context.bot.send_message(chat_id=user_id, text=f"-{nick}- [추적변경] 잔고 조회 오류: {str(e)}")
                return
            
            sell_qty_81b = int(hldg_qty * sell_rate_81b * 0.01)
            if sell_qty_81b <= 0:
                context.bot.send_message(chat_id=user_id,
                    text=f"[{ts_name}({ts_code})] 보유수량 부족으로 미처리 (보유:{hldg_qty}주, 매도비율:{sell_rate_81b}%)")
                return
            
            thread_conn = db.connect(conn_string)
            try:
                with thread_conn.cursor() as cur:
                    cur.execute("""
                        WITH target AS (
                            SELECT acct_no, code, trail_day, trail_dtm, trail_tp
                            FROM trading_trail
                            WHERE acct_no = %s AND code = %s
                            AND trail_day = prev_business_day_char(CURRENT_DATE)
                            AND trail_tp IN ('1', '2', 'L')
                            AND basic_qty > 0
                            ORDER BY trail_dtm DESC
                            LIMIT 1
                        )
                        UPDATE trading_trail SET
                            trail_dtm = %s, trail_tp = '1', stop_price = %s, target_price = %s, exit_price = %s,
                            proc_min = %s, mod_dt = %s, basic_price = %s, basic_qty = %s, basic_amt = %s,
                            trail_plan = %s, trail_price = NULL, trail_rate = NULL,
                            trail_qty = NULL, trail_amt = NULL, volumn = NULL
                        FROM target
                        WHERE trading_trail.acct_no = target.acct_no
                          AND trading_trail.code = target.code
                          AND trading_trail.trail_day = target.trail_day
                          AND trading_trail.trail_dtm = target.trail_dtm
                          AND trading_trail.trail_tp = target.trail_tp
                        RETURNING 1
                    """, (
                        t_acct_no, ts_code,
                        hour_minute_81b, stop_price_81b, target_price_81b, exit_price_81b,
                        hour_minute_81b, datetime.now(), int(hold_price), hldg_qty, hold_amt,
                        parts81B[3].strip()
                    ))
                    was_updated = cur.fetchone() is not None
                thread_conn.commit()
                if was_updated:
                    context.bot.send_message(chat_id=user_id,
                        text=f"[{ts_name}(<code>{ts_code}</code>)] 보유가:{format(int(hold_price), ',d')}원, 보유량:{format(hldg_qty, ',d')}주, "
                            f"목표가:{format(target_price_81b, ',d')}원, 이탈가:{format(stop_price_81b, ',d')}원, "
                            f"최종이탈가:{format(exit_price_81b, ',d')}원, 매도비율:{sell_rate_81b}% 추적변경 처리",
                        parse_mode='HTML')
                else:
                    context.bot.send_message(chat_id=user_id,
                        text=f"[{ts_name}({ts_code})] 목표가:{format(target_price_81b, ',d')}원, 이탈가:{format(stop_price_81b, ',d')}원, "
                            f"최종이탈가:{format(exit_price_81b, ',d')}원, 매도비율:{sell_rate_81b}% 추적변경 미처리")
            except Exception as e:
                thread_conn.rollback()
                context.bot.send_message(chat_id=user_id, text=f"-{nick}- [추적변경] 오류: {str(e)}")
            finally:
                thread_conn.close()

        threads_81b = []
        rows_81b = g_trail_state_accounts if g_trail_state_accounts else []
        if not rows_81b:
            context.bot.send_message(chat_id=user_id, text=f"[{ts_name}({ts_code})] 추적변경 대상 계좌가 없습니다.")
            return
        for row in rows_81b:
            t_acct_no = row[0]
            nick = row[1]
            try:
                ac_row = account(nick)
                t_access_token = ac_row['access_token']
                t_app_key = ac_row['app_key']
                t_app_secret = ac_row['app_secret']
            except Exception as e:
                context.bot.send_message(chat_id=user_id, text=f"-{nick}- [추적변경] 계정 조회 오류: {str(e)}")
                continue
            t = threading.Thread(
                target=process_nick_81b,
                args=(nick, t_acct_no, t_access_token, t_app_key, t_app_secret)
            )
            threads_81b.append(t)
            t.start()
        for t in threads_81b:
            t.join()
        return

    if menuNum == 'FB':
        initMenuNum()
        fb_code = g_fibo_code
        fb_name = g_fibo_name
        parts_fb = user_text.strip().split(',')
        if (len(parts_fb) < 3
                or not parts_fb[0].strip().isdecimal()
                or not parts_fb[1].strip().isdecimal()
                or not is_positive_int(parts_fb[2].strip())):
            context.bot.send_message(chat_id=user_id,
                text=f"[{fb_name}] 고가(자동:0), 저가(자동:0), 매도비율(1~100) 형식이 올바르지 않습니다.")
            menuNum = 'FB'
            return
        try:
            ac_fb = account(arguments[1])
            ap_fb = inquire_price(ac_fb['access_token'], ac_fb['app_key'], ac_fb['app_secret'], fb_code)
            cur_price = int(ap_fb['stck_prpr'])

            # 고가/저가 0이면 최근 1개월 일봉 고가/저가 자동 조회
            input_high = int(parts_fb[0].strip())
            input_low  = int(parts_fb[1].strip())
            sell_rate  = int(parts_fb[2].strip())

            if input_high == 0 or input_low == 0:
                auto_high, auto_low = get_period_high_low(
                    ac_fb['access_token'], ac_fb['app_key'], ac_fb['app_secret'], fb_code, period="D", count=30
                )
                if input_high == 0:
                    input_high = auto_high if auto_high else cur_price
                if input_low == 0:
                    input_low = auto_low if auto_low else cur_price

            if input_high <= input_low:
                context.bot.send_message(chat_id=user_id,
                    text=f"[{fb_name}] 고가({format(input_high, ',d')})가 저가({format(input_low, ',d')}) 이하입니다.")
                return

            input_high = round_to_valid_price(input_high, get_tick_size(input_high))
            input_low = round_to_valid_price(input_low, get_tick_size(input_low))
            diff = input_high - input_low

            # 피보나치 되돌림 수준 계산 (고가 → 저가 방향)
            fb_levels = [
                (0.0,   input_high),
                (23.6,  round_to_valid_price(int(round(input_high - diff * 0.236)), get_tick_size(int(round(input_high - diff * 0.236))))),
                (38.2,  round_to_valid_price(int(round(input_high - diff * 0.382)), get_tick_size(int(round(input_high - diff * 0.382))))),
                (50.0,  round_to_valid_price(int(round(input_high - diff * 0.500)), get_tick_size(int(round(input_high - diff * 0.500))))),
                (61.8,  round_to_valid_price(int(round(input_high - diff * 0.618)), get_tick_size(int(round(input_high - diff * 0.618))))),
                (76.4,  round_to_valid_price(int(round(input_high - diff * 0.764)), get_tick_size(int(round(input_high - diff * 0.764))))),
                (100.0, input_low),
            ]

            # 보유수량 조회 (매도수량 계산용)
            hldg_qty = 0
            try:
                c_bal = stock_balance(ac_fb['access_token'], ac_fb['app_key'], ac_fb['app_secret'], ac_fb['acct_no'], "")
                for i in range(len(c_bal)):
                    if c_bal['pdno'][i] == fb_code:
                        hldg_qty = int(c_bal['hldg_qty'][i])
                        break
            except Exception:
                pass

            sell_qty_per_level = int(hldg_qty * sell_rate / 100) if hldg_qty > 0 else 0

            closest_idx = min(range(len(fb_levels)), key=lambda i: abs(fb_levels[i][1] - cur_price))
            fb_buttons = []
            for idx, (ratio, price) in enumerate(fb_levels):
                marker = " ◀" if idx == closest_idx else ""
                btn_text = f"{ratio:.1f}%  {format(price, ',d')}원{marker}"
                cb_data = f"fibo_price:{fb_code}:{price}:{sell_qty_per_level}"
                fb_buttons.append(InlineKeyboardButton(btn_text, callback_data=cb_data))

            info_text = (
                f"[{fb_name}(<code>{fb_code}</code>)] 피보나치 매도가\n"
                f"현재가: {format(cur_price, ',d')}원 | 고가: {format(input_high, ',d')}원 | 저가: {format(input_low, ',d')}원\n"
                f"보유수량: {format(hldg_qty, ',d')}주 | 매도비율: {sell_rate}% → 매도수량: {format(sell_qty_per_level, ',d')}주"
            )
            context.bot.send_message(
                chat_id=user_id,
                text=info_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(build_menu(fb_buttons, 1))
            )
        except Exception as e:
            context.bot.send_message(chat_id=user_id, text=f"[피보나치매도] 오류: {str(e)}")
        return

    # kis_trading_set.py 에서 전송한 종목 교체 고려 대상 이탈가(stop_price), 목표가(target_price), 최종이탈가(exit_price), 매도비율(trail_plan) 입력 처리
    if menuNum == 'tp':
        val_text = user_text.split(',', 3)
        if val_text[3] == '0':
            initMenuNum()
            g_tp_pending.pop(user_id, None)
            context.bot.send_message(chat_id=user_id, text="취소하였습니다.")
            return
        if len(val_text) < 4 or not val_text[0].isdecimal() or not val_text[1].isdecimal() or not val_text[2].isdecimal() or not val_text[3].isdigit() or not (1 <= int(val_text[3]) <= 100):
            context.bot.send_message(
                chat_id=user_id,
                text="이탈가(현재가:0), 목표가(현재가5%:0), 최종이탈가(저가:0), 매도비율(취소:0) 미존재 또는 부적합"
            )
            return
        initMenuNum()
        item = g_tp_pending.pop(user_id, None)
        if not item:
            context.bot.send_message(chat_id=user_id, text="선택된 종목이 없습니다. 버튼을 다시 선택하세요.")
            return
        trail_plan = str(int(val_text[3]))
        try:
            ac_tp = account(arguments[1])
            ap = inquire_price(ac_tp['access_token'], ac_tp['app_key'], ac_tp['app_secret'], item['code'])
            stop_price = int(ap['stck_prpr']) if val_text[0].strip() == '0' else int(val_text[0].strip())
            stop_price = round_to_valid_price(stop_price, get_tick_size(stop_price))
            target_price = int(int(ap['stck_prpr']) + int(ap['stck_prpr']) * 0.05) if val_text[1].strip() == '0' else int(val_text[1].strip())
            target_price = round_to_valid_price(target_price, get_tick_size(target_price))
            exit_price = int(ap['stck_lwpr']) if val_text[2].strip() == '0' else int(val_text[2].strip())
            exit_price = round_to_valid_price(exit_price, get_tick_size(exit_price))
            if target_price <= stop_price:
                context.bot.send_message(chat_id=user_id, text="목표가(" + format(target_price, ',d') + ")가 이탈가(" + format(stop_price, ',d') + ") 이하입니다.")
            elif target_price <= exit_price:
                context.bot.send_message(chat_id=user_id, text="목표가(" + format(target_price, ',d') + ")가 최종이탈가(" + format(exit_price, ',d') + ") 이하입니다.")
            elif stop_price < exit_price:
                context.bot.send_message(chat_id=user_id, text="이탈가(" + format(stop_price, ',d') + ")가 최종이탈가(" + format(exit_price, ',d') + ") 미만입니다.")
            else:    
                c_tp = get_conn()
                with c_tp.cursor() as cur_tp:
                    cur_tp.execute("""
                        UPDATE trading_trail
                        SET stop_price = %s, target_price = %s, exit_price = %s, trail_plan = %s, trail_tp = '1', mod_dt = now()
                        WHERE acct_no = %s AND code = %s
                        AND trail_day = %s AND trail_dtm = %s AND trail_tp = %s
                    """, (stop_price, target_price, exit_price, trail_plan, item['acct_no'], item['code'],
                        item['trail_day'], item['trail_dtm'], item['trail_tp']))
                    updated = cur_tp.rowcount
                c_tp.commit()
                context.bot.send_message(
                    chat_id=user_id,
                    text=f"[{item['name']}(<code>{item['code']}</code>)] 이탈가:{format(stop_price, ',d')}원, 목표가:{format(target_price, ',d')}원, 최종이탈가:{format(exit_price, ',d')}원, 매도비율:{trail_plan}% 저장({updated}건)",
                    parse_mode='HTML'
                )
        except Exception as e_tp:
            try:
                get_conn().rollback()
            except Exception:
                pass
            context.bot.send_message(chat_id=user_id, text=f"이탈가, 목표가, 최종이탈가, 매도비율 업데이트 오류: {str(e_tp)}")
        return

    if menuNum == '51N':
        initMenuNum()
        c51n_code    = g_corr_code
        c51n_company = g_corr_company
        c51n_dvsn    = g_corr_dvsn
        if not c51n_code:
            context.bot.send_message(chat_id=user_id, text="선택된 종목이 없습니다. 다시 시도하세요.")
            return
        if not user_text.strip().isdecimal():
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{c51n_company}({c51n_code})] 정정가는 숫자(현재가:0)로 입력하세요."
            )
            menuNum = '51N'
            return
        try:
            ac_51n = account(arguments[1])
            ap_51n = inquire_price(ac_51n['access_token'], ac_51n['app_key'], ac_51n['app_secret'], c51n_code)
            c51n_cur_price = int(ap_51n['stck_prpr'])
        except Exception as e:
            context.bot.send_message(chat_id=user_id, text=f"[{c51n_company}] 현재가 조회 오류: {str(e)}")
            return
        c51n_revise_price = c51n_cur_price if user_text.strip() == '0' else int(user_text.strip())
        c51n_revise_price = round_to_valid_price(c51n_revise_price, get_tick_size(c51n_revise_price))
        target_nicks_51n = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_51n(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            t_nick_label = nick if nick else arguments[1]
            dvsn_label = "매도" if c51n_dvsn == "01" else "매수"
            try:
                output_51n = daily_order_complete(
                    t_access_token, t_app_key, t_app_secret, t_acct_no, c51n_code, "", c51n_dvsn
                )
                if not output_51n:
                    context.bot.send_message(chat_id=user_id,
                        text=f"-{t_nick_label}-[{c51n_company}] {dvsn_label} 미체결 주문 없음")
                    return
                tdf_51n = pd.DataFrame(output_51n)
                tdf_51n = tdf_51n[tdf_51n['rmn_qty'].astype(int) > 0]
                if tdf_51n.empty:
                    context.bot.send_message(chat_id=user_id,
                        text=f"-{t_nick_label}-[{c51n_company}] {dvsn_label} 미체결 주문 없음")
                    return
                for _, row in tdf_51n.iterrows():
                    order_no   = row['odno']
                    remain_qty = int(row['rmn_qty'])
                    ord_price  = int(row['ord_unpr'])
                    try:
                        c_51n = order_cancel_revice(
                            t_access_token, t_app_key, t_app_secret, t_acct_no,
                            "01", order_no, remain_qty, c51n_revise_price
                        )
                        if c_51n is not None and c_51n['ODNO'] != "":
                            context.bot.send_message(
                                chat_id=user_id,
                                text=(f"-{t_nick_label}-[{c51n_company}(<code>{c51n_code}</code>)] "
                                      f"{dvsn_label} 주문정정 완료 "
                                      f"{format(ord_price, ',')}→{format(c51n_revise_price, ',')}원 "
                                      f"주문번호:<code>{str(int(c_51n['ODNO']))}</code>"),
                                parse_mode='HTML'
                            )
                            try:
                                with get_conn().cursor() as cur_51n:
                                    cur_51n.execute("""
                                        UPDATE trading_trail SET trail_tp = %s, mod_dt = %s
                                        WHERE acct_no = %s AND code = %s
                                        AND trail_day = %s AND order_no = %s
                                    """, ("U", datetime.now(), t_acct_no, c51n_code,
                                          datetime.now().strftime("%Y%m%d"), str(int(order_no))))
                                    get_conn().commit()
                            except Exception:
                                pass
                        else:
                            context.bot.send_message(chat_id=user_id,
                                text=f"-{t_nick_label}-[{c51n_company}] {dvsn_label} 주문정정 실패")
                    except Exception as e:
                        context.bot.send_message(chat_id=user_id,
                            text=f"-{t_nick_label}-[{c51n_company}] {dvsn_label} 주문정정 오류: {str(e)}")
            except Exception as e:
                context.bot.send_message(chat_id=user_id,
                    text=f"-{t_nick_label}-[{c51n_company}] 주문체결 조회 오류: {str(e)}")

        threads_51n = []
        for nick in target_nicks_51n:
            if nick is not None:
                try:
                    ac_51n2 = account(nick)
                except Exception as e:
                    context.bot.send_message(chat_id=user_id, text=f"-{nick}- 계좌조회 오류: {str(e)}")
                    continue
                t_acct_no_51n = ac_51n2['acct_no']
                t_token_51n   = ac_51n2['access_token']
                t_key_51n     = ac_51n2['app_key']
                t_sec_51n     = ac_51n2['app_secret']
            else:
                t_acct_no_51n = ac_51n['acct_no']
                t_token_51n   = ac_51n['access_token']
                t_key_51n     = ac_51n['app_key']
                t_sec_51n     = ac_51n['app_secret']
            t = threading.Thread(target=process_nick_51n,
                                 args=(nick, t_acct_no_51n, t_token_51n, t_key_51n, t_sec_51n))
            threads_51n.append(t)
            t.start()
            time.sleep(0.5)
        for t in threads_51n:
            t.join()
        return

    if menuNum == 'SL':
        initMenuNum()
        s3x_code    = g_sell3x_code
        s3x_company = g_sell3x_company
        if not s3x_code:
            context.bot.send_message(chat_id=user_id, text="선택된 종목이 없습니다. 다시 시도하세요.")
            return
        try:
            ac_s3x = account(arguments[1])
            ap_s3x = inquire_price(ac_s3x['access_token'], ac_s3x['app_key'], ac_s3x['app_secret'], s3x_code)
            s3x_cur_price = int(ap_s3x['stck_prpr'])
        except Exception as e:
            context.bot.send_message(chat_id=user_id, text=f"[{s3x_company}] 현재가 조회 오류: {str(e)}")
            return

        # 입력: 매도가(현재가:0), 매도비율(%)
        parts_s3x = user_text.strip().split(',')
        if (len(parts_s3x) < 2
                or not parts_s3x[0].strip().isdecimal()
                or not parts_s3x[1].strip().isdecimal()
                or not (1 <= int(parts_s3x[1].strip()) <= 100)):
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{s3x_company}] 매도가(현재가:0), 매도비율(1~100) 형식이 올바르지 않습니다."
            )
            menuNum = 'SL'
            return
        s3x_sell_price = s3x_cur_price if parts_s3x[0].strip() == '0' else int(parts_s3x[0].strip())
        s3x_sell_price = round_to_valid_price(s3x_sell_price, get_tick_size(s3x_sell_price))
        s3x_sell_ratio = int(parts_s3x[1].strip())

        target_nicks_s3x = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_s3x(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            t_nick_label = nick if nick else arguments[1]
            t_sell_price = s3x_sell_price
            try:
                # 주문정보 존재시 취소 처리
                if order_cancel_proc(t_access_token, t_app_key, t_app_secret, str(t_acct_no), s3x_code, '01') != 'success':
                    return
                e_s3x = stock_balance(t_access_token, t_app_key, t_app_secret, str(t_acct_no), "")
                ord_psbl_qty_s3x = 0
                for j, _ in enumerate(e_s3x.index):
                    if e_s3x['pdno'][j] == s3x_code:
                        ord_psbl_qty_s3x = int(e_s3x['ord_psbl_qty'][j])
                        break
                if ord_psbl_qty_s3x <= 0:
                    context.bot.send_message(chat_id=user_id, text=f"-{t_nick_label}-[{s3x_company}] 주문가능수량 없음")
                    return
                t_sell_qty = max(1, int(ord_psbl_qty_s3x * s3x_sell_ratio / 100))
                c_s3x = order_cash(False, t_access_token, t_app_key, t_app_secret, str(t_acct_no),
                                   s3x_code, "00", str(t_sell_qty), str(t_sell_price))
                if c_s3x is not None and c_s3x['ODNO'] != "":
                    time.sleep(0.5)
                    output1 = daily_order_complete(t_access_token, t_app_key, t_app_secret,
                                                   t_acct_no, s3x_code, c_s3x['ODNO'], '01')
                    tdf = pd.DataFrame(output1)
                    d = tdf[['odno', 'avg_prvs', 'ord_unpr', 'tot_ccld_qty', 'tot_ccld_amt']]
                    for k, _ in enumerate(d.index):
                        d_order_no    = int(d['odno'][k])
                        d_order_price = d['avg_prvs'][k] if int(d['avg_prvs'][k]) > 0 else d['ord_unpr'][k]
                        d_ccld_qty    = d['tot_ccld_qty'][k]
                        d_ccld_amt    = d['tot_ccld_amt'][k]
                        context.bot.send_message(
                            chat_id=user_id,
                            text=(f"-{t_nick_label}-[{s3x_company}(<code>{s3x_code}</code>)] "
                                  f"매도가:{format(int(d_order_price), ',d')}원 "
                                  f"체결량:{format(int(d_ccld_qty), ',d')}주 "
                                  f"체결금액:{format(int(d_ccld_amt), ',d')}원 "
                                  f"주문번호:<code>{d_order_no}</code>"),
                            parse_mode='HTML'
                        )
                else:
                    context.bot.send_message(chat_id=user_id,
                        text=f"-{t_nick_label}-[{s3x_company}] {format(t_sell_price, ',d')}원 매도주문 실패")
            except Exception as e:
                context.bot.send_message(chat_id=user_id,
                    text=f"-{t_nick_label}-[{s3x_company}(<code>{s3x_code}</code>)] 매도주문 오류: {str(e)}",
                    parse_mode='HTML')

        threads_s3x = []
        for nick in target_nicks_s3x:
            if nick is not None:
                try:
                    ac_s3x2 = account(nick)
                except Exception as e:
                    context.bot.send_message(chat_id=user_id, text=f"-{nick}- 계좌조회 오류: {str(e)}")
                    continue
                t_acct_no_s3x = ac_s3x2['acct_no']
                t_token_s3x   = ac_s3x2['access_token']
                t_key_s3x     = ac_s3x2['app_key']
                t_sec_s3x     = ac_s3x2['app_secret']
            else:
                t_acct_no_s3x = ac_s3x['acct_no']
                t_token_s3x   = ac_s3x['access_token']
                t_key_s3x     = ac_s3x['app_key']
                t_sec_s3x     = ac_s3x['app_secret']
            t = threading.Thread(target=process_nick_s3x,
                                 args=(nick, t_acct_no_s3x, t_token_s3x, t_key_s3x, t_sec_s3x))
            threads_s3x.append(t)
            t.start()
            time.sleep(0.5)
        for t in threads_s3x:
            t.join()
        return

    if menuNum == '61S':
        initMenuNum()
        rs_code = g_rsv_sell_code
        rs_name = g_rsv_sell_name
        if not rs_code:
            context.bot.send_message(chat_id=user_id, text="선택된 종목이 없습니다. 다시 시도하세요.")
            return
        # 입력: 매도가(현재가:0), 매도비율(1~100), 예약종료일(YYYYMMDD)
        parts_61s = user_text.strip().split(',')
        if (len(parts_61s) < 2
                or not parts_61s[0].strip().isdecimal()
                or not parts_61s[1].strip().isdecimal()
                or not (1 <= int(parts_61s[1].strip()) <= 100)):
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{rs_name}] 입력 형식 오류: 매도가(현재가:0), 매도비율(1~100), 예약종료일(YYYYMMDD)"
            )
            menuNum = '61S'
            return
        ord_price_61s  = int(parts_61s[0].strip())
        ord_price_61s  = round_to_valid_price(ord_price_61s, get_tick_size(ord_price_61s)) if ord_price_61s > 0 else 0
        ord_ratio_61s  = int(parts_61s[1].strip())   # 매도비율(%)
        if len(parts_61s) >= 3 and len(parts_61s[2].strip()) == 8 and parts_61s[2].strip().isdigit():
            ord_end_dt_61s = parts_61s[2].strip()
        else:
            ord_end_dt_61s = (datetime.today() + timedelta(days=30)).strftime('%Y%m%d')
        ord_end_dt_61s = get_previous_business_day(ord_end_dt_61s)

        try:
            ac_61s_ref = account(arguments[1])
        except Exception as e:
            context.bot.send_message(chat_id=user_id, text=f"계좌조회 오류: {str(e)}")
            return

        target_nicks_61s = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_61s(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            t_nick_label = nick if nick else arguments[1]
            try:
                e_61s = stock_balance(t_access_token, t_app_key, t_app_secret, str(t_acct_no), "")
                ord_psbl_qty_61s = 0
                for j, _ in enumerate(e_61s.index):
                    if e_61s['pdno'][j] == rs_code:
                        ord_psbl_qty_61s = int(e_61s['ord_psbl_qty'][j])
                        break
                if ord_psbl_qty_61s <= 0:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"-{t_nick_label}-[{rs_name}] 주문가능수량 없음"
                    )
                    return
                # 매도비율로 수량 계산
                ord_qty_61s = max(1, int(ord_psbl_qty_61s * ord_ratio_61s / 100))
                dvsn_cd_61s = "01" if ord_price_61s == 0 else "00"
                rsv_result_61s = order_reserve(
                    t_access_token, t_app_key, t_app_secret,
                    str(t_acct_no), rs_code, str(ord_qty_61s), str(ord_price_61s),
                    "01", dvsn_cd_61s, ord_end_dt_61s
                )
                if rsv_result_61s and rsv_result_61s.get('RSVN_ORD_SEQ', '') != '':
                    context.bot.send_message(
                        chat_id=user_id,
                        text=(f"-{t_nick_label}-[{rs_name}(<code>{rs_code}</code>)] "
                              f"예약매도주문 완료 | 예약번호: <code>{rsv_result_61s['RSVN_ORD_SEQ']}</code> | "
                              f"가격: {format(ord_price_61s, ',d') if ord_price_61s > 0 else '시장가'}원 | "
                              f"수량: {format(ord_qty_61s, ',d')}주 ({ord_ratio_61s}%) | 종료일: {ord_end_dt_61s}"),
                        parse_mode='HTML'
                    )
                else:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"-{t_nick_label}-[{rs_name}] 예약매도주문 실패"
                    )
            except Exception as e:
                context.bot.send_message(
                    chat_id=user_id,
                    text=f"-{t_nick_label}-[{rs_name}(<code>{rs_code}</code>)] 예약매도주문 오류: {str(e)}",
                    parse_mode='HTML'
                )

        threads_61s = []
        for nick in target_nicks_61s:
            if nick is not None:
                try:
                    ac_61s = account(nick)
                except Exception as e:
                    context.bot.send_message(chat_id=user_id, text=f"-{nick}- 계좌조회 오류: {str(e)}")
                    continue
                t_acct_no_61s  = ac_61s['acct_no']
                t_token_61s    = ac_61s['access_token']
                t_key_61s      = ac_61s['app_key']
                t_secret_61s   = ac_61s['app_secret']
            else:
                t_acct_no_61s  = ac_61s_ref['acct_no']
                t_token_61s    = ac_61s_ref['access_token']
                t_key_61s      = ac_61s_ref['app_key']
                t_secret_61s   = ac_61s_ref['app_secret']
            t = threading.Thread(target=process_nick_61s,
                                 args=(nick, t_acct_no_61s, t_token_61s, t_key_61s, t_secret_61s))
            threads_61s.append(t)
            t.start()
            time.sleep(0.5)
        for t in threads_61s:
            t.join()
        return

    if menuNum == '62N':
        initMenuNum()
        rc_code = g_rsv_corr_code
        rc_name = g_rsv_corr_name
        rc_dvsn = g_rsv_corr_dvsn
        if not rc_code:
            context.bot.send_message(chat_id=user_id, text="선택된 종목이 없습니다. 다시 시도하세요.")
            return
        # 입력: 정정가(현재가:0), 예약종료일(YYYYMMDD)
        parts_62n = user_text.strip().split(',')
        if not parts_62n[0].strip().isdecimal():
            context.bot.send_message(
                chat_id=user_id,
                text=f"[{rc_name}] 입력 형식 오류: 정정가(현재가:0), 예약종료일(YYYYMMDD)"
            )
            menuNum = '62N'
            return
        ord_price_62n = int(parts_62n[0].strip())
        if ord_price_62n > 0:
            ord_price_62n = round_to_valid_price(ord_price_62n, get_tick_size(ord_price_62n))
        if len(parts_62n) >= 2 and len(parts_62n[1].strip()) == 8 and parts_62n[1].strip().isdigit():
            ord_end_dt_62n = parts_62n[1].strip()
        else:
            ord_end_dt_62n = (datetime.today() + timedelta(days=30)).strftime('%Y%m%d')
        ord_end_dt_62n = get_previous_business_day(ord_end_dt_62n)

        # 현재가 조회 (정정가 0 → 현재가)
        try:
            ac_62n_ref = account(arguments[1])
            if ord_price_62n == 0:
                ap_62n = inquire_price(ac_62n_ref['access_token'], ac_62n_ref['app_key'], ac_62n_ref['app_secret'], rc_code)
                ord_price_62n = int(ap_62n['stck_prpr'])
                ord_price_62n = round_to_valid_price(ord_price_62n, get_tick_size(ord_price_62n))
        except Exception as e:
            context.bot.send_message(chat_id=user_id, text=f"[{rc_name}] 현재가 조회 오류: {str(e)}")
            return

        target_nicks_62n = g_selected_accounts[:] if g_selected_accounts else [None]

        def process_nick_62n(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
            t_nick_label = nick if nick else arguments[1]
            now_t = datetime.now()
            t_start = (now_t + timedelta(days=1)).strftime("%Y%m%d") if now_t > now_t.replace(hour=15, minute=40, second=0, microsecond=0) else now_t.strftime("%Y%m%d")
            t_end   = (now_t + relativedelta(months=1)).strftime("%Y%m%d")
            t_end   = get_previous_business_day(t_end)
            try:
                output_62n = order_reserve_complete(t_access_token, t_app_key, t_app_secret, t_start, t_end, str(t_acct_no), "")
                if not output_62n:
                    context.bot.send_message(chat_id=user_id, text=f"-{t_nick_label}-[{rc_name}] 예약정보 없음")
                    return
                df_62n = pd.DataFrame(output_62n)
                matched_62n = df_62n[(df_62n['pdno'] == rc_code) & (df_62n['sll_buy_dvsn_cd'] == rc_dvsn) & (df_62n['cncl_ord_dt'] == "")]
                if matched_62n.empty:
                    context.bot.send_message(chat_id=user_id, text=f"-{t_nick_label}-[{rc_name}] 정정 가능한 예약주문 없음")
                    return
                for _, row in matched_62n.iterrows():
                    rsvn_seq   = str(int(row['rsvn_ord_seq']))
                    rsvn_qty   = int(row['ord_rsvn_qty'])
                    sll_buy_cd = row['sll_buy_dvsn_cd']
                    try:
                        rsv_corr_result = order_reserve_cancel_revice(
                            t_access_token, t_app_key, t_app_secret,
                            str(t_acct_no), "01",
                            rc_code, str(rsvn_qty), str(ord_price_62n),
                            sll_buy_cd, "00", ord_end_dt_62n, rsvn_seq
                        )
                        if rsv_corr_result and rsv_corr_result.get('NRML_PRCS_YN', '') == 'Y':
                            context.bot.send_message(
                                chat_id=user_id,
                                text=(f"-{t_nick_label}-[{rc_name}(<code>{rc_code}</code>)] "
                                      f"예약정정 완료 | 예약번호: {rsvn_seq} | "
                                      f"정정가: {format(ord_price_62n, ',d')}원 | "
                                      f"수량: {format(rsvn_qty, ',d')}주 | 종료일: {ord_end_dt_62n}"),
                                parse_mode='HTML'
                            )
                        else:
                            context.bot.send_message(
                                chat_id=user_id,
                                text=f"-{t_nick_label}-[{rc_name}] 예약번호:{rsvn_seq} 예약정정 실패"
                            )
                    except Exception as e:
                        context.bot.send_message(
                            chat_id=user_id,
                            text=f"-{t_nick_label}-[{rc_name}(<code>{rc_code}</code>)] 예약정정 오류: {str(e)}",
                            parse_mode='HTML'
                        )
            except Exception as e:
                context.bot.send_message(
                    chat_id=user_id,
                    text=f"-{t_nick_label}-[{rc_name}] 예약조회 오류: {str(e)}"
                )

        threads_62n = []
        for nick in target_nicks_62n:
            if nick is not None:
                try:
                    ac_62n = account(nick)
                except Exception as e:
                    context.bot.send_message(chat_id=user_id, text=f"-{nick}- 계좌조회 오류: {str(e)}")
                    continue
                t_acct_no_62n  = ac_62n['acct_no']
                t_token_62n    = ac_62n['access_token']
                t_key_62n      = ac_62n['app_key']
                t_secret_62n   = ac_62n['app_secret']
            else:
                t_acct_no_62n  = ac_62n_ref['acct_no']
                t_token_62n    = ac_62n_ref['access_token']
                t_key_62n      = ac_62n_ref['app_key']
                t_secret_62n   = ac_62n_ref['app_secret']
            t = threading.Thread(target=process_nick_62n,
                                 args=(nick, t_acct_no_62n, t_token_62n, t_key_62n, t_secret_62n))
            threads_62n.append(t)
            t.start()
            time.sleep(0.5)
        for t in threads_62n:
            t.join()
        return

    # 입력메시지가 6자리 이상인 경우,
    if len(user_text) >= 6:
        # 입력메시지가 앞의 1자리가 숫자인 경우,
        if user_text[:1].isdecimal():
            # 입력메시지가 종목코드에 존재하는 경우
            if len(stock_code[stock_code.code == user_text[:6]].values) > 0:
                code = stock_code[stock_code.code == user_text[:6]].code.values[0].strip()  ## strip() : 공백제거
                company = stock_code[stock_code.code == user_text[:6]].company.values[0].strip()  ## strip() : 공백제거
            else:
                # KRX 목록 미존재 → KIS API로 직접 확인 (ETN/특수ETF 등 알파벳 포함 비표준 코드 대응)
                _candidate_code = user_text[:6]
                _code_found = False
                try:
                    _ac_chk = account(arguments[1])
                    # inquire_price는 장외시간에 NX 마켓코드 사용 → 비표준코드 조회 실패
                    # 항상 J(KRX)로 직접 조회 후, 실패시 UN(통합)으로 재시도
                    _headers_chk = {
                        "Content-Type": "application/json",
                        "authorization": f"Bearer {_ac_chk['access_token']}",
                        "appKey": _ac_chk['app_key'],
                        "appSecret": _ac_chk['app_secret'],
                        "tr_id": "FHKST01010100"
                    }
                    _price_url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
                    for _mrkt_cd in ["J", "UN"]:
                        _params_chk = {'FID_COND_MRKT_DIV_CODE': _mrkt_cd, 'FID_INPUT_ISCD': _candidate_code}
                        _res_chk = requests.get(_price_url, headers=_headers_chk, params=_params_chk, verify=False, timeout=10)
                        _ar_chk = resp.APIResp(_res_chk)
                        _out_chk = getattr(_ar_chk.getBody(), 'output', None)
                        print(f"[코드조회] {_candidate_code} mrkt={_mrkt_cd} rt_cd={_ar_chk.getErrorCode()} msg={_ar_chk.getErrorMessage()} prpr={_out_chk.get('stck_prpr') if _out_chk else None}")
                        if _out_chk and int(_out_chk.get('stck_prpr', 0)) > 0:
                            code = _candidate_code
                            company = (_out_chk.get('hts_kor_isnm') or '').strip() or _candidate_code
                            _code_found = True
                            break
                except Exception as _e_chk:
                    print(f"[코드조회] {_candidate_code} KIS API 오류: {_e_chk}")
                if not _code_found:
                    code = ""
                    context.bot.send_message(chat_id=user_id, text=_candidate_code + " : 미존재 종목")
        else:
            if not ',' in user_text:
                # 입력메시지가 종목명에 존재하는 경우
                if len(stock_code[stock_code.company == user_text].values) > 0:
                    code = stock_code[stock_code.company == user_text].code.values[0].strip()  ## strip() : 공백제거
                    company = stock_code[stock_code.company == user_text].company.values[0].strip()  ## strip() : 공백제거
                else:
                    code = ""
                    ext = user_text + " : 미존재 종목"
                    context.bot.send_message(chat_id=user_id, text=ext)
            else:
                name_text = user_text.split(',')[0].strip()  # ',' 기준으로 첫 번째 값만 사용

                # 입력메시지가 종목명에 존재하는 경우
                if len(stock_code[stock_code.company == name_text].values) > 0:
                    code = stock_code[stock_code.company == name_text].code.values[0].strip()  ## strip() : 공백제거
                    company = stock_code[stock_code.company == name_text].company.values[0].strip()  ## strip() : 공백제거
                else:
                    code = ""
                    ext = name_text + " : 미존재 종목"
                    context.bot.send_message(chat_id=user_id, text=ext)
    else:
        if not ',' in user_text:
            # 입력메시지가 종목명에 존재하는 경우
            if len(stock_code[stock_code.company == user_text].values) > 0:
                code = stock_code[stock_code.company == user_text].code.values[0].strip()  ## strip() : 공백제거
                company = stock_code[stock_code.company == user_text].company.values[0].strip()  ## strip() : 공백제거
            else:
                code = ""
                ext = user_text + " : 미존재 종목"
                context.bot.send_message(chat_id=user_id, text=ext)                        

    if code != "":

        ac = account(arguments[1])
        acct_no = ac['acct_no']
        access_token = ac['access_token']
        app_key = ac['app_key']
        app_secret = ac['app_secret']
        a = ""
        # 입력 종목코드 현재가 시세
        a = inquire_price(access_token, app_key, app_secret, code)
        stck_prpr = a['stck_prpr']                      # 현재가
        stck_hgpr = a['stck_hgpr']                      # 고가
        stck_lwpr = a['stck_lwpr']                      # 저가
        upper_limit = float(a["stck_mxpr"])             # 상한가
        lower_limit = float(a["stck_llam"])             # 하한가
        prdy_ctrt = a['prdy_ctrt']                      # 전일 대비율
        acml_vol = a['acml_vol']                        # 누적거래량
        prdy_vrss_vol_rate = a['prdy_vrss_vol_rate']    # 전일 대비 거래량 비율
        market_kor = a['rprs_mrkt_kor_name']            # 대표시장
        industry_kor = a['bstp_kor_isnm']               # 업종
        hts_avls = a['hts_avls']                        # 시가총액
        pbr = a['pbr']
        bps = a['bps']
        g_low_price = stck_lwpr

        # 종목 기본정보 (시장구분·업종·규모·매수금액 제안·손절금액 제안)
        stock_info_str = ""
        suggest_buy_amt = 0
        _suggest_loss = 50_000
        _mr_si = 0
        try:
            try:
                _mktcap = int(str(hts_avls).replace(',', ''))
            except Exception:
                _mktcap = 0
            if   _mktcap >= 10000: _size, _amt_min, _amt_max, _amt_desc = '대형주', 2_000_000, 10_000_000, '유동성 높음/안정적'
            elif _mktcap >= 3000:  _size, _amt_min, _amt_max, _amt_desc = '중형주', 1_000_000,  5_000_000, '유동성 보통/중간 변동성'
            else:                  _size, _amt_min, _amt_max, _amt_desc = '소형주',   500_000,  2_000_000, '변동성 높음/소액 분산 권장'
            try:
                with get_conn().cursor() as _cur_si:
                    _cur_si.execute(
                        'SELECT market_ratio FROM public."stockFundMng_stock_fund_mng" WHERE acct_no = %s',
                        (str(acct_no),)
                    )
                    _si_row = _cur_si.fetchone()
                    if _si_row and _si_row[0]:
                        _mr_si = float(_si_row[0])
                        _suggest_loss = int(max(50_000, min(250_000, 50_000 + (_mr_si / 100) * 200_000)))
            except Exception:
                pass

            suggest_buy_amt = int(
                max(_amt_min, min(_amt_max,
                    _amt_min + (_mr_si / 100) * (_amt_max - _amt_min)
                ))
            ) if _mr_si > 0 else _amt_min

            stock_info_str = (
                "\n─────────────────\n"
                f"  [{market_kor}] {_size} | 업종: {industry_kor} | 시총: {format(_mktcap, ',d')}억원 | 시장비율: {_mr_si:.0f}%\n"
                f"  매수금액 권장: {format(_amt_min, ',d')}~{format(_amt_max, ',d')}원 ({_amt_desc})\n"
                f"  매수금액 제안: {format(suggest_buy_amt, ',d')}원 | 손절금액 제안: {format(_suggest_loss, ',d')}원"
            )
        except Exception as _e_si:
            print(f"[stock_info] 조회 오류: {_e_si}")

        print("menuNum : ", menuNum)

        if menuNum == '21':
            initMenuNum()
            parts21 = user_text.split(',', 4)
            if len(parts21) < 5 or not parts21[1].strip().isdecimal() or not parts21[2].strip().isdecimal() or not parts21[3].strip().isdecimal() or not parts21[4].strip().isdecimal():
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 미존재 또는 부적합")
            else:
                buy_price_21 = int(stck_prpr) if parts21[1].strip() == '0' else int(parts21[1].strip())
                buy_price_21 = round_to_valid_price(buy_price_21, get_tick_size(buy_price_21))
                loss_price_21 = int(stck_lwpr) if parts21[2].strip() == '0' else int(parts21[2].strip())
                loss_price_21 = round_to_valid_price(loss_price_21, get_tick_size(loss_price_21))
                input_buy_amt_21 = int(parts21[3].strip())    # 입력 매수금액
                item_loss_sum_21 = int(parts21[4].strip())    # 입력 손절금액

                if buy_price_21 <= loss_price_21:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(" + format(buy_price_21, ',d') + ")가 이탈가(" + format(loss_price_21, ',d') + ") 이하입니다.")
                else:
                    # 공통 손절율
                    loss_rate_21 = round((100 - (loss_price_21 / buy_price_21) * 100) * -1, 2)

                    # ① 손절금액 기준
                    loss_buy_qty_21 = int(round(item_loss_sum_21 / (buy_price_21 - loss_price_21)))
                    loss_buy_amt_21 = buy_price_21 * loss_buy_qty_21

                    # ② 매수금액 기준
                    amt_buy_qty_21 = int(round(input_buy_amt_21 / buy_price_21))
                    amt_buy_amt_21 = buy_price_21 * amt_buy_qty_21
                    amt_item_loss_21 = (buy_price_21 - loss_price_21) * amt_buy_qty_21

                    # 콜백에서 사용할 전역 상태 저장
                    global g_buy21_code, g_buy21_company, g_buy21_buy_price, g_buy21_loss_price
                    global g_buy21_buy_qty, g_buy21_buy_amt
                    global g_buy21_loss_buy_qty, g_buy21_loss_buy_amt
                    global g_buy21_amt_buy_qty, g_buy21_amt_buy_amt
                    g_buy21_code = code
                    g_buy21_company = company
                    g_buy21_buy_price = buy_price_21
                    g_buy21_loss_price = loss_price_21
                    g_buy21_buy_qty = 0
                    g_buy21_buy_amt = 0
                    g_buy21_loss_buy_qty = loss_buy_qty_21
                    g_buy21_loss_buy_amt = loss_buy_amt_21
                    g_buy21_amt_buy_qty = amt_buy_qty_21
                    g_buy21_amt_buy_amt = amt_buy_amt_21

                    selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "현재계좌"
                    preview_text = (
                        "[선택계좌: " + selected_str + "]\n"
                        "[" + company + "(<code>" + code + "</code>)]\n"
                        "매수가: " + format(buy_price_21, ',d') + "원 | 이탈가: " + format(loss_price_21, ',d') + "원 | 손절율: " + str(loss_rate_21) + "%\n"
                        "─────────────────\n"
                        "  손절금액 기준\n"
                        "  매수금액: " + format(loss_buy_amt_21, ',d') + "원 | 매수량: " + format(loss_buy_qty_21, ',d') + "주 | 손실금액: " + format(item_loss_sum_21, ',d') + "원\n"
                        "─────────────────\n"
                        "  매수금액 기준\n"
                        "  매수금액: " + format(amt_buy_amt_21, ',d') + "원 | 매수량: " + format(amt_buy_qty_21, ',d') + "주 | 손실금액: " + format(amt_item_loss_21, ',d') + "원"
                        + stock_info_str
                    )
                    button_list = build_button(["손절금액", "매수금액", "다시계산", "취소"], "buy21")
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, 2))
                    context.bot.send_message(chat_id=user_id, text=preview_text, reply_markup=show_markup, parse_mode='HTML')         

        elif menuNum == '61B':
            initMenuNum()
            # 입력: 종목코드, 매수가(현재가:0), 매수금액(원), 예약종료일(YYYYMMDD)
            parts_61b = user_text.split(',')
            if len(parts_61b) < 3 or not parts_61b[1].strip().isdecimal() or not parts_61b[2].strip().isdecimal():
                context.bot.send_message(
                    chat_id=user_id,
                    text=f"[{company}] 입력 형식 오류: 종목코드, 매수가(현재가:0), 매수금액(원), 예약종료일(YYYYMMDD)"
                )
            else:
                ord_price_61b  = int(parts_61b[1].strip())
                ord_budget_61b = int(parts_61b[2].strip())   # 매수금액
                if len(parts_61b) >= 4 and len(parts_61b[3].strip()) == 8 and parts_61b[3].strip().isdigit():
                    ord_end_dt_61b = parts_61b[3].strip()
                else:
                    ord_end_dt_61b = (datetime.today() + timedelta(days=30)).strftime('%Y%m%d')
                ord_end_dt_61b = get_previous_business_day(ord_end_dt_61b)

                # 현재가 조회 (매수가 0 → 현재가로 수량 계산)
                try:
                    ac_61b_ref = account(arguments[1])
                    if ord_price_61b == 0:
                        ap_61b = inquire_price(ac_61b_ref['access_token'], ac_61b_ref['app_key'], ac_61b_ref['app_secret'], code)
                        calc_price_61b = int(ap_61b['stck_prpr'])
                    else:
                        calc_price_61b = ord_price_61b
                    calc_price_61b = round_to_valid_price(calc_price_61b, get_tick_size(calc_price_61b))
                    ord_price_61b  = round_to_valid_price(ord_price_61b, get_tick_size(ord_price_61b)) if ord_price_61b > 0 else 0
                except Exception as e:
                    context.bot.send_message(chat_id=user_id, text=f"[{company}] 현재가 조회 오류: {str(e)}")
                    return

                if calc_price_61b <= 0:
                    context.bot.send_message(chat_id=user_id, text=f"[{company}] 매수가 산출 오류")
                    return
                ord_qty_61b = max(1, ord_budget_61b // calc_price_61b)

                target_nicks_61b = g_selected_accounts[:] if g_selected_accounts else [None]

                def process_nick_61b(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    t_nick_label = nick if nick else arguments[1]
                    buy_expect = calc_price_61b * ord_qty_61b
                    try:
                        b_61b = inquire_psbl_order(t_access_token, t_app_key, t_app_secret, t_acct_no)
                        if int(b_61b) < buy_expect:
                            context.bot.send_message(
                                chat_id=user_id,
                                text=f"-{t_nick_label}-[{company}] 매수가능금액 부족: {format(int(b_61b), ',d')}원 (필요: {format(buy_expect, ',d')}원)"
                            )
                            return
                        dvsn_cd_61b = "01" if ord_price_61b == 0 else "00"
                        rsv_result_61b = order_reserve(
                            t_access_token, t_app_key, t_app_secret,
                            str(t_acct_no), code, str(ord_qty_61b), str(ord_price_61b),
                            "02", dvsn_cd_61b, ord_end_dt_61b
                        )
                        if rsv_result_61b and rsv_result_61b.get('RSVN_ORD_SEQ', '') != '':
                            context.bot.send_message(
                                chat_id=user_id,
                                text=(f"-{t_nick_label}-[{company}(<code>{code}</code>)] "
                                      f"예약매수주문 완료 | 예약번호: <code>{rsv_result_61b['RSVN_ORD_SEQ']}</code> | "
                                      f"가격: {format(ord_price_61b, ',d') if ord_price_61b > 0 else '시장가'}원 | "
                                      f"수량: {format(ord_qty_61b, ',d')}주 (매수금액: {format(ord_budget_61b, ',d')}원) | "
                                      f"종료일: {ord_end_dt_61b}"),
                                parse_mode='HTML'
                            )
                        else:
                            context.bot.send_message(
                                chat_id=user_id,
                                text=f"-{t_nick_label}-[{company}] 예약매수주문 실패"
                            )
                    except Exception as e:
                        context.bot.send_message(
                            chat_id=user_id,
                            text=f"-{t_nick_label}-[{company}(<code>{code}</code>)] 예약매수주문 오류: {str(e)}",
                            parse_mode='HTML'
                        )

                threads_61b = []
                for nick in target_nicks_61b:
                    if nick is not None:
                        try:
                            ac_61b = account(nick)
                        except Exception as e:
                            context.bot.send_message(chat_id=user_id, text=f"-{nick}- 계좌조회 오류: {str(e)}")
                            continue
                        t_acct_no_61b  = ac_61b['acct_no']
                        t_token_61b    = ac_61b['access_token']
                        t_key_61b      = ac_61b['app_key']
                        t_secret_61b   = ac_61b['app_secret']
                    else:
                        t_acct_no_61b  = acct_no
                        t_token_61b    = access_token
                        t_key_61b      = app_key
                        t_secret_61b   = app_secret
                    t = threading.Thread(target=process_nick_61b,
                                         args=(nick, t_acct_no_61b, t_token_61b, t_key_61b, t_secret_61b))
                    threads_61b.append(t)
                    t.start()
                    time.sleep(0.5)
                for t in threads_61b:
                    t.join()

        elif menuNum == '71':
            initMenuNum()
            parts71 = user_text.split(',', 4)
            if len(parts71) < 5 or not parts71[1].strip().isdecimal() or not parts71[2].strip().isdecimal() or not parts71[3].strip().isdecimal() or not parts71[4].strip().isdecimal():
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(현재가:0), 이탈가(저가:0), 매수금액, 손절금액 미존재 또는 부적합")
            else:
                buy_price_71 = int(stck_prpr) if parts71[1].strip() == '0' else int(parts71[1].strip())
                buy_price_71 = round_to_valid_price(buy_price_71, get_tick_size(buy_price_71))
                loss_price_71 = int(stck_lwpr) if parts71[2].strip() == '0' else int(parts71[2].strip())
                loss_price_71 = round_to_valid_price(loss_price_71, get_tick_size(loss_price_71))
                input_buy_amt_71 = int(parts71[3].strip())    # 입력 매수금액
                item_loss_sum_71 = int(parts71[4].strip())    # 입력 손절금액

                if buy_price_71 <= loss_price_71:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(" + format(buy_price_71, ',d') + ")가 이탈가(" + format(loss_price_71, ',d') + ") 이하입니다.")
                else:
                    # 공통 손절율
                    loss_rate_71 = round((100 - (loss_price_71 / buy_price_71) * 100) * -1, 2)

                    # ① 손절금액 기준
                    loss_buy_qty_71 = int(round(item_loss_sum_71 / (buy_price_71 - loss_price_71)))
                    loss_buy_amt_71 = buy_price_71 * loss_buy_qty_71

                    # ② 매수금액 기준
                    amt_buy_qty_71 = int(round(input_buy_amt_71 / buy_price_71))
                    amt_buy_amt_71 = buy_price_71 * amt_buy_qty_71
                    amt_item_loss_71 = (buy_price_71 - loss_price_71) * amt_buy_qty_71

                    # 진행 콜백에서 사용할 전역 상태 저장
                    global g_trail71_code, g_trail71_company, g_trail71_buy_price, g_trail71_loss_price
                    global g_trail71_item_loss_sum, g_trail71_buy_qty, g_trail71_buy_amt
                    global g_trail71_year_day, g_trail71_hour_minute
                    global g_trail71_loss_buy_qty, g_trail71_loss_buy_amt
                    global g_trail71_amt_buy_qty, g_trail71_amt_buy_amt
                    g_trail71_code = code
                    g_trail71_company = company
                    g_trail71_buy_price = buy_price_71
                    g_trail71_loss_price = loss_price_71
                    g_trail71_item_loss_sum = item_loss_sum_71
                    g_trail71_buy_qty = 0
                    g_trail71_buy_amt = 0
                    g_trail71_loss_buy_qty = loss_buy_qty_71
                    g_trail71_loss_buy_amt = loss_buy_amt_71
                    g_trail71_amt_buy_qty = amt_buy_qty_71
                    g_trail71_amt_buy_amt = amt_buy_amt_71
                    g_trail71_year_day = datetime.now().strftime("%Y%m%d")
                    g_trail71_hour_minute = datetime.now().strftime('%H%M%S')

                    # 시장비율 초과 체크
                    mr_warn71 = ""
                    try:
                        with get_conn().cursor() as cur_mr71:
                            cur_mr71.execute("""
                                SELECT sfm.market_ratio, sfm.prvs_rcdl_excc_amt,
                                       COALESCE((SELECT SUM(sb.eval_sum)
                                                 FROM public."stockBalance_stock_balance" sb
                                                 WHERE sb.acct_no = sfm.acct_no
                                                   AND (sb.trading_plan NOT IN ('i', 'h') OR sb.trading_plan IS NULL)
                                                   AND sb.proc_yn = 'Y'), 0)
                                FROM public."stockFundMng_stock_fund_mng" sfm
                                WHERE sfm.acct_no = %s
                            """, (str(acct_no),))
                            row_mr71 = cur_mr71.fetchone()
                        if row_mr71:
                            mr71_ratio = float(row_mr71[0])
                            mr71_cash = int(row_mr71[1])
                            mr71_scts = int(row_mr71[2])
                            mr71_tot = mr71_cash + mr71_scts
                            if mr71_tot > 0:
                                ratio_loss71 = (mr71_scts + loss_buy_amt_71) / mr71_tot * 100
                                ratio_amt71  = (mr71_scts + amt_buy_amt_71)  / mr71_tot * 100
                                warns71 = []
                                if mr71_ratio < ratio_loss71:
                                    warns71.append(f"손절기준({format(loss_buy_amt_71, ',d')}원)→{ratio_loss71:.1f}%")
                                if mr71_ratio < ratio_amt71:
                                    warns71.append(f"매수금액기준({format(amt_buy_amt_71, ',d')}원)→{ratio_amt71:.1f}%")
                                if warns71:
                                    mr_warn71 = f"\n⚠ 시장비율({mr71_ratio:.0f}%) 초과: {', '.join(warns71)}"
                    except Exception as e_mr71:
                        print(f"[menuNum=71] market_ratio 체크 오류: {e_mr71}")

                    selected_str = ", ".join(g_selected_accounts) if g_selected_accounts else "현재계좌"
                    preview_text = (
                        "[선택계좌: " + selected_str + "]\n"
                        "[" + company + "(<code>" + code + "</code>)]\n"
                        "매수가: " + format(buy_price_71, ',d') + "원 | 이탈가: " + format(loss_price_71, ',d') + "원 | 손절율: " + str(loss_rate_71) + "%\n"
                        "─────────────────\n"
                        "  손절금액 기준\n"
                        "  매수금액: " + format(loss_buy_amt_71, ',d') + "원 | 매수량: " + format(loss_buy_qty_71, ',d') + "주 | 손실금액: " + format(item_loss_sum_71, ',d') + "원\n"
                        "─────────────────\n"
                        "  매수금액 기준\n"
                        "  매수금액: " + format(amt_buy_amt_71, ',d') + "원 | 매수량: " + format(amt_buy_qty_71, ',d') + "주 | 손실금액: " + format(amt_item_loss_71, ',d') + "원"
                        + stock_info_str
                        + mr_warn71
                    )
                    button_list = build_button(["손절금액", "매수금액", "다시계산", "취소"], "trail71")
                    show_markup = InlineKeyboardMarkup(build_menu(button_list, 2))
                    context.bot.send_message(chat_id=user_id, text=preview_text, reply_markup=show_markup, parse_mode='HTML')
                    
                    try:
                        c71 = get_conn()
                        with c71.cursor() as cur71:
                            cur71.execute(
                                """UPDATE public."interestItem_interest_item"
                                   SET proc_yn = 'N', last_chg_date = now()
                                   WHERE code = %s AND proc_yn = 'Y'""",
                                (code)
                            )
                        c71.commit()
                    except Exception as e71:
                        print(f"[menuNum=71] interestItem proc_yn 업데이트 오류: {e71}")

        elif menuNum == '72':
            val_text = user_text.split(',', 4)
            if val_text[4] == '0':
                initMenuNum()
                context.bot.send_message(chat_id=user_id, text="취소하였습니다.")
                return
            initMenuNum()
            
            if len(val_text) < 4 or not val_text[1].isdecimal() or not val_text[2].isdecimal() or not val_text[3].isdecimal() or not val_text[4].isdigit() or not (1 <= int(val_text[4]) <= 100):
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 이탈가(현재가:0), 목표가(현재가5%:0), 최종이탈가(저가:0), 매도비율(취소:0) 미존재 또는 부적합")
                return

            trail_plan = str(int(val_text[4]))
            stop_price = int(stck_prpr) if val_text[1].strip() == '0' else int(val_text[1].strip())
            stop_price = round_to_valid_price(stop_price, get_tick_size(stop_price))
            target_price = int(int(stck_prpr) + int(stck_prpr) * 0.05) if val_text[2].strip() == '0' else int(val_text[2].strip())
            target_price = round_to_valid_price(target_price, get_tick_size(target_price))
            exit_price = int(stck_lwpr) if val_text[3].strip() == '0' else int(val_text[3].strip())
            exit_price = round_to_valid_price(exit_price, get_tick_size(exit_price))
            if target_price <= stop_price:
                context.bot.send_message(chat_id=user_id, text="목표가(" + format(target_price, ',d') + ")가 이탈가(" + format(stop_price, ',d') + ") 이하입니다.")
            elif target_price <= exit_price:
                context.bot.send_message(chat_id=user_id, text="목표가(" + format(target_price, ',d') + ")가 최종이탈가(" + format(exit_price, ',d') + ") 이하입니다.")
            elif stop_price < exit_price:
                context.bot.send_message(chat_id=user_id, text="이탈가(" + format(stop_price, ',d') + ")가 최종이탈가(" + format(exit_price, ',d') + ") 미만입니다.")
            else:  

                year_day = datetime.now().strftime("%Y%m%d")                                                # 날짜-8자리(YYYYMMDD, 현재일자:0)
                hour_minute = datetime.now().strftime('%H%M%S')                                             # 시간-6자리(HHMMSS, 현재일시:0)
                sell_rate = int(val_text[4])
                
                target_nicks = g_selected_accounts if g_selected_accounts else [None]

                def process_nick_72(nick, t_acct_no, t_access_token, t_app_key, t_app_secret):
                    # 계좌잔고 조회
                    c = stock_balance(t_access_token, t_app_key, t_app_secret, t_acct_no, "")

                    hold_price = 0
                    hldg_qty = 0
                    hold_amt = 0

                    for i, _ in enumerate(c.index):
                        if code == c['pdno'][i]:
                            hold_price = float(c['pchs_avg_pric'][i])
                            hldg_qty = int(c['hldg_qty'][i])
                            hold_amt = int(c['pchs_amt'][i])

                    # 매매추적 보유가, 보유수량, 추적유형 조회 (스레드별 별도 DB 연결)
                    thread_conn = db.connect(conn_string)
                    try:
                        sell_qty = int(hldg_qty * sell_rate * 0.01)

                        if sell_qty > 0:
                            try:
                                with thread_conn.cursor() as cur:

                                    # 보유가
                                    base_price = int(hold_price)
                                    # 보유량 (신규 매수 시 hldg_qty=0이므로 fallback 없이 그대로 사용)
                                    base_qty = hldg_qty
                                    # 보유금액 (신규 매수 시 hold_amt=0이므로 fallback 없이 그대로 사용)
                                    base_amt = hold_amt
                                    
                                    sell_amt = int(target_price * sell_qty)
                                    loss_amt = int((base_price - exit_price) * hldg_qty)

                                    merge_query = """
                                        WITH ins AS (
                                            INSERT INTO trading_trail (
                                                acct_no,
                                                code,
                                                name,
                                                trail_day,
                                                trail_dtm,
                                                trail_tp,
                                                stop_price,
                                                target_price,
                                                trail_plan,
                                                basic_price,
                                                basic_qty,
                                                basic_amt,
                                                proc_min,
                                                trade_tp,
                                                exit_price,
                                                loss_amt,
                                                crt_dt,
                                                mod_dt
                                            )
                                            SELECT
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                            RETURNING 1 AS flag
                                        )
                                        SELECT flag FROM ins;
                                        """
                                    cur.execute(merge_query, (
                                        t_acct_no, code, company, year_day, hour_minute, "1", stop_price, target_price, trail_plan, base_price, base_qty, base_amt, hour_minute, 'M', exit_price, loss_amt, datetime.now(), datetime.now()
                                    ))
                                    was_updated = cur.fetchone() is not None
                                    thread_conn.commit()

                                    if was_updated:
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "{<code>"+code+"</code>}] 보유가 : " + format(base_price, ',d') + "원, 보유량 : " + format(base_qty, ',d') + "주, 보유금액 : " + format(base_amt, ',d') + "원, 목표가 : " + format(target_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도금액 : " + format(sell_amt, ',d') + "원, 이탈가 : " + format(stop_price, ',d') + "원, 최종이탈가 : " + format(exit_price, ',d') + "원, 손실금액 : " + format(loss_amt, ',d') + "원 매매추적 처리", parse_mode='HTML')
                                    else:
                                        context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "] 목표가 : " + format(target_price, ',d') + "원, 매도량 : " + format(sell_qty, ',d') + "주, 매도금액 : " + format(sell_amt, ',d') + "원, 이탈가 : " + format(stop_price, ',d') + "원, 최종이탈가 : " + format(exit_price, ',d') + "원, 손실금액 : " + format(loss_amt, ',d') + "원 매매추적 미처리")
                            
                            except Exception as e:
                                thread_conn.rollback()
                                print(f"Error 발생: {e}")
                                context.bot.send_message(chat_id=user_id, text=f"-{nick}- 처리 중 오류가 발생했습니다: {e}")   

                        else:
                            context.bot.send_message(chat_id=user_id, text="-"+ nick +"-[" + company + "] 보유수량 부족 미처리")                                                                     

                    finally:
                        thread_conn.close()

                threads_72 = []
                for nick in target_nicks:
                    if nick is not None:
                        ac = account(nick)
                        t_acct_no = ac['acct_no']
                        t_access_token = ac['access_token']
                        t_app_key = ac['app_key']
                        t_app_secret = ac['app_secret']
                    else:
                        t_acct_no = acct_no
                        t_access_token = access_token
                        t_app_key = app_key
                        t_app_secret = app_secret
                    t = threading.Thread(target=process_nick_72, args=(nick if nick is not None else arguments[1], t_acct_no, t_access_token, t_app_key, t_app_secret))
                    threads_72.append(t)
                    t.start()
                for t in threads_72:
                    t.join()

        elif menuNum == '91':
            initMenuNum()
            commandBot = user_text.split(sep=',', maxsplit=2)
            if (len(commandBot) < 3
                    or not commandBot[1].strip().isdecimal()
                    or not commandBot[2].strip().isdecimal()):
                context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(현재가:0), 이탈가(저가:0) 미존재 또는 부적합")
            else:
                buy_price  = int(stck_prpr) if commandBot[1].strip() == '0' else int(commandBot[1].strip())
                buy_price = round_to_valid_price(buy_price, get_tick_size(buy_price))
                loss_price = int(stck_lwpr) if commandBot[2].strip() == '0' else int(commandBot[2].strip())
                loss_price = round_to_valid_price(loss_price, get_tick_size(loss_price))
                buy_amt = int(suggest_buy_amt)   # 매수금액 제안 (stock_info_str 기준)
                item_loss_sum = int(_suggest_loss)     # 손절금액 제안 (stock_info_str 기준)

                if buy_price <= loss_price:
                    context.bot.send_message(chat_id=user_id, text="[" + company + "] 매수가(" + format(buy_price, ',d') + ")가 이탈가(" + format(loss_price, ',d') + ") 이하입니다.")
                else:
                    loss_rate = round((100 - (loss_price / buy_price) * 100) * -1, 2)

                    # ① 손절금액 기준
                    loss_buy_qty = int(round(item_loss_sum / (buy_price - loss_price)))
                    loss_buy_amt = buy_price * loss_buy_qty

                    # ② 매수금액 기준
                    amt_buy_qty = int(round(buy_amt / buy_price)) if buy_amt > 0 else 0
                    amt_buy_amt = buy_price * amt_buy_qty
                    amt_item_loss = (buy_price - loss_price) * amt_buy_qty

                    # 매수 가능(현금) 조회
                    b = inquire_psbl_order(access_token, app_key, app_secret, acct_no)
                    print("매수 가능(현금) : " + format(int(b), ',d'))

                    shortage_str1 = "손절금액 기준: "
                    shortage_str2 = "매수금액 기준: "
                    if int(b) < loss_buy_amt:
                        shortage_str1 += format(loss_buy_amt - int(b), ',d') + "원 부족\n"
                    else:
                        shortage_str1 += "\n"
                    if amt_buy_amt > 0 and int(b) < amt_buy_amt:
                        shortage_str2 += format(amt_buy_amt - int(b), ',d') + "원 부족\n"
                    else:
                        shortage_str2 += "\n"

                    preview_text = (
                        "[" + company + "(<code>" + code + "</code>)]\n"
                        "매수가: " + format(buy_price, ',d') + "원 | 이탈가: " + format(loss_price, ',d') + "원 | 손절율: " + str(loss_rate) + "%"
                        + stock_info_str + "\n"
                        "─────────────────\n"
                        + shortage_str1 +
                        "  매수금액: " + format(loss_buy_amt, ',d') + "원 | 매수량: " + format(loss_buy_qty, ',d') + "주 | 손실금액: " + format(item_loss_sum, ',d') + "원\n"
                        "─────────────────\n"
                        + shortage_str2 +
                        "  매수금액: " + format(amt_buy_amt, ',d') + "원 | 매수량: " + format(amt_buy_qty, ',d') + "주 | 손실금액: " + format(amt_item_loss, ',d') + "원"
                    )
                    context.bot.send_message(chat_id=user_id, text=preview_text, parse_mode='HTML')


# 텔레그램봇 응답 처리
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))
dispatcher.add_handler(CommandHandler("start", start))

# 텔레그램봇 polling
updater.start_polling()
