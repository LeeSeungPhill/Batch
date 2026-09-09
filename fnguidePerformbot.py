import re
import json
import time
import pandas as pd
from telegram.ext import Updater
from telegram.ext import MessageHandler, Filters, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import requests
from io import StringIO
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.font_manager as fm
from datetime import datetime, timedelta
import urllib3
from pykrx import stock
from mplfinance.original_flavor import candlestick2_ohlc
import matplotlib.ticker as mticker
import psycopg2 as db
import kis_api_resp as resp

URL_BASE = "https://openapi.koreainvestment.com:9443"

urllib3.disable_warnings()
matplotlib.use('Agg')
_nanum_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
fm.fontManager.addfont(_nanum_path)
_nanum_prop = fm.FontProperties(fname=_nanum_path)
plt.rcParams['font.family'] = _nanum_prop.get_name()
plt.rcParams['axes.unicode_minus'] = False
# 한국거래소 상장법인목록 다운로드
krx_url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
krx_res = requests.get(krx_url, timeout=10)
krx_res.encoding = 'EUC-KR'
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

# PostgreSQL 연결 설정
# conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'"
# DB 연결
conn = db.connect(conn_string)
cur001 = conn.cursor()
cur001.execute("select bot_token1 from \"stockAccount_stock_account\" where nick_name = 'kwphills75'")
result_001 = cur001.fetchone()
cur001.close()
token = result_001[0]

_pending_register = {}  # {chat_id: 관심종목 등록 대기 데이터}

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

def auth(APP_KEY, APP_SECRET):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body), verify=False, timeout=10)
    return res.json()["access_token"]

def get_phills2_account():
    c = get_conn()
    cur = c.cursor()
    cur.execute("""
        SELECT acct_no, access_token, app_key, app_secret,
               token_publ_date, substr(token_publ_date, 0, 9) AS token_day
        FROM "stockAccount_stock_account"
        WHERE nick_name = 'phills2'
    """)
    row = cur.fetchone()
    cur.close()
    if row is None:
        raise ValueError("DB에 'phills2' 계정 정보가 없습니다.")
    acct_no, access_token, app_key, app_secret = row[0], row[1], row[2], row[3]
    today = datetime.now().strftime("%Y%m%d")
    valid_date = datetime.strptime(row[4], '%Y%m%d%H%M%S')
    if (datetime.now() - valid_date).days >= 1 or row[5] != today:
        access_token = auth(app_key, app_secret)
        token_publ_date = datetime.now().strftime("%Y%m%d%H%M%S")
        cur2 = c.cursor()
        cur2.execute(
            "UPDATE \"stockAccount_stock_account\" SET access_token = %s, token_publ_date = %s, last_chg_date = %s WHERE acct_no = %s",
            (access_token, token_publ_date, datetime.now(), acct_no)
        )
        c.commit()
        cur2.close()
    return {'acct_no': acct_no, 'access_token': access_token, 'app_key': app_key, 'app_secret': app_secret}

# 계좌의 market_ratio 조회 (제안매수금액/제안손절금액 계산에 사용)
def get_market_ratio(acct_no):
    try:
        c = get_conn()
        with c.cursor() as cur:
            cur.execute(
                'SELECT market_ratio FROM public."stockFundMng_stock_fund_mng" WHERE acct_no = %s',
                (str(acct_no),)
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])
    except Exception:
        pass
    return 0.0

def inquire_price(access_token, app_key, app_secret, code):
    t = datetime.now().strftime('%H%M')
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST01010100"
    }
    params = {
        'FID_COND_MRKT_DIV_CODE': "J" if '0900' <= t < '1530' else "NX",
        'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    ar = resp.APIResp(res)
    return ar.getBody().output

# 종목 기본정보(시장구분·업종·시가총액·대차잔고비율) 조회 전용.
# inquire_price()는 시세 조회 목적상 장운영시간 외에는 FID_COND_MRKT_DIV_CODE를 NX(넥스트레이드)로 전환하는데,
# NXT는 코스피/코스닥 전종목을 커버하지 않아 소형주 등에서 output이 비거나 불완전할 수 있어 항상 "J"로 고정 조회.
def inquire_price_info(access_token, app_key, app_secret, code):
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST01010100"
    }
    params = {
        'FID_COND_MRKT_DIV_CODE': "J",
        'FID_INPUT_ISCD': code
    }
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
    data = res.json()
    return data.get('output') if data.get('rt_cd') == '0' else None

def get_period_high_low(access_token, app_key, app_secret, code, period="D", count=30):
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
        "FID_INPUT_ISCD": code,
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

# 일봉 OHLCV 최근 100거래일 조회 (최신→과거 내림차순)
def get_daily_ohlcv(access_token, app_key, app_secret, code):
    try:
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
            "FID_INPUT_ISCD": code,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "1",
        }
        PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
        URL = f"{URL_BASE}/{PATH}"
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        data = res.json()
        rows = data.get("output") or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []

# 일별 공매도 추이 최근 60일 조회 (최신→과거)
def get_short_selling(access_token, app_key, app_secret, code):
    try:
        today  = datetime.now().strftime('%Y%m%d')
        d60ago = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appKey": app_key,
            "appSecret": app_secret,
            "tr_id": "FHPST04830000",
            "custtype": "P",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": d60ago,
            "FID_INPUT_DATE_2": today,
        }
        PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-short-over"
        URL = f"{URL_BASE}/{PATH}"
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        data = res.json()
        if data.get('rt_cd') == '0':
            rows = data.get('output2') or []
            return rows if isinstance(rows, list) else []
        return []
    except Exception:
        return []

# 최근 투자자별(외국인/기관) 순매수 거래대금 조회 (최신→과거)
def get_investor_trend(access_token, app_key, app_secret, code):
    try:
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appKey": app_key,
            "appSecret": app_secret,
            "tr_id": "FHKST01010900",
            "custtype": "P",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }
        PATH = "uapi/domestic-stock/v1/quotations/inquire-investor"
        URL = f"{URL_BASE}/{PATH}"
        res = requests.get(URL, headers=headers, params=params, verify=False, timeout=10)
        data = res.json()
        if data.get('rt_cd') == '0' and isinstance(data.get('output'), list):
            return data['output']
        return []
    except Exception:
        return []

def _adx(highs, lows, closes, period=14):
    """Wilder's ADX. 입력은 오름차순(과거→최신). (adx, +DI, -DI) 반환."""
    n = len(highs)
    if n < period * 2 + 1:
        return None, None, None
    trs, pDMs, mDMs = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i-1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up, dn = highs[i] - highs[i-1], lows[i-1] - lows[i]
        pDMs.append(up if up > dn and up > 0 else 0.0)
        mDMs.append(dn if dn > up and dn > 0 else 0.0)
    def ws(arr, p):
        s = sum(arr[:p]); res = [s]
        for x in arr[p:]:
            s = s - s / p + x; res.append(s)
        return res
    def ws_avg(arr, p):
        s = sum(arr[:p]) / p; res = [s]
        for x in arr[p:]:
            s = s + (x - s) / p; res.append(s)
        return res
    atr_s = ws(trs, period); pdi_s = ws(pDMs, period); mdi_s = ws(mDMs, period)
    dxs = []
    for a, p, m in zip(atr_s, pdi_s, mdi_s):
        pd_ = p / a * 100 if a else 0; md_ = m / a * 100 if a else 0
        dxs.append(abs(pd_ - md_) / (pd_ + md_) * 100 if (pd_ + md_) else 0)
    adx_s    = ws_avg(dxs, period)
    a_last   = atr_s[-1]
    plus_di  = pdi_s[-1] / a_last * 100 if a_last else 0
    minus_di = mdi_s[-1] / a_last * 100 if a_last else 0
    return adx_s[-1], plus_di, minus_di

def _obv_trend(closes, volumes, n=5):
    """최근 n일 OBV 변화율(%). closes/volumes는 최신→과거(내림차순)."""
    cls  = list(reversed(closes))
    vols = list(reversed(volumes))
    obv  = 0; obs = [0]
    for i in range(1, len(cls)):
        if   cls[i] > cls[i-1]: obv += vols[i]
        elif cls[i] < cls[i-1]: obv -= vols[i]
        obs.append(obv)
    if len(obs) < n + 1: return 0.0
    prev = obs[-(n+1)]
    return (obs[-1] - prev) / abs(prev) * 100 if abs(prev) > 1 else 0.0

def calc_peak_trough_trend(highs: list, closes: list, lows: list, dates: list):
    """종가 리스트(날짜 오름차순) 기준 지그재그 고점/저점으로 현재 추세와 그 시작일 계산.
    고점: 전일 대비 상승 + 익일 대비 하락. 저점: 전일 대비 하락 + 익일 대비 상승.
    추세: 마지막 고점 재돌파 → Uptrend, 마지막 저점 재이탈 → Downtrend, 그 외 → Sideways."""
    n = len(closes)
    if n < 3:
        return None

    high_pts = [None] * n
    low_pts  = [None] * n
    for i in range(1, n - 1):
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            high_pts[i] = highs[i]
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            low_pts[i] = lows[i]

    trends = []
    last_high, last_low = None, None
    for i in range(n):
        if high_pts[i] is not None:
            last_high = high_pts[i]
        if low_pts[i] is not None:
            last_low = low_pts[i]
        if last_high is not None and highs[i] > last_high:
            trends.append('Uptrend')
        elif last_low is not None and lows[i] < last_low:
            trends.append('Downtrend')
        else:
            trends.append('Sideways')

    cur_trend = trends[-1]
    start_idx = n - 1
    while start_idx > 0 and trends[start_idx - 1] == cur_trend:
        start_idx -= 1
    return {'trend': cur_trend, 'start_date': dates[start_idx]}

def calc_chart_score(rows):
    """rows: FHKST01010400 output (최신→과거 내림차순)."""
    if not rows or len(rows) < 25:
        return {'score': None, 'detail': {}}
    def _f(v):
        try: return float(v)
        except: return 0.0
    closes  = [_f(r.get('stck_clpr', 0)) for r in rows]
    highs   = [_f(r.get('stck_hgpr', 0)) for r in rows]
    lows    = [_f(r.get('stck_lwpr', 0)) for r in rows]
    volumes = [_f(r.get('acml_vol',  0)) for r in rows]
    cur = closes[0]
    ma5  = sum(closes[:5])  / 5
    ma20 = sum(closes[:20]) / 20
    ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None

    # ── 추세강도 MA5/20/60 (30점) ──────────────────────────────
    if ma60:
        if   ma5 > ma20 > ma60:                          trend_sc = 30
        elif ma5 > ma20 and ma20 < ma60:                 trend_sc = 22
        elif ma5 > ma60 and ma5 <= ma20:                 trend_sc = 16
        elif abs(ma5 - ma20) / ma20 < 0.01:             trend_sc = 10
        elif ma5 < ma20 and ma20 > ma60:                 trend_sc =  5
        else:                                            trend_sc =  0
    else:
        if   ma5 > ma20 * 1.02:  trend_sc = 22
        elif ma5 > ma20:         trend_sc = 16
        elif ma5 > ma20 * 0.99:  trend_sc = 10
        else:                    trend_sc =  0

    # ── ADX (25점) ──────────────────────────────────────────────
    asc_h = list(reversed(highs)); asc_l = list(reversed(lows)); asc_c = list(reversed(closes))
    adx, plus_di, minus_di = _adx(asc_h, asc_l, asc_c)
    if adx is None:
        adx_sc = 8
    elif adx >= 40 and plus_di > minus_di:   adx_sc = 25
    elif adx >= 25 and plus_di > minus_di:   adx_sc = 20
    elif adx >= 25 and plus_di <= minus_di:  adx_sc =  5
    elif adx >= 20:                          adx_sc = 12
    else:                                    adx_sc =  8

    # ── 이격도 MA20 (20점) ───────────────────────────────────────
    deviation = (cur - ma20) / ma20 * 100 if ma20 else 0.0
    d = deviation
    if   -3  <= d <=  5:   dev_sc = 20
    elif  5  <  d <= 10:   dev_sc = 15
    elif -8  <= d <  -3:   dev_sc = 15
    elif -15 <= d <  -8:   dev_sc = 12
    elif 10  <  d <= 15:   dev_sc = 10
    elif  d < -15:         dev_sc =  8
    else:                  dev_sc =  5   # d > 15

    # ── 거래량비율 5일/20일 (15점) ────────────────────────────────
    vol_avg5  = sum(volumes[:5])  / 5
    vol_avg20 = sum(volumes[:20]) / 20
    vol_ratio = vol_avg5 / vol_avg20 * 100 if vol_avg20 else 100
    v = vol_ratio
    if   v > 150:  vol_sc = 15
    elif v > 120:  vol_sc = 12
    elif v >  90:  vol_sc =  9
    elif v >  70:  vol_sc =  6
    else:          vol_sc =  3

    # ── 전일대비거래량 (10점) ─────────────────────────────────────
    vod = volumes[0] / volumes[1] * 100 if len(volumes) >= 2 and volumes[1] > 0 else 100
    if   vod > 150:  vod_sc = 10
    elif vod > 120:  vod_sc =  8
    elif vod >  80:  vod_sc =  6
    elif vod >  50:  vod_sc =  4
    else:            vod_sc =  2

    return {
        'score': trend_sc + adx_sc + dev_sc + vol_sc + vod_sc,
        'detail': {
            'ma5': round(ma5), 'ma20': round(ma20),
            'ma60': round(ma60) if ma60 else None,
            'trend_score': trend_sc,
            'adx': round(adx, 1) if adx else None,
            'plus_di': round(plus_di, 1) if plus_di else None,
            'minus_di': round(minus_di, 1) if minus_di else None,
            'adx_score': adx_sc,
            'deviation': round(deviation, 2), 'deviation_score': dev_sc,
            'vol_ratio_5_20': round(vol_ratio, 1), 'vol_score': vol_sc,
            'vod_ratio': round(vod, 1), 'vod_score': vod_sc,
        }
    }

def calc_supply_score(ohlcv_rows, inv_rows, price_out, ssts_rows=None):
    """ohlcv_rows: FHKST01010400 output (OHLCV, OBV용)
       inv_rows:   inquire-investor output list (외국인/기관 거래대금)
       price_out:  inquire-price output (대차잔고비율)
       ssts_rows:  공매도 일별 추이 rows (ssts_vol_rlim 포함, 없으면 None)"""
    if not inv_rows:
        return {'score': None, 'detail': {}}

    def _si(v):
        try: return int(v)
        except: return 0

    def _sf(v):
        try: return float(v)
        except: return 0.0

    n5 = min(5, len(inv_rows))
    # 외국인/기관 5일 순매수 거래대금 (백만원 → 억원, 빈 문자열 안전 처리)
    frgn_5d = sum(_si(r.get('frgn_ntby_tr_pbmn', 0)) for r in inv_rows[:n5]) / 100
    orgn_5d = sum(_si(r.get('orgn_ntby_tr_pbmn', 0)) for r in inv_rows[:n5]) / 100

    # 공매도 5일 평균비율 (ssts_rows 없으면 0)
    if ssts_rows:
        nd5 = min(5, len(ssts_rows))
        ssts_avg = sum(_sf(r.get('ssts_vol_rlim', 0)) for r in ssts_rows[:nd5]) / nd5
    else:
        ssts_avg = 0.0

    # 대차잔고비율 (당일)
    loan_rate = _sf((price_out or {}).get('whol_loan_rmnd_rate', 0))

    # OBV 5일 변화율 (OHLCV 없으면 0)
    obv_chg = 0.0
    if ohlcv_rows and len(ohlcv_rows) >= 6:
        closes  = [_sf(r.get('stck_clpr', 0)) for r in ohlcv_rows]
        volumes = [_sf(r.get('acml_vol',  0)) for r in ohlcv_rows]
        obv_chg = _obv_trend(closes, volumes, n=5)

    # ── 외국인 5일 거래대금 (30점) ───────────────────────────────
    fr = frgn_5d
    if   fr >  200:  frgn_sc = 30
    elif fr >   50:  frgn_sc = 24
    elif fr >   10:  frgn_sc = 18
    elif fr >    0:  frgn_sc = 14
    elif fr >  -10:  frgn_sc =  8
    elif fr >  -50:  frgn_sc =  3
    else:            frgn_sc =  0

    # ── 기관 5일 거래대금 (25점) ─────────────────────────────────
    og = orgn_5d
    if   og >  200:  orgn_sc = 25
    elif og >   50:  orgn_sc = 20
    elif og >   10:  orgn_sc = 15
    elif og >    0:  orgn_sc = 11
    elif og >  -10:  orgn_sc =  6
    elif og >  -50:  orgn_sc =  2
    else:            orgn_sc =  0

    # ── 공매도 5일 평균비율 (20점, 역배점) ───────────────────────
    sv = ssts_avg
    if   sv <  1:  ssts_sc = 20
    elif sv <  2:  ssts_sc = 16
    elif sv <  3:  ssts_sc = 12
    elif sv <  5:  ssts_sc =  8
    elif sv < 10:  ssts_sc =  4
    else:          ssts_sc =  0

    # ── 대차잔고비율 (15점, 역배점) ──────────────────────────────
    lr = loan_rate
    if   lr <  0.5:  loan_sc = 15
    elif lr <  1.0:  loan_sc = 12
    elif lr <  2.0:  loan_sc =  9
    elif lr <  5.0:  loan_sc =  5
    elif lr < 10.0:  loan_sc =  2
    else:            loan_sc =  0

    # ── OBV 5일 추세 (10점) ─────────────────────────────────────
    if   obv_chg >  3:  obv_sc = 10
    elif obv_chg >  0:  obv_sc =  7
    elif obv_chg > -3:  obv_sc =  5
    else:               obv_sc =  2

    return {
        'score': frgn_sc + orgn_sc + ssts_sc + loan_sc + obv_sc,
        'detail': {
            'frgn_5d_eok':  round(frgn_5d, 1),  'frgn_score': frgn_sc,
            'orgn_5d_eok':  round(orgn_5d, 1),  'orgn_score': orgn_sc,
            'ssts_5d_avg':  round(ssts_avg, 2),  'ssts_score': ssts_sc,
            'loan_rate':    loan_rate,            'loan_score': loan_sc,
            'obv_chg_pct':  round(obv_chg, 2),   'obv_score':  obv_sc,
        }
    }

# 텔레그램봇 updater(토큰, 입력값)
updater = Updater(token=token, use_context=True)
dispatcher = updater.dispatcher

# 날짜형식 변환(년월)
def get_date_str(s):

    date_str = ''
    r = re.search("\d{4}/\d{2}", s)

    if r:
        date_str = r.group()
        date_str = date_str.replace('/', '-')

    return date_str

# FnGuide 재무정보 조회
def get_dividend(code):

    URL = "https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=A%s&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701" % (code)
    with requests.Session() as session:
        r = session.get(URL, timeout=10)
    r.encoding='utf-8'
    data = pd.read_html(r.text)

    if not data:
        print(f"[{code}] 재무 데이터가 존재하지 않습니다.")
        return None
    else:
        IS_temp = data[0] # 연간
        #IS_temp = data[1] # 분기
        if 'IFRS(별도)' in IS_temp.columns:
            IS_temp.index = IS_temp['IFRS(별도)'].values
            IS_temp.drop(['IFRS(별도)', '전년동기', '전년동기(%)'], inplace=True, axis=1)
        elif 'IFRS(개별)' in IS_temp.columns:
            IS_temp.index = IS_temp['IFRS(개별)'].values
            IS_temp.drop(['IFRS(개별)', '전년동기', '전년동기(%)'], inplace=True, axis=1)
        else:
            print(f"[{code}] FnGuide 재무 데이터 형식이 예상과 다릅니다 (사이트 개편/차단 가능성).")
            return None

        for i, name in enumerate(IS_temp.index):

            if '참여한' in name:
                name = name.strip().replace('계산에 참여한 계정 펼치기', '')
                name = name.replace(' ', '')
                IS_temp.rename(index = {str(IS_temp.index[i]): str(name)}, inplace=True) # rename 으로 index 다시 설정

        cols = list(IS_temp.columns)
        cols = [get_date_str(x) for x in cols]
        IS_temp.columns = cols
        IS_temp = IS_temp.T

        IS_temp.drop('매출원가', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('매출총이익', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('영업이익(발표기준)', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('판매비와관리비', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('금융원가', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('기타비용', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('종속기업,공동지배기업및관계기업관련손익', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('세전계속사업이익', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('법인세비용', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('계속영업이익', axis=1, inplace=True, errors='ignore')
        IS_temp.drop('중단영업이익', axis=1, inplace=True, errors='ignore')

        IS_temp.index = pd.to_datetime(IS_temp.index)
        IS_temp = IS_temp[pd.notnull(IS_temp.index)]
        result = IS_temp.fillna(0)
        print(result)
        return result

# 텔레그램봇 응답 message handler
def echo(update, context):
    user_id = update.effective_chat.id
    user_text = update.message.text

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

    # 입력메시지가 6자리 이상인 경우,
    if len(user_text) >= 6:
        # 입력메시지가 앞의 1자리가 숫자인 경우,
        if user_text[:1].isdecimal():
            # 입력메시지가 종목코드에 존재하는 경우
            if len(stock_code[stock_code.code == user_text[:6]].values) > 0:
                code = stock_code[stock_code.code == user_text[:6]].code.values[0].strip()  ## strip() : 공백제거
                company = stock_code[stock_code.code == user_text[:6]].company.values[0].strip()  ## strip() : 공백제거
            else:
                code = ""
                ext = user_text[:6] + " : 미존재 종목"
                context.bot.send_message(chat_id=user_id, text=ext)
        else:
            # 입력메시지가 종목명에 존재하는 경우
            if len(stock_code[stock_code.company == user_text].values) > 0:
                code = stock_code[stock_code.company == user_text].code.values[0].strip()  ## strip() : 공백제거
                company = stock_code[stock_code.company == user_text].company.values[0].strip()  ## strip() : 공백제거
            else:
                code = ""
                ext = user_text + " : 미존재 종목"
                context.bot.send_message(chat_id=user_id, text=ext)

    else:
        # 입력메시지가 종목명에 존재하는 경우
        if len(stock_code[stock_code.company == user_text].values) > 0:
            code = stock_code[stock_code.company == user_text].code.values[0].strip()  ## strip() : 공백제거
            company = stock_code[stock_code.company == user_text].company.values[0].strip()  ## strip() : 공백제거
        else:
            code = ""
            ext = user_text + " : 미존재 종목"
            context.bot.send_message(chat_id=user_id, text=ext)

    def get_chart(code):
        title = company + '[' + code + ']'
        pre_day = datetime.today() - timedelta(500)
        start = pre_day.strftime("%Y-%m-%d")
        end = datetime.today().strftime("%Y-%m-%d")
        # pykrx를 이용한 OHLCV 조회
        df = stock.get_market_ohlcv_by_date(start, end, code)

        # 컬럼명 한글화 및 순서 조정
        df.rename(columns={
            '시가': '시가',
            '고가': '고가',
            '저가': '저가',
            '종가': '종가',
            '거래량': '거래량'
        }, inplace=True)

        df = df[['시가', '고가', '저가', '종가', '거래량']]

        fig = plt.figure(figsize=(10, 7))
        fig.set_facecolor('white')

        num_row = 2
        gs = gridspec.GridSpec(num_row, 1, height_ratios=(3.5, 1.5))

        ax_top = fig.add_subplot(gs[0, :])

        ## 분봉(캔들) 차트
        candlestick2_ohlc(ax_top, df['시가'], df['고가'], df['저가'], df['종가'],
                          width=0.8,  ## 막대 폭 비율 조절
                          colorup='r',  ## 종가가 시가보다 높은 경우에 색상
                          colordown='b'  ## 종가가 시가보다 낮은 경우에 색상
                          )
        xticks = range(len(df))[::5]
        xticklabels = [x.strftime('%m-%d') for x in df.index[::5]]
        ax_top.set_xticks(xticks)
        ax_top.set_xticklabels(xticklabels, fontsize=8)
        ax_top.tick_params(axis='x', rotation=90)
        ax_top.set_title(title, fontsize=15, fontproperties=_nanum_prop)
        ax_top.grid()

        # 색깔 구분을 위한 함수
        color_fuc = lambda x: 'r' if x >= 0 else 'b'
        color_list = list(df['거래량'].diff().fillna(0).apply(color_fuc))

        ## 거래량 바 차트
        ax_bottom = fig.add_subplot(gs[1, :])

        ax_bottom.bar(range(len(df)), df['거래량'], color=color_list)
        ax_bottom.yaxis.set_major_locator(mticker.FixedLocator(ax_bottom.get_yticks()))
        ax_bottom.set_yticklabels(['{:.0f}'.format(x) for x in ax_bottom.get_yticks()])

        xticks = range(len(df))[::5]
        xticklabels = [x.strftime('%Y-%m-%d') for x in df.index[::5]]
        ax_bottom.set_xticks(xticks)
        ax_bottom.set_xticklabels(xticklabels, fontsize=8)
        ax_bottom.tick_params(axis='x', rotation=90)
        ax_bottom.grid()

        plt.savefig('/home/terra/chart/save2.png')
        plt.close(fig)

    def send_stock_summary(code):
        try:
            ac_sum = get_phills2_account()
            price_out = inquire_price_info(ac_sum['access_token'], ac_sum['app_key'], ac_sum['app_secret'], code)
            time.sleep(0.3)
            ohlcv_rows = get_daily_ohlcv(ac_sum['access_token'], ac_sum['app_key'], ac_sum['app_secret'], code)
            time.sleep(0.3)
            ssts_rows = get_short_selling(ac_sum['access_token'], ac_sum['app_key'], ac_sum['app_secret'], code)
            time.sleep(0.3)
            inv_rows = get_investor_trend(ac_sum['access_token'], ac_sum['app_key'], ac_sum['app_secret'], code)

            chart = calc_chart_score(ohlcv_rows)
            supply = calc_supply_score(ohlcv_rows, inv_rows, price_out, ssts_rows=ssts_rows)
            chart_score, chart_d = chart.get('score'), chart.get('detail') or {}
            supply_score, supply_d = supply.get('score'), supply.get('detail') or {}

            # 추세/추세시작일: 일봉(최신→과거) → 날짜 오름차순 변환 후 지그재그 고점/저점 기준 산출
            trend_kr = {'Uptrend': '상승추세', 'Downtrend': '하락추세', 'Sideways': '횡보'}
            asc_rows = list(reversed(ohlcv_rows))
            trend_result = calc_peak_trough_trend(
                [int(r.get('stck_hgpr') or 0) for r in asc_rows],
                [int(r.get('stck_clpr') or 0) for r in asc_rows],
                [int(r.get('stck_lwpr') or 0) for r in asc_rows],
                [r.get('stck_bsop_date') for r in asc_rows],
            )
            if trend_result:
                trend_str = trend_kr.get(trend_result['trend'], trend_result['trend'])
                sd = trend_result['start_date'] or ''
                start_date_str = f"{sd[:4]}-{sd[4:6]}-{sd[6:8]}" if len(sd) == 8 else (sd or '-')
                trend_line = f"추세: {trend_str} (시작일 {start_date_str})\n"
            else:
                trend_line = "추세: -\n"

            market = (price_out or {}).get('rprs_mrkt_kor_name', '') or '-'
            industry = (price_out or {}).get('bstp_kor_isnm', '') or '-'
            try:
                mktcap = int(str((price_out or {}).get('hts_avls', '0')).replace(',', ''))
            except Exception:
                mktcap = 0

            if   mktcap >= 10000: size = '대형주'
            elif mktcap >= 3000:  size = '중형주'
            else:                 size = '소형주'

            if size == '대형주':
                amt_min, amt_max, amt_desc = 2000000, 10000000, '유동성 높음, 안정적'
            elif size == '중형주':
                amt_min, amt_max, amt_desc = 1000000, 5000000, '유동성 보통, 중간 변동성'
            else:
                amt_min, amt_max, amt_desc = 500000, 2000000, '변동성 높음, 소액 분산 권장'

            # 제안매수금액/제안손절금액: 계좌 market_ratio 기준 amt_min~amt_max, 5만~25만원 사이 선형보간
            market_ratio_v = get_market_ratio(ac_sum['acct_no'])
            if market_ratio_v > 0:
                suggest_loss_amt = int(max(50_000, min(250_000, 50_000 + (market_ratio_v / 100) * 200_000)))
                suggest_buy_amt  = int(max(amt_min, min(amt_max, amt_min + (market_ratio_v / 100) * (amt_max - amt_min))))
            else:
                suggest_loss_amt = 50_000
                suggest_buy_amt  = amt_min

            def _v(x, suffix=''):
                return f"{x}{suffix}" if x is not None else '-'

            text = (
                f"{market}[{size}] {industry}(시가총액 {format(mktcap, ',d')}억원)\n"
                f"제안매수금액: {format(suggest_buy_amt, ',d')}원, 제안손절금액: {format(suggest_loss_amt, ',d')}원\n"
                f"{trend_line}\n"
                f"[차트점수] {chart_score if chart_score is not None else '-'}점\n"
                f"  · 추세(MA5/20/60): {_v(chart_d.get('ma5'))}/{_v(chart_d.get('ma20'))}/{_v(chart_d.get('ma60'))} → {_v(chart_d.get('trend_score'))}점\n"
                f"  · ADX: {_v(chart_d.get('adx'))}(+DI {_v(chart_d.get('plus_di'))}/-DI {_v(chart_d.get('minus_di'))}) → {_v(chart_d.get('adx_score'))}점\n"
                f"  · 이격도(MA20): {_v(chart_d.get('deviation'), '%')} → {_v(chart_d.get('deviation_score'))}점\n"
                f"  · 거래량비율(5/20일): {_v(chart_d.get('vol_ratio_5_20'), '%')} → {_v(chart_d.get('vol_score'))}점\n"
                f"  · 전일대비거래량: {_v(chart_d.get('vod_ratio'), '%')} → {_v(chart_d.get('vod_score'))}점\n\n"
                f"[수급점수] {supply_score if supply_score is not None else '-'}점\n"
                f"  · 외국인 5일: {_v(supply_d.get('frgn_5d_eok'), '억원')} → {_v(supply_d.get('frgn_score'))}점\n"
                f"  · 기관 5일: {_v(supply_d.get('orgn_5d_eok'), '억원')} → {_v(supply_d.get('orgn_score'))}점\n"
                f"  · 공매도 5일평균: {_v(supply_d.get('ssts_5d_avg'), '%')} → {_v(supply_d.get('ssts_score'))}점\n"
                f"  · 대차잔고비율: {_v(supply_d.get('loan_rate'), '%')} → {_v(supply_d.get('loan_score'))}점\n"
                f"  · OBV 5일변화: {_v(supply_d.get('obv_chg_pct'), '%')} → {_v(supply_d.get('obv_score'))}점"
            )
            context.bot.send_message(chat_id=user_id, text=text)
        except Exception as e:
            context.bot.send_message(chat_id=user_id, text=f"[종목 정보] 조회 오류: {str(e)}")

    if len(code) > 0:
        get_chart(code)
        with open('/home/terra/chart/save2.png', 'rb') as f:        
            context.bot.send_photo(chat_id=user_id, photo=f)
        send_stock_summary(code)

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


def callback_get(update, context):
    data_selected = update.callback_query.data
    query = update.callback_query
    command = data_selected.split(",")[-1] if "," in data_selected else data_selected

    if command.startswith("interest_register_"):
        # 가격 자동 조회 후 확인 버튼 표시
        ii_reg_code = command[len("interest_register_"):]
        try:
            query.answer("관심종목 조회 중...")
        except Exception:
            pass
        try:
            ac_reg = get_phills2_account()
            match_reg = stock_code[stock_code.code == ii_reg_code]
            ii_reg_name = match_reg.company.values[0].strip() if len(match_reg) > 0 else ii_reg_code
            ap_reg = inquire_price(ac_reg['access_token'], ac_reg['app_key'], ac_reg['app_secret'], ii_reg_code)
            today_high = int(ap_reg['stck_hgpr'])
            today_low  = int(ap_reg['stck_lwpr'])
            d20_high, d20_low = get_period_high_low(ac_reg['access_token'], ac_reg['app_key'], ac_reg['app_secret'],
                                                     ii_reg_code, period="D", count=20)
            y1_high, y1_low  = get_period_high_low(ac_reg['access_token'], ac_reg['app_key'], ac_reg['app_secret'],
                                                    ii_reg_code, period="M", count=12)
            d20_high = d20_high if d20_high is not None else 0
            d20_low  = d20_low  if d20_low  is not None else 0
            y1_high  = y1_high  if y1_high  is not None else 0
            y1_low   = y1_low   if y1_low   is not None else 0

            chat_id = query.message.chat_id
            _pending_register[chat_id] = {
                'code': ii_reg_code,
                'name': ii_reg_name,
                'acct_reg': str(ac_reg['acct_no']),
                'through_price': today_high,
                'leave_price': today_low,
                'd20_high': d20_high,
                'd20_low': d20_low,
                'y1_high': y1_high,
                'y1_low': y1_low,
            }
            try:
                query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("현재값으로 등록", callback_data=f"menu,interest_confirm_{ii_reg_code}"),
                InlineKeyboardButton("직접입력", callback_data=f"menu,interest_manual_{ii_reg_code}"),
            ]])
            context.bot.send_message(
                chat_id=chat_id,
                text=(f"[{ii_reg_name}(<code>{ii_reg_code}</code>)] 관심종목 등록\n"
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
            query.edit_message_text(text=f"[관심종목 등록] 오류: {str(e)}")

    elif command.startswith("interest_confirm_"):
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

# 텔레그램봇 응답 처리
echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
dispatcher.add_handler(echo_handler)
dispatcher.add_handler(CallbackQueryHandler(callback_get))

# 텔레그램봇 polling
updater.start_polling()