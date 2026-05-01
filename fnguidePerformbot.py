import re
import json
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
        else:
            IS_temp.index = IS_temp['IFRS(개별)'].values
            IS_temp.drop(['IFRS(개별)', '전년동기', '전년동기(%)'], inplace=True, axis=1)

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

    if len(code) > 0:
        dividend = get_dividend(code)

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

    def get_sales_sum(col):

        dict = {}
        count = 0

        for x in dividend.index:

            if count > 4:
                break
            else:
                row = str(x)
                idx = 0

                for val in dividend[col]:
                    dfrow = str(dividend[col].index[idx])

                    if row[0:10] == dfrow[0:10]:
                        dict[dfrow[0:7]] = format(int(val), ',d')

                    idx += 1

                count += 1

        return dict

    def return_print(*message):
        io = StringIO()
        print(*message, file=io)
        return io.getvalue()

    if len(code) > 0 and dividend is not None:
        get_chart(code)
        with open('/home/terra/chart/save2.png', 'rb') as f:
            context.bot.send_photo(chat_id=user_id, photo=f)

        text0 = return_print("<" + company + ">")
        text1 = return_print("[매출액]")
        if "매출액" in dividend.columns:
            for date in get_sales_sum("매출액").keys():
                text1 = text1+return_print("%s : %s" % (date, get_sales_sum("매출액")[date]))
        text2 = return_print("[영업이익]")
        if "영업이익" in dividend.columns:
            for date in get_sales_sum("영업이익").keys():
                text2 = text2+return_print("%s : %s" % (date, get_sales_sum("영업이익")[date]))
        text3 = return_print("[당기순이익]")
        if "당기순이익" in dividend.columns:
            for date in get_sales_sum("당기순이익").keys():
                text3 = text3+return_print("%s : %s" % (date, get_sales_sum("당기순이익")[date]))
        text4 = return_print("[금융수익]")
        if "금융수익" in dividend.columns:
            for date in get_sales_sum("금융수익").keys():
                text4 = text4+return_print("%s : %s" % (date, get_sales_sum("금융수익")[date]))
        text5 = return_print("[기타수익]")
        if "기타수익" in dividend.columns:
            for date in get_sales_sum("기타수익").keys():
                text5 = text5+return_print("%s : %s" % (date, get_sales_sum("기타수익")[date]))

        context.bot.send_message(chat_id=user_id, text=text0+text1+text2+text3+text4+text5)

def _do_interest_register(chat_id, context, pending):
    try:
        c_reg = get_conn()
        with c_reg.cursor() as cur_reg:
            cur_reg.execute("""
                INSERT INTO public."interestItem_interest_item"
                    (acct_no, code, name, through_price, leave_price, resist_price, support_price,
                     trend_high_price, trend_low_price, interest_day, interest_dtm, proc_yn, last_chg_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Y', %s)
                ON CONFLICT (acct_no, code) DO UPDATE SET
                    name             = EXCLUDED.name,
                    through_price    = EXCLUDED.through_price,
                    leave_price      = EXCLUDED.leave_price,
                    resist_price     = EXCLUDED.resist_price,
                    support_price    = EXCLUDED.support_price,
                    trend_high_price = EXCLUDED.trend_high_price,
                    trend_low_price  = EXCLUDED.trend_low_price,
                    last_chg_date    = EXCLUDED.last_chg_date
            """, (pending['acct_reg'], pending['code'], pending['name'],
                  pending['through_price'], pending['leave_price'],
                  pending['d20_high'], pending['d20_low'],
                  pending['y1_high'], pending['y1_low'],
                  datetime.now().strftime('%Y%m%d'), datetime.now().strftime('%H%M%S'),
                  datetime.now()))
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