import re
import pandas as pd
from telegram.ext import Updater
from telegram.ext import MessageHandler, Filters
import requests
from io import StringIO
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from datetime import datetime, timedelta
import urllib3
from pykrx import stock
from mplfinance.original_flavor import candlestick2_ohlc
import matplotlib.ticker as mticker
import psycopg2 as db

urllib3.disable_warnings()
matplotlib.use('SVG')
plt.rcParams["font.family"] = "NanumGothic"
# 해당 링크는 한국거래소에서 상장법인목록을 엑셀로 다운로드하는 링크입니다.
# 다운로드와 동시에 Pandas에 excel 파일이 load가 되는 구조입니다.
stock_code = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
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
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)
cur001 = conn.cursor()
cur001.execute("select bot_token1 from \"stockAccount_stock_account\" where nick_name = 'kwphills75'")
result_001 = cur001.fetchone()
cur001.close()
token = result_001[0]

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
def get_dividiend(code):

    session = requests.Session()
    URL = "https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=A%s&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701" % (code)
    r = session.get(URL)
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

        IS_temp.drop('매출원가', axis=1, inplace=True)
        IS_temp.drop('매출총이익', axis=1, inplace=True)
        IS_temp.drop('영업이익(발표기준)', axis=1, inplace=True)
        IS_temp.drop('판매비와관리비', axis=1, inplace=True)
        IS_temp.drop('금융원가', axis=1, inplace=True)
        IS_temp.drop('기타비용', axis=1, inplace=True)
        IS_temp.drop('종속기업,공동지배기업및관계기업관련손익', axis=1, inplace=True)
        IS_temp.drop('세전계속사업이익', axis=1, inplace=True)
        IS_temp.drop('법인세비용', axis=1, inplace=True)
        IS_temp.drop('계속영업이익', axis=1, inplace=True)
        IS_temp.drop('중단영업이익', axis=1, inplace=True)

        IS_temp.index = pd.to_datetime(IS_temp.index)
        IS_temp = IS_temp[pd.notnull(IS_temp.index)]
        result = IS_temp.fillna(0)
        print(result)
        return result

# 텔레그램봇 응답 message handler
def echo(update, context):
    user_id = update.effective_chat.id
    user_text = update.message.text

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
        dividend = get_dividiend(code)

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
        ax_top.set_title(title, fontsize=15)
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

        plt.savefig('/home/terra/Public/Batch/save2.png')

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
        context.bot.send_photo(chat_id=user_id, photo=open('/home/terra/Public/Batch/save2.png', 'rb'))

        text0 = return_print("<" + company + ">")
        text1 = return_print("[매출액]")
        for date in get_sales_sum("매출액").keys():
            #print("%s : %s" % (date, get_sales_sum("매출액")[date]))
            text1 = text1+return_print("%s : %s" % (date, get_sales_sum("매출액")[date]))
        text2 = return_print("[영업이익]")
        for date in get_sales_sum("영업이익").keys():
            #print("%s : %s" % (date, get_sales_sum("영업이익")[date]))
            text2 = text2+return_print("%s : %s" % (date, get_sales_sum("영업이익")[date]))
        text3 = return_print("[당기순이익]")
        for date in get_sales_sum("당기순이익").keys():
            #print("%s : %s" % (date, get_sales_sum(4)[date]))
            text3 = text3+return_print("%s : %s" % (date, get_sales_sum("당기순이익")[date]))
        text4 = return_print("[금융수익]")
        for date in get_sales_sum("금융수익").keys():
            #print("%s : %s" % (date, get_sales_sum("금융수익")[date]))
            text4 = text4+return_print("%s : %s" % (date, get_sales_sum("금융수익")[date]))
        text5 = return_print("[기타수익]")
        for date in get_sales_sum("기타수익").keys():
            #print("%s : %s" % (date, get_sales_sum("기타수익")[date]))
            text5 = text5+return_print("%s : %s" % (date, get_sales_sum("기타수익")[date]))

        context.bot.send_message(chat_id=user_id, text=text0+text1+text2+text3+text4+text5)

# 텔레그램봇 응답 처리
echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
dispatcher.add_handler(echo_handler)

# 텔레그램봇 polling
updater.start_polling()