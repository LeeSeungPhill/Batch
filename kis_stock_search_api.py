import kis_api_resp as resp
import requests
import telegram
import psycopg2 as db
import math
from datetime import datetime

#URL_BASE = "https://openapivts.koreainvestment.com:29443"   # 모의투자서비스
URL_BASE = "https://openapi.koreainvestment.com:9443"       # 실전서비스

# PostgreSQL 연결 설정
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

def search(access_token, app_key, app_secret, user_token, search_choice):

    today = datetime.now().strftime("%Y%m%d")
    time = datetime.now().strftime("%H%M")

    # DB 연결
    conn = db.connect(conn_string)

    # 조건검색명
    if search_choice == '0':
        search_name = "거래폭발"
    elif search_choice == '1':
        search_name = "단기추세"
    elif search_choice == '2':
        search_name = "투자혁명"
    elif search_choice == '3':
        search_name = "파워급등주"
    elif search_choice == '4':
        search_name = "파워종목"        

    # 텔레그램봇 사용할 token
    if user_token == 'chichipa':
        token = "6353758449:AAG6LVdzgSRDSspoSzSJZVGnGw1SGHlAgi4"
    elif user_token == 'phills13':
        token = "5721274603:AAHiwtuara7M-I-MIzcrt3E8TZBCRUpBUB4"
    elif user_token == 'phills15':
        token = "6376313566:AAFPYOKj5_yyZ5jZJJ4JXJPqpyZXXo3fZ4M"
    elif user_token == 'phills2':
        token = "5458112774:AAGwNnfjuC75WdK2ZYm_mttmXajzkhyvaHc"
    elif user_token == 'phills75':
        token = "7242807146:AAH9fbu34tKKNaDDtJ2ew6zYPhzXkVvc9KA"    
    elif user_token == 'yh480825':
        token = "8143915544:AAEF-wVvqg9XZFKkVF4zUjm5LYC648OSWOg"    
    else:
        token = "6008784254:AAEcJaePafd6Bh0riGL57OjhZ_ZoFxe6Fw0"     

    # 텔레그램 연동 토큰값 설정
    bot = telegram.Bot(token)

    def inquire_search_result(access_token, app_key, app_secret, id, seq):

        headers = {"Content-Type": "application/json",
                "authorization": f"Bearer {access_token}",
                "appKey": app_key,
                "appSecret": app_secret,
                "tr_id": "HHKST03900400",
                "custtype": "P"}
        params = {
                'user_id': id,
                'seq': seq
        }
        PATH = "/uapi/domestic-stock/v1/quotations/psearch-result"
        URL = f"{URL_BASE}/{PATH}"
        res = requests.get(URL, headers=headers, params=params, verify=False)
        ar = resp.APIResp(res)
        #ar.printAll()
        return ar.getBody().output2

    item_search = inquire_search_result(access_token, app_key, app_secret, 'phills2', search_choice)  # 종목조건검색 조회

    number = 0
    cur01 = conn.cursor()

    for i in item_search:

        number = number + 1

        print("순위 : " + str(number))
        print("종목코드 : " + i['code'])
        print("종목명 : " + i['name'])
        print("현재가 : " + format(math.ceil(float(i['price'])), ',d')) 
        print("등락률 : " + i['chgrate'])
        print("거래량 : " + format(math.ceil(float(i['acml_vol'])), ',d'))
        print("전일대비 : " + i['chgrate2'])
        print("고가 : " + format(math.ceil(float(i['high'])), ',d')) 
        print("저가 : " + format(math.ceil(float(i['low'])), ',d'))     
        print("시가총액 : " + format(int(round(float(i['stotprice']))), ',d'))
    
        # 텔레그램 메시지 전송
        telegram_text = "<" + search_name + "(" + str(number) + ")> " + i['name'] + "[" + i['code'] + "] 현재가 : " + format(math.ceil(float(i['price'])), ',d') + "원, 등략율 : " + str(round(float(i['chgrate']), 2)) + "%, 거래량 : " + format(math.ceil(float(i['acml_vol'])), ',d') + "주, 전일대비 : " + str(round(float(i['chgrate2']), 2)) + "%, 고가 : " + format(math.ceil(float(i['high'])), ',d') + "원, 저가 : " + format(math.ceil(float(i['low'])), ',d') + "원, 시가총액 : " + format(int(round(float(i['stotprice']))), ',d') + "억원"
        # 텔레그램 메시지 전송
        bot.send_message(chat_id="2147256258", text=telegram_text)

        ins_param1 = (
            time,
            math.ceil(float(i['low'])),
            math.ceil(float(i['high'])), 
            math.ceil(float(i['price'])),
            i['chgrate'],
            math.ceil(float(i['acml_vol'])),
            i['chgrate2'],
            int(round(float(i['stotprice']))), 
            datetime.now(),
            today,
            search_name, 
            i['code'],
            today, 
            time, 
            search_name, 
            i['code'], 
            i['name'], 
            math.ceil(float(i['low'])), 
            math.ceil(float(i['high'])), 
            math.ceil(float(i['price'])), 
            i['chgrate'], 
            math.ceil(float(i['acml_vol'])), 
            i['chgrate2'], 
            int(round(float(i['stotprice']))), 
            datetime.now()
        )

        insert_query = "with upsert as (update stock_search_form set search_time = %s, low_price = %s, high_price = %s, current_price = %s, day_rate = %s, volumn = %s, volumn_rate = %s, market_total_sum = %s, cdate = %s where search_day = %s and search_name = %s and code = %s returning * ) insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
        cur01.execute(insert_query, ins_param1)
        conn.commit()

    cur01.close()
    conn.close()