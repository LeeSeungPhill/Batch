import psycopg2 as db
#import kis_api_vps as ka
import kis_api_prod as ka
import datetime
import math

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
# DB 연결
conn = db.connect(conn_string)

today = datetime.datetime.now().strftime("%Y%m%d")
time = datetime.datetime.now().strftime("%H%M")

cur0 = conn.cursor()
cur0.execute("select name from stock_holiday where holiday = '"+today+"'")
result_one = cur0.fetchone()
cur0.close()

if result_one == None:
    ka.auth()
    a = ka.getTREnv()

    try:
        item_search1 = ka.inquire_search_result("phills2", "0")
        number = 0
        cur01 = conn.cursor()

        for i in item_search1:

            number = number + 1

            ins_param1 = (
                time,
                math.ceil(float(i['low'])),
                math.ceil(float(i['high'])), 
                math.ceil(float(i['price'])),
                i['chgrate'],
                math.ceil(float(i['acml_vol'])),
                i['chgrate2'],
                int(round(float(i['stotprice']))), 
                datetime.datetime.now(),
                today,
                "거래폭발", 
                i['code'],
                today, 
                time, 
                "거래폭발", 
                i['code'], 
                i['name'], 
                math.ceil(float(i['low'])), 
                math.ceil(float(i['high'])), 
                math.ceil(float(i['price'])), 
                i['chgrate'], 
                math.ceil(float(i['acml_vol'])), 
                i['chgrate2'], 
                int(round(float(i['stotprice']))), 
                datetime.datetime.now()
            )

            insert_query = "with upsert as (update stock_search_form set search_time = %s, low_price = %s, high_price = %s, current_price = %s, day_rate = %s, volumn = %s, volumn_rate = %s, market_total_sum = %s, cdate = %s where search_day = %s and search_name = %s and code = %s returning * ) insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
            cur01.execute(insert_query, ins_param1)
            conn.commit()

        item_search2 = ka.inquire_search_result("phills2", "1")
        number = 0
        for i in item_search2:

            number = number + 1
        
            ins_param1 = (
                time,
                math.ceil(float(i['low'])),
                math.ceil(float(i['high'])), 
                math.ceil(float(i['price'])),
                i['chgrate'],
                math.ceil(float(i['acml_vol'])),
                i['chgrate2'],
                int(round(float(i['stotprice']))), 
                datetime.datetime.now(),
                today,
                "단기추세", 
                i['code'],
                today, 
                time, 
                "단기추세", 
                i['code'], 
                i['name'], 
                math.ceil(float(i['low'])), 
                math.ceil(float(i['high'])), 
                math.ceil(float(i['price'])), 
                i['chgrate'], 
                math.ceil(float(i['acml_vol'])), 
                i['chgrate2'], 
                int(round(float(i['stotprice']))), 
                datetime.datetime.now()
            )

            insert_query = "with upsert as (update stock_search_form set search_time = %s, low_price = %s, high_price = %s, current_price = %s, day_rate = %s, volumn = %s, volumn_rate = %s, market_total_sum = %s, cdate = %s where search_day = %s and search_name = %s and code = %s returning * ) insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
            cur01.execute(insert_query, ins_param1)
            conn.commit()

        item_search3 = ka.inquire_search_result("phills2", "2")
        number = 0
        for i in item_search3:

            number = number + 1
        
            ins_param1 = (
                time,
                math.ceil(float(i['low'])),
                math.ceil(float(i['high'])), 
                math.ceil(float(i['price'])),
                i['chgrate'],
                math.ceil(float(i['acml_vol'])),
                i['chgrate2'],
                int(round(float(i['stotprice']))), 
                datetime.datetime.now(),
                today,
                "투자혁명", 
                i['code'],
                today, 
                time, 
                "투자혁명", 
                i['code'], 
                i['name'], 
                math.ceil(float(i['low'])), 
                math.ceil(float(i['high'])), 
                math.ceil(float(i['price'])), 
                i['chgrate'], 
                math.ceil(float(i['acml_vol'])), 
                i['chgrate2'], 
                int(round(float(i['stotprice']))), 
                datetime.datetime.now()
            )

            insert_query = "with upsert as (update stock_search_form set search_time = %s, low_price = %s, high_price = %s, current_price = %s, day_rate = %s, volumn = %s, volumn_rate = %s, market_total_sum = %s, cdate = %s where search_day = %s and search_name = %s and code = %s returning * ) insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
            cur01.execute(insert_query, ins_param1)
            conn.commit()

        # item_search4 = ka.inquire_search_result("phills2", "3")
        # number = 0
        # for i in item_search4:

        #     number = number + 1
        
        #     ins_param1 = (
        #         time,
        #         math.ceil(float(i['low'])),
        #         math.ceil(float(i['high'])), 
        #         math.ceil(float(i['price'])),
        #         i['chgrate'],
        #         math.ceil(float(i['acml_vol'])),
        #         i['chgrate2'],
        #         int(round(float(i['stotprice']))), 
        #         datetime.datetime.now(),
        #         today,
        #         "파워급등주", 
        #         i['code'],
        #         today, 
        #         time, 
        #         "파워급등주", 
        #         i['code'], 
        #         i['name'], 
        #         math.ceil(float(i['low'])), 
        #         math.ceil(float(i['high'])), 
        #         math.ceil(float(i['price'])), 
        #         i['chgrate'], 
        #         math.ceil(float(i['acml_vol'])), 
        #         i['chgrate2'], 
        #         int(round(float(i['stotprice']))), 
        #         datetime.datetime.now()
        #     )

        #     insert_query = "with upsert as (update stock_search_form set search_time = %s, low_price = %s, high_price = %s, current_price = %s, day_rate = %s, volumn = %s, volumn_rate = %s, market_total_sum = %s, cdate = %s where search_day = %s and search_name = %s and code = %s returning * ) insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
        #     cur01.execute(insert_query, ins_param1)
        #     conn.commit()

        # item_search5 = ka.inquire_search_result("phills2", "4")
        # number = 0
        # for i in item_search5:

        #     number = number + 1

        #     ins_param1 = (
        #         time,
        #         math.ceil(float(i['low'])),
        #         math.ceil(float(i['high'])), 
        #         math.ceil(float(i['price'])),
        #         i['chgrate'],
        #         math.ceil(float(i['acml_vol'])),
        #         i['chgrate2'],
        #         int(round(float(i['stotprice']))), 
        #         datetime.datetime.now(),
        #         today,
        #         "파워종목", 
        #         i['code'],
        #         today, 
        #         time, 
        #         "파워종목", 
        #         i['code'], 
        #         i['name'], 
        #         math.ceil(float(i['low'])), 
        #         math.ceil(float(i['high'])), 
        #         math.ceil(float(i['price'])), 
        #         i['chgrate'], 
        #         math.ceil(float(i['acml_vol'])), 
        #         i['chgrate2'], 
        #         int(round(float(i['stotprice']))), 
        #         datetime.datetime.now()
        #     )

        #     insert_query = "with upsert as (update stock_search_form set search_time = %s, low_price = %s, high_price = %s, current_price = %s, day_rate = %s, volumn = %s, volumn_rate = %s, market_total_sum = %s, cdate = %s where search_day = %s and search_name = %s and code = %s returning * ) insert into stock_search_form(search_day, search_time, search_name, code, name, low_price, high_price, current_price, day_rate, volumn, volumn_rate, market_total_sum, cdate) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
        #     cur01.execute(insert_query, ins_param1)
        #     conn.commit()

        # DB 연결된 커서 설정
        cur1 = conn.cursor()        
        d = ka.get_acct_balance('True');  # 계좌 잔고 조회

        for i, name in enumerate(d.index):
            u_dnca_tot_amt = int(d['dnca_tot_amt'][i])  # 예수금총금액
            u_prvs_rcdl_excc_amt = int(d['prvs_rcdl_excc_amt'][i])  # 가수도 정산 금액
            u_thdt_buy_amt = int(d['thdt_buy_amt'][i])  # 금일 매수 금액
            u_thdt_sll_amt = int(d['thdt_sll_amt'][i])  # 금일 매도 금액
            u_thdt_tlex_amt = int(d['thdt_tlex_amt'][i])  # 금일 제비용 금액
            u_scts_evlu_amt = int(d['scts_evlu_amt'][i])  # 유저 평가 금액
            u_tot_evlu_amt = int(d['tot_evlu_amt'][i])  # 총평가금액
            u_nass_amt = int(d['nass_amt'][i])  # 순자산금액(세금비용 제외)
            u_pchs_amt_smtl_amt = int(d['pchs_amt_smtl_amt'][i])  # 매입금액 합계금액
            u_evlu_amt_smtl_amt = int(d['evlu_amt_smtl_amt'][i])  # 평가금액 합계금액
            u_evlu_pfls_smtl_amt = int(d['evlu_pfls_smtl_amt'][i])  # 평가손익 합계금액
            u_bfdy_tot_asst_evlu_amt = int(d['bfdy_tot_asst_evlu_amt'][i])  # 전일총자산 평가금액
            u_asst_icdc_amt = int(d['asst_icdc_amt'][i])  # 자산 증감액

        insert_query1 = "with upsert as (update dly_acct_balance set dnca_tot_amt = %s, prvs_excc_amt = %s, td_buy_amt = %s, td_sell_amt = %s, td_tex_amt = %s, user_evlu_amt = %s, tot_evlu_amt = %s, nass_amt = %s, pchs_amt = %s, evlu_amt = %s, evlu_pfls_amt = %s, ytdt_tot_evlu_amt = %s, asst_icdc_amt = %s, last_chg_date = %s where dt = %s and acct = %s returning * ) insert into dly_acct_balance(acct, dt, dnca_tot_amt, prvs_excc_amt, td_buy_amt, td_sell_amt, td_tex_amt, user_evlu_amt, tot_evlu_amt, nass_amt, pchs_amt, evlu_amt, evlu_pfls_amt, ytdt_tot_evlu_amt, asst_icdc_amt, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
        # insert 인자값 설정
        record_to_insert1 = ([u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_thdt_buy_amt, u_thdt_sll_amt, u_thdt_tlex_amt, u_scts_evlu_amt, u_tot_evlu_amt, u_nass_amt, u_pchs_amt_smtl_amt, u_evlu_amt_smtl_amt, u_evlu_pfls_smtl_amt, u_bfdy_tot_asst_evlu_amt, u_asst_icdc_amt, datetime.datetime.now(), today, a.my_acct,
            a.my_acct, today, u_dnca_tot_amt, u_prvs_rcdl_excc_amt, u_thdt_buy_amt, u_thdt_sll_amt, u_thdt_tlex_amt, u_scts_evlu_amt, u_tot_evlu_amt, u_nass_amt, u_pchs_amt_smtl_amt, u_evlu_amt_smtl_amt, u_evlu_pfls_smtl_amt, u_bfdy_tot_asst_evlu_amt, u_asst_icdc_amt, datetime.datetime.now()])
        # DB 연결된 커서의 쿼리 수행
        cur1.execute(insert_query1, record_to_insert1)
        conn.commit()

        cur11 = conn.cursor()
        # 보유잔고 정보 proc_yn = 'N' 대상 삭제 처리
        delete_query1 = "delete from \"stockBalance_stock_balance\" where proc_yn = 'N' and acct_no = %s and TO_CHAR(last_chg_date, 'YYYYMMDD') < %s"
        # insert 인자값 설정
        record_to_delete1 = ([a.my_acct, today])
        # DB 연결된 커서의 쿼리 수행
        cur11.execute(delete_query1, record_to_delete1)
        conn.commit()

        cur2 = conn.cursor()
        e = ka.get_acct_balance();  # 계좌 잔고 조회

        for i, name in enumerate(e.index):
            e_code = e['pdno'][i]                           # 종목코드
            e_name = e['prdt_name'][i]                      # 종목명
            e_buy_qty = int(e['thdt_buyqty'][i])            # 금일매수수량
            e_sell_qty = int(e['thdt_sll_qty'][i])          # 금일매도수량
            e_purchase_price = e['pchs_avg_pric'][i]        # 매입단가
            e_purchase_qty = int(e['hldg_qty'][i])          # 보유수량
            e_purchase_amt = int(e['pchs_amt'][i])          # 매입금액
            e_current_price = int(e['prpr'][i])             # 현재가
            e_eval_amt = int(e['evlu_amt'][i])              # 평가금액
            e_earnings_rate = e['evlu_pfls_rt'][i]          # 수익율
            e_valuation_amt = int(e['evlu_pfls_amt'][i])    # 평가손익금액

            f = ka.get_current_price(e_code)
            f_open_price = int(f['stck_oprc'])              # 시가
            f_high_price = int(f['stck_hgpr'])              # 최고가
            f_low_price = int(f['stck_lwpr'])               # 최저가
            f_volumn = int(f['acml_vol'])                   # 누적거래량
            f_volumn_rate = float(f['prdy_vrss_vol_rate'])  # 전일대비거래량비율

            insert_query2 = "with upsert as (update dly_stock_balance set buy_qty = %s, sell_qty = %s, purchase_price = %s, purchase_qty = %s, purchase_amt = %s, current_price = %s, open_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, eval_sum = %s, earnings_rate = %s, valuation_sum = %s, last_chg_date = %s, sign_resist_price = (select sign_resist_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"'), sign_support_price = (select sign_support_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"'), end_target_price = (select end_target_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"'), end_loss_price = (select end_loss_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"') where dt = %s and code = %s and acct = %s returning * ) insert into dly_stock_balance(acct, dt, name, code, buy_qty, sell_qty, purchase_price, purchase_qty, purchase_amt, current_price, open_price, high_price, low_price, volumn, volumn_rate, eval_sum, earnings_rate, valuation_sum, last_chg_date, sign_resist_price, sign_support_price, end_target_price, end_loss_price) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (select sign_resist_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select sign_support_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select end_target_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"'), (select end_loss_price from \"stockBalance_stock_balance\" where acct_no = '"+a.my_acct+"' and proc_yn = 'Y' and code = '"+e_code+"') where not exists(select * from upsert)"
            # insert 인자값 설정
            record_to_insert2 = ([e_buy_qty, e_sell_qty, round(float(e_purchase_price)), e_purchase_qty, e_purchase_amt, e_current_price, f_open_price, f_high_price, f_low_price, f_volumn, float(f_volumn_rate), e_eval_amt, float(e_earnings_rate), e_valuation_amt, datetime.datetime.now(), today, e_code[:6], a.my_acct,
                a.my_acct, today, e_name, e_code[:6], e_buy_qty, e_sell_qty, round(float(e_purchase_price)), e_purchase_qty, e_purchase_amt, e_current_price, f_open_price, f_high_price, f_low_price, f_volumn, float(f_volumn_rate), e_eval_amt, float(e_earnings_rate), e_valuation_amt, datetime.datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur2.execute(insert_query2, record_to_insert2)
            conn.commit()

        # 관심정보 조회
        cur21 = conn.cursor()
        cur21.execute("select code, name, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price from \"interestItem_interest_item\" where acct_no = '"+a.my_acct+"'")
        result = cur21.fetchall()
        cur21.close()
        cur22 = conn.cursor()

        # 관심종목 이탈가, 돌파가, 지지가, 저항가, 추세하단가, 추세상단가를 각각 현재시세의 최고가 최저가 비교
        for i in result:
            print("종목코드 : " + i[0])
            print("종목명 : " + i[1])
            print("돌파가 : " + format(int(i[2]), ',d'))
            print("이탈가 : " + format(int(i[3]), ',d'))
            print("저항가 : " + format(int(i[4]), ',d'))
            print("지지가 : " + format(int(i[5]), ',d'))
            print("추세상단가 : " + format(int(i[6]), ',d'))
            print("추세하단가 : " + format(int(i[7]), ',d'))
            code = i[0]                                         # 종목코드
            name = i[1]                                         # 종목명

            if len(i[0]) == 4:
                b = ka.inquire_daily_indexchartprice(i[0], today)
                open_price = int(float(b['bstp_nmix_oprc']))    # 시가포인트
                high_price = int(float(b['bstp_nmix_hgpr']))    # 최고포인트
                low_price = int(float(b['bstp_nmix_lwpr']))     # 최저포인트
                current_price = int(float(b['bstp_nmix_prpr'])) # 현재포인트
                volumn = int(b['acml_vol'])                     # 누적거래량
                volumn_rate = float(int(b['acml_vol']) / int(b['prdy_vol']) * 100)   # 전일대비거래량비율

            else:
                f = ka.get_current_price(code)
                open_price = int(f['stck_oprc'])                # 시가
                high_price = int(f['stck_hgpr'])                # 최고가
                low_price = int(f['stck_lwpr'])                 # 최저가
                current_price = int(f['stck_prpr'])             # 종가
                volumn = int(f['acml_vol'])                     # 누적거래량
                volumn_rate = float(f['prdy_vrss_vol_rate'])    # 전일대비거래량비율    
            
            insert_query22 = "with upsert as (update dly_stock_interest set current_price = %s, open_price = %s, high_price = %s, low_price = %s, volumn = %s, volumn_rate = %s, through_price = %s, leave_price = %s, resist_price = %s, support_price = %s, trend_high_price = %s, trend_low_price = %s, last_chg_date = %s where dt = %s and code = %s and acct = %s returning * ) insert into dly_stock_interest(acct, dt, name, code, current_price, open_price, high_price, low_price, volumn, volumn_rate, through_price, leave_price, resist_price, support_price, trend_high_price, trend_low_price, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert)"
            # insert 인자값 설정
            record_to_insert22 = ([current_price, open_price, high_price, low_price, volumn, float(volumn_rate), int(i[2]), int(i[3]), int(i[4]), int(i[5]), int(i[6]), int(i[7]), datetime.datetime.now(), today, code, a.my_acct,
                a.my_acct, today, name, code, current_price, open_price, high_price, low_price, volumn, float(volumn_rate), int(i[2]), int(i[3]), int(i[4]), int(i[5]), int(i[6]), int(i[7]), datetime.datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur22.execute(insert_query22, record_to_insert22)
            conn.commit()    

        cur3 = conn.cursor()
        g = ka.get_my_complete(today)  # 일별 주문 체결 조회

        for i, name in enumerate(g.index):
            #u_odno = int(g['odno'][i])  # 주문번호
            #u_orgn_odno = int(g['orgn_odno'][i])  # 원주문번호
            if g['odno'][i] != "":
                u_odno = int(g['odno'][i])
            else:
                u_odno = 0
            if g['orgn_odno'][i] != "":
                u_orgn_odno = int(g['orgn_odno'][i])
            else:
                u_orgn_odno = 0
            u_order_type = g['sll_buy_dvsn_cd_name'][i]  # 주문유형
            u_order_dt = g['ord_dt'][i]  # 주문읿자
            u_order_tmd = g['ord_tmd'][i]  # 주문시각
            u_name = g['prdt_name'][i]  # 종목명
            u_order_price = int(g['ord_unpr'][i])  # 주문단가
            u_order_qty = int(g['ord_qty'][i])  # 주문수량
            u_total_complete_qty = int(g['tot_ccld_qty'][i])  # 총체결수량
            u_remain_qty = int(g['rmn_qty'][i])  # 잔여수량
            u_cncl_yn = g['cncl_yn'][i]  # 취소여부
            u_cncl_qty = int(g['cncl_cfrm_qty'][i])  # 취소확인수량
            u_complete_avg_price = int(g['avg_prvs'][i])  # 체결평균가
            u_total_complete_amt = int(g['tot_ccld_amt'][i])  # 총체결금액

            insert_query3 = "with upsert as (update dly_order_completion set org_order_no = %s, order_type = %s, order_tmd = %s, name = %s, order_price = %s, order_qty = %s, total_complete_qty = %s, remain_qty = %s, cncl_yn = %s, cncl_qty = %s, complete_avg_price = %s, total_complete_amt = %s, last_chg_date = %s where order_no = %s and order_dt = %s and acct = %s returning * ) insert into dly_order_completion(acct, order_no, org_order_no, order_type, order_dt, order_tmd, name, order_price, order_qty, total_complete_qty, remain_qty, cncl_yn, cncl_qty, complete_avg_price, total_complete_amt, last_chg_date) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s where not exists(select * from upsert);"
            # insert 인자값 설정
            record_to_insert3 = (
            [u_orgn_odno, u_order_type, u_order_tmd, u_name, u_order_price, u_order_qty, u_total_complete_qty, u_remain_qty,
             u_cncl_yn, u_cncl_qty, u_complete_avg_price, u_total_complete_amt, datetime.datetime.now(), u_odno, today, a.my_acct,
             a.my_acct, u_odno, u_orgn_odno, u_order_type, u_order_dt, u_order_tmd, u_name, u_order_price, u_order_qty,
             u_total_complete_qty, u_remain_qty, u_cncl_yn, u_cncl_qty, u_complete_avg_price, u_total_complete_amt,
             datetime.datetime.now()])
            # DB 연결된 커서의 쿼리 수행
            cur3.execute(insert_query3, record_to_insert3)
            conn.commit()

        cur01.close()
        cur1.close()
        cur11.close()
        cur2.close()
        cur22.close()
        cur3.close()
        conn.close()

    except Exception as ex:
        cur01.close()
        cur1.close()
        cur11.close()
        cur2.close()
        cur22.close()
        cur3.close()
        conn.close()
        print('잘못된 인덱스입니다.', ex)
else:
    conn.close()
    print("Today is Holiday")
