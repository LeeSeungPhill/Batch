import psycopg2 as db

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

# DB 연결
conn = db.connect(conn_string)

cur1 = conn.cursor()
cur1.execute("CALL upd_dly_stock_item(%s, %s, %s);", ['', ''])
result = cur1.fetchall()
cur1.close()
conn.commit()

if result != None:
    for i in result:
        print(i[0])
        print(i[1])

