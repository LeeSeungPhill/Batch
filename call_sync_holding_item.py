import sys
import psycopg2 as db

arguments = sys.argv

parameter = arguments[1]

# PostgreSQL 연결 설정
conn_string = "dbname='fund_risk_mng' host='localhost' port='5432' user='postgres' password='sktl2389!1'"
#conn_string = "dbname='my_develop' host='localhost' port='5432' user='postgres' password='sktl2389!1'"

# DB 연결
conn = db.connect(conn_string)

cur1 = conn.cursor()
cur1.execute("CALL sync_holding_item(%s, %s);", [(str(parameter)),''])
result = cur1.fetchall()
cur1.close()
conn.commit()

if result != None:
    for i in result:
        print(i[0])
