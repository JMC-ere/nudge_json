import pymysql
import json


def mysqlSelect(connect_info):

    with open(connect_info, 'r') as f:
        connect_info = json.load(f)
    try:
        # MySQL Connection 연결
        conn = pymysql.connect(host=connect_info['maria_db_dev']['host'],
                               user=connect_info['maria_db_dev']['user'],
                               password=connect_info['maria_db_dev']['password'],
                               db=connect_info['maria_db_dev']['db'],
                               charset='utf8', cursorclass=pymysql.cursors.DictCursor)

        # Connection 으로부터 Cursor 생성
        curs = conn.cursor()

        # SQL문 실행
        sql = connect_info['maria_db_dev']['sql']
        curs.execute(sql)

        # 데이타 Fetch
        rows = curs.fetchall()
    except Exception as err:
        print(err)

    conn.close()

    return rows