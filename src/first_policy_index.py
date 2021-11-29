import pymysql
import logging
import logging.handlers
import os
import json
import telegram
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import sys

nudge_data = datetime.now() + timedelta(days=1)
nudge_date_2 = nudge_data.strftime("%Y%m%d"+'000000')
file_path = sys.argv[1]
# file_path = "config/connect_info.json"
# connect_info = "config/connect_info.json"


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
        conn.close()
        return rows
    except Exception as err:
        print(err)
        exit()


def makeJsonData(arr_data, connect_info, update):
    with open(connect_info, 'r') as f:
        connect_info = json.load(f)
    try:
        es = Elasticsearch(['1.255.145.176',
                            '1.255.145.177',
                            '1.255.145.178'],
                           port=9200)  # 환경에 맞게 바꿀 것
        es.info()

        check_cnt = 0
        index_name = "index-nudge-policy"

        for data in arr_data:

            # DB json 데이터 처리
            if data['manual_text']:
                data['manual_text'] = json.loads(data['manual_text'])
            if data['ext_info']:
                data['ext_info'] = json.loads(data['ext_info'])

            # 데이터 생성시간
            # data['@timestamp'] = datetime.today()

            data['log_time'] = nudge_data.strftime("%Y.%m.%d")
            data['stb_ver'] = "v524"
            data['nudge_date'] = nudge_date_2

            # mapping type
            # synop : vod 시놉랜딩
            # app : app 런칭
            # voice_search : 음성검색결과 half
            # voice_nugu : NUGU 음성 명령 구동
            # channel : 채널 이동
            # home : 홈UI 메뉴 이동
            # popup : 팝업 브라우징

            data['icon_type'] = data.pop("icon_id")

            if data['menu_id'] != "":
                a = data['menu_id'].replace("menu", "")
                menus_nudge_date = a
                data['menu_nudge_date'] = str(int(nudge_date_2) + int(menus_nudge_date))
            elif data['menu_id'] == "":
                data['menu_nudge_date'] = 0

            data['senior_nudge_date'] = str(int(nudge_date_2) + 17)
            data['zapping_nudge_date'] = str(int(nudge_date_2) + 18)

            if data['manual_text'] is None:
                data['manual_text'] = ""

            # manual_text 배열이 2개이상일 경우 \n 처리
            if len(data['manual_text']) >= 2:
                data['manual_text'] = "\\n".join(data['manual_text'])
            else:
                data['manual_text'] = "".join(data['manual_text'])

            if data['wait_time'] is None:
                data['wait_time'] = 0

            if data['ext_info'] is None:
                data['ext_info'] = []

            if update == "update":
                if check_cnt < 1:
                    es.delete_by_query(index=index_name, body={
                        "query": {
                            "bool": {
                              "filter": [
                                  {"term": {"log_time": data['log_time']}},
                                  {"term": {"stb_ver": "v532"}}
                                ]
                            }
                          }
                    })
                check_cnt += 1
                es.index(index=index_name, doc_type='_doc', body=data)

            elif update != "update":
                es.index(index=index_name, doc_type='_doc', body=data)

        es.indices.refresh(index=index_name)
    except Exception as err:
        print(err)


def make_log(type, connect_info):
    with open(connect_info, 'r') as f:
        connect_info = json.load(f)
    log_path = "./logs/" + nudge_data.strftime("%Y.%m.%d") + '.log'
    logger = logging.getLogger(__name__)
    fileHandler = logging.FileHandler(log_path)
    formatter = logging.Formatter('[%(asctime)s][%(filename)s:%(lineno)s] >> %(message)s')
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    logger.setLevel(level=logging.DEBUG)
    try:
        index_es = Elasticsearch(['1.255.145.176',
                                  '1.255.145.177',
                                  '1.255.145.178'],
                                 port=9200)  # 환경에 맞게 바꿀 것
        index_es.info()
        index_name = "index-nudge-policy"

        body = """
                {
                  "size" : 0,
                  "query": {
                    "bool": {
                      "filter": [
                          {"term": {"log_time": "%s"}},
                          {"term": {"stb_ver": "v524"}}
                        ]
                    }
                  },
                  "aggs": {
                    "NAME": {
                      "terms": {
                        "field": "seg_id",
                        "size": 100
                      }
                    }
                  }
                }
            """

        body_result = body % nudge_data.strftime("%Y.%m.%d")
        es_t = index_es.search(index=index_name, body=body_result)
        t_result = es_t['aggregations']['NAME']['buckets']
        log_result = ""
        for row1 in t_result:
            log_result += "seg" + str(row1['key']) + " : " + str(row1['doc_count']) + ", "

        logger.debug(type + ": " + log_result)

    except Exception as e:
        print(e)
        logger.error(e)

    del_log_path = "./logs"
    for f in os.listdir(del_log_path):  # 디렉토리를 조회한다
        f = os.path.join(del_log_path, f)
        if os.path.isfile(f):  # 파일이면
            timestamp_now = datetime.now().timestamp()  # 타임스탬프(단위:초)
            # st_mtime(마지막으로 수정된 시간)기준 X일 경과 여부
            is_old = os.stat(f).st_mtime < timestamp_now - (7 * 24 * 60 * 60)
            if is_old:  # X일 경과했다면
                try:
                    os.remove(f)  # 파일을 지운다
                    print(f, 'is deleted')  # 삭제완료 로깅
                except OSError:  # Device or resource busy (다른 프로세스가 사용 중)등의 이유
                    print(f, 'can not delete')  # 삭제불가 로깅

if file_path == "":
    print("connect_info.json 파일의 경로를 입력해주세요.")
    sys.exit()

if __name__ == '__main__':
    # mysql조회
    arr_db_data = mysqlSelect(file_path)

    if len(sys.argv) == 2:
        # json 파싱 및 ES 적재
        update = ""
        makeJsonData(arr_db_data, file_path, update)
        make_log("nudge", file_path)
    else:
        update = sys.argv[2]
        makeJsonData(arr_db_data, file_path, update)
        make_log("nudge_update", file_path)
