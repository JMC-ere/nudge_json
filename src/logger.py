import logging
import logging.handlers
from datetime import datetime
from elasticsearch import Elasticsearch
import os
import json


def make_log(type, connect_info):

    with open(connect_info, 'r') as f:
        connect_info = json.load(f)

    log_path = "./logs/" + datetime.today().strftime("%Y.%m.%d") + '.log'
    logger = logging.getLogger(__name__)
    fileHandler = logging.FileHandler(log_path)
    formatter = logging.Formatter('[%(asctime)s][%(filename)s:%(lineno)s] >> %(message)s')
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    logger.setLevel(level=logging.DEBUG)
    try:
        index_es = Elasticsearch(connect_info['elastic']['url'])  # 환경에 맞게 바꿀 것
        index_es.info()
        index_name = "index-nudge-policy"

        body = """
                {
              "size" : 0,
              "query": {
                "match": {
                  "log_time": "%s"
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

        body_result = body % datetime.today().strftime("%Y.%m.%d")
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


