from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json
import traceback
# nudge_data = datetime.now() + timedelta(days=1)
# nudge_date_2 = nudge_data.strftime("%Y%m%d" + '000000')
nudge_date = datetime.now() + timedelta(days=1)
nudge_date = nudge_date.strftime("%Y%m%d" + '000000')


def makeJsonData(arr_data, connect_info, update):
    with open(connect_info, 'r') as f:
        connect_info = json.load(f)

    index_name = "index-nudge-policy"

    es = Elasticsearch(['1.255.145.176',
                        '1.255.145.177',
                        '1.255.145.178'], port=9200)  # 환경에 맞게 바꿀 것
    es.info()

    try:
        if len(arr_data) > 0:
            today = datetime.today().strftime("%Y.%m.%d")
            qry = """
            {
              "query": {
                  "bool": {
                      "filter": [
                          {"term": {"log_time": "%s"}},
                          {"term": {"stb_ver": "v524"}}
                      ]
                  }
              }
            }  
            """
            es.delete_by_query(index=index_name, body=qry % today)
        else:
            pass
    except Exception as err:
        print(err)

    try:

        for data in arr_data:

            # DB json 데이터 처리
            if data['manual_text']:
                data['manual_text'] = json.loads(data['manual_text'])
            if data['ext_info']:
                data['ext_info'] = json.loads(data['ext_info'])

            # 데이터 생성시간
            data['@timestamp'] = datetime.today()

            data['log_time'] = datetime.today().strftime("%Y.%m.%d")

            data['nudge_date'] = nudge_date

            # mapping type
            # synop : vod 시놉랜딩
            # app : app 런칭
            # voice_search : 음성검색결과 half
            # voice_nugu : NUGU 음성 명령 구동
            # channel : 채널 이동
            # home : 홈UI 메뉴 이동
            # popup : 팝업 브라우징

            data['icon_type'] = data.pop("icon_id")
            data['stb_ver'] = 'v524'
            if data['menu_id'] != "":
                a = data['menu_id'].replace("menu", "")
                menus_nudge_date = a
                data['menu_nudge_date'] = str(int(nudge_date) + int(menus_nudge_date))
            elif data['menu_id'] == "":
                data['menu_nudge_date'] = 0

            data['senior_nudge_date'] = str(int(nudge_date) + 17)
            data['zapping_nudge_date'] = str(int(nudge_date) + 18)

            if data['manual_text'] is None:
                data['manual_text'] = ""

            # manual_text 배열이 2개이상일 경우 \n 처리
            if len(data['manual_text']) >= 2:
                data['manual_text'] = "\n".join(data['manual_text'])
            else:
                data['manual_text'] = "".join(data['manual_text'])

            if data['wait_time'] is None:
                data['wait_time'] = 0

            if data['ext_info'] is None:
                data['ext_info'] = []


            es.index(index=index_name, body=data)

        es.indices.refresh(index=index_name)
    except Exception as err:
        print(traceback.format_exc())
        print("!!!", err)

