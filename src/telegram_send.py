import json
import telegram
from datetime import datetime
from elasticsearch import Elasticsearch


def telegram_monitor(connect_info) :
    with open(connect_info, 'r') as f:
        connect_info = json.load(f)

        index_es = Elasticsearch(connect_info['elastic']['url'])  # 환경에 맞게 바꿀 것
        index_es.info()

        index_name = "index-nudge-policy"

        bot = telegram.Bot(token="1049808110:AAGUYRvxgZLYNcmQFn3p8yO9VSqzQyPavls")

        er_message = index_name + "-" + datetime.today().strftime("%Y.%m.%d[%T]") + "\n"

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

        try:
            es_t = index_es.search(index=index_name, body=body_result)
            t_result = es_t['aggregations']['NAME']['buckets']
            message = er_message
            for row1 in t_result:
                message += "개발Seg" + str(row1['key']) + " : " + str(row1['doc_count']) + "건\n"

            bot.sendMessage(chat_id='1228894509', text=message)
            # bot.sendMessage(chat_id='976803858', text=message)
            # bot.sendMessage(chat_id='1070666335', text=message)

        except Exception as e:
            bot.sendMessage(chat_id='1228894509', text=er_message + " : " + str(e))
            # bot.sendMessage(chat_id='976803858', text=er_message + " : " + str(e))
            # bot.sendMessage(chat_id='1070666335', text=er_message + " : " + str(e))
