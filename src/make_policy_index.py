from es_connect_index_seg_data import makeJsonData
from maria_db_connect import mysqlSelect
from logger import make_log
from telegram_send import telegram_monitor

if __name__ == '__main__':

    file_path = './config/connect_info.json'

    # mysql조회
    arr_db_data = mysqlSelect(file_path)

    # json 파싱 및 ES 적재
    update = ""
    makeJsonData(arr_db_data, file_path, update)
    make_log("nudge", file_path)

    telegram_monitor(file_path)





