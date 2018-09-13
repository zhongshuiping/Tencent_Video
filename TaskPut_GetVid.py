# -*- coding: utf-8 -*-
from bo_lib.general.redis_helper import RedisHelper
from bo_lib.general.mongodb_helper import MongoDBHelper
from bo_lib.general.slack_notifier import BONotifier
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from Tencent_Video.scrapy_helper import delete_old_logs
import json, datetime

r = RedisHelper().client

GetVidSpider_key = 'TX_Video_GetVidSpider_key'
GetVidSpider = 'GetVidSpider'
video_list_coll = MongoDBHelper().get_collection(collection_name='video_list', database_name='TX_Video')

def task_put():
    delete_old_logs(GetVidSpider, 5)
    cid_cursor = video_list_coll.find({},
                                           {'_id': 0, 'url': 1, 'cid': 1, 'type_name': 1})
    cid_record_list = [x for x in cid_cursor]
    cid_cursor.close()
    try:
        BONotifier().msg('GetVidSpider: 所有栏目list=={}'.format(len(cid_record_list)), '@kang')
    except:
        pass

    ThreadPoolExecutor(10).map(input_onetask, cid_record_list)

def input_onetask(cid_dict):
    data_str = json.dumps(cid_dict)
    r.rpush(GetVidSpider_key, data_str)

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(task_put, 'cron', hour='12', id='TaskPut_GetVid')
    sched.start()
if __name__ == '__main__':
    cron_job()
