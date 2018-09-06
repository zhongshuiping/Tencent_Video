# -*- coding: utf-8 -*-
from bo_lib.general.redis_helper import RedisHelper
from bo_lib.general.mongodb_helper import MongoDBHelper
from bo_lib.general.slack_notifier import BONotifier
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from Tencent_Video.scrapy_helper import delete_old_logs
import json

r = RedisHelper().client

PlayInfoSpider_key = 'TX_Video_PlayInfoSpider_key'
PlayInfoSpider = 'PlayInfoSpider'
cid_vid_coll = MongoDBHelper().get_collection(collection_name='cid_vid', database_name='TX_Video')

def task_put():
    delete_old_logs(PlayInfoSpider, 5)
    cid_cursor = cid_vid_coll.find({},
                                           {'_id': 0, 'vids': 1, 'cid': 1, 'type_name': 1})
    unhandle_cid_list = [x for x in cid_cursor]
    cid_cursor.close()
    cid_record_list = []
    for unhandle_cid_dict in unhandle_cid_list:
        type_name = unhandle_cid_dict['type_name']
        cid = unhandle_cid_dict['cid']
        if type_name != '综艺':
            used_cid = cid
            data = {
                'cid': cid,
                'used_cid': used_cid,
                'type_name': type_name,
            }
            cid_record_list.append(data)
        else:
            if unhandle_cid_dict.get('vids', []):
                used_cid = unhandle_cid_dict['vids'][0]
                data = {
                    'cid': cid,
                    'used_cid': used_cid,
                    'type_name': type_name,
                }
                cid_record_list.append(data)
    try:
        BONotifier().msg('PlayInfoSpider: 所有栏目list=={}'.format(len(cid_record_list)), '@kang')
    except:
        pass

    ThreadPoolExecutor(10).map(input_onetask, cid_record_list)

def input_onetask(cid_dict):
    data_str = json.dumps(cid_dict)
    r.rpush(PlayInfoSpider_key, data_str)

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(task_put, 'cron', day_of_week='sat', hour='13', id='GetTvidSpider')
    #sched.add_job(task_put, 'cron', hour='14', minute='45', id='GetTvidSpider')
    sched.start()
if __name__ == '__main__':
    task_put()
