# -*- coding: utf-8 -*-
from bo_lib.general.redis_helper import RedisHelper
from bo_lib.general.mongodb_helper import MongoDBHelper
from bo_lib.general.slack_notifier import BONotifier
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from Tencent_Video.scrapy_helper import delete_old_logs
import json

r = RedisHelper().client

VideoInfoSpider_key = 'TX_Video_VideoInfoSpider_key'
VideoInfoSpider = 'VideoInfoSpider'
cid_vid_coll = MongoDBHelper().get_collection(collection_name='cid_vid', database_name='TX_Video')
video_info_coll = MongoDBHelper().get_collection(collection_name='video_info', database_name='TX_Video')

def task_put():
    delete_old_logs(VideoInfoSpider, 5)
    cid_cursor = cid_vid_coll.find({},
                                           {'_id': 0, 'vids': 1, 'cid': 1, 'type_name': 1})
    unhandle_cid_list = [x for x in cid_cursor]
    cid_cursor.close()
    unique_id_cursor = video_info_coll.find({}, {'_id': 0, 'unique_id': 1})
    unique_id_set = set([x['unique_id'] for x in unique_id_cursor])
    unique_id_cursor.close()
    cid_record_list = []
    for unhandle_cid_dict in unhandle_cid_list:
        type_name = unhandle_cid_dict['type_name']
        cid = unhandle_cid_dict['cid']
        if unhandle_cid_dict.get('vids', []):
            for vid in unhandle_cid_dict['vids']:
                unique_id = cid + '_' + vid
                if unique_id in unique_id_set:
                    continue
                data = {
                    'cid': cid,
                    'vid': vid,
                    'type_name': type_name,
                }
                cid_record_list.append(data)
    try:
        BONotifier().msg('VideoInfoSpider: 所有栏目list=={}'.format(len(cid_record_list)), '@kang')
    except:
        pass

    ThreadPoolExecutor(10).map(input_onetask, cid_record_list)

def input_onetask(cid_dict):
    data_str = json.dumps(cid_dict)
    r.rpush(VideoInfoSpider_key, data_str)

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(task_put, 'cron', hour='14', id='TaskPut_VideoInfo')
    sched.start()
if __name__ == '__main__':
    task_put()
