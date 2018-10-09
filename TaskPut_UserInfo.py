# -*- coding: utf-8 -*-

from bo_lib.general.redis_helper import RedisHelper
import random
import os
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

r = RedisHelper().client
user_redis_key = 'TX_Video_UserInfoSpider_key'
combo_flag = True
COMBO_TOPID = ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20']

def get_sample():
    uid_list = []
    if not combo_flag:
        uid_list = random.sample(range(0, 10000 * 10000 * 10), 100 * 10000) # 全部随机抽取50w
    else:
        for topid in COMBO_TOPID:
            uid_list.extend(random.sample(range(int(topid) * 10000 * 10000,
                                                (int(topid)+1) * 10000 * 10000),
                                          5 * 10000))
    return uid_list

def generate_uid():
    uid_list = get_sample()
    ThreadPoolExecutor(30).map(input_onetask, uid_list)

def job():
    generate_uid()
    os.system("scrapy crawl UserInfoSpider")

def input_onetask(uid):
    r.rpush(user_redis_key, uid)

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', day_of_week='sun', hour='0', id='TX_Video_UserInfoSpider')
    sched.start()

if __name__ == '__main__':
    generate_uid()