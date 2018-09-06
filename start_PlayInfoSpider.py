# -*- coding: utf-8 -*-

import os
from apscheduler.schedulers.blocking import BlockingScheduler

def job():
    os.system("scrapy crawl PlayInfoSpider")

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', day_of_week='sat', hour='13', minute='10', id='startGetTvidSpider')
    sched.start()

if __name__ == '__main__':
    job()