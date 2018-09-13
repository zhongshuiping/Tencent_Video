# -*- coding: utf-8 -*-

import os
from apscheduler.schedulers.blocking import BlockingScheduler

def job():
    os.system("scrapy crawl VideoInfoSpider")

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', hour='14', minute='10', id='VideoInfoSpider')
    sched.start()

if __name__ == '__main__':
    cron_job()