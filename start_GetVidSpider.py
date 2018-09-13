# -*- coding: utf-8 -*-

import os
from apscheduler.schedulers.blocking import BlockingScheduler

def job():
    os.system("scrapy crawl GetVidSpider")

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', hour='12', minute='10', id='GetVidSpider')
    sched.start()

if __name__ == '__main__':
    cron_job()