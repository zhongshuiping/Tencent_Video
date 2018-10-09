# -*- coding: utf-8 -*-

import os
from apscheduler.schedulers.blocking import BlockingScheduler

def job():
    os.system("scrapy crawl PlayInfoDemoSpider")

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', minute='0,10,20,30,40,50', id='PlayInfoDemoSpider')
    sched.start()

if __name__ == '__main__':
    cron_job()