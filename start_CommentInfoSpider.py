# -*- coding: utf-8 -*-

import os
from apscheduler.schedulers.blocking import BlockingScheduler

def job():
    os.system("scrapy crawl CommentInfoSpider")

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', hour='15', minute='30', id='CommentInfoSpider')
    sched.start()

if __name__ == '__main__':
    cron_job()