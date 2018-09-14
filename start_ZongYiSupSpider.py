import os
from apscheduler.schedulers.blocking import BlockingScheduler


def job():
    os.system("scrapy crawl ZongYiCidSupSpider")

def cron_job():
    sched = BlockingScheduler()
    sched.add_job(job, 'cron', hour='7', id='ZongYiCidSupSpider')
    sched.start()

if __name__ == '__main__':
    cron_job()

