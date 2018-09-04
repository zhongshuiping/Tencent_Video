import os
from apscheduler.schedulers.blocking import BlockingScheduler


def job():
    os.system("scrapy crawl VideoListSpider")

def cron_job():
    sched = BlockingScheduler()
    # sched.add_job(job, 'cron', hour='16', minute='30', day_of_week='tue,thu,sat', id='iqiyi_video')
    sched.add_job(job, 'cron', day_of_week='sat', hour='5', id='iqiyi_video')
    sched.start()

if __name__ == '__main__':
    job()

