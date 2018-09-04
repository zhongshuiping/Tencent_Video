# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from bo_lib.general.mongodb_helper import MongoDBHelper
import datetime, hashlib
from .items import *
from bo_lib.general.slack_notifier import BONotifier

class TencentVideoPipeline(object):
    def __init__(self):
        self.client = MongoDBHelper()
        self.video_list_coll = self.client.get_collection(collection_name='video_list', database_name='TX_Video')
        self.history_video_list_coll = self.client.get_collection(collection_name='history_video_list', database_name='TX_Video')

    def process_item(self, item, spider):
        info = item['info']
        info['ts'] = datetime.datetime.utcnow()
        info['ts_string'] = str(datetime.date.today())

        if isinstance(item, VideoListItem):
            self.process_video_list(item)




        return item

    def process_video_list(self, item):
        video_info = item['info']
        self.video_list_coll.update_one({'cid': video_info['cid']}, {'$set': video_info}, upsert=True)
        self.history_video_list_coll.insert_one(video_info)


    def open_spider(self, spider):
        try:BONotifier().msg('Tencent_Video {} opened'.format(spider.name), '@kang')
        except:pass

    def close_spider(self, spider):
        try:BONotifier().msg('Tencent_Video {} closed'.format(spider.name), '@kang')
        except:pass
