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
        self.video_info_coll = self.client.get_collection(collection_name='video_info', database_name='TX_Video')
        self.history_video_list_coll = self.client.get_collection(collection_name='history_video_list', database_name='TX_Video')
        self.cid_vid_coll = self.client.get_collection(collection_name='cid_vid', database_name='TX_Video')
        self.play_info_coll = self.client.get_collection(collection_name='play_info', database_name='TX_Video')
        self.comment_info_coll = self.client.get_collection(collection_name='comment_info', database_name='TX_Video')

    def process_item(self, item, spider):
        info = item['info']
        info['ts'] = datetime.datetime.utcnow()
        info['ts_string'] = str(datetime.date.today())

        if isinstance(item, VideoListItem):
            self.process_video_list(info)
        elif isinstance(item, VidItem):
            self.process_cid_vid(info)
        elif isinstance(item, PlayInfoItem):
            self.process_play_info(info)
        elif isinstance(item, CommentInfoItem):
            self.process_comment_info(info)
        elif isinstance(item, VideoInfoItem):
            self.process_video_info(info)

        return item

    def process_video_list(self, info):
        self.video_list_coll.update_one({'cid': info['cid']}, {'$set': info}, upsert=True)
        self.history_video_list_coll.insert_one(info)

    def process_video_info(self, info):
        self.video_info_coll.update_one({'unique_id': info['unique_id']}, {'$set': info}, upsert=True)

    def process_play_info(self, info):
        self.play_info_coll.insert_one(info)

    def process_comment_info(self, info):
        self.comment_info_coll.insert_one(info)

    def process_cid_vid(self, info):
        cid = info.pop('cid')
        flag = info['flag']
        if flag == 1:
            vids = info['vids']
            vids_coll_dict = self.cid_vid_coll.find_one({'cid': cid}, {'_id': 0, 'vids': 1})
            if vids_coll_dict:
                info['vids'] = list(set(vids) | set(vids_coll_dict['vids']))
            self.cid_vid_coll.update_one({'cid': cid}, {'$set': info}, upsert=True)
        elif flag == 2:
            self.cid_vid_coll.update_one({'cid': cid}, {'$set': info}, upsert=True)

    def open_spider(self, spider):
        try:BONotifier().msg('Tencent_Video {} opened'.format(spider.name), '@kang')
        except:pass

    def close_spider(self, spider):
        try:BONotifier().msg('Tencent_Video {} closed'.format(spider.name), '@kang')
        except:pass
