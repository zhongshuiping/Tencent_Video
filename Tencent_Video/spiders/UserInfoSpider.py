# -*- coding: utf-8 -*-
"""
# 目的： 随机生成用户id， 抓取用户信息，计算付费用户

"""

import scrapy
from scrapy_redis.spiders import RedisSpider
from ..items import UserInfoItem
import json
import os
import datetime
from ..scrapy_helper import delete_old_logs


class UserInfoSpider(RedisSpider):
    name = 'UserInfoSpider'
    redis_key = 'TX_Video_UserInfoSpider_key'
    os.makedirs('logs', exist_ok=True)
    handle_httpstatus_list = [503, 429, 302, 402]

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
            'Host': 'video.coral.qq.com',
            'Proxy-Connection': 'keep-alive',

        },
        'LOG_FILE': 'logs/UserInfoSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 4,
        'RETRY_TIMES': 30,
        'CONCURRENT_REQUESTS': 30,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'EXTENSIONS': {'bo_lib.scrapy_tools.CloseSpiderRedis': 0},
        'CLOSE_SPIDER_AFTER_IDLE_TIMES': 3,
        'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.user_info_url = 'http://video.coral.qq.com/user/{userid}'
        delete_old_logs(self.name, 7)

    def make_request_from_data(self, data):
        userid = data.decode('utf-8')
        url = self.user_info_url.format(userid=userid)
        params = {'userid': userid}

        return scrapy.Request(url,
                              callback=self.parse,
                              meta={'params': params},
                              dont_filter=True)

    def parse(self, response):
        params = response.meta['params']
        userid = params['userid']
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('超过重试次数,url：{}，状态码：{},继续重试'.format(response.url, response.status))
            yield scrapy.Request(response.url,
                                 callback=self.parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        try:
            user_data_dict = json.loads(response.text)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 callback=self.parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        user_info_dict = user_data_dict.get('data')
        if user_info_dict:
            data = {
                'userid': userid,
                'user_info': user_info_dict,
            }
        else:
            data = {
                'userid': userid,
                'no_exists': True,
            }

        item = UserInfoItem()
        item['info'] = data
        yield item

