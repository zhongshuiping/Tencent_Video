import scrapy
from scrapy_redis.spiders import RedisSpider
import json, re
import os
import datetime
from ..items import PlayInfoItem

class PlayInfoSpider(RedisSpider):
    name = 'PlayInfoSpider'
    redis_key = 'TX_Video_PlayInfoSpider_key'
    handle_httpstatus_list = [503, 429, 402, 404, 302, 564]
    os.makedirs('logs', exist_ok=True)
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Host': 'v.qq.com',
            'Proxy-Connection': 'keep-alive',

        },
        'LOG_FILE': 'logs/PlayInfoSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 7,
        'RETRY_TIMES': 30,
        'CONCURRENT_REQUESTS': 30,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'EXTENSIONS': {'bo_lib.scrapy_tools.CloseSpiderRedis': 0},
        'CLOSE_SPIDER_AFTER_IDLE_TIMES': 3,
        'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.play_url = 'https://v.qq.com/x/cover/{cid}.html'

    def make_request_from_data(self, data):
        cid_dict = json.loads(data.decode('utf-8'))
        cid = cid_dict['cid']
        used_cid = cid_dict['used_cid']
        type_name = cid_dict['type_name']
        url = self.play_url.format(cid=used_cid)
        params = {'cid': cid,
                  'used_cid': used_cid,
                  'type_name': type_name,
                  }
        if type_name == '综艺':
            return scrapy.Request(url,
                                  callback=self.zongyi_parse,
                                  meta={'params': params},
                                  dont_filter=True)
        else:
            return scrapy.Request(url,
                                  callback=self.common_parse,
                                  meta={'params': params},
                                  dont_filter=True)

    def common_parse(self, response):
        params = response.meta['params']
        type_name = params['type_name']
        cid = params['cid']
        used_cid = params['used_cid']
        if response.status in [302, 404]: #302 404 页面无播放量 直接入库
            data = {
                'cid': cid,
                'type_name': type_name,
                'used_cid': used_cid,
                'play_count': -1,
                'positive_play_count': -1,
                'err_type': response.status,
            }
            item = PlayInfoItem()
            item['info'] = data
            yield item
            return
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('超过重试次数,url：{}，状态码：{},继续重试'.format(response.url, response.status))
            yield scrapy.Request(response.url,
                                 callback=self.common_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        '''
        try:
            data_str = re.findall(r'COVER_INFO = (\{\S+?\})\s+?var', response.text)[0]
            #album_data_dict = json.loads(data_str)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 callback=self.common_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        '''
        positive_play_count = -1
        play_count = -1
        try:positive_play_count = int(re.findall(r'"positive_view_all_count":(\d+)', response.text)[0])
        except:self.logger.warning('该专辑无正片播放量字段请check，url：{}'
                                   .format(response.url))
        try:play_count = int(re.findall(r'"view_all_count":(\d+)', response.text)[0])
        except:self.logger.warning('该专辑无播放量字段请check，url：{}'
                                   .format(response.url))
        if positive_play_count == -1 or play_count == -1:
            data = {
                'cid': cid,
                'type_name': type_name,
                'used_cid': used_cid,
                'play_count': play_count,
                'positive_play_count': positive_play_count,
                'err_type': 'no_play_count_fields',
            }
            item = PlayInfoItem()
            item['info'] = data
            yield item
        else:
            data = {
                'cid': cid,
                'type_name': type_name,
                'used_cid': used_cid,
                'play_count': play_count,
                'positive_play_count': positive_play_count,
            }
            item = PlayInfoItem()
            item['info'] = data
            yield item

    def zongyi_parse(self, response):
        params = response.meta['params']
        type_name = params['type_name']
        cid = params['cid']
        used_cid = params['used_cid']
        if response.status in [302, 404]: #302 404 页面无播放量 直接入库
            data = {
                'cid': cid,
                'type_name': type_name,
                'used_cid': used_cid,
                'play_count': -1,
                'positive_play_count': -1,
                'err_type': response.status,
            }
            item = PlayInfoItem()
            item['info'] = data
            yield item
            return
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('超过重试次数,url：{}，状态码：{},继续重试'.format(response.url, response.status))
            yield scrapy.Request(response.url,
                                 callback=self.zongyi_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        '''
        try:
            data_str = re.findall(r'COLUMN_INFO = (\{\S+?\})\s+?var', response.text)[0]
            #album_data_dict = json.loads(data_str)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 callback=self.zongyi_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        '''
        positive_play_count = -1
        play_count = -1
        try:positive_play_count = int(re.findall(r'"c_allnumc_m":(\d+)', response.text)[0])
        except:self.logger.warning('该综艺专辑无正片播放量字段请check，url：{}'
                                   .format(response.url))
        try:play_count = int(re.findall(r'"c_allnumc":(\d+)', response.text)[0])
        except:self.logger.warning('该综艺专辑无播放量字段请check，url：{}'
                                   .format(response.url))
        if positive_play_count == -1 or play_count == -1:
            data = {
                'cid': cid,
                'type_name': type_name,
                'used_cid': used_cid,
                'play_count': play_count,
                'positive_play_count': positive_play_count,
                'err_type': 'no_play_count_fields',
            }
            item = PlayInfoItem()
            item['info'] = data
            yield item
        else:
            data = {
                'cid': cid,
                'type_name': type_name,
                'used_cid': used_cid,
                'play_count': play_count,
                'positive_play_count': positive_play_count,
            }
            item = PlayInfoItem()
            item['info'] = data
            yield item




