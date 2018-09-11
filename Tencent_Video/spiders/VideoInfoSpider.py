import scrapy
from scrapy_redis.spiders import RedisSpider
import json, re
import os
import datetime
from ..items import VideoInfoItem

class VideoInfoSpider(RedisSpider):
    name = 'VideoInfoSpider'
    redis_key = 'TX_Video_VideoInfoSpider_key'
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
        'LOG_FILE': 'logs/VideoInfoSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 7,
        'RETRY_TIMES': 30,
        'CONCURRENT_REQUESTS': 40,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'EXTENSIONS': {'bo_lib.scrapy_tools.CloseSpiderRedis': 0},
        'CLOSE_SPIDER_AFTER_IDLE_TIMES': 3,
        'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.album_play_url = 'https://v.qq.com/x/cover/{cid}.html'
        self.video_play_url = 'https://v.qq.com/x/cover/{cid}/{vid}.html'

    def make_request_from_data(self, data):
        cid_dict = json.loads(data.decode('utf-8'))
        cid = cid_dict['cid']
        vid = cid_dict['vid']
        type_name = cid_dict['type_name']
        params = {'cid': cid,
                  'vid': vid,
                  'type_name': type_name,
                  }
        if type_name == '综艺':
            url = self.album_play_url.format(cid=vid)
        else:
            url = self.video_play_url.format(cid=cid, vid=vid)
        return scrapy.Request(url,
                                  callback=self.parse,
                                  meta={'params': params},
                                  dont_filter=True)

    def parse(self, response):
        params = response.meta['params']
        type_name = params['type_name']
        cid = params['cid']
        vid = params['vid']
        if response.status in [302, 404]: # 302 404 页面无播放量 直接入库
            data = {
                'cid': cid,
                'type_name': type_name,
                'vid': vid,
                'online_status': 0,
                'err_type': response.status,
                'unique_id': cid + '_' + vid,
            }
            item = VideoInfoItem()
            item['info'] = data
            yield item
            return
        if response.status in self.handle_httpstatus_list: # 非[302, 404]继续重试
            self.logger.warning('超过重试次数,url：{}，状态码：{},继续重试'.format(response.url, response.status))
            yield scrapy.Request(response.url,
                                 callback=self.parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return

        try:
            data_str = re.findall(r'VIDEO_INFO = (\{.*\})\s', response.text)[0] # 正则匹配任意个字符贪婪匹配
            video_info_dict = json.loads(data_str)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}, \n{}'
                                .format(e, response.status, response.url, data_str))
            yield scrapy.Request(response.url,
                                 callback=self.parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        data = {
            'cid': cid,
            'type_name': type_name,
            'vid': vid,
            'online_status': 1,
            'video_info': video_info_dict,
            'unique_id': cid + '_' + vid,
        }
        item = VideoInfoItem()
        item['info'] = data
        yield item





