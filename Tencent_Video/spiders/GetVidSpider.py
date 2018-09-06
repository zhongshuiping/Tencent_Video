# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider
from ..items import VidItem
import datetime, json
import re, os

class GetVidSpider(RedisSpider):
    name = 'GetVidSpider'
    redis_key = 'TX_Video_GetVidSpider_key'
    handle_httpstatus_list = [503, 429, 302, 402]
    os.makedirs('logs', exist_ok=True)
    custom_settings = {
        'LOG_FILE': 'logs/GetVidSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 7,
        'RETRY_TIMES': 30,
        'CONCURRENT_REQUESTS': 50,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'EXTENSIONS': {'bo_lib.scrapy_tools.CloseSpiderRedis': 0},
        'CLOSE_SPIDER_AFTER_IDLE_TIMES': 3,
        'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.common_headers = {
            "Host": "node.video.qq.com",
            "Proxy-Connection": "keep-alive",
            "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        }
        self.zongyi_headers = {
            "Host": "list.video.qq.com",
            "Proxy-Connection": "keep-alive",
            "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        }
        self.common_url = 'https://node.video.qq.com/x/api/float_vinfo2?cid={cid}'
        self.zongyi_url = 'https://list.video.qq.com/fcgi-bin/list_common_cgi?' \
                          'otype=json&novalue=1&nounion=0&platform=1&version=10000' \
                          '&intfname=web_integrated_lid_list&tid=543&appkey=ebe7ee92f568e876' \
                          '&appid=20001174&sourceid=10001&listappid=10385&listappkey=10385' \
                          '&playright=2&sourcetype=1&cidorder=1&locate_type=0&lid={cid}' \
                          '&pagesize=30&offset={offset}' # pagesize最大30 offset为偏移量

    def make_request_from_data(self, data):
        cid_dict = json.loads(data.decode('utf-8'))
        cid = cid_dict['cid']
        type_name = cid_dict['type_name']
        album_url = cid_dict['url']
        if type_name == '综艺':
            url = self.zongyi_url.format(cid=cid, offset=0)
            params = {'cid': cid,
                      'type_name': type_name,
                      'album_url': album_url,
                      'offset': 0,
                      'vids': [],
                      }
            return scrapy.Request(url,
                                  headers=self.zongyi_headers,
                                  callback=self.zongyi_parse,
                                  meta={'params': params},
                                  dont_filter=True)
        else:
            url = self.common_url.format(cid=cid)
            params = {'cid': cid,
                      'type_name': type_name,
                      'album_url': album_url,
                      }
            return scrapy.Request(url,
                                  headers=self.common_headers,
                                  callback=self.common_parse,
                                  meta={'params': params},
                                  dont_filter=True)

    def common_parse(self, response):
        params = response.meta['params']
        type_name = params['type_name']
        cid = params['cid']
        album_url = params['album_url']
        if response.status in self.handle_httpstatus_list:
            yield scrapy.Request(response.url,
                                 headers=self.common_headers,
                                 callback=self.common_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        try:
            album_data_dict = json.loads(response.text)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 headers=self.common_headers,
                                 callback=self.common_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        c_dict = album_data_dict.get('c', {})
        vid_list = c_dict.get('video_ids', [])
        if not vid_list:
            data = {
                'cid': cid,
                'type_name': type_name,
                'album_url': album_url,
                'get_vid_url': response.url,
                'flag': 2,
            }
            item = VidItem()
            item['info'] = data
            yield item
        else:
            data = {
                'cid': cid,
                'type_name': type_name,
                'album_url': album_url,
                'get_vid_url': response.url,
                'flag': 1,
                'vids': vid_list,
            }
            item = VidItem()
            item['info'] = data
            yield item

    def zongyi_parse(self, response):
        params = response.meta['params']
        if response.status in self.handle_httpstatus_list:
            yield scrapy.Request(response.url,
                                 headers=self.zongyi_headers,
                                 callback=self.zongyi_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        try:
            data_str = re.findall(r'QZOutputJson=(\{[\s\S]+\})', response.text)[0]
            album_data_dict = json.loads(data_str)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 headers=self.zongyi_headers,
                                 callback=self.zongyi_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return

        #vid_total = album_data_dict['total']
        vid_list = [each['id'] for each in album_data_dict['jsonvalue'].get('results', [])]
        if not vid_list:
            if not params['vids']:
                data = {
                    'cid': params['cid'],
                    'type_name': params['type_name'],
                    'album_url': params['album_url'],
                    'get_vid_url': response.url,
                    'flag': 2,
                }
                item = VidItem()
                item['info'] = data
                yield item
            else:
                data = {
                    'cid': params['cid'],
                    'type_name': params['type_name'],
                    'album_url': params['album_url'],
                    'get_vid_url': response.url,
                    'flag': 1,
                    'vids': list(set(params['vids'])),
                }
                item = VidItem()
                item['info'] = data
                yield item
        else:
            params['offset'] += 30
            url = self.zongyi_url.format(cid=params['cid'], offset=params['offset'])
            params['vids'].extend(vid_list)
            yield scrapy.Request(url,
                                 headers=self.zongyi_headers,
                                 callback=self.zongyi_parse,
                                 meta={'params': params},
                                 dont_filter=True)





