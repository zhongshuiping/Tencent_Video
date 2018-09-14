# -*- coding: utf-8 -*-
import scrapy
from ..items import VideoListItem
from pyquery import PyQuery as pq
import datetime, json
import re, os
from ..scrapy_helper import delete_old_logs

class ZongYiCidSupSpider(scrapy.Spider):
    name = 'ZongYiCidSupSpider'
    handle_httpstatus_list = [503, 429, 302, 402]
    os.makedirs('logs', exist_ok=True)
    custom_settings = {
        'LOG_FILE': 'logs/ZongYiCidSupSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 7,
        'RETRY_TIMES': 30,
        'CONCURRENT_REQUESTS': 70,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.album_headers = {
            "Host": "v.qq.com",
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
        self.album_url = 'https://v.qq.com/detail/{cid_h}/{cid}.html'
        self.zongyi_url = 'https://list.video.qq.com/fcgi-bin/list_common_cgi?' \
                          'otype=json&novalue=1&nounion=0&platform=1&version=10000' \
                          '&intfname=web_integrated_lid_list&tid=543&appkey=ebe7ee92f568e876' \
                          '&appid=20001174&sourceid=10001&listappid=10385&listappkey=10385' \
                          '&playright=2&sourcetype=1&cidorder=1&locate_type=0&lid={cid}' \
                          '&pagesize=1&offset=0' # pagesize最大30 offset为偏移量
        delete_old_logs(self.name, 7)

    def start_requests(self):
        for i in range(1, 100000):
            cid = str(i)
            type_name = '综艺'
            album_url = self.album_url.format(cid_h=cid[0], cid=cid)
            url = self.zongyi_url.format(cid=cid)
            params = {'cid': cid,
                      'type_name': type_name,
                      'url': album_url,
                      }
            yield scrapy.Request(url,
                                  headers=self.zongyi_headers,
                                  callback=self.zongyi_parse,
                                  meta={'params': params},
                                  dont_filter=True)

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

        results = album_data_dict['jsonvalue'].get('results', [])
        if results:
            '''
            data = {
                'cid': params['cid'],
                'type_name': params['type_name'],
                'url': params['album_url'],
                'zongyi_sup': 1,
                'title': results[0]['fields']['title'],
            }
            item = VideoListItem()
            item['info'] = data
            yield item
            '''
            params['title'] = results[0]['fields']['title']
            params['zongyi_sup'] = 1
            yield scrapy.Request(params['url'],
                                 headers=self.album_headers,
                                 callback=self.album_parse,
                                 meta={'params': params},
                                 dont_filter=True)
        else:
            self.logger.debug('无效cid或僵尸专辑：{}'.format(params['cid']))

    def album_parse(self, response):
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('超过重试次数,继续重试,状态码：{},url：{}'.format(response.status, response.url))
            yield scrapy.Request(response.url,
                                 headers=self.album_headers,
                                 callback=self.album_parse,
                                 meta=response.meta,
                                 dont_filter=True)
            return
        try:
            doc = pq(response.text)
        except Exception as e:
            self.logger.warning('pq对象创建失败，url：{},失败原因：{}'.format(response.url, e))
            yield scrapy.Request(response.url,
                                 headers=self.album_headers,
                                 callback=self.album_parse,
                                 meta=response.meta,
                                 dont_filter=True)
            return
        params = response.meta['params']
        title_a = doc('.video_title_cn > a')
        if title_a:
            params['title'] = title_a.text()
        else:
            self.logger.info('页面无title标签 url:{}'.format(response.url))
        item = VideoListItem()
        item['info'] = params
        yield item





