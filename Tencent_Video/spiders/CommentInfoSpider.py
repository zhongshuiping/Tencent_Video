# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider
from ..items import CommentInfoItem
import datetime, json
import re, os

class CommentInfoSpider(RedisSpider):
    name = 'CommentInfoSpider'
    redis_key = 'TX_Video_CommentInfoSpider_key'
    handle_httpstatus_list = [503, 429, 402]
    os.makedirs('logs', exist_ok=True)
    custom_settings = {
        'LOG_FILE': 'logs/CommentInfoSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 30,
        'CONCURRENT_REQUESTS': 50,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'EXTENSIONS': {'bo_lib.scrapy_tools.CloseSpiderRedis': 0},
        'CLOSE_SPIDER_AFTER_IDLE_TIMES': 5,
        'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.get_comment_id_headers = {
            "Host": "ncgi.video.qq.com",
            "Proxy-Connection": "keep-alive",
            "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        }
        self.commentnum_headers = {
            "Host": "coral.qq.com",
            "Proxy-Connection": "keep-alive",
            "User-Agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        }
        self.special_type_list = ['电影', '综艺']
        self.get_cid_comment_id_url = 'https://ncgi.video.qq.com/fcgi-bin/video_comment_id?otype=json&op=3&cid={cid}'
        self.get_vid_comment_id_url = 'https://ncgi.video.qq.com/fcgi-bin/video_comment_id?otype=json&op=3&vid={vid}'
        #self.get_columnid_comment_id_url = 'https://ncgi.video.qq.com/fcgi-bin/video_comment_id?otype=json&op=3&column={cid}'
        self.commentnum_url = 'https://coral.qq.com/article/{comment_id}/commentnum'

    def make_request_from_data(self, data):
        cid_dict = json.loads(data.decode('utf-8'))
        cid = cid_dict['cid']
        used_id = cid_dict['used_id']
        type_name = cid_dict['type_name']

        if type_name in self.special_type_list:
            url = self.get_cid_comment_id_url.format(cid=used_id)
            params = {'cid': cid,
                      'used_id': used_id,
                      'type_name': type_name,
                      }
            return scrapy.Request(url,
                                  headers=self.get_comment_id_headers,
                                  callback=self.parse,
                                  meta={'params': params},
                                  dont_filter=True)
        else:
            url = self.get_vid_comment_id_url.format(cid=used_id)
            params = {'cid': cid,
                      'used_id': used_id,
                      'type_name': type_name,
                      }
            return scrapy.Request(url,
                                  headers=self.get_comment_id_headers,
                                  callback=self.parse,
                                  meta={'params': params},
                                  dont_filter=True)

    def parse(self, response):
        params = response.meta['params']
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('超过重试次数,url：{}，状态码：{},继续重试'.format(response.url, response.status))
            yield scrapy.Request(response.url,
                                  headers=self.get_comment_id_headers,
                                  callback=self.parse,
                                  meta={'params': params},
                                  dont_filter=True)
            return
        try:
            data_str = re.findall(r'QZOutputJson=(\{[\s\S]+\})', response.text)[0]
            comment_id_dict = json.loads(data_str)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 headers=self.get_comment_id_headers,
                                 callback=self.parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        comment_id = comment_id_dict['comment_id']
        srcid = comment_id_dict['srcid']
        url = self.commentnum_url.format(comment_id=comment_id)
        params['comment_id'] = comment_id
        params['srcid'] = srcid
        yield scrapy.Request(url,
                             headers=self.commentnum_headers,
                             callback=self.commentnum_parse,
                             meta={'params': params},
                             dont_filter=True)

    def commentnum_parse(self, response):
        params = response.meta['params']
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('超过重试次数,url：{}，状态码：{},继续重试'.format(response.url, response.status))
            yield scrapy.Request(response.url,
                                 headers=self.commentnum_headers,
                                 callback=self.commentnum_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        try:
            comment_num_dict = json.loads(response.text)
        except Exception as e:
            self.logger.warning('json串解析出错，重试：{}, {}, {}'.format(e, response.status, response.url))
            yield scrapy.Request(response.url,
                                 headers=self.commentnum_headers,
                                 callback=self.commentnum_parse,
                                 meta={'params': params},
                                 dont_filter=True)
            return
        comment_num = comment_num_dict['commentnum']
        try:comment_num = int(comment_num)
        except:self.logger.warning('评论量字段为非数字，url：{}，comment_num：{}'.format(response.url, comment_num))
        params['comment_num'] = comment_num
        item = CommentInfoItem()
        item['info'] = params
        yield item






