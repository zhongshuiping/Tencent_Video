import scrapy, os
import datetime, re
from pyquery import PyQuery as pq
from ..items import VideoListItem
from ..scrapy_helper import delete_old_logs

class VideoListSpider(scrapy.Spider):
    name = 'VideoListSpider'
    handle_httpstatus_list = [503, 429, 302, 402]

    os.makedirs('logs', exist_ok=True)

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent':
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Host": 'v.qq.com',
            "Proxy-Connection": "keep-alive",

        },
        'CONCURRENT_REQUESTS': 70,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'CONCURRENT_REQUESTS_PER_IP': 0,
        'DOWNLOAD_DELAY': 0,
        'DOWNLOAD_TIMEOUT': 7,
        'LOG_FILE': 'logs/VideoListSpider_' + str(datetime.datetime.now()) + '.log',
        'REDIRECT_ENABLED': False,  # 禁止跳转
        'RETRY_TIMES': 40,
        # 'DOWNLOADER_MIDDLEWARES': {'bo_lib.scrapy_tools.BOProxyMiddlewareVPS': 740},
    }

    def __init__(self):
        self.target_channel_name = ['电视剧', '综艺', '动漫', '少儿', '纪录片']
        self.crawl_all_channel = ['电影', '微电影']
        self.filter = ['全部']
        self.judge_more_filter_thr = 5000
        self.host = 'https://v.qq.com'
        delete_old_logs(self.name, 3)

    def start_requests(self):
        start_url = 'http://v.qq.com/x/list/movie'
        yield scrapy.Request(start_url,
                             callback=self.parse,
                             dont_filter=True)

    def parse(self, response):
        if response.status in self.handle_httpstatus_list:
            self.logger.warning('起始页超过重试次数,状态码：{},继续重试'.format(response.status))
            yield scrapy.Request(response.url,
                                 callback=self.parse,
                                 dont_filter=True)
            return
        try:
            doc = pq(response.text)
        except Exception as e:
            self.logger.warning('pq对象创建失败，url：{},失败原因：{}'.format(response.url, e))
            yield scrapy.Request(response.url,
                                 callback=self.parse,
                                 dont_filter=True)
            return
        for each in doc('.mod_filter a').items():
            type_name = each.text()
            if 'http' not in each.attr.href:
                each.attr.href = self.host + each.attr.href
            parms = {'type_name': type_name,
                     'channel_url': each.attr.href,
                     'route': [type_name]}
            if type_name in self.target_channel_name:
                yield scrapy.Request(each.attr.href,
                                     callback=self.final_list_page,
                                     meta={'parms': parms},
                                     dont_filter=True)
            if type_name in self.crawl_all_channel:
                yield scrapy.Request(each.attr.href,
                                     callback=self.parse_crawl_all,
                                     meta={'parms': parms},
                                     dont_filter=True)

    def parse_crawl_all(self, response):
        try:
            doc = pq(response.text)
        except Exception as e:
            self.logger.warning('pq对象创建失败，url：{},状态码：{},失败原因：{}'.format(response.url, response.status, e))
            yield scrapy.Request(response.url,
                                 callback=self.parse_crawl_all,
                                 meta=response.meta,
                                 dont_filter=True)
            return
        parms = response.meta['parms']
        self.judge_more_filter(response)
        for each in doc('.filter_line a').items():
            if each.text() in self.filter:
                continue
            next_parms = parms
            next_parms['route'] = [parms['type_name'], each.text()]
            if 'http' not in each.attr.href:
                each.attr.href = parms['channel_url'] + each.attr.href
            yield scrapy.Request(each.attr.href,
                                 callback=self.judge_more_filter,
                                 meta={'parms': next_parms},
                                 dont_filter=True)

    def judge_more_filter(self, response):
        try:
            doc = pq(response.text)
        except Exception as e:
            self.logger.warning('pq对象创建失败，url：{},状态码：{},失败原因：{}'.format(response.url, response.status, e))
            yield scrapy.Request(response.url,
                                 callback=self.judge_more_filter,
                                 meta=response.meta,
                                 dont_filter=True)
            return
        parms = response.meta['parms']
        if int(list(doc('.option_txt > em').items())[0].text()) >= self.judge_more_filter_thr:
            for each in doc('.filter_tabs a').items():
                self.logger.info('route:{}==>{}'.format(parms['route'], each.text()))
                if 'http' not in each.attr.href:
                    each.attr.href = parms['channel_url'] + each.attr.href
                yield scrapy.Request(each.attr.href,
                                     callback=self.final_list_page,
                                     meta={'parms': parms},
                                     dont_filter=True)
        else:
            self.logger.info('route:{}'.format(parms['route']))
            self.final_list_page(response)

    def final_list_page(self, response):
        try:
            doc = pq(response.text)
        except Exception as e:
            self.logger.warning('pq对象创建失败，url：{},状态码：{},失败原因：{}'.format(response.url, response.status, e))
            yield scrapy.Request(response.url,
                                 callback=self.final_list_page,
                                 meta=response.meta,
                                 dont_filter=True)
            return
        parms = response.meta['parms']
        for each in doc('.page_next').items():
            if 'javascript' in each.attr.href:
                continue
            else:
                if 'http' not in each.attr.href:
                    each.attr.href = parms['channel_url'] + each.attr.href
                yield scrapy.Request(each.attr.href,
                                     callback=self.final_list_page,
                                     meta=response.meta,
                                     dont_filter=True)
        item_l = list(doc('.list_item').items())
        for item in item_l:
            data = {}
            a = item('a[data-float]')
            data['title'] = a('img').attr['alt']  # ''.join([em.attr['alt'] for em in a('img').items()])
            data['url'] = a.attr.href
            data['cid'] = a.attr['data-float']  # re.findall(r'cover/(.+?)\.html',data['url'])[0]
            data['score'] = ''.join([em.text() for em in item('.figure_score > em').items()])
            data['type_name'] = parms['type_name']
            data['v_info'] = item('.figure_caption_score > span').text()
            data['request_url'] = response.url
            if item('a>.mark_v'):
                alt_str = item('a>.mark_v>img').attr['alt']
                if alt_str == '腾讯出品':
                    data['video_self'] = 1
                elif alt_str == 'VIP':
                    data['video_vip'] = 1
                elif alt_str == '独播':
                    data['video_db'] = 1
                elif alt_str == '预告片':
                    data['video_yg'] = 1
                elif alt_str == 'VIP用券':
                    data['video_voucher'] = 1
                elif alt_str == '限免':
                    data['video_xm'] = 1
                elif alt_str == '付费':
                    data['video_paid'] = 1
                else:
                    self.logger.info('未知类别的alt：{},request_url: {}'.format(alt_str, data['request_url']))
            data['video_self'] = data.get('video_self', 0)
            data['video_vip'] = data.get('video_vip', 0)
            data['video_db'] = data.get('video_db', 0)
            data['video_yg'] = data.get('video_yg', 0)
            data['video_voucher'] = data.get('video_voucher', 0)
            data['video_xm'] = data.get('video_xm', 0)
            data['video_paid'] = data.get('video_paid', 0)
            if not data['type_name'] == '综艺':
                item_ = VideoListItem()
                item_['info'] = data
                yield item_
            else:
                next_parms = data
                yield scrapy.Request(data['url'],
                                     callback=self.variety_final_page,
                                     meta={'parms': next_parms},
                                     dont_filter=True)

    def variety_final_page(self, response):
        if response.status in [302]:
            self.logger.warning('重定向url超过重试次数,放弃：{}'.format(response.url))
            return
        try:
            doc = pq(response.text)
        except Exception as e:
            self.logger.warning('pq对象创建失败，url：{},状态码：{},失败原因：{}'.format(response.url, response.status, e))
            yield scrapy.Request(response.url,
                                 callback=self.variety_final_page,
                                 meta=response.meta,
                                 dont_filter=True)
            return
        data = response.meta['parms']
        album_a = doc('.player_title > a')
        if album_a:
            url = album_a.attr.href if 'http' in album_a.attr.href else self.host + album_a.attr.href
            data['url'] = url
            data['cid'] = re.findall(r'/./(.+?)\.html', url)[0]
            item_ = VideoListItem()
            item_['info'] = data
            yield item_
        else:
            self.logger.info('该综艺栏目没有专辑页面，url：{}'.format(response.url))


