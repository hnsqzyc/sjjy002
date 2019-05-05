import scrapy
import json
import re
import os
import csv
import codecs
import copy
import hashlib
import time
import logging
from random import choice
from sjjy.user_ids import user_id
from scrapy import signals
from scrapy.item import Item, Field
from scrapy.http import Request, FormRequest
from scrapy.utils.project import get_project_settings
from sjjy.connection import RedisConnection, MongodbConnection

settings = get_project_settings()


class UniversalRow(Item):
    # This is a row wrapper. The key is row and the value is a dict
    # The dict wraps key-values of all fields and their values
    row = Field()
    table = Field()
    image_urls = Field()


class JjySpider(scrapy.Spider):
    name = 'jjy_01'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(JjySpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)

        return spider

    def __init__(self, params, *args, **kwargs):

        super(JjySpider, self).__init__(self.name, *args, **kwargs)
        # dispatcher.connect(self.spider_closed, signals.spider_closed)
        paramsjson = json.loads(params)
        self.remote_resource = paramsjson.get('remote_resource', True)
        self.enable_proxy = paramsjson.get('enable_proxy', True)

    def spider_opened(self, spider):
        logging.info("爬取开始了...")

        self.redis_conn = RedisConnection(settings['REDIS']).get_conn()
        self.mongo_conn = MongodbConnection(settings['MONGODB']).get_conn()
        # self.db = self.mongo_conn.sjjy_tk_backups
        self.db = self.mongo_conn.sjjy
        self.sjjy = self.db.sjjy

    def spider_closed(self, spider):

        logging.info('爬取结束了...')

    def start_requests(self):
        while 1: # 可能造成重复
            result = self.sjjy.find({'status': 8}, {'_id': 0, 'realUid': 1}).limit(100)
            if result.count():
                for res in result:
                    meta = {}
                    meta['realUid'] = res['realUid']
                    print('拿到以后不用等下载图片数量及链接,马上把状态修改了...')
                    logging.info('拿到以后不用等下载图片数量及链接,马上把状态修改了...')
                    result = self.sjjy.update({'realUid':  meta['realUid']}, {'$set': {'status': 7}})  #  拿到以后不用等下载图片数量及链接,马上把状态修改了
                    result1 = self.sjjy.update({'realUid': meta['realUid']},{'$set': {'img_url_li':[]}}) # 把原来的链接清空
                    url = 'http://www.jiayuan.com/{}?fxly=search_v2_28'.format(meta['realUid'])

                    header = {
                        # "Host": "www.jiayuan.com",
                        "Connection": "keep-alive",
                        "Cache-Control": "max-age=0",
                        "Upgrade-Insecure-Requests": "1",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
                        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                        "Referer": "http://search.jiayuan.com/v2/index.php?key=&sex=f&stc=23:1&sn=default&sv=1&p=1&pt=1&ft=off&f=select&mt=d",
                        "Accept-Encoding": "gzip, deflate",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                        # "Cookie": "accessID = 20190403221417339401;ip_loc = 11"
                    }
                    yield Request(url=url, headers=header, callback=self.parse_photo_num, dont_filter=True, meta=meta)
            else:
                logging.info('状态码为8的为空...')
                return

    def parse_photo_num(self, response):

        meta = response.request.meta
        realUid = meta['realUid']
        # print('response_photo_num', response.text)
        try:
            res_img = response.xpath('//ul[@class="nav_l"]//li[2]/a/text()')[0]


            img_num = re.search(r'照片\((\d+?)\)', str(res_img)).group(1)
            print('img_num', realUid, img_num)
            logging.info('realUid: %s' % (realUid,))

            # TODO 插入照片数量
            # self.sjjy.update({'realUid': realUid}, {'$set': {'photo_num': int(img_num)}})
            if int(img_num) > 4:
                # 如果照片数量不小于5张, 遍历存储
                img_link = response.xpath('//div[@id="bigImg"]/ul/li//img/@_src').extract()
                # print('img_link', img_link)
                img_li = []
                for img_url in img_link:
                    meta['img_url'] = img_url
                    meta['img_id'] = re.search(r'(.*?).jpg', img_url).group(1)[-8:]
                    logging.info('img_url %s' % img_url)
                    print('img_url', img_url)
                    img_li.append(img_url)
                logging.info('正在下载图片链接...')
                res = self.sjjy.update({'realUid': realUid}, {'$addToSet': {'img_url_li': img_li}})
                logging.info('下载图片链接后修改Uid状态为1...')
               # result = self.sjjy.update({'realUid': realUid}, {'$set': {'status': 1}})

            else:

                logging.info('图片数量小于4修改Uid状态为1...')
               # result = self.sjjy.update({'realUid': realUid}, {'$set': {'status': 1}})

        except (IndexError, AttributeError) as f:
            print('下载图片链接后修改Uid状态为1...')
           # result = self.sjjy.update({'realUid': realUid}, {'$set': {'status': 1}})
            self.sjjy.update({'realUid': realUid}, {'$set': {'photo_num': 900}})
            print('img_num', realUid, f, '空值...')

    # def submit_image_request(self, meta):
    #     img_url = meta['img_url']
    #     head = re.search(r'http://(.*?)/', img_url).group(1)
    #     header = {
    #         # "Host:": head,
    #         "Connection": "keep-alive",
    #         "Upgrade-Insecure-Requests": "1",
    #         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    #         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    #         "Referer": "http://www.jiayuan.com/{}?fxly=search_v2_index".format(meta['realUid']),
    #         "Accept-Encoding": "gzip, deflate",
    #         # "Accept-Language": "zh-CN,zh;q=0.9"
    #     }
    #     print('head', header)
    #
    #     if not os.path.exists(settings['DATA_DIR'] + str(meta['realUid']) + meta['sexValue']):
    #         os.mkdir(settings['DATA_DIR'] + str(meta['realUid']) + meta['sexValue'])
    #     save_location = os.path.join(settings['DATA_DIR'], str(meta['realUid']) + meta['sexValue'])
    #
    #     file_name = os.path.join(save_location, str(meta['img_id']) + '.jpg')
    #
    #     meta['file_name'] = file_name
    #     print('正在下载:' + file_name)
    #     return Request(meta['img_url'], headers=header, callback=self.download_image, meta=meta)
    #     # request.urlretrieve(meta['pic_url'], meta['file_name'])
    #
    # def download_image(self, response):
    #     meta = response.request.meta
    #     res = response.body
    #     try:
    #         with open(meta['file_name'], 'wb') as f:
    #             f.write(res)
    #             f.close()
    #         logging.info('已经下载...')
    #     except FileNotFoundError:
    #         print('捕捉到文件名有误...')
