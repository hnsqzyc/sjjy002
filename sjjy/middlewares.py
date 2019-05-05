# -*- coding: utf-8 -*-

import json
import base64
import logging
import time
import re
import datetime
from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from twisted.web._newclient import ResponseNeverReceived
from twisted.internet.error import TimeoutError, ConnectionRefusedError, ConnectError
from scrapy.utils.project import get_project_settings

settings = get_project_settings()


class ResourceMiddleware(object):
    def process_request(self, request, spider):

        # get resources from redis
        if spider.remote_resource:

            # 检测可用资源
            if spider.redis_conn.llen(spider.settings['BXS_RESOURCE_POOL']) == 0:
                logging.warning('%s 合成资源池已空， 等待然后重新提交。。。' % time.asctime())
                time.sleep(0.5)
                new_request = request.copy()
                new_request.dont_filter = True

                # 重新提交请求
                return new_request

            else:
                resource_str = spider.redis_conn.rpop(spider.settings['BXS_RESOURCE_POOL'])
                logging.debug('提取资源：%s' % resource_str)
                request.meta['resource_str'] = resource_str
                request.meta['resource'] = json.loads(resource_str.replace(b'\'', b'"'))
                # 循环利用资源
                spider.redis_conn.lpush(spider.settings['BXS_RESOURCE_POOL'], str(request.meta['resource']))

        elif spider.enable_proxy:
            request.meta['resource'] = spider.resource


class UserAgentMiddleware(UserAgentMiddleware):
    """Set cusotm user agents"""

    def process_request(self, request, spider):
        if spider.remote_resource:

            # 检测可用资源
            resource_str = spider.redis_conn.rpop(spider.settings['BXS_RESOURCE_POOL_UA'])
            logging.debug('提取ua：%s' % resource_str)  # 提取出来的是字节
            request.headers["User-Agent"] = resource_str.decode()
            # 循环利用资源
            spider.redis_conn.lpush(spider.settings['BXS_RESOURCE_POOL_UA'], resource_str)


class CookiesMiddleware(object):
    def process_request(self, request, spider):
        if spider.remote_resource:
            if request.url.startswith('https://m.weibo.cn/api/container/getIndex') or request.url.startswith('http://wx'):
                pass
            else:
                # 检测可用资源
                if spider.redis_conn.llen(spider.settings['BXS_RESOURCE_POOL_CK']) == 0:
                    logging.warning('%s Cookies合成资源池已空， 等待然后重新提交。。。' % time.asctime())
                    time.sleep(5)
                    new_request = request.copy()
                    new_request.dont_filter = True
                    # 重新提交请求
                    return new_request
                else:
                    resource_cookie = spider.redis_conn.rpop(spider.settings['BXS_RESOURCE_POOL_CK']).decode()
                    request.meta['resource_cookie'] = resource_cookie
                    logging.debug('提取cookie：%s' % resource_cookie)
                    logging.debug('Time: %s, Cookie: %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), resource_cookie))
                    request_cookie = json.loads(resource_cookie.replace('\'', '"'))
                    request.cookies = request_cookie['cookie']
                    # 循环利用资源
                    spider.redis_conn.lpush(spider.settings['BXS_RESOURCE_POOL_CK'], resource_cookie)
        else:
            request.cookies = spider.resource['cookie']


class ProxyMiddleware(object):
    # 遇到这些类型的错误直接当做代理不可用处理掉, 不再传给retrymiddleware
    DONT_RETRY_ERRORS = (TimeoutError, ConnectionRefusedError, ResponseNeverReceived, ConnectError, ValueError)

    def process_request(self, request, spider):

        if spider.remote_resource:
            # 检测可用资源
            if spider.redis_conn.llen(spider.settings['BXS_RESOURCE_POOL_PR']) == 0:
                logging.warning('%s Proxy合成资源池已空， 等待然后重新提交。。。' % time.asctime())
                time.sleep(5)
                new_request = request.copy()
                new_request.dont_filter = True

                # 重新提交请求
                return new_request
            else:
                resource_str = spider.redis_conn.rpop(spider.settings['BXS_RESOURCE_POOL_PR']).decode()
                logging.debug('提取proxy：%s' % resource_str)
                logging.debug('Time: %s, Proxy: %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), resource_str))
                if request.url.startswith("http://"):
                    request.meta['proxy'] = "http://" + resource_str
                elif request.url.startswith("https://"):
                    request.meta['proxy'] = "https://" + resource_str

                # 循环利用资源
                spider.redis_conn.lpush(spider.settings['BXS_RESOURCE_POOL_PR'], resource_str)

        elif spider.enable_proxy:  # local proxy
            request.meta['proxy'] = "https://" + request.meta['resource']['proxy']['ip'] + ":" + str(
                request.meta['resource']['proxy']['port'])

    """
            检查response.status, 根据status是否在允许的状态码中决定是否切换到下一个proxy, 或者禁用proxy
    """

    def process_response(self, request, response, spider):

        if spider.remote_resource:
            # status在因代理错误可能出现的status列表中, 则认为代理无效, 切换代理
            if response.status in spider.settings['HANDLE_PROXY_ERROR_CODES']:

                # logging.warning('Proxy response status error found: %s, re-submitting request' % json.dumps(
                #     request.meta['resource']['proxy_info']))
                #
                # # 记录失效的资源
                # request.meta['resource']['invalid_time'] = time.asctime()
                # request.meta['resource']['invalid_source'] = 'proxy'
                # spider.redis_conn.lpush(spider.settings['INVALID_BXS_RESOURCE_POOL'], request.meta['resource'])
                #
                # # 从资源池移除失效的资源（如果资源已经放回资源池的时候才使用）
                # logging.info('发现返回异常，删除可能失效的资源：%s' % request.meta['resource_str'])
                # spider.redis_conn.lrem(spider.settings['BXS_RESOURCE_POOL'], request.meta['resource_str'], 1)
                print('response_status', response.status)
                print('request.url', request.url)
                logging.info('Proxy_process_response 发现返回异常, 重新提交请求...')

                # 重新提交请求
                return self.resumit_request(request)

            if request.url.startswith('https://m.weibo.cn/api/container/getSecond') and json.loads(response.text).get('ok') == 0 and request.meta.get('page') == 1:
                print('Cookie 失效, 待更新....')
                if spider.remote_resource:
                    # 记录失效的资源
                    spider.redis_conn.lpush(spider.settings['INVALID_BXS_RESOURCE_POOL_CK'], request.meta['resource_cookie'])

                    # 从资源池移除失效的资源（如果资源已经放回资源池的时候才使用）
                    logging.warning('发现Cookie异常，删除失效资源：%s' % request.meta['resource_cookie'])
                    spider.redis_conn.lrem(spider.settings['BXS_RESOURCE_POOL_CK'], request.meta['resource_cookie'], 0)

                # 重新提交请求
                return self.resumit_request(request)


            elif not response.body:
                logging.warning('Response is empty, cookie id is: %s, re-submitting request' % request.meta['resource'][
                    'cookie_cell'])
                # 记录失效的资源
                request.meta['resource']['invalid_time'] = time.asctime()
                request.meta['resource']['invalid_source'] = 'cookie'
                spider.redis_conn.lpush(spider.settings['INVALID_BXS_RESOURCE_POOL'], request.meta['resource'])

                # 从资源池移除失效的资源（如果资源已经放回资源池的时候才使用）
                logging.info('发现返回异常，删除可能失效的资源：%s' % request.meta['resource_str'])
                spider.redis_conn.lrem(spider.settings['BXS_RESOURCE_POOL'], request.meta['resource_str'], 0)

                return self.resumit_request(request)

            # elif response.status == 302:
            #     print(request)
            #     print(request.url)
            #     logging.warning('估计是为了反爬进行的跳转验证...')
            #     time.sleep(1)
            #     return self.resumit_request(request)

            else:
                # 循环利用资源
                return response
        else:
            return response

    def resumit_request(self, request):

        new_request = request.copy()
        # request.meta['resource']['invalid_time'] = ''
        # request.meta['resource']['invalid_source'] = ''
        new_request.dont_filter = True

        # 重新提交请求
        return new_request

    """
            处理由于使用代理导致的连接异常
    """

    def process_exception(self, request, exception, spider):

        if isinstance(exception, self.DONT_RETRY_ERRORS):

            logging.warning('Proxy exception found, exception: %s' % exception)
            logging.warning('Proxy exception found, proxy: %s, re-submit request' % json.dumps(
                request.meta['proxy']))

            if spider.remote_resource:
                # 记录失效的资源
                spider.redis_conn.lpush(spider.settings['INVALID_BXS_RESOURCE_POOL_PR'], request.meta['proxy'])

                #从资源池移除失效的资源（如果资源已经放回资源池的时候才使用）
                logging.warning('发现代理异常，删除失效资源：%s' % request.meta['proxy'])
                print(request.meta['proxy'].replace(r'https://', ''))
                # proxy =
                if request.meta['proxy'].startswith('https://'):
                    spider.redis_conn.lrem(spider.settings['BXS_RESOURCE_POOL_PR'], 0, request.meta['proxy'].replace(r'https://', '')) # 注意http, https
                else:
                    spider.redis_conn.lrem(spider.settings['BXS_RESOURCE_POOL_PR'], 0, request.meta['proxy'].replace(r'http://', '')) # 注意http, https

            # 重新提交请求
            return self.resumit_request(request)


class SjjySpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SjjyDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
