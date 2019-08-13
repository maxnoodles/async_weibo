import asyncio
import random
import time

import aiohttp
from aiostream import stream
from motor.motor_asyncio import AsyncIOMotorClient
import aioredis
import requests


class WeiBoSpider:

    def __init__(self):
        self.headers = {
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'upgrade-insecure-requests': '1',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'authority': 'm.weibo.cn',
        }

        self.ua_list = [
            'Mozilla/5.0 (Linux; U; Android 8.1.0; zh-cn; BLA-AL00 Build/HUAWEIBLA-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/8.9 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.1.0; ALP-AL00 Build/HUAWEIALP-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/63.0.3239.83 Mobile Safari/537.36 T7/10.13 baiduboxapp/10.13.0.11 (Baidu; P1 8.1.0)',
            'Mozilla/5.0 (Linux; Android 6.0.1; OPPO A57 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/63.0.3239.83 Mobile Safari/537.36 T7/10.13 baiduboxapp/10.13.0.10 (Baidu; P1 6.0.1)',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16A366 MicroMessenger/6.7.3(0x16070321) NetType/WIFI Language/zh_CN',
            'Mozilla/5.0 (iPhone 6s; CPU iPhone OS 11_4_1 like Mac OS X) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0 MQQBrowser/8.3.0 Mobile/15B87 Safari/604.1 MttCustomUA/2 QBWebViewType/1 WKType/1',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 10_1 like Mac OS X) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0 MQQBrowser/8.8.2 Mobile/14B72c Safari/602.1 MttCustomUA/2 QBWebViewType/1 WKType/1',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 11_2 like Mac OS X) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0 MQQBrowser/8.8.2 Mobile/15B87 Safari/604.1 MttCustomUA/2 QBWebViewType/1 WKType/1',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 10_1_1 like Mac OS X) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0 MQQBrowser/8.8.2 Mobile/14B100 Safari/602.1 MttCustomUA/2 QBWebViewType/1 WKType/1',
            'Mozilla/5.0 (Linux; U; Android 5.1; zh-CN; m2 note Build/LMY47D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.9.2.712 U3/0.8.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Linux; Android 6.0.1; OPPO A57 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/63.0.3239.83 Mobile Safari/537.36 T7/10.13 baiduboxapp/10.13.0.10 (Baidu; P1 6.0.1)'
        ]

        self.base_url = 'https://m.weibo.cn/api/container/getIndex?&containerid={}&page=1'
        self.client = AsyncIOMotorClient(host='127.0.0.1', port=27017)

        self.col_read = self.client['WeiBo']['new_brand_name']
        self.col_write = self.client['WeiBo']['weibo_json_temp']
        self.proxy = ''

        self.event = asyncio.Event()

    async def start_request(self):
        """
        包装 url 使用协程抓取
        :return:
        """
        self.r = await aioredis.create_redis_pool(('localhost', 6379))
        cursor = self.col_read.find({})

        # 使用代理
        # while True:
        #     proxy_text = requests.get('http://47.102.147.138:8000/random').text
        #     if proxy_text:
        #         self.proxy = 'http://' + proxy_text
        #         break
        #     time.sleep(0.1)
        #
        # print(self.proxy)

        async with aiohttp.TCPConnector(limit=250, force_close=True, enable_cleanup_closed=True) as tc:
            async with aiohttp.ClientSession(connector=tc) as session:
                tasks = (asyncio.create_task(self.fetch(mblog, session)) async for mblog in cursor)
                await self.branch(tasks)

    async def branch(self, tasks, limit=950):
        """异步切片，限制并发"""
        # index = 0
        while True:
            xs = stream.preserve(tasks)
            ys = xs[0:limit]
            t = await stream.list(ys)
            if not t:
                break
            await asyncio.create_task(asyncio.wait(t))

            print(f'休眠300s')
            await asyncio.sleep(300)
            print('休眠结束')

        self.r.close()
        await self.r.wait_closed()
        self.client.close

    async def fetch(self, mblog, session):
        """
        讲返回的内容不做解析直接入库
        :param mblog:
        :param session:
        :return:
        """

        url = self.base_url.format(mblog['containerid'])
        # print(url)

        self.headers.update({'user-agent': random.choice(self.ua_list)})

        # proxy_auth = aiohttp.BasicAuth('d88', 'd88')
        # async with session.get(url, headers=self.headers, timeout=10, proxy=self.proxy, proxy_auth=proxy_auth) as res:

        async with session.get(url, headers=self.headers) as res:
            if res.status == 200:
                res_text = await res.text()
                # 异步插入mongodb
                await self.col_write.insert_one({'containerid': mblog['containerid'], 'text': res_text})
                # 异步插入redis
                print(f'推入的 {url}')
                await self.r.lpush('weibo_json', res_text)
            else:
                print(f'状态码异常，为{res.status}', url)


def run():
    print('开始抓取微博数据')
    spider = WeiBoSpider()
    now = time.time()
    # 不能使用 asyncio.run()，和 motor 冲突
    loop = asyncio.get_event_loop()
    loop.run_until_complete(spider.start_request())
    # 定时执行，不能关闭循环
    # loop.close()
    times = time.time() - now
    print('抓取微博数据耗时: ', times)


if __name__ == '__main__':
    run()
