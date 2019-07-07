import asyncio
import datetime
import re

from multiprocessing import Pool
import jsonpath
import pymongo
import redis
import json
import time
from motor.motor_asyncio import AsyncIOMotorClient
import aioredis


class ParseContent:

    def __init__(self):
        # self.redis_list = 'weibo_json'
        self.redis_list = 'weibo_json_temp'
        self.keywords = ['全国', '门店', '深圳', '商场', '线下', '实体店', '折扣', '发布', '限量', '新品',
                         '发售', '特卖', '特惠', '促销', '全新']
        self.valid_flag = 1
        self.mongo_client = AsyncIOMotorClient()

        self.mongo_valid_col = self.mongo_client['WeiBo']['monitor_valid_msg111']
        self.mongo_all_col = self.mongo_client['WeiBo']['monitor_all_msg111']

    async def parse_weibo_json(self):
        self.r = await aioredis.create_redis_pool(('localhost', 6379), password='noodles')

        """
        将抓取到的微博 json 数据解析入库。TODO: 可以使用多进程或者异步拓展
        :return:
        """
        while True:
            weibo_json = await self.r.blpop(self.redis_list, timeout=3)
            if weibo_json is None:
                print('redis 没有数据了')
                break
            weibo_json = json.loads(weibo_json[1])

            """
            正确的内容 "ok" 为 1
            错误的内容如下:
                {
                    "ok": 0,
                    "msg": "这里还没有内容",
                    "data": {
                        "cards": [

                        ]
                    }
                }
            """

            if weibo_json.get('ok'):
                data = weibo_json.get('data')
                cards = data.get('cards')
                # 抓取标识，抓取时会跳过置顶的微博，为了确保抓到3条有效数据，设置这个变量。
                flag = 0
                for card in cards:
                    if flag == 3:
                        break

                    mblog = card.get('mblog')
                    # 跳过置顶的微博
                    if not mblog.get('isTop'):
                        mblog_dic = dict()

                        mblog_dic['mblog_containerid'] = data.get('cardlistInfo').get('containerid')
                        mblog_dic['mblog_url'] = card.get('scheme')
                        mblog_dic['mblog_created_at'] = mblog.get('created_at')
                        mblog_dic['mblog_id'] = mblog.get('id')

                        # 微博附带的图片列表，方便后续用百度ocr接口识别是否包含关键字
                        mblog_dic['mblog_pic'] = jsonpath.jsonpath(card, '$..pics..url')
                        mblog_dic['create_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        mblog_dic['mblog_text'] = mblog.get('text')
                        if mblog_dic['mblog_text']:
                            # 去除文本中的html标签
                            mblog_dic['mblog_text'] = re.sub(r'<a.*?</a>|<br />|<span.*?>|</span>|<img.*?/>', '',
                                                             mblog_dic['mblog_text'])
                            # 如果文本包含关键词，则添加到有效列表。
                            for keyword in self.keywords:
                                if keyword in mblog_dic['mblog_text']:
                                    mblog_dic['mblog_keyword'] = keyword
                                    break

                        flag += 1
                        print(mblog_dic)
                        # 根据是否包含关键词插入不同的集合
                        if mblog_dic.get('mblog_keyword'):
                            await self.mongo_valid_col.update_one({'mblog_id': mblog_dic['mblog_id']}, {'$set': mblog_dic},
                                                                    upsert=True)
                        else:
                            await self.mongo_all_col.update_one({'mblog_id': mblog_dic['mblog_id']}, {'$set': mblog_dic},
                                                          upsert=True)

        self.r.close()
        await self.r.wait_closed()


def run():
    p = ParseContent()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(p.parse_weibo_json())
    loop.close()


if __name__ == '__main__':
    #
    print('开始解析 weibo 的 json 数据')
    now = time.time()

    run()

    times = time.time() - now
    print('微博解析耗时: ', times)
