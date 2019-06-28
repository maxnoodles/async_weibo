import datetime
import re

import jsonpath
import pymongo
import multiprocessing
import redis
import json


class ParseContent:

    def __init__(self):
        self.rd = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True)
        self.redis_list = 'weibo_json'
        self.keywords = ['全国', '门店', '深圳', '商场', '线下', '实体店', '折扣', '发布', '限量', '新品',
                          '发售', '特卖', '特惠', '促销', '全新']
        self.valid_flag = 1
        self.mongo_client = pymongo.MongoClient()
        self.mongo_db = self.mongo_client['WeiBo']
        self.mongo_valid_col = self.mongo_client['WeiBo']['monitor_valid_msg111']
        self.mongo_all_col = self.mongo_client['WeiBo']['monitor_all_msg111']

    def parse_weibo_json(self):
        a = self.rd.blpop(self.redis_list)
        weibo_json = json.loads(a[1])

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
                        # 讲文本中的html标签去除
                        mblog_dic['mblog_text'] = re.sub(r'<a.*?</a>|<br />|<span.*?>|</span>|<img.*?/>', '', mblog_dic['mblog_text'])
                        # 如果文本包含关键词，则添加到有效列表。
                        for keyword in self.keywords:
                            if keyword in mblog_dic['mblog_text']:
                                mblog_dic['mblog_keyword'] = keyword
                                break

                    flag += 1
                    print(mblog_dic)
                    # 根据是否包含关键词插入不同的集合
                    if mblog_dic.get('mblog_keyword'):
                        self.mongo_valid_col.update_one({'mblog_id': mblog_dic['mblog_id']}, {'$set': mblog_dic}, upsert=True)
                    else:
                        self.mongo_all_col.update_one({'mblog_id': mblog_dic['mblog_id']}, {'$set': mblog_dic}, upsert=True)


if __name__ == '__main__':
    p = ParseContent()
    print(p.rd.llen('weibo_json'))
    p.parse_weibo_json()
    print(p.rd.llen('weibo_json'))
