import re

import jsonpath
import pymongo
import pymysql
import redis
import json
import time
import datetime

from weibo_util import change_time


class ParseContent:

    def __init__(self):
        self.rd = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True)
        self.redis_list = 'weibo_json'

        self.to_mysql_keywords = ['全国', '门店', '深圳', '实体店', '商场'
                                  '线下商店', '线下精品店', '线下百货', '线下店', '线下专柜', '线下专卖店',
                                  '线下任意', '线下各大', '线下指定', '线下实体', '线下体验', '线下沙龙']

        self.to_mongodb_keywords = ['折扣', '限量', '新品', '发售', '特卖', '特惠', '促销', '全新', '预售',
                                    '上新', '活动', '联名', '优惠', '免费', '上市', '首发']

        self.valid_flag = 1
        self.mongodb_client = pymongo.MongoClient()
        self.mongodb_db = self.mongodb_client['WeiBo']
        self.mongodb_to_mysql_col = self.mongodb_client['WeiBo']['to_mysql_msg']
        self.mongodb_valid_col = self.mongodb_client['WeiBo']['valid_msg']
        self.mongodb_invalid_col = self.mongodb_client['WeiBo']['invalid_msg']
        self.mongodb_brand_name_col = self.mongodb_client['WeiBo']['new_brand_name']
        self.mongodb_brand_database_col = self.mongodb_client['WeiBo']['brand_database']

        self.mysql_db = pymysql.connect(host='localhost', user='root', password='root', db='d88_localhost', port=3306)
        self.to_mysql_count = 0
        self.now_datetime = datetime.datetime.now()

    def get_and_parse_weibo_json(self):
        """
        将抓取到的微博 json 数据解析入库。TODO: 可以使用多进程或者异步拓展
        """
        while True:
            weibo_json = self.rd.blpop(self.redis_list, timeout=350)
            if weibo_json is None:
                print('redis 没有数据了')
                break
            else:
                self.parse_weibo_json(weibo_json)
        # print(f'一共插入往mysql插入 {self.to_mysql_count} 条数据')

        self.mysql_db.commit()
        self.mysql_db.close()
        self.mongodb_client.close()

    def parse_weibo_json(self, weibo_json):
        """
        主要解析函数
        :param weibo_json: json
        :return:
        """

        # blpop 获取的是一个元祖 （队列名，数据）
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
            # 抓取标识，抓取时会跳过置顶的微博，为了减少无效数据，设置这个变量。
            flag = 0
            for card in cards:
                if flag == 4:
                    return
                flag += 1
                mblog = card.get('mblog')
                # 跳过置顶和没有文字的微博
                if not mblog.get('isTop') and mblog.get('text'):
                    mblog_dic = dict()

                    mblog_created_at = mblog.get('created_at')
                    mblog_created_at = change_time(mblog_created_at)

                    # 如果微博时间距离今日超过15天，不写入数据库
                    if self.now_datetime - mblog_created_at > datetime.timedelta(days=15):
                        continue

                    mblog_dic['mblog_created_at'] = mblog_created_at
                    mblog_dic['create_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    mblog_dic['mblog_url'] = card.get('scheme')
                    mblog_dic['mblog_id'] = mblog.get('id')

                    cardlistInfo = data.get('cardlistInfo')
                    if cardlistInfo:
                        mblog_dic['mblog_containerid'] = cardlistInfo.get('containerid')
                    else:
                        mblog_dic['mblog_containerid'] = ''

                    # 到微博表查询微博名
                    weibo_info = self.mongodb_brand_name_col.find_one({'containerid': mblog_dic['mblog_containerid']})
                    if weibo_info is not None:
                        mblog_dic['weibo_name'] = weibo_info.get('weibo_name')

                    # 微博附带的图片列表，方便后续用百度ocr接口识别是否包含关键字
                    # mblog_dic['mblog_pic'] = jsonpath.jsonpath(card, '$..pics..url')

                    mblog_dic['mblog_text'] = mblog.get('text')
                    # 如果存在文本，对文本中的关键词进行处理，并判断写入的数据库
                    # 去除文本中的html标签
                    mblog_dic['mblog_text'] = re.sub(r'<a.*?</a>|<br />|<span.*?>|</span>|<img.*?/>', '',
                                                     mblog_dic['mblog_text'])
                    # 如果文本包含关键词，则添加到有效列表。
                    mongodb_col, keyword = self.to_mongodb_col(mblog_dic['mblog_text'])
                    mblog_dic['mblog_keyword'] = keyword

                    print(mongodb_col, mblog_dic)
                    # 插入有效的 mysql 表
                    if mongodb_col == 'to_mysql_msg':
                        result = self.save_to_mongodb(mblog_dic, self.mongodb_to_mysql_col)
                        # if result:
                        #     self.save_to_mysql(mblog_dic)
                        #     self.to_mysql_count += 1

                    # 插入有效的 mongodb 表
                    elif mongodb_col == 'valid_msg':
                        self.save_to_mongodb(mblog_dic, self.mongodb_valid_col)

                    # 文本不包含关键词，插入无效集合
                    else:
                        self.save_to_mongodb(mblog_dic, self.mongodb_invalid_col)

    def to_mongodb_col(self, mblog_text):
        """
        根据关键词放回插入的 Mongodb 集合
        :param mblog_text: string
        :return: 准备插入的 Mongodb 集合名，命中的关键词
        """
        # 判断是否包含 mysql 的关键词列表
        for keyword in self.to_mysql_keywords:
            if keyword in mblog_text:
                mongodb_col = 'to_mysql_msg'
                return mongodb_col, keyword

        # 判断是否包含 mongodb 的关键词列表
        for keyword in self.to_mongodb_keywords:
            if keyword in mblog_text:
                mongodb_col = 'valid_msg'
                return mongodb_col, keyword

        # 都不包含返回None
        return None, None

    @staticmethod
    def save_to_mongodb(dic, col):
        """

        :param dic: dict
        :param col: Mongodb集合对象
        :return: Bool
        """
        if '_containerid' not in col.index_information():
            col.create_index('containerid', name='_containerid')

        flag = col.find_one({'mblog_id': dic['mblog_id']})
        if not flag:
            col.insert_one(dic)
            return True
        return False

    def save_to_mysql(self, mblog_dic):
        """
        将字典转换成写入 Mysql 的列表数据
        :param mblog_dic: dict
        :return:
        """
        multiple_store = self.mongodb_brand_database_col.find({'weibo_containerid': int(mblog_dic['mblog_containerid'])})
        store_ids = set([store['store_id'] for store in multiple_store])
        cursor = self.mysql_db.cursor()

        # format_time = change_time(mblog_dic['mblog_created_at'])
        weibo_create_time = mblog_dic['mblog_created_at']

        # 构建 mysql 所需数据
        start_time = int(datetime.datetime.timestamp(weibo_create_time))
        end_time = int(datetime.datetime.timestamp(weibo_create_time + datetime.timedelta(days=15)))
        status = 2
        create_time = int(time.time())
        update_time = int(time.time())
        creator_id = 1
        type = 0

        for store_id in store_ids:
            to_mysql_list = [store_id, mblog_dic['mblog_text'], start_time, end_time,
                             status, create_time, update_time, creator_id, type]

            sql = """
                    insert into d88_discount (store_id, content, start_time, end_time, status, 
                                              create_time, update_time, creator_id, type)
                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, to_mysql_list)


def run():
    now = time.time()
    parse = ParseContent()

    print(f'开始解析 weibo 的 json 数据, 一共 {parse.rd.llen(parse.redis_list)} 条数据')
    parse.get_and_parse_weibo_json()

    # dic = {'create_time': '2019-07-04 12:56:25', 'mblog_containerid': '1076036392766054',
    #        'mblog_url': 'https://m.weibo.cn/status/HARXVwpXc?' \
    #                     'mblogid=HARXVwpXc&luicode=10000011&lfid=1076032605762464',
    #        'mblog_created_at': '06-27',
    #        'mblog_id': '4387872557726266',
    #        'mblog_text': ' 好嗨鸥，力度满满的夏季折扣已安排上！精选商品低至6折起，'
    #                      '官网和线下门店同时进行喔~快把握住机会，更新你的夏日衣橱吧！ ',
    #        'mblog_keyword': '门店'}
    # p.save_to_mysql(dic)

    times = time.time() - now
    print('微博解析耗时: ', times)


if __name__ == '__main__':
    run()

