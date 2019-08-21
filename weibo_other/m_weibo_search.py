import requests
import time

from jsonpath import jsonpath
from scrapy import Selector
import pandas as pd
import re
import pymongo
from faker import Faker
import redis
import logging
import logging.config
import yaml
from pathlib import Path
import time


def setup_logging(default_path='config.yaml', default_level=logging.INFO):

    log_dir = Path.cwd() / 'logs'
    if not log_dir.exists():
        Path.mkdir(log_dir)
    if Path(default_path).exists():
        with open(default_path, 'r', encoding='utf8') as f:
            config = yaml.full_load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
    logger = logging.getLogger('main')
    return logger


logger = setup_logging()

client = pymongo.MongoClient()
col = client['WeiBo']['m_brand_search']
r = redis.Redis()

path = r'C:\Users\Administrator\Desktop\D88品牌库整理版(2019.03.12).xlsx'
df = pd.read_excel(io=path, sheet_name='服饰手袋中端', usecols=[1])


def change_brand_name(x):
    if all(['\u4e00' < i < '\u9fff' for i in x]):
        return x
    else:
        x = re.sub(r'[\u4e00-\u9fff/\（\）]', '', x)
        x = re.sub(r'[\·\+\=]|\s', '', x)
        return x


df['品牌'] = df.apply(lambda x: change_brand_name(x['品牌']), axis=1)
brand_name_lists = df['品牌'].values.tolist()

brand_name_lists = list(filter(lambda x: x != '', brand_name_lists))
print(brand_name_lists)


headers = {
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

fields_list = ['weibo_id', 'weibo_name', 'weibo_organization', 'weibo_follow',
               'weibo_fans', 'weibo_mobile_href']


def get_proxy():
    proxy_url = 'http://127.0.0.1:5010/get/'
    proxy_ip = requests.get(url=proxy_url).text
    proxy = {
        'http': f"http://{proxy_ip}",
        # 'https': f"http://{proxy_ip}",
    }
    return proxy


def get_data(response):
    dic = dict()
    cards = response.get('data').get('cards')
    if len(cards):
        user = jsonpath(cards, expr='$..user')[0]
        # user = cards[0].get('card_group')[0].get('user')
        weibo_id = user.get('id')
        weibo_name = user.get('screen_name')
        weibo_follow = user.get('follow_count')
        weibo_fans = user.get('followers_count')
        # weibo_organization = cards[0].get('card_group')[0].get('desc1')
        weibo_organization = desc1 = jsonpath(cards, expr='$..desc1')[0]
        weibo_mobile_href = f'https://m.weibo.cn/u/{weibo_id}'

        for field in fields_list:
            dic[field] = eval(field)

        return dic


brand_name_lists = ['李宁', '西铁城腕表', ]

for brand_name in brand_name_lists:
    # time.sleep(1)
    url = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D{}%26isv%3D3'.format(brand_name)
    r.sadd('m_weobo_search_unfinish', url)
    if r.sismember('m_weobo_search_finish', url):
        logger.debug(f'该{url}已存在，跳过请求')
        continue

    # proxy = get_proxy()

    try:
        resp = requests.get(url=url, headers=headers, timeout=5)
        # resp = requests.get(url=url, headers=headers, timeout=5, proxy=proxy)
    except:
        logger.warning(f'requests请求异常，品牌名：{brand_name}，链接为--{url}')
        continue
    if resp.status_code == 200:
        resp = resp.json()
        if resp.get('ok') == 1:
            dic = get_data(resp)
            dic['d88_brand_name'] = brand_name
            print(dic)
            col.update_one({'weibo_id': dic['weibo_id']}, {'$set': dic}, upsert=True)
            r.sadd('m_weobo_search_finish', url)
        else:
            logger.warning(f'json数据异常，品牌名：{brand_name}，链接--{url}')
    else:
        logger.warning(f'状态码异常，品牌名：{brand_name}，{resp.status_code}，链接--{url}')








