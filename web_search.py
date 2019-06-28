import requests
import time
from scrapy import Selector
import pandas as pd
import re
import pymongo
import redis
import logging
import logging.config
import yaml
from pathlib import Path
import time
from fake_useragent import UserAgent

def setup_logging(default_path='config.yaml', default_level=logging.INFO):

    log_dir = Path.cwd() / 'logs'
    if not Path(log_dir).exists:
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
col = client['WeiBo']['brand_search']
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
    'User-Agent': UserAgent().random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

fields_list = ['weibo_id', 'weibo_name', 'weibo_organization', 'weibo_href', 'weibo_follow',
               'weibo_fans', 'weibo_msg', 'weibo_mobile_href']

f = open('error.txt', 'a', encoding='utf8')

# brand_name_lists = ['李宁']
for brand_name in brand_name_lists:
    url = 'https://s.weibo.com/user?q={}&auth=org_vip'.format(brand_name)
    r.sadd('weobo_search_unfinish', url)
    if r.sismember('weobo_search_finish', url):
        logger.debug(f'该{url}已存在，跳过请求')
        continue

    proxy_url = 'http://127.0.0.1:5010/get/'
    proxy_ip = requests.get(url=proxy_url).text
    proxy = {
        'http': f"http://{proxy_ip}",
    }

    try:
        resp = requests.get(url=url, headers=headers, proxies=proxy, timeout=5)
        if resp.status_code == 200:
            html = Selector(response=resp)
            dic = dict()
            dic['d88_brand_name'] = brand_name

            weibo_infos = html.xpath('//div[@class="info"]')
            if weibo_infos:
                for weibo_info in html.xpath('//div[@class="info"]'):
                    weibo_id = html.xpath('.//a//@uid').get(default='')
                    weibo_name = ''.join(html.xpath('(.//a[@class="name"])[1]//text()').getall())
                    weibo_organization = html.xpath('.//p[2]/text()').get(default='')
                    weibo_href = 'https:' + html.xpath('(.//a[@class="name"])[1]//@href').get(default='')
                    weibo_follow = html.xpath('.//p[3]/span[1]/a/text()').get(default='')
                    weibo_fans = html.xpath('.//p[3]/span[2]/a/text()').get(default='')
                    weibo_msg = html.xpath('.//p[3]/span[3]/a/text()').get(default='')
                    weibo_mobile_href = f'https://m.weibo.cn/u/{weibo_id}'

                for field in fields_list:
                    try:
                        dic[field] = eval(field)
                    except:
                        pass
                print(dic)
                col.update_one({'weibo_id': dic['weibo_id']}, {'$set': dic}, upsert=True)
                r.sadd('weobo_search_finish', url)
            else:
                logger.info(f'{url}搜索结果异常,请处理{"*"*20}')
        else:
            logger.info(f'{url}状态码异常, 为{resp.status_code}，请处理!!!!!!!!')
    except:
        logger.info(f'{url}请求异常!!!!!!!!')

