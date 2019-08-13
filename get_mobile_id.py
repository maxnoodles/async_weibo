import os
import re
import time
import requests
import pymongo
from fake_useragent import UserAgent
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import ConnectTimeout

client = pymongo.MongoClient()
col = client['WeiBo']['new_brand_name']
col2 = client['WeiBo']['weibo_error_brand']

cookies = {
    'SINAGLOBAL': '8980115783552.889.1560851799158',
    'SUHB': '0qjEE6brG8SEH-',
    'SUBP': '0033WrSXqPxfM72wWs9jqgMF55529P9D9WWUhFbupwsUQh8kY0f70gk15JpV2h.EShBceKnXS-WpMC4odcXt',
    'SUB': '_2AkMqV7sZdcPxrAFZn_kdxGrrbo9H-jyZgtLvAn7uJhMyAxgv7nI3qSVutBF-XDu65pUxKCCcilQwBqg2ywYQ3Pc-',
    'UOR': ',,bbs.51testing.com',
    'YF-Page-G0': 'aac25801fada32565f5c5e59c7bd227b|1562895209|1562895209',
    '_s_tentry': '-',
    'Apache': '3564703312244.2773.1562895209863',
    'ULV': '1562895209912:9:3:3:3564703312244.2773.1562895209863:1562753627031',
    'YF-V5-G0': '451b3eb7a5a4008f8b81de1fcc8cf90e',
}

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': UserAgent().random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Referer': 'https://s.weibo.com/weibo/%25E4%25BC%2598%25E8%25A1%25A3%25E5%25BA%2593?topnav=1&wvr=6&b=1',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}
retry_times = 0


def get_id(url):
    global retry_times
    while True:
        proxy = requests.get('http://47.102.147.138:8000/random').text
        if proxy:
            break
        else:
            print('等待代理中')
            time.sleep(0.1)
    proxy_user = 'd88'
    proxy_pwd = 'd88'
    proxies = {
        'http': f'http://{proxy_user}:{proxy_pwd}@{proxy}',
        'https': f'https://{proxy_user}:{proxy_pwd}@{proxy}',
    }
    try:
        response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies, timeout=10)
        print(url)
        if response.status_code == 200:
            weibo_id = re.search(r"\$CONFIG\['oid'\]='(\d+)'", response.text).group(1)
            return weibo_id
        else:
            print('IP被封，请更换IP')
            return False
    except AttributeError as e:
        print(e, '类型错误')
        error_text = '微博Url错误'
        return error_text
    except requests.exceptions.Timeout as e:
        if retry_times < 3:
            retry_times += 1
            print(e, '超时递归')
            dic = get_id(url)
            return dic
        else:
            retry_times = 0
            return False


def mongo_repeat(dic):
    doc = col.find_one({'weibo_id': dic['weibo_id']})
    if doc:
        del dic['weibo_mobile_href']
        error = '已经有相同的微博id了'
        dic['old_name'] = doc["d88_brand_name"]
        dic['error'] = error
        print(dic)
        col2.update_one({'d88_brand_name':dic['d88_brand_name']}, {'$set':dic}, True)
    else:
        print(dic)
        col.update_one({'weibo_id': dic['weibo_id']}, {'$set': dic}, True)


if __name__ == '__main__':
    path = 'C:Users\Administrator\Desktop\D88_brand_name整理版(1).xlsx'
    brand_df = pd.read_excel(io=path, usecols=['brand_name', '微博'])
    brand_df.dropna(inplace=True)
    brands = brand_df.values.tolist()

    x = 0
    mongo_brands = col.find({})
    mongo_brands = set([mongo_brand['d88_brand_name'] for mongo_brand in mongo_brands])
    not_mongo_brand = []
    for brand in brands:
        if brand[0] not in mongo_brands:
            not_mongo_brand.append(brand)

    for index, brand in enumerate(not_mongo_brand[x:]):
        dic = {}
        dic['d88_brand_name'] = brand[0]
        dic['weibo_href'] = brand[1]

        if 'weibo' not in brand[1]:
            dic['error'] = '微博Url错误'
            print(dic)
            col2.insert_one(dic)
            continue

        result = get_id(brand[1])
        if '错误' not in result:
            dic['weibo_mobile_href'] = 'https://m.weibo.cn/u/' + result
            dic['weibo_id'] = result
            print(x + index)
            mongo_repeat(dic)
            # col.update_one({'weibo_id': dic['weibo_id']}, {'$set': dic}, True)
        else:
            dic['error'] = result
            print(dic)
            col2.insert_one(dic)


    client.close()
    # with ThreadPoolExecutor(max_worker=10)
