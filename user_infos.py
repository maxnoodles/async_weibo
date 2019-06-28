import random
import re
import time
from multiprocessing import Pool

from jsonpath import jsonpath
import requests
import pymongo
from fake_useragent import UserAgent

client = pymongo.MongoClient()
col3 = client['WeiBo']['true_brand_search_containerid']


headers = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': UserAgent().random
}


url1 = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={}'


def get_proxy():
    proxy = '61.145.49.30'
    proxies = {
        'http': 'https://' + proxy + ':3127',
        'https': 'https://' + proxy + ':3127',
    }
    return proxies


def get_infos(uid):
    url = url1.format(uid)
    proxy = get_proxy()
    resp1 = requests.get(url, headers=headers, proxies=proxy, verify=False)
    if resp1.status_code == 200:
        data = resp1.json()
        dic = dict()
        containerid = jsonpath(data, '$..tabs[?(@.title=="微博").containerid]')
        dic['containerid'] = containerid[0]
        dic['weibo_name'] = ''.join(jsonpath(data, '$..screen_name'))
        weibo_organization = jsonpath(data, '$..verified_reason')
        if weibo_organization:
            dic['weibo_organization'] = weibo_organization[0]
        dic['weibo_msg'] = jsonpath(data, '$..statuses_count')[0]
        dic['weibo_mobile_href'] = f'https://m.weibo.cn/u/{uid}'
        dic['weibo_follow'] = jsonpath(data, '$..follow_count')[0]
        dic['weibo_fans'] = jsonpath(data, '$..followers_count')[0]
        print(dic)
        col3.update_one({'weibo_id': uid}, {'$set': dic})
    else:
        print('！！！状态码错误：', resp1.status_code, uid, url)
        return


if __name__ == '__main__':

    urls = col3.find({})
    no_id_urls = []
    for i in urls:
        if not i.get('containerid'):
            no_id_urls.append(i['weibo_id'])

    # print(len(no_id_urls))
    # print(no_id_urls)

    pool = Pool(processes=20)
    pool.map(get_infos, no_id_urls)
    pool.close()
    pool.join()

