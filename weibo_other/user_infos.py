import random
import re
import time
from multiprocessing import Pool

from jsonpath import jsonpath
import requests
import pymongo
from fake_useragent import UserAgent
import urllib3
urllib3.disable_warnings()

client = pymongo.MongoClient()
col = client['WeiBo']['new_brand_name']

headers = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': UserAgent().random
}

url1 = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={}'
retry_times = 0


def get_proxy():
    while True:
        proxy = requests.get('http://xxxx/random').text
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
    return proxies


def get_infos(uid):
    global retry_times
    url = url1.format(uid)
    proxy = get_proxy()
    try:
        resp1 = requests.get(url, headers=headers, proxies=proxy, verify=False, timeout=10)
        if resp1.status_code == 200:
            data = resp1.json()
            containerid = jsonpath(data, '$..tabs[?(@.title=="微博").containerid]')
            if containerid:
                containerid = containerid[0]
            weibo_name = ''.join(jsonpath(data, '$..screen_name'))

            # weibo_organization = jsonpath(data, '$..verified_reason')
            # if weibo_organization:
            #     dic['weibo_organization'] = weibo_organization[0]
            # dic['weibo_msg'] = jsonpath(data, '$..statuses_count')[0]
            # dic['weibo_mobile_href'] = f'https://m.weibo.cn/u/{uid}'
            # dic['weibo_follow'] = jsonpath(data, '$..follow_count')[0]
            # dic['weibo_fans'] = jsonpath(data, '$..followers_count')[0]

            print(containerid, weibo_name, url)
            col.update_one({'weibo_id': uid},
                           {'$set': {'containerid': containerid, 'weibo_name': weibo_name}},
                           True)
        else:
            print('！！！状态码错误：', resp1.status_code, uid, url)
            time.sleep(10)
            return False
    except AttributeError as e:
        print(e, '类型错误')
        error_text = '微博Url错误'
        return error_text
    except requests.exceptions.Timeout as e:
        if retry_times < 3:
            retry_times += 1
            print(e, '超时递归')
            dic = get_infos(uid)
            return dic
        else:
            retry_times = 0
            return False


if __name__ == '__main__':

    docs = col.find({})
    uids = [doc['weibo_id'] for doc in docs if not doc.get('containerid')]
    a = ''
    # print(len(no_id_urls))
    # print(no_id_urls)

    pool = Pool()
    pool.map(get_infos, uids)
    pool.close()
    pool.join()
    client.close()

