import random
import time

import pymongo

import requests
import jsonpath
import re

client = pymongo.MongoClient()
col = client['WeiBo']['true_brand_search_containerid']
col2 = client['WeiBo']['monitor_weibo_msg']
weibo_users = list(col.find({}))

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://m.weibo.cn/',
    'MWeibo-Pwa': '1',
    'X-Requested-With': 'XMLHttpRequest',
}

ua_list = [
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

start_time = time.time()
base_url = 'https://m.weibo.cn/api/container/getIndex?&containerid={}'
fields = ['mblog_d88_brand_name', 'mblog_weibo_name',  'mblog_id', 'pic',
          'mblog_created_at', 'mblog_text','mblog_weibo_url', 'mblog_url', ]


def ctime():
    return time.time() - start_time


for index, weibo_user in enumerate(weibo_users):

    url = base_url.format(weibo_user['containerid'])
    headers['User-Agent'] = random.choice(ua_list)

    proxy_resp = requests.get('http://47.102.147.138:8000/random')
    if proxy_resp == 200 and proxy_resp.text:
        proxy_user = 'd88'
        proxy_pwd = 'd88'
        proxies = {
            'http': f'http://{proxy_user}:{proxy_pwd}@{proxy_resp.text}',
            'https': f'https://{proxy_user}:{proxy_pwd}@{proxy_resp.text}',
        }
    else:
        proxies = {}

    resp = requests.get(url, headers=headers, proxies=proxies)
    # resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        resp = resp.json()
        cards = resp.get('data').get('cards')
        if len(cards):
            for card in cards:
                dic = dict()
                mblog = card.get('mblog')
                if not mblog.get('isTop'):
                    mblog_d88_brand_name = weibo_user['d88_brand_name']
                    mblog_weibo_name = weibo_user['weibo_name']
                    mblog_weibo_url = weibo_user['weibo_href']
                    mblog_url = card.get('scheme')
                    mblog_created_at = mblog.get('created_at')
                    mblog_id = mblog.get('id')
                    mblog_text = mblog.get('text')
                    mblog_text = re.sub(r'<a.*?</a>|<br />|<span.*?>|</span>|<img.*?/>', '', mblog_text)
                    pic = jsonpath.jsonpath(card, '$..pics..url')

                    for field in fields:
                        try:
                            dic[field] = eval(field)
                        except:
                            print(f'！！错误的字段{field}')
                    print(dic)
                    # col.update_one({'mblog_id': dic['mblog_id']}, {'$set': dic}, upsert=True)
                    break
    else:
        print(index, f'状态码错误，为{resp.status_code}', url, ctime())
        # break
                # break





