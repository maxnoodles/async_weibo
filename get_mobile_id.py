import re
import time
from jsonpath import jsonpath
import requests
import pymongo
from fake_useragent import UserAgent
from multiprocessing import Pool

client = pymongo.MongoClient()
col3 = client['WeiBo']['true_brand_search_containerid']

cookies = {
    'SINAGLOBAL': '8980115783552.889.1560851799158',
    'SUHB': '0qjEE6brG8SEH-',
    'SUBP': '0033WrSXqPxfM72wWs9jqgMF55529P9D9WWUhFbupwsUQh8kY0f70gk15JpV2h.EShBceKnXS-WpMC4odcXt',
    'SUB': '_2AkMqV7sZdcPxrAFZn_kdxGrrbo9H-jyZgtLvAn7uJhMyAxgv7nI3qSVutBF-XDu65pUxKCCcilQwBqg2ywYQ3Pc-',
    'UOR': ',,m.weibo.cn',
    '_s_tentry': '-',
    'Apache': '4778665803385.76.1561357573589',
    'ULV': '1561357573596:4:4:2:4778665803385.76.1561357573589:1561348723442',
    'YF-V5-G0': '125128c5d7f9f51f96971f11468b5a3f',
    'YF-Page-G0': '6affec4206bb6dbb51f160196beb73f2|1561366356|1561366356',
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


def get_id(url):
    proxy = '61.145.33.239'
    proxies = {
        'http': 'https://' + proxy + ':3127',
        'https': 'https://' + proxy + ':3127',
    }
    response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
    if response.status_code == 200:
        weibo_id = re.search(r"\$CONFIG\['oid'\]='(\d+)'", response.text).group(1)
        print(weibo_id, url)
        col3.update_one({'weibo_href': url}, {'$set': {'weibo_id': weibo_id}})
    else:
        print('IP被封，请更换IP')
        return


if __name__ == '__main__':

    urls = col3.find({})
    no_id_urls = []
    for i in urls:
        if not i.get('weibo_id'):
            no_id_urls.append(i['weibo_href'])

    print(len(no_id_urls))
    print(no_id_urls)

    # pool = Pool(processes=10)
    # pool.map(get_id, no_id_urls)
    # pool.close()
    # pool.join()
    # get_id(no_id_urls[0])



    # proxy = '61.145.33.239'
    # proxies = {
    #     'http': 'https://' + proxy + ':3127',
    #     'https': 'https://' + proxy + ':3127',
    # }
    # print(proxies)
    # res = requests.get(url='https://httpbin.org/get', headers=headers, proxies=proxies)
    # print(res.text)
