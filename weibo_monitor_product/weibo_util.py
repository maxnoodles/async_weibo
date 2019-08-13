import datetime
import re


def change_time(test_time, to_timestamp=False):
    now = datetime.datetime.now()
    if re.search(r'^\d{2}-\d{2}$', test_time):
        # 匹配格式 ‘07-03’
        test_time = f'{now.year}-{test_time}'
        format_time = datetime.datetime.strptime(test_time, '%Y-%m-%d')

    elif re.search(r'^\d{4}-\d{2}-\d{2}$', test_time):
        # 匹配格式 ‘2018-03-05’
        format_time = datetime.datetime.strptime(test_time, '%Y-%m-%d')

    elif re.search(r'^(\d+)小时前$', test_time):
        # 匹配格式 ‘21小时前’
        hours = re.match(r'^(\d+)小时前$', test_time).group(1)
        format_time = now - datetime.timedelta(hours=int(hours))

    elif re.search(r'昨天\s?\d{2}:\d{2}', test_time):
        # 匹配格式 ‘昨天 00:38’
        hour, minute = re.search(r'昨天\s?(\d{2}):(\d{2})', test_time).groups()
        # format_time = now - datetime.timedelta(days=1)
        yesterday = now.day - 1
        format_time = now.replace(day=yesterday, hour=int(hour), minute=int(minute))

    elif re.search(r'(\d+)分钟前', test_time):
        # 匹配格式 ‘49分钟前’
        minute = re.match(r'(\d+)分钟前', test_time).group(1)
        format_time = now - datetime.timedelta(minutes=int(minute))

    elif re.search(r'刚刚', test_time):
        # 匹配格式 ‘刚刚’
        format_time = now

    if to_timestamp:
        format_time = datetime.datetime.timestamp(format_time)
    return format_time


if __name__ == '__main__':
    a = '2018-03-05'
    b = '07-03'
    c = '21小时前'
    d = '昨天 00:38'
    e = '49分钟前'
    f = '刚刚'

    print(a, b, c, d, e, f, sep='\n')

    a = change_time(a, True)
    b = change_time(b)
    c = change_time(c)
    d = change_time(d)
    e = change_time(e)
    f = change_time(f)

    print(a, b, c, d, e, f, sep='\n')

    today = datetime.datetime.now()
    if today.day - b.day > 1:
        print('error')
