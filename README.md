"# async_weibo"

因为微博的接口 IP 限制策略是 10s 检测一次，然后封禁5分钟，所以要在 10s 内发起尽可能多的请求。  
本项目采用 asynico + aiohttp 百兆带宽，最大并发300个链接，10s 能抓取 950 有效请求，并将其接口的 json 存入 redis 的 list 中。  
然后解析脚本通过 blpop  实时取出 redis 中的 json 进行解析。  
采用 apscheduler 定时执行。  



`async_weibo_spider` 是本项目的主文件夹,下面是文件介绍  

`weibo_spider.py` 微博接口请求脚本  

`weibo_parse.py` 微博 json 解析脚本(测试过多进程和异步解析，因涉及到大量数据库操作，异步解析效果会更好一点)  

`weibo_util.py` 工具脚本，将 weibo 的时间转换为 datetime  

`to_edit.py` 将昨天的微博导出 execl  

`scheduler` 总调度函数  

`timed_to_mysql.py` 业务脚本  





`weibo_other` 这个文件夹里面是一些微博的搜索接口和获取微博用户id的脚本文件


