from multiprocessing import Process
import time
import weibo_parse
import weibo_spider


class Scheduler:

    @staticmethod
    def weibo_spider_run():
        """
        微博爬虫定时执行
        :return:
        """
        while True:
            weibo_spider.run()
            time.sleep(3600)

    @staticmethod
    def weibo_parse_run():
        """
        微博 json 数据定时解析
        :return:
        """
        while True:
            weibo_parse.run()
            time.sleep(3600)


def run():
    scheduler = Scheduler()

    spider_process = Process(target=scheduler.weibo_spider_run)
    spider_process.start()

    parse_process = Process(target=scheduler.weibo_parse_run)
    parse_process.start()


if __name__ == '__main__':
    run()

