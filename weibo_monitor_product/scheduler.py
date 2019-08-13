from multiprocessing import Process
import time
import weibo_parse
import weibo_spider
from apscheduler.schedulers.blocking import BlockingScheduler
import to_edit
import timed_to_mysql


class Scheduler:

    @staticmethod
    def weibo_spider_run():
        """
        微博爬虫定时执行
        :return:
        """
        weibo_spider.run()

    @staticmethod
    def weibo_parse_run():
        """
        微博 json 数据定时解析
        :return:
        """
        weibo_parse.run()
        to_edit.main()


def run():
    print('执行抓取解析任务')
    scheduler = Scheduler()

    spider_process = Process(target=scheduler.weibo_spider_run)
    spider_process.start()

    parse_process = Process(target=scheduler.weibo_parse_run)
    parse_process.start()
    print('抓取解析任务执行完毕')


if __name__ == '__main__':
    sched = BlockingScheduler()
    print('休眠中，准备执行任务')

    sched.add_job(run, 'cron', day_of_week='0-6', hour=6, minute=00, end_date='2020-07-01')
    sched.add_job(timed_to_mysql.main, 'cron', day_of_week='0-6', hour=18, minute=40, end_date='2020-07-01')
    sched.start()

    # run()

