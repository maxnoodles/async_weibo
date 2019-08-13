import pandas as pd
import pymongo
import datetime
import itertools
import redis


class YesterdayWeiBoDateToEdit:

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['WeiBo']
        self.to_mysql_col = self.db['to_mysql_msg']
        self.to_mongo_col = self.db['valid_msg']
        self.brand_database = self.db['brand_database']

        self.redis_cli = redis.StrictRedis(host='192.168.2.218')

    def get_mongo_data(self):
        now = datetime.datetime.today()
        today = datetime.datetime(year=now.year, month=now.month, day=now.day)
        yesterday = today - datetime.timedelta(days=1)

        # 只读取昨天00:00 到今天00:00的数据
        find_sql = {'mblog_created_at': {'$gte': yesterday, '$lte': today}}
        to_mysql_yesterday_data = self.to_mysql_col.find(find_sql)
        to_mongo_yesterday_data = self.to_mongo_col.find(find_sql)
        data = itertools.chain(to_mysql_yesterday_data, to_mongo_yesterday_data)
        return data

    def data_handle(self, data):

        data_df = pd.DataFrame(data)
        data_df = data_df.drop(columns=['_id', 'mblog_id'])

        # 根据微博名和发布时间排序
        data_df = data_df.sort_values(by=['weibo_name', 'mblog_created_at'], ascending=False)
        data_df.rename(columns={'mblog_containerid': 'containerid'}, inplace=True)

        brand_database_data = self.brand_database.find()
        brand_database = pd.DataFrame(brand_database_data)
        brand_database.drop_duplicates('containerid', 'first', True)
        brand_database = brand_database[['containerid', 'category']]

        to_edit_df = pd.merge(data_df, brand_database, how='left', on='containerid')

        to_edit_df['containerid'] = to_edit_df['containerid'].astype('str')
        columns_sort = ['containerid', 'weibo_name', 'mblog_created_at', 'category', 'mblog_keyword', 'mblog_text', 'mblog_url']
        to_edit_df = to_edit_df[columns_sort]
        return to_edit_df

        # to_edit_df = pd.merge(data_df, brand_database, how='inner', on='containerid')
        #
        # not_in_database_df = pd.concat([to_edit_df, data_df], sort=False, ignore_index=True)
        # # 因为没有在数据库的品牌中，也有重复的 containerid, 所以先以 mblog_url 去除含有重复的项，得到不在数据仓库的数据
        # not_in_database_df.drop_duplicates('mblog_url', False, True)
        # # 再通过保留重复的第一条，去除重复的 containerid
        # not_in_database_df.drop_duplicates('containerid', 'first', True)
        #
        # columns_sort = ['containerid', 'weibo_name', 'mblog_created_at', 'category', 'mblog_keyword', 'mblog_text', 'mblog_url']
        # to_edit_df = to_edit_df[columns_sort]
        # not_in_database_df = not_in_database_df[columns_sort]
        #
        # return to_edit_df, not_in_database_df

    @staticmethod
    def export_data(df):
        today = datetime.datetime.today()
        today_format = datetime.datetime.strftime(today, '%Y-%m-%d')
        # path = fr'C:Users\Administrator\Desktop\WeiBo_{today_format}.xlsx'
        path = fr'\\192.168.2.222\public\爬虫产品数据\每日数据\WeiBo_{today_format}.xlsx'
        df.to_excel(path, encoding='utf-8', index=False)

    def run(self):
        mongo_data = self.get_mongo_data()
        to_edit_df = self.data_handle(mongo_data)
        self.export_data(to_edit_df)
        # self.export_data(not_in_database_df, not_in_database=True)
        self.client.close()


def main():
    to_edit = YesterdayWeiBoDateToEdit()
    to_edit.run()


if __name__ == '__main__':
    main()
