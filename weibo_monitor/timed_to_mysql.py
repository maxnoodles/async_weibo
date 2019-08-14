import pymongo
import datetime
import time
import pymysql
import traceback


class TimedToMysql:
    def __init__(self):

        self.mysql_read_db = pymysql.connect(host='192.168.2.218',
                                             user='root',
                                             password='2012Dibaba',
                                             db='my_test',
                                             port=3306,
                                             cursorclass=pymysql.cursors.DictCursor
                                         )

        self.mysql_write_db = pymysql.connect(host='localhost',
                                              user='root',
                                              password='root',
                                              db='d88_localhost',
                                              port=3306)

        self.mongodb_client = pymongo.MongoClient()
        self.mongodb_brand_database_col = self.mongodb_client['WeiBo']['brand_database']
        self.filter_containerid_set = set()
        self.count = 0

    def read_to_mysql(self):
        today = datetime.date.today()
        today = today.strftime('%Y-%m-%d')
        data = []
        try:
            with self.mysql_read_db.cursor() as cursor:
                sql = f"SELECT * FROM `weibo_brand_sales` WHERE `date` = '{today}'"
                cursor.execute(sql)
            self.mysql_read_db.commit()
            data = cursor.fetchall()
            print(data)
        except Exception:
            traceback.print_exc()
        finally:
            self.mysql_read_db.close()
            return data

    def save_to_mysql(self, data):
        """
        将字典转换成写入 Mysql 的列表数据
        :param mblog_dic: dict
        :return:
        """
        # print(data)
        multiple_store = self.mongodb_brand_database_col.find({'containerid': str(data['weibo_id'])})
        store_ids = set([store['store_id'] for store in multiple_store])

        # 构建 mysql 所需数据
        start_time = int(datetime.datetime.timestamp(data['start_time']))
        end_time = int(datetime.datetime.timestamp(data['end_time']))
        status = 2
        create_time = int(datetime.datetime.timestamp(data['date']))
        update_time = int(datetime.datetime.timestamp(data['date']))
        creator_id = 1
        _type = 0

        with self.mysql_write_db.cursor() as cursor:
            for store_id in store_ids:
                to_mysql_list = [store_id, data['content'], start_time, end_time,
                                 status, create_time, update_time, creator_id, _type]
                print(to_mysql_list)
                self.count += 1

                sql = """
                        insert into d88_discount (store_id, content, start_time, end_time, status,
                                                  create_time, update_time, creator_id, type)
                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, to_mysql_list)

    def run(self):
        if self.mysql_read_db:
            mysql_data = self.read_to_mysql()
            print(f'今天一共编辑了 {len(mysql_data)} 条有效信息')
            for data in mysql_data:
                containerid = data['weibo_id']
                if containerid not in self.filter_containerid_set:
                    self.filter_containerid_set.add(containerid)
                    self.save_to_mysql(data)
                else:
                    print('相同的containerid：', containerid)

        self.mysql_write_db.commit()
        self.mysql_write_db.close()
        self.mongodb_client.close()


def main():
    print('准备写入测试mysql数据库')
    jobs = TimedToMysql()
    jobs.run()
    print(f'写入测试数据库结束, 一共写入了 {jobs.count} 条数据')


if __name__ == '__main__':
    main()



