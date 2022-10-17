# coding=utf8
import sqlite3

class act_sqlite(object):
    def __init__(self):
        """
        search from sqlite database
        or
        act sql and commit
        """
        self.db = sqlite3.connect('test.db')
        self.cur = self.db.cursor()

    def insert_data(self,tablename,values):
        deletesql = "delete from " + tablename
        # print(deletesql)
        self.cur.execute(deletesql)
        all = len(values)
        counter = 1
        for data in values:
            process = counter * 100 // all
            sql = "INSERT INTO " + tablename + " VALUES" + str(data)
            self.cur.execute(sql)
            print('\r ' + tablename + '信息入库进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), end='')
            counter += 1
        print('\n', end='')

        self.db.commit()

    def search_singlesql(self,sql):

        self.cur.execute(sql)
        data = self.cur.fetchall()
        return data

    def end_action(self):
        self.cur.close()
        self.db.close()