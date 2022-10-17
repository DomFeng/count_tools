# -*- coding: utf-8 -*-

import pymysql
import configuration.setting as Setting

def searchposition():
    result = {}
    db = pymysql.connect(host=Setting.host, port=Setting.port, user=Setting.user, password=Setting.password, database=Setting.database,
                         charset="utf8")
    cursor = db.cursor()
    sql = "select stkid,tdTotalOpenQty from  bside_portfolio bp where productNum in ('1','2','3')"
    cursor.execute(sql)
    data = cursor.fetchall()
    for singledata in data:
        if singledata[0] not in result:
            result[singledata[0]] = []
        result[singledata[0]].append(singledata[1])

    cursor.close()
    db.close()
    return result

a = searchposition()
for key in a:
    if a[key][0] != a[key][1] or a[key][0] != a[key][2] or a[key][2] != a[key][1]:
        print(key,' 证券 tdTotalOpenQty 不一致')


