import pymysql
"""
修改回放日志、修改委托价格为可下单价格
"""
def search():
    result = {}
    db = pymysql.connect(host="172.20.0.124", port=3307, user="testpoc", password="testpoc", database="coredb-version",
                         charset="utf8")
    cursor = db.cursor()
    sql = "select exchid,stkid,minorderprice,maxorderprice,closeprice from stkinfo s where exchid in ('0','1')"
    cursor.execute(sql)
    data = cursor.fetchall()
    for x in data:
        if (x[0],x[1]) not in result:
            if x[4] < x[2] or x[4] > x[3]:
                result[(x[0], x[1])] = [x[2], x[3], (x[2] + x[3])/2]
            else:
                result[(x[0],x[1])] = [x[2], x[3], x[4]]
    cursor.close()
    db.close()
    return result
def changelog():
    pricedic = search()
    source = open('OrderAndCancelInfo_old.log')
    # source = open('fixlog103503.log','r',encoding='utf-8')
    targrt = open('OrderAndCancelInfo_newprice.log','w')
    dic = {'SS':'0','SZ':'1','ZK':'1','SH':'0'}
    result = {}
    for line in source.readlines():

        if '35=D' in line:
            stkidinfo = line.strip().split('55=')[1].split('')[0]
            stkid = stkidinfo.split('.')[0]
            try:
                exch = dic[line.strip().split('207=')[1].split('')[0]]
            except:
                pass
            price = line.strip().split('44=')[1].split('')[0]
            try:
                res = line.strip().replace('44=' + price,'44=' + str(pricedic[(exch,stkid)][2]))
                print(res,file = targrt)
            except:
                print(line.strip(), file=targrt)

        else:
            print(line.strip(),file = targrt)

    for x in result:
        result[x] = set(result[x])

    source.close()
    targrt.close()
    print('价格修改完成')
    return result


if __name__ == '__main__':
    a = changelog()

