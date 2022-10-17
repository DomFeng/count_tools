# -*- coding: utf-8 -*-
import pymysql
import configuration.setting as Setting

class searchmysql(object):
    def __init__(self):
        """
        search mysql or act sql and commit
        """
        self.host=Setting.host
        self.port=Setting.port
        self.user=Setting.user
        self.password=Setting.password
        self.database=Setting.database

        self.db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        self.cursor = self.db.cursor()

    #执行单条sql
    def act_single_sql(self,sql):
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        self.db.commit()
        return data

    #执行多条sql
    def act_any_sql(self,sqllist):
        for sql in sqllist:
            self.cursor.execute(sql)
        self.db.commit()

    """
    查询mysql 数据库 获取对应股东代码 的持仓信息 
    { (股东代码 , 证券代码) : [今日可用,今日累计买入成交数,今日买入在途数,今日卖出成交数,今日卖出冻结数]}
    """

    def search_tradingnum_time(self,reglist):
        if len(reglist) == 1:
            sql = "select count(*),min(occurtime),max(occurtime) from tradingresult where regid = '" + str(reglist[0]) + "'"
        else:
            sql = "select count(*),min(occurtime),max(occurtime) from tradingresult where regid in " + str(tuple(reglist))
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data[0]

    """
    查询mysql 数据库 获取对应股东代码 的持仓信息 
    { (股东代码 , 证券代码 ,买卖方向) : [今日可用,今日累计买入成交数,今日买入在途数,今日卖出成交数,今日卖出冻结数]}
    """

    def searchposition(self,reglist):
        result = {}
        for regid in reglist:
            sql = "select regid,stkid,bsflag,ydUsableqty,tdusableqty,tdtotalopenqty,tdopeningqty,tdtotalcloseqty,TDCloseFrozenQty from BSide_Portfolio where regid = '" + str(
                regid) + "'"
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            for x in data:
                result[(x[0],x[1],x[2])] = [x[3], x[4], x[5], x[6], x[7],x[8]]
        return result


    """
    查询mysql 数据库 获取资金账号信息 可用金额，冻结金额 
    { 资金账号 ：[可用金额 , 冻结金额]}
    """
    def searchaccount(self,acctidlist):
        result = {}
        for acctid in acctidlist:
            sql = "select b.acctid,b.UsableAmt,b.tradeFrozenAmt from bside_asset b where acctid = '" + acctid + "'"
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            result[data[0][0]] = [float(data[0][1]), float(data[0][2])]

        return result

    """
    更新压测账户初始金额
    """
    def updateaccount(self,acctidlist):
        for acctid in acctidlist:
            sql = "update bside_asset set currentAmt = 10000000000000.00,UsableAmt = 10000000000000.00,tradeFrozenAmt = 0.00 where acctid = '" + acctid + "'"
            print(sql)
            # print(sql)
            self.cursor.execute(sql)
        self.db.commit()

    """
    查询mysql 数据库 删除股东列表对应持仓
    """

    def dropposition(self,reglist):

        for regid in reglist:
            sql1 = "delete from BSide_Portfolio where regid = '" + str(regid) + "'"
            print(sql1)
            self.cursor.execute(sql1)

        self.db.commit()

    """
    查询mysql 数据库 删除股东列表对应委托信息
    """
    def dropopenorder(self,reglist):

        for regid in reglist:
            sql = "delete from openorder where regid = '" + str(regid) + "'"
            self.cursor.execute(sql)
            sql = "delete from openorderdetail where regid = '" + str(regid) + "'"
            self.cursor.execute(sql)
            sql = "delete from tradingresult where regid = '" + str(regid) + "'"
            self.cursor.execute(sql)
        self.db.commit()

    """查询委托表reject合同号列表"""
    def search_rejlist(self,reglist,ordertime):
        result = []

        sql = "select o.ContractNum  from openorderdetail o where StatusCode = 'Rejected' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        for singledata in data:
            result.append(singledata[0])

        return result

    """查询委托表详细数据"""
    def search_alldata(self,reglist,ordertime):
        result = {}

        sql = "select ContractNum,orderQty,knockQty,withdrawQty from openorder where regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        for singledata in data:
            result[singledata[0]] = singledata[1::]

        return result

    """委托表订单总笔数数"""
    def search_allopenorder(self,reglist,ordertime):

        sql = "select count(*) from openorder where statuscode != 'Registered' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()

        return int(data[0][0])

    """委托表非法总笔数"""
    def search_allrejectedorder(self,reglist,ordertime):

        sql = "select count(*) from openorder where statuscode = 'Rejected' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()

        return int(data[0][0])

    """委托表明细表撤单总笔数"""
    def search_allcancelorder(self,reglist,ordertime):

        sql = "select count(*) from openorderdetail where OrderFlag = 'D' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()

        return int(data[0][0])

    """委托表订单撤成总笔数"""
    def search_allcancelled(self,reglist,ordertime):

        sql = "select count(*) from openorder where statuscode like '%celled' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()

        return int(data[0][0])

    """成交表成交笔数"""
    def search_allfillnum(self,reglist,ordertime):
        occurtimelen = len(ordertime)
        truetime = ''
        for index in range(occurtimelen):
            if index == 8 and ordertime[8] == '0':
                pass
            else:
                truetime += ordertime[index]

        print(truetime)
        sql = "select count(*) from tradingresult where regid in {} and occurtime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), truetime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()[0]

        return data[0]

    """成交表 {cont：num}"""
    def search_full_trading(self,reglist,truetime):
        res = []
        result = {}

        sql = "select contractnum from tradingresult where regid in {} and occurtime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), truetime)
        print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        for x in data:
            res.append(x[0])

        for x in res:
            if x in result:
                result[x] += 1
            else:
                result[x] = 1

        return result


    def end_action(self):
        self.cursor.close()
        self.db.close()