# -*- coding: utf-8 -*-
import os,time
import yaml
import copy
import pymysql
import sqlite3
#################修改项#######################
from orderlog.insertdata import insert
from orderlog.getlogbak import getbacklog
#################修改项#######################
from getsmltlog import getsmlog
from orderlog.change_log import changelog,putlog,mklog,mklog_mc,mk_certain_log
import configuration.setting as Setting
import configuration.fixlog_setting as fix_Setting


class searchmysql(object):
    def __init__(self):
        self.host=Setting.host
        self.port=Setting.port
        self.user=Setting.user
        self.password=Setting.password
        self.database=Setting.database

    """委托表订单总笔数数"""
    def search_allopenorder(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorder where regid in {} and ordertime > '{}'".format(
            str(tuple(reglist)).replace(",)", ")"), ordertime)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return int(data[0][0])

    """委托表明细表撤单总笔数"""
    def search_allcancelorder(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorderdetail where OrderFlag = 'D' and regid in {} and ordertime > '{}'".format(
            str(tuple(reglist)).replace(",)", ")"), ordertime)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return int(data[0][0])

    """委托表非法总笔数"""
    def search_allrejectedorder(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorder where statuscode = 'Rejected' and regid in {} and ordertime > '{}'".format(
            str(tuple(reglist)).replace(",)", ")"), ordertime)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return int(data[0][0])
    """成交表成交笔数"""
    def search_allfillnum(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from tradingresult where regid in {} and occurtime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        cursor.execute(sql)
        data = cursor.fetchall()[0]
        cursor.close()
        db.close()
        return data[0]

    """挂单笔数"""
    def search_allpending(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorder where statuscode in ('Pending_Dealing','Partially_Filled') and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        cursor.execute(sql)
        data = cursor.fetchall()[0]
        cursor.close()
        db.close()
        return data[0]
    """完结总数"""
    def search_over(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorder where statuscode in ('Fully_Filled','Rejected') and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        cursor.execute(sql)
        data = cursor.fetchall()[0]
        cursor.close()
        db.close()
        return data[0]

    """
    查询mysql 数据库 获取对应股东代码 的持仓信息 
    { (股东代码 , 证券代码) : [今日可用,今日累计买入成交数,今日买入在途数,今日卖出成交数,今日卖出冻结数]}
    """

    def search_tradingnum_time(self,reglist):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*),min(occurtime),max(occurtime) from tradingresult where regid in " + str(tuple(reglist))
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return data[0]

    """
    查询mysql 数据库 获取对应股东代码 的持仓信息 
    { (股东代码 , 证券代码) : [今日可用,今日累计买入成交数,今日买入在途数,今日卖出成交数,今日卖出冻结数]}
    """

    def searchposition(self,reglist):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for regid in reglist:
            sql = "select regid,stkid,ydUsableqty,tdusableqty,tdtotalopenqty,tdopeningqty,tdtotalcloseqty,YDCloseFrozenQty from BSide_Portfolio where regid = '" + str(
                regid) + "'"
            cursor.execute(sql)
            data = cursor.fetchall()
            for x in data:
                result[(x[0], x[1])] = [x[2], x[3], x[4], x[5], x[6], x[7]]
        cursor.close()
        db.close()
        return result

    """
    查询mysql 数据库 删除股东列表对应委托信息
    """

    def dropopenorder(self,reglist):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for regid in reglist:
            sql = "delete from openorder where regid = '" + str(regid) + "'"
            cursor.execute(sql)
            sql = "delete from openorderdetail where regid = '" + str(regid) + "'"
            cursor.execute(sql)
            sql = "delete from tradingresult where regid = '" + str(regid) + "'"
            cursor.execute(sql)
        cursor.close()
        db.commit()
        db.close()
        return result

    """
    查询mysql 数据库 删除股东列表对应持仓
    """

    def dropposition(self,reglist):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for regid in reglist:
            sql1 = "delete from BSide_Portfolio where regid = '" + str(regid) + "'"
            print(sql1)
            cursor.execute(sql1)

        cursor.close()
        db.commit()
        db.close()
        return result

    """
    查询mysql 数据库 获取对应股东代码 的 所有挂单信息 
    { 合同号 ： [价格,委托数量,成交数量,撤单数量,(股东代码,证券代码),买卖方向]}
    """

    def searchopenorder(self,reglist, ordertime):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for regid in reglist:
            # sql = "select o.ContractNum,o.orderPrice,o.orderQty,o.regId,o.stkId,o.BsType,o.knockQty,o.StatusCode from openorder o where Statuscode in ('Pending_Dealing','Registered') and regid = '" + str(regid) +"' and ordertime > '" + ordertime + "'"
            sql = "select o.ContractNum,o.orderPrice,o.orderQty,o.regId,o.stkId,o.BsType,o.knockQty,o.StatusCode from openorder o where Statuscode in ('Registered') and regid = '" + str(
                regid) + "' and ordertime > '" + ordertime + "'"

            cursor.execute(sql)
            data = cursor.fetchall()
            for x in data:
                if x[5] == 'B':
                    bs = 1
                else:
                    bs = 2
                result[x[0]] = [float(x[1]), int(x[2]), 0, 0, (x[3], x[4]), bs]
        cursor.close()
        db.close()
        return result

    """
    查询数据库，根据合同号 得到 委托价格、委托数量、成交数量、撤单数量
    """

    def openorder_N(self,cont):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select o.ContractNum,o.orderPrice,o.orderQty,o.knockQty,o.withdrawQty,o.StatusCode from openorder o where contractnum = '" + cont + "'"
        cursor.execute(sql)
        data = cursor.fetchall()
        result = list(data[0])
        result[1] = float(result[1])
        cursor.close()
        db.close()
        return result

    """
    获取委托表所有数据
    """

    def openorder_all(self,reglist, ordertime):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for regid in reglist:
            sql = "select o.ContractNum,o.orderPrice,o.orderQty,o.knockQty,o.withdrawQty,o.StatusCode from openorder o where regid = '" + str(regid) + "' and ordertime > '" + ordertime + "'"
            # print(sql)
            cursor.execute(sql)
            data = cursor.fetchall()
            for x in data:
                result[x[0]] = [x[0], float(x[1]), x[2], x[3], x[4], x[5]]
        cursor.close()
        db.close()
        return result

    """
    获取成交表所有数据
    """

    def tradingresult_all(self,reglist):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for regid in reglist:
            sql = "select t.ContractNum,t.BsType,t.reckoningAmt,t.knockQty,t.stampTax,t.tradetransFee,t.transRuleFee,t.commision from tradingresult t where regid = '" + regid + "'"
            cursor.execute(sql)
            data = cursor.fetchall()
            for x in data:
                # 合同号：买卖方向，成交总金额，成交数量，印花税，过户费，交易规费，手续费
                if x[0] not in result:
                    result[x[0]] = [x[p] for p in range(8)]
                else:
                    for y in range(2, 8):
                        result[x[0]][y] += x[y]
        cursor.close()
        db.close()
        return result

    """
    查询mysql 数据库 获取资金账号信息 可用金额，冻结金额 
    { 资金账号 ：[可用金额 , 冻结金额]}
    """

    def searchaccount(self,acctidlist):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for acctid in acctidlist:
            sql = "select b.acctid,b.UsableAmt,b.tradeFrozenAmt from bside_asset b where acctid = '" + acctid + "'"
            cursor.execute(sql)
            data = cursor.fetchall()
            result[data[0][0]] = [float(data[0][1]), float(data[0][2])]

        cursor.close()
        db.close()
        return result

    """
    更新压测账户初始金额
    """

    def updateaccount(self,acctidlist):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        for acctid in acctidlist:
            sql = "update bside_asset set currentAmt = 1000000000.00,UsableAmt = 1000000000.00,tradeFrozenAmt = 0.00 where acctid = '" + acctid + "'"
            print(sql)
            # print(sql)
            cursor.execute(sql)
        cursor.close()
        db.commit()
        db.close()

    """
    获取委托表行数
    """

    def searchopenorderline(self,reglist, ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorder where regid in {} and ordertime > '{}'".format(
            str(tuple(reglist)).replace(",)", ")"), ordertime)
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return int(data[0][0])



"""
获取yaml 配置文件信息
"""
class yamldata(object):
    def __init__(self):
        #################修改项#######################
        self.yamldir = Setting.settingpath
        self.search = searchmysql()

        """回放前mysql数据库相关持仓数据"""
        self.positionsource = {}
        self.accountlist = []
        self.reglist = []
        self.acct_reg = {}
        self.acct_usab_frozen = {}
        self.reg_pos_data = {}
        self.fee_set = {}
        self.getdata()

    """ 
    从配置文件获取相关数据（资金账号、股东、收费标准）
    根据mysql数据库数据获取资金账号初始数据
    获取mysql数据库持仓初始数据
    """
    def getdata(self):
        with open(self.yamldir,'r',encoding='utf-8') as yaml_r:
            settingdata = yaml.load(yaml_r, Loader=yaml.FullLoader)
        feedata = settingdata['fee']
        self.acctdata = settingdata['account']
        self.accountlist = self.acctdata
        posdata = settingdata['position']
        for acctid in posdata:
            self.acct_reg[acctid] = []
            for regid in posdata[acctid]:
                self.acct_reg[acctid].append(regid)

        self.acct_usab_frozen = self.search.searchaccount(self.acctdata)

        for feetype in feedata:
            self.fee_set[feetype] = [float(x) for x in feedata[feetype].split()]

        for x in posdata:
            for y in posdata[x]:
                self.reglist.append(y)

        self.positionsource = self.search.searchposition(self.reglist)

        print(self.acctdata)
        print(self.accountlist)
        print(self.acct_usab_frozen)
        print(self.acct_reg)

        # print(self.acct_reg)

    def update_account(self):
        if (input(' --- 更新压测账户初始资金(仅限输入yes更新):') == 'yes'):
            self.search.updateaccount(self.acctdata)
        else:
            pass

    def update_account_imme(self):
        self.search.updateaccount(self.acctdata)




"""
主函数 -- 分析计算结果
"""
class get_info_tools(object):
    # from datetime import datetime
    def __init__(self):
        from datetime import datetime
        with open('./logbacktime.log','a',encoding='utf-8') as f:
            self.ordertime = datetime.now().strftime("%Y%m%d%H%M%S")
            self.getbacklog = getbacklog()
            self.getbacklog.drop()
            self.getsmlog = getsmlog()
            self.logdir = './log_list/'
            print(' --- 获取初始数据' + '\n' + ' ...')
            self.frontdata()
            print(' --- occurtime : ',self.ordertime)
            print(' --- occurtime : ',self.ordertime,file=f)

            # print('\n --- 全部持仓插入成功')
            print(' --- 初始数据获取结束')

    """
    根据回放结果（撮合日志）,从从上一部获取的资金、持仓数据中获取相关数据、并对添加上一步未查到的持仓数据（默认设置各项为0）
    """
    def start(self):
        self.logdata = []
        self.getlist()
        self.get_data()
        self.changedreg_stk = []
        self.changedacct = []
        self.orderdata = {}
        self.change()
        self.orderchangge = []
    """
    根据配置文件 的 资金账号、股东账号 获取相关初始资金、持仓数据
    """
    def frontdata(self):
        self.data = yamldata()
        self.search = self.data.search

        self.feeset = self.data.fee_set
        self.acct_reg = self.data.acct_reg

        self.feeset2 = copy.deepcopy(self.feeset)
        self.feeset2['印花税'] = [0.0, 0.0]
        self.reglist = []
        for acctid in self.acct_reg:
            for reg in self.acct_reg[acctid]:
                self.reglist.append(reg)

        print(self.reglist)

        # """
        # 删除委托表数据？
        self.getsmlog.restart()

        print(' --- 深圳撮合重启成功')
        if input(' --- 是否需要一键删除资金持仓并重新插入更新(仅限输入yes确认)：') == 'yes':
            self.search.dropopenorder(self.reglist)
            print(' --- 委托成交数据已删除 ---')
            self.search.dropposition(self.reglist)
            print(' --- 持仓表数据已删除 ---')
            self.insert = insert()
            self.insert.insertdata('SZ', 1)
            self.insert.insertdata('SS', 0)
            self.insert.insertdata_se('SZ', 1)
            self.insert.insertdata_se('SS', 0)
            self.data.update_account_imme()
        else:
            if input(' --- 是否需要清除委托成交数据(仅限输入yes清除)：') == 'yes':
                self.search.dropopenorder(self.reglist)
                print(' --- 委托成交数据已删除 ---')
            else:
                pass
            if input(' --- 是否需要清除持仓数据(仅限输入yes清除)：') == 'yes':
                self.search.dropposition(self.reglist)
                print(' --- 持仓表数据已删除 ---')
            else:
                pass

            self.insert = insert()
            self.line = self.insert.line
            if input(' --- 是否需要插入持仓数据(仅限输入yes插入)：') == 'yes':
                self.insert.insertdata('SZ',1)
                self.insert.insertdata('SS',0)
                self.insert.insertdata_se('SZ',1)
                self.insert.insertdata_se('SS',0)

            else:
                pass
            self.data.update_account()

        self.data = yamldata()
        # self.data.update_account()
        self.accountlist = self.data.accountlist
        self.datasource = self.data.positionsource
        self.acct_reg = self.data.acct_reg

        # print(self.acct_reg)

        self.acct_usab_frozen = self.data.acct_usab_frozen
        self.reg_pos_data = self.data.reg_pos_data

        # self.getsmlog.restart()
        # print(' --- 深圳撮合重启成功')

    """
    获取撮合日志路径、以及路径下的文件
    """
    def getlist(self):
        Catalog_file = os.listdir(self.logdir) #返回绝对路径
        self.filelist = [self.logdir + x for x in Catalog_file]

    """
    解析撮合日志数据 -- 规范格式 
    E:\learnwork\tools\count_tools\log_list\orderSmltLog.log
    """
    def get_data(self):
        # result = []
        # with open ( './log_list/orderSmltLog.log','r',encoding='utf-8') as log:
        #     for line in log:
        #         listdata = line.strip().split()
        #         if len(listdata) == 14:
        #             plus = listdata[9] + ' '+ listdata[10]
        #             ture_list = listdata[:9:]
        #             ture_list.append(plus)
        #             ture_list.extend(listdata[11::])
        #             result.append(ture_list)
        #         else:
        #             result.append(line.strip().split())
        # self.logdata = result
        #
        # return result
        self.logdata = self.getsmlog.getlog()
        return self.logdata

    """
    依据self.logdata 生成 合同号:订单数据 的字典
    """
    # def change(self):
    #     with open('problem.log','w',encoding= 'utf-8') as p:
    #         for data in self.logdata:
    #             # print(data)
    #             contractnum,reg,stk,type,bs,price,ordernum,fillnum,surplusnum = data[0],data[10],data[1],data[9],int(data[6]),float(data[3]),int(data[4]),int(data[5]),int(data[7])
    #             # contractnum,reg,stk,type,bs,price,ordernum,fillnum,surplusnum = data[0],data[10],data[1],data[9],int(data[7]),float(data[3]),int(data[4]),int(data[5]),int(data[6])
    #             if reg not in self.reglist:
    #                 continue
    #             if contractnum not in self.orderdata:
    #                 if type != 'Canceled':
    #                     self.orderdata[contractnum] = [0,ordernum,0,0,0,0]
    #             if type == 'Reject':
    #                 self.orderdata[contractnum][3] = ordernum
    #                 self.orderdata[contractnum][4] = (reg,stk)
    #             if type in ['FILL','Partially FILL']:
    #                 self.orderdata[contractnum][0] = price
    #                 self.orderdata[contractnum][1] = ordernum
    #                 self.orderdata[contractnum][2] += fillnum
    #                 self.orderdata[contractnum][4] = (reg,stk)
    #                 self.orderdata[contractnum][5] = bs
    #             if type in ['Canceled']:
    #                 if contractnum not in self.orderdata:
    #                     self.orderdata[contractnum] = [price,ordernum,0,fillnum,(reg,stk),bs]
    #                 else:
    #                     self.orderdata[contractnum][3] += fillnum
    #         orderline = self.orderdata.__len__()
    #
    #         print(self.reglist)
    #
    #         # 判断是否都落库了
    #         if orderline != 0:
    #             while True:
    #                 print('\r 等待app写库: {0}{1}%'.format('▉'*(searchopenorderline(self.reglist,self.ordertime)//1000),(min(100.00,round(searchopenorderline(self.reglist,self.ordertime) / orderline * 100,2)))),end='')
    #                 time.sleep(0.5)
    #                 if searchopenorderline(self.reglist,self.ordertime) >= orderline:
    #                     print('\r 等待app写库: {0}{1}%'.format('▉' * (searchopenorderline(self.reglist,self.ordertime) // 1000),100.00))
    #                     break
    #         num = self.line // 250
    #         if num != 0:
    #             for x in range(num + 1):
    #                 print('\r 等待委托状态更改: {0}{1}%'.format('▉' * x , min(100.00,round(x * (100 / num),2))),end = '')
    #                 time.sleep(1.5)
    #         print('\n',end = '')
    #         """添加日志"""
    #         for cont in self.orderdata:
    #             print(cont,':',self.orderdata[cont],file = p)
    #
    #         """搜索mysql数据库委托表信息 增加挂单以及registered的委托数据"""
    #         extend_data = searchopenorder(self.reglist,self.ordertime)
    #         self.orderdata.update(extend_data)
    #
    #         """添加日志"""
    #         for cont in self.orderdata:
    #             print('添加挂单数据--',cont,':',self.orderdata[cont],file = p)
    #
    #         """遍历委托数据字典,从前置的持仓数据中获取相关信息,并给前置没有相关数据的设置默认值0"""
    #         for x in self.orderdata:
    #             if self.orderdata[x][4] in self.datasource:
    #                 self.reg_pos_data[self.orderdata[x][4]] = self.datasource[self.orderdata[x][4]]
    #             else:
    #                 self.reg_pos_data[self.orderdata[x][4]] = [0,0,0,0,0]
    #
    #         """遍历获取相关股东的所有委托信息 输出字典{合同号：各项数据}"""
    #         self.all_openorder = openorder_all(self.reglist,self.ordertime)
    #
    #         """添加日志"""
    #         for cont in self.orderdata:
    #             print('所有委托数据--',cont,':',self.orderdata[cont],file = p)
    #
    #         self.all_tradingresult = tradingresult_all(self.reglist)

    """
    乱序撮合
    """
    def change(self):
        """撮合确认笔数"""
        self.szch_Confirmnum = 0
        """撮合非法笔数"""
        self.szch_Rejectednum = 0
        """成交笔数（分笔时分几笔是几笔）"""
        self.szch_Fillnum = 0
        """全成交笔数"""
        self.szch_Fullfillednum = 0
        """撤单笔数"""
        self.szch_Calcelnum = 0
        with open('problem.log','w',encoding= 'utf-8') as p:
            for data in self.logdata:

                contractnum,reg,stk,type,bs,price,ordernum,fillnum,surplusnum = data[0],data[10],data[1],data[9],int(data[7]),float(data[3]),int(data[4]),int(data[5]),int(data[6])
                if reg not in self.reglist:
                    continue
                if contractnum not in self.orderdata:
                    if type != 'Canceled':
                        self.orderdata[contractnum] = [price,ordernum,0,0,(reg,stk),bs]
                if type == 'Reject':
                    self.orderdata[contractnum][3] = ordernum
                    self.orderdata[contractnum][4] = (reg,stk)
                    self.szch_Rejectednum += 1
                if type in ['Partially FILL']:
                    self.orderdata[contractnum][0] = price
                    self.orderdata[contractnum][1] = ordernum
                    self.orderdata[contractnum][2] += fillnum
                    self.orderdata[contractnum][4] = (reg,stk)
                    self.orderdata[contractnum][5] = bs
                    self.szch_Fillnum += 1
                if type in ['FILL']:
                    self.orderdata[contractnum][0] = price
                    self.orderdata[contractnum][1] = ordernum
                    self.orderdata[contractnum][2] += fillnum
                    self.orderdata[contractnum][4] = (reg,stk)
                    self.orderdata[contractnum][5] = bs
                    self.szch_Fillnum += 1
                    self.szch_Fullfillednum += 1

                if type in ['Confirm']:
                    self.szch_Confirmnum += 1
                if type in ['Canceled']:
                    self.szch_Calcelnum += 1
                    if contractnum not in self.orderdata:
                        self.orderdata[contractnum] = [price,ordernum,0,surplusnum,(reg,stk),bs]
                    else:
                        self.orderdata[contractnum][3] += surplusnum
            orderline = self.orderdata.__len__()

            print(' ---',self.reglist)
            # # 判断是否开始回放
            # F_num_time = ()
            # countnum = 0
            # countnum1 = 0
            # while countnum1 <= 1500:
            #     N_num_time = search_tradingnum_time(self.reglist)
            #     if N_num_time != F_num_time:
            #         countnum += 1
            #         countnum1 = 0
            #     if countnum >= 1:
            #         print('\r --- 回放开始',N_num_time,countnum1,'--> 1500',end = '')
            #         time.sleep(0.01)
            #     if countnum > 1 and N_num_time == F_num_time:
            #         print('\r --- 回放开始',N_num_time,countnum1,'--> 1500',end = '')
            #         time.sleep(0.01)
            #         countnum1 += 1
            #     F_num_time = N_num_time

            # 判断回放是否结束

            # if orderline != 0:
            #     while True:
            #         print('\r 等待app写库: {0}{1}%'.format('▉'*(searchopenorderline(self.reglist,self.ordertime)//1000),(min(100.00,round(searchopenorderline(self.reglist,self.ordertime) / orderline * 100,2)))),end='')
            #         time.sleep(0.5)
            #         if searchopenorderline(self.reglist,self.ordertime) >= orderline:
            #             print('\r 等待app写库: {0}{1}%'.format('▉' * (searchopenorderline(self.reglist,self.ordertime) // 1000),100.00))
            #             break
            # num = self.line // 250
            # if num != 0:
            #     for x in range(num + 1):
            #         print('\r 等待委托状态更改: {0}{1}%'.format('▉' * x , min(100.00,round(x * (100 / num),2))),end = '')
            #         time.sleep(1.5)

            print('\n',end = '')
            """添加日志"""
            for cont in self.orderdata:
                print(cont,':',self.orderdata[cont],file = p)

            """搜索mysql数据库委托表信息 增加挂单以及registered的委托数据"""
            extend_data = self.search.searchopenorder(self.reglist,self.ordertime)
            # self.orderdata.update(extend_data)
            for contnum in extend_data:
                if contnum not in self.orderdata:
                    self.orderdata[contnum] = extend_data[contnum]
            """添加日志"""
            for cont in self.orderdata:
                print('添加挂单数据--',cont,':',self.orderdata[cont],file = p)

            """遍历委托数据字典,从前置的持仓数据中获取相关信息,并给前置没有相关数据的设置默认值0"""
            for x in self.orderdata:
                if self.orderdata[x][4] in self.datasource:
                    self.reg_pos_data[self.orderdata[x][4]] = self.datasource[self.orderdata[x][4]]
                else:
                    self.reg_pos_data[self.orderdata[x][4]] = [0,0,0,0,0,0]

            for reg_stk in self.datasource:
                if reg_stk not in self.reg_pos_data:
                    self.reg_pos_data[reg_stk] = self.datasource[reg_stk]

            """遍历获取相关股东的所有委托信息 输出字典{合同号：各项数据}"""
            self.all_openorder = self.search.openorder_all(self.reglist,self.ordertime)

            """添加日志"""
            for cont in self.orderdata:
                print('所有委托数据--',cont,':',self.orderdata[cont],file = p)

            self.all_tradingresult = self.search.tradingresult_all(self.reglist)


    '''
    price : 委托价格
    orderqty ： 委托数量
    knockqty ： 成交数量
    withdrawqty ：撤单数量
    bs： 买卖方向 
    '''

    def get_fee(self,price,orderqty,knockqty,withdrawqty,bs):
        price = round(price,2)
        # 订单状态终结 （全部成交/未成交部分全部撤单）
        ifmin = 0
        if orderqty == (knockqty + withdrawqty):
            # fee = float(0)
            # if knockqty > 0:
            #     for feetype in self.feeset2:
            #         bili = knockqty * price * self.feeset2[feetype][0]
            #         min = self.feeset2[feetype][1]
            #         if min > bili:
            #             fee += min
            #             ifmin = 1
            #         else:
            #             fee += round(bili,2)
            if bs == 0:
                acct_usable_change = 0  # 资金可用金额变化
                acct_frozen_change = 0  # 资金冻结变化
                pos_ydusable_change = 0   # 持仓可用数量变化
                pos_usable_change = 0   # 持仓可用数量变化
                pos_going_change = 0    # 持仓买入在途变化
                pos_totalopen_change = 0# 持仓买入成交变化
                pos_totalclose_change = 0 # 持仓卖出成交变化
                pos_frozen_change = 0   # 持仓卖出冻结变化
            if bs == 1:
                fee = float(0)
                if knockqty > 0:
                    for feetype in self.feeset2:
                        bili = knockqty * price * self.feeset2[feetype][0]
                        min = self.feeset2[feetype][1]
                        if min > bili:
                            fee += min
                            ifmin = 1
                        else:
                            fee += round(bili,2)
                acct_usable_change = -(fee + price * knockqty)
                acct_frozen_change = 0
                pos_ydusable_change = 0  # 持仓可用数量变化
                pos_usable_change = 0  # 持仓可用数量变化
                pos_going_change = 0
                pos_totalopen_change = knockqty# 持仓买入成交变化
                pos_totalclose_change = 0 # 持仓卖出成交变化
                pos_frozen_change = 0
            if bs == 2:
                fee = float(0)
                if knockqty > 0:
                    for feetype in self.feeset:
                        bili = knockqty * price * self.feeset[feetype][0]
                        min = self.feeset[feetype][1]
                        if min > bili:
                            fee += min
                            ifmin = 1
                        else:
                            fee += round(bili,2)

                acct_usable_change = price * knockqty - fee
                acct_frozen_change = 0
                pos_ydusable_change = - knockqty   # 持仓可用数量变化
                pos_usable_change = 0   # 持仓可用数量变化
                pos_going_change = 0
                pos_totalopen_change = 0         # 持仓买入成交变化
                pos_totalclose_change = knockqty # 持仓卖出成交变化
                pos_frozen_change = 0
        # 订单状态未终结 ，部分成交，剩余部分未撤单
        elif orderqty > (knockqty + withdrawqty):
            if bs == 0:
                acct_usable_change = 0
                acct_frozen_change = 0
                pos_ydusable_change = 0  # 持仓可用数量变化
                pos_usable_change = 0  # 持仓可用数量变化
                pos_going_change = 0
                pos_totalopen_change = 0         # 持仓买入成交变化
                pos_totalclose_change = 0 # 持仓卖出成交变化
                pos_frozen_change = 0
            if bs == 1:
                # fee = float(0)
                # for feetype in self.feeset2:
                #     fee += max([orderqty * price * self.feeset2[feetype][0], self.feeset2[feetype][1]])

                fee = float(0)
                if knockqty > 0:
                    for feetype in self.feeset:
                        bili = orderqty * price * self.feeset[feetype][0]
                        min = self.feeset[feetype][1]
                        if min > bili:
                            fee += min
                            ifmin = 1
                        else:
                            fee += round(bili, 2)


                acct_usable_change = -(fee + price * orderqty)
                acct_frozen_change = (fee + price * orderqty) * (orderqty - knockqty) / orderqty
                pos_ydusable_change = 0  # 持仓可用数量变化
                pos_usable_change = 0  # 持仓可用数量变化
                pos_going_change = orderqty - knockqty
                pos_totalopen_change = knockqty         # 持仓买入成交变化
                pos_totalclose_change = 0 # 持仓卖出成交变化
                pos_frozen_change = 0
            if bs == 2:
                if knockqty == 0:
                    # fee = float(0)
                    # for feetype in self.feeset:
                    #     fee += max(round(orderqty * price * self.feeset[feetype][0],2), self.feeset[feetype][1])

                    fee = float(0)
                    if knockqty > 0:
                        for feetype in self.feeset:
                            bili = orderqty * price * self.feeset[feetype][0]
                            min = self.feeset[feetype][1]
                            if min > bili:
                                fee += min
                                ifmin = 1
                            else:
                                fee += round(bili, 2)

                    acct_usable_change = knockqty * price - fee * knockqty / orderqty
                    acct_frozen_change = 0
                    pos_ydusable_change = - orderqty  # 持仓可用数量变化
                    pos_usable_change = 0  # 持仓可用数量变化
                    pos_going_change = 0
                    pos_totalopen_change = 0         # 持仓买入成交变化
                    pos_totalclose_change = knockqty # 持仓卖出成交变化
                    pos_frozen_change = orderqty - knockqty
                else:
                    fee = float(0)
                    if knockqty > 0:
                        for feetype in self.feeset:
                            bili = knockqty * price * self.feeset[feetype][0]
                            min = self.feeset[feetype][1]
                            if min > bili:
                                fee += round(min * knockqty / orderqty,2)
                                ifmin = 1

                            else:
                                fee += round(bili, 2)
                    acct_usable_change = knockqty * price - fee
                    acct_frozen_change = 0
                    pos_ydusable_change = - orderqty  # 持仓可用数量变化
                    pos_usable_change = 0  # 持仓可用数量变化
                    pos_going_change = 0
                    pos_totalopen_change = 0         # 持仓买入成交变化
                    pos_totalclose_change = knockqty # 持仓卖出成交变化
                    pos_frozen_change = orderqty - knockqty
        if  orderqty == withdrawqty:
            acct_usable_change = 0  # 资金可用金额变化
            acct_frozen_change = 0  # 资金冻结变化
            pos_ydusable_change = 0  # 持仓可用数量变化
            pos_usable_change = 0  # 持仓可用数量变化
            pos_going_change = 0  # 持仓买入在途变化
            pos_totalopen_change = 0  # 持仓买入成交变化
            pos_totalclose_change = 0  # 持仓卖出成交变化
            pos_frozen_change = 0  # 持仓卖出冻结变化
        return  [round(acct_usable_change,2),round(acct_frozen_change,2),pos_ydusable_change,pos_usable_change,pos_totalopen_change,pos_going_change,pos_totalclose_change,pos_frozen_change,ifmin]

    """
    根据委托结果字典 ,计算最终结果
    """
    def changedata(self):
        with open('change.log','w',encoding='utf-8') as f:
            orderline = self.orderdata.__len__()
            counter = 1
            for contractnum in self.orderdata:
                try:
                    turedata = self.all_openorder[contractnum]
                except:
                    print(contractnum,'数据库中无相关订单！！！！！！！！！！！！！！！',file = f)
                    continue
                singledata = []
                print(contractnum,file = f)
                print('    变化：',file = f)

                singledata.append(contractnum)
                data = self.orderdata[contractnum]
                res = self.get_fee(data[0],data[1],data[2],data[3],data[5])
                for acctid in self.acct_reg:
                    if data[4][0] in self.acct_reg[acctid]:
                        true_acctid = acctid
                        break
                self.acct_usab_frozen[true_acctid][0] += res[0]
                self.acct_usab_frozen[true_acctid][1] += res[1]
                self.acct_usab_frozen[true_acctid][0] = round(self.acct_usab_frozen[true_acctid][0],2)
                self.acct_usab_frozen[true_acctid][1] = round(self.acct_usab_frozen[true_acctid][1],2)
                print('      资金账号：' + true_acctid ,file= f)
                print('         可用: ' + str(res[0]),file= f)
                print('         冻结: ' + str(res[1]),file= f)

                singledata.append(true_acctid)
                for acct_reg in self.reg_pos_data:
                    if acct_reg == data[4]:
                        self.reg_pos_data[acct_reg][0] += res[2]
                        self.reg_pos_data[acct_reg][1] += res[3]

                        self.reg_pos_data[acct_reg][2] += res[4]
                        self.reg_pos_data[acct_reg][3] += res[5]
                        self.reg_pos_data[acct_reg][4] += res[6]
                        self.reg_pos_data[acct_reg][5] += res[7]
                        print('       股东： ' + acct_reg[0] , file=f)
                        print('         证券代码： ' + acct_reg[1] + ' 持仓:' , file=f)
                        print('             昨日可用     ：' + str(res[2]),file= f)
                        print('             今日可用     ：' + str(res[3]),file= f)

                        print('             今日买入成交数：' + str(res[4]),file= f)
                        print('             今日买入冻结数：' + str(res[5]),file= f)
                        print('             今日卖出成交数：' + str(res[6]),file= f)
                        print('             今日卖出冻结数：' + str(res[7]),file= f)
                        allnum = orderline
                        process = counter * 100 // allnum
                        bili = min(round(process,2),100.00)
                        print('\r 当前委托计算进度: {0}{1}%'.format('▉' * process,bili),contractnum,'信息已录入', end = '')
                        # time.sleep(0.0001)
                        counter += 1
                        singledata.append(acct_reg[0])
                        singledata.append(acct_reg[1])
                        singledata.append(res[0])
                        singledata.append(res[1])
                        singledata.append(res[2])
                        singledata.append(res[3])
                        singledata.append(res[4])
                        singledata.append(res[5])
                        singledata.append(res[6])
                        singledata.append(res[7])

                        singledata.append(res[8])


                singledata.extend(self.orderdata[contractnum][1:4:])
                singledata.append(turedata[1])
                singledata.append(turedata[2])
                singledata.append(turedata[3])
                singledata.append(turedata[4])
                singledata.append(turedata[5])
                if self.orderdata[contractnum][1:4:] == turedata[2:5:]:
                    singledata.append(0)
                else:
                    singledata.append(1)

                if contractnum in self.all_tradingresult:
                    singledata.append(self.all_tradingresult[contractnum][1])
                    singledata.append(float(self.all_tradingresult[contractnum][2]))
                    singledata.append(float(self.all_tradingresult[contractnum][2]) - res[0])
                else:
                    singledata.extend(['B/S',0,0])
                self.orderchangge.append(tuple(singledata))

        """从数据库获取交易相关证券的stktype和tradetype"""
        # self.allposdata = self.dbdata.search_posiinfo(self.reglist)


if __name__ == '__main__':

    ifchangelog = fix_Setting.ifChangelog
    if ifchangelog == 'yes':
        changenum = fix_Setting.changeNum
        cloridadd = fix_Setting.addTag
        repeat = fix_Setting.loopNum
        ifyunsu = str(fix_Setting.playbackPattern)
        if ifyunsu == '2':
            timeus = fix_Setting.orderInterval
        elif ifyunsu == '3':
            timeus = fix_Setting.orderInterval
            singlenum = fix_Setting.singleNum
            time_jg = fix_Setting.singleInterval
            if not fix_Setting.ifCertainType:
                try:
                    mklog_mc(changelog(int(changenum), int(repeat), cloridadd), int(timeus),int(singlenum),int(time_jg))
                except:
                    print('更新回放日志失败')

            else:
                mklog_mc(mk_certain_log(fix_Setting.exchId,
                                        fix_Setting.stkId,
                                        fix_Setting.orderPrice,
                                        fix_Setting.orderQty,
                                        fix_Setting.bsFlag,
                                        fix_Setting.ifCancel,
                                        fix_Setting.changeNum,
                                        fix_Setting.loopNum,
                                        fix_Setting.addTag),
                         int(timeus),
                         int(singlenum),
                         int(time_jg))
        else:
            timeus = 0
        if ifyunsu in ['2']:
            if not fix_Setting.ifCertainType:

                if len(cloridadd) == 0:
                    try:
                        mklog(changelog(int(changenum),int(repeat)),int(timeus))
                    except:
                        print('更新回放日志失败')
                else:
                    try:
                        mklog(changelog(int(changenum),int(repeat),cloridadd),int(timeus))
                    except:
                        print('更新回放日志失败')
            else:
                mklog(mk_certain_log(fix_Setting.exchId,
                                        fix_Setting.stkId,
                                        fix_Setting.orderPrice,
                                        fix_Setting.orderQty,
                                        fix_Setting.bsFlag,
                                        fix_Setting.ifCancel,
                                        fix_Setting.changeNum,
                                        fix_Setting.loopNum,
                                        fix_Setting.addTag),
                         int(timeus))
        if ifyunsu in ['1']:
            if len(cloridadd) == 0:
                try:
                    mklog(changelog(int(changenum), int(repeat)), int(timeus))
                except:
                    print('更新回放日志失败')
            else:
                try:
                    mklog(changelog(int(changenum), int(repeat), cloridadd), int(timeus))
                except:
                    print('更新回放日志失败')
        putlog()

    result = get_info_tools()
    print(' --- 等待app重启...')
    print(' --- 等待日志回放...')

    # 判断是否开始回放
    F_num_time = ()
    countnum = 0
    countnum1 = 0
    while countnum1 <= 4000:
        N_num_time = result.search.search_tradingnum_time(result.reglist)
        if N_num_time != F_num_time:
            countnum += 1
            countnum1 = 0
        if countnum >= 1:
            print('\r --- 回放开始', N_num_time, countnum1, '--> 4000', end='')
            time.sleep(0.01)
        if countnum > 1 and N_num_time == F_num_time:
            print('\r --- 回放开始', N_num_time, countnum1, '--> 4000', end='')
            time.sleep(0.01)
            countnum1 += 1
        F_num_time = N_num_time

    input('\n --- 手动暂停')

    result.start()

    acct_red = result.acct_reg
    begin_acct_usable_frozen = copy.deepcopy(result.acct_usab_frozen)
    begin_reg_pos_data = copy.deepcopy(result.reg_pos_data)


    """深复制保留初始数据"""

    front_acct_reg = copy.deepcopy(result.acct_reg)
    front_acct_usable_frozen = copy.deepcopy(result.acct_usab_frozen)
    front_reg_data = copy.deepcopy(result.reg_pos_data)

    """执行费用以及持仓计算"""
    result.changedata()
    end_acct_usable_frozen = result.acct_usab_frozen
    end_reg_pos_data = result.reg_pos_data
    print('\n --- 计算结束')


    """ 当前mysql库中的资金表相关数据 """
    nowaccountdata = result.search.searchaccount(result.accountlist)
    """ 当前mysql库中的持仓表相关数据 """
    nowpositiondata = result.search.searchposition(result.reglist)
    # stkinfo = result.allposdata()
    """要插入的资金数据"""
    acctdata = []
    """要插入的持仓数据"""
    posdata = []

    """获取内存相关数据（资金、持仓）"""
    nc_insert = []
    try:
        nc = getbacklog()
        nc_data = nc.search_nc()
        nc_acctdata = nc_data[0]
        for acctid in nc_acctdata:
            A = acctid
            B = nc_acctdata[acctid][0]
            C = nc_acctdata[acctid][1]
            D = nc_acctdata[acctid][2]
            E = nc_acctdata[acctid][3]
            nc_insert.append((A,B,C,D,E))
    except:
        print('内存日志获取资金可用冻结失败')
    for acctid in front_acct_usable_frozen:
        a,b,c,d = front_acct_usable_frozen[acctid][0],front_acct_usable_frozen[acctid][1],result.acct_usab_frozen[acctid][0],result.acct_usab_frozen[acctid][1]
        e = nowaccountdata[acctid][0]
        f = nowaccountdata[acctid][1]
        g = float(c - e)
        h = float(d - f)
        if a == c and b == d:
            symbol = 0
        else:
            symbol = 1
        acctdata.append((acctid,a,b,e,f,c,d,g,h,symbol))

    for reg_stk in nowpositiondata:
        singledata = []
        try:
            if front_reg_data[reg_stk] == result.reg_pos_data[reg_stk]:
                symbol = 0
            else:
                symbol = 1
        except:
            symbol = 1
        for acctid in front_acct_reg:
            if reg_stk[0] in front_acct_reg[acctid]:
                singledata.append(acctid)
                break
        singledata.append(reg_stk[0])
        singledata.append(reg_stk[1])
        try:
            singledata.extend(front_reg_data[reg_stk])
        except:
            singledata.extend([0,0,0,0,0,0])

        singledata.extend(nowpositiondata[reg_stk])
        try:
            singledata.extend(result.reg_pos_data[reg_stk])
        except:
            singledata.extend([0,0,0,0,0,0])
        d_data = []
        for x in range(6):
            try:
                d_data.append(result.reg_pos_data[reg_stk][x] - nowpositiondata[reg_stk][x])
            except:
                d_data.append(0)
        singledata.extend(d_data)
        try:
            if result.reg_pos_data[reg_stk] == nowpositiondata[reg_stk]:
                compareflag = 0
            else:
                compareflag = 1
        except:
            compareflag = 1
        singledata.append(symbol)
        singledata.append(compareflag)
        # singledata.append()
        posdata.append(tuple(singledata))

    # 创建与数据库的连接
    conn1 = sqlite3.connect('test.db')
    # 创建游标
    cur1 = conn1.cursor()

    """
    初始化sqlite3数据（删除相关表）
    nc_acctdata
    """
    sql_frush1 = 'delete from account_compare'
    sql_frush2 = 'delete from position_compare'
    sql_frush3 = 'delete from order_change'
    sql_frush4 = 'delete from nc_acctdata'

    cur1.execute(sql_frush1)
    cur1.execute(sql_frush2)
    cur1.execute(sql_frush3)
    cur1.execute(sql_frush4)

    """
    插入数据 （资金变化、持仓变化、每笔委托明细）
    """
    all0 = len(nc_insert)
    counter = 1
    for data in nc_insert:
        process = counter * 100 // all0

        sql = "INSERT INTO nc_acctdata VALUES" + str(data)
        cur1.execute(sql)
        print('\r 内存资金入库进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), end='')
        counter += 1
    print('\n', end='')
    all1 = len(acctdata)
    counter = 1
    for data in acctdata:
        process = counter * 100 // all1

        sql = "INSERT INTO account_compare VALUES" + str(data)
        cur1.execute(sql)
        print('\r 资金对比入库进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),end = '')
        counter += 1
    print('\n',end = '')
    all2 = len(posdata)
    counter = 1
    for data in posdata:
        process = counter * 100 // all2

        sql = "INSERT INTO position_compare VALUES" + str(data)
        cur1.execute(sql)
        print('\r 持仓对比入库进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),end = '')
        # time.sleep(0.001)
        counter += 1

    print('\n',end = '')
    all3 = len(result.orderchangge)
    counter = 1
    for data in result.orderchangge:
        process = counter * 100 // all3

        sql = "INSERT INTO order_change VALUES" + str(data)
        cur1.execute(sql)
        print('\r 委托数据入库进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),end = '')
        counter += 1

    conn1.commit()
    # 关闭游标
    cur1.close()
    # 关闭连接
    conn1.close()