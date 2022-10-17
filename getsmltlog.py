import paramiko,shutil,time,openpyxl,pymysql,sqlite3
from orderlog.getlogbak import getbacklog
import configuration.setting as Setting
import  os

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
        #########################
        sql = "select count(*) from openorder where statuscode != 'Registered' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        ###########################
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return int(data[0][0])

    """成交表 {cont：num}"""

    def search_full_trading(self, reglist, truetime):
        res = []
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                             database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        #########################
        sql = "select contractnum from tradingresult where regid in {} and occurtime > '{}'".format(
            str(tuple(reglist)).replace(",)", ")"), truetime)
        print(sql)
        ###########################
        cursor.execute(sql)
        data = cursor.fetchall()
        for x in data:
            res.append(x[0])

        for x in res:
            if x in result:
                result[x] += 1
            else:
                result[x] = 1

        cursor.close()
        db.close()
        return result

    """委托表订单撤成总笔数"""
    def search_allcancelled(self,reglist,ordertime):
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from openorder where statuscode like '%celled' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
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
        sql = "select count(*) from openorderdetail where OrderFlag = 'D' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
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
        sql = "select count(*) from openorder where statuscode = 'Rejected' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        db.close()
        return int(data[0][0])
    """成交表成交笔数"""
    def search_allfillnum(self,reglist,ordertime):
        occurtimelen = len(occurtime)
        truetime = ''
        for index in range(occurtimelen):
            if index == 8 and ordertime[8] == '0':
                pass
            else:
                truetime += ordertime[index]

        print(truetime)
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select count(*) from tradingresult where regid in {} and occurtime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), truetime)
        print(sql)
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
        print(sql)
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
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()[0]
        cursor.close()
        db.close()
        return data[0]
    """查询委托表详细数据"""
    def search_alldata(self,reglist,ordertime):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select ContractNum,orderQty,knockQty,withdrawQty from openorder where regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            for singledata in data:
                result[singledata[0]] = singledata[1::]
            cursor.close()
            db.close()
            return result
        except:
            print('exec err')

    """查询委托明细表合约 - 对冲关联数据"""
    def search_se(self,reglist,ordertime):
        result = {}
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select o.ContractNum ,o.clordId from openorderdetail o where clordid != 'NULL' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for singledata in data:
            result[singledata[1]] = singledata[0]
        cursor.close()
        db.close()
        return result

    """查询委托表reject合同号列表"""
    def search_rejlist(self,reglist,ordertime):
        result = []
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        sql = "select o.ContractNum  from openorderdetail o where StatusCode = 'Rejected' and regid in {} and ordertime > '{}'".format(str(tuple(reglist)).replace(",)", ")"), ordertime)
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for singledata in data:
            result.append(singledata[0])
        cursor.close()
        db.close()
        return result

class getsmlog(object):
    def __init__(self):
        self.hostname = Setting.sz_hostname
        self.port = Setting.sz_port
        self.username = Setting.sz_username
        self.password = Setting.sz_password
        self.path = Setting.sz_path

        self.getbacklog  = getbacklog()
        self.search = searchmysql()
        self.szchdata = {}
        self.reglist = Setting.reglist
        self.reglist_A = Setting.reglist_A
        self.reglist_B = Setting.reglist_B
        self.reglist_C = Setting.reglist_C

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
    """获取撮合日志、规范格式"""
    def getlog(self):
        hostname = self.hostname
        port = self.port
        username = self.username
        password = self.password

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, port, username, password, compress=True)
        stdin, stdout, stderr = client.exec_command('cd '+ self.path +';ls')
        logdata = stdout.read().decode('utf-8').split()
        print(logdata)
        for data in logdata:
            if 'orderSmltLog' in data:
                logname = data

        print(logname)

        path = self.path + logname
        sftp_client = client.open_sftp()

        remote_file = sftp_client.open(path)  # 文件路径
        result = []
        for line in remote_file:

            listdata = line.strip().split()
            if listdata[10] == 'FILL':
                plus = listdata[9] + ' ' + listdata[10]
                ture_list = listdata[:9:]
                ture_list.append(plus)
                ture_list.extend(listdata[11::])
                result.append(ture_list)
            else:
                result.append(line.strip().split())

        client.close()
        return result
    """重启撮合"""
    def restart(self):
        hostname = self.hostname
        port = self.port
        username = self.username
        password = self.password

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, port, username, password, compress=True)
        stdin, stdout, stderr = client.exec_command('ps -ef | grep SzTgwSmlt')
        piddata = stdout.read().decode('utf-8').split()
        print(piddata)
        try:
            for num in range(len(piddata)):
                if piddata[num] == './SzTgwSmlt':
                    if 'pts' in piddata[num - 2] or '?' in piddata[num - 2]:
                        pid = piddata[num - 6]
            print(' --- 原撮合进程：',pid,'')
            client.exec_command('cd ' + self.path + ';kill -9 ' + pid)
        except:
            print("原撮合未启动")
        client.exec_command('cd ' + self.path + ';rm -rf orderSmltLog.log.*;rm -rf tgwSmlt.log.*')
        ###################################################################################

        # client.exec_command('cd ' + self.path + ';./sz-tgw.sh start')
        # client.exec_command('cd ' + self.path + ';./sz-tgw.sh start')
        client.exec_command('cd ' + self.path + ';./sz-tgw-smlt-service.sh start')

        time.sleep(3)
        stdin, stdout, stderr = client.exec_command('ps -ef | grep SzTgwSmlt')
        piddata = stdout.read().decode('utf-8').split()
        # print(piddata)
        for num in range(len(piddata)):
            if piddata[num] == './SzTgwSmlt':
                if 'pts' in piddata[num - 2] or '?' in piddata[num - 2]:
                    pid = piddata[num - 6]
        print(' --- 当前撮合进程：',pid,'')
        client.close()
    """分析撮合日志"""
    def change(self):
        self.logdata = self.getlog()
        """统计 成交数 {cont：成交分笔数}"""
        self.szch_filldata = {}

        for data in self.logdata:

            contractnum,reg,stk,type,bs,price,ordernum,fillnum,surplusnum = data[0],data[10],data[1],data[9],int(data[7]),float(data[3]),int(data[4]),int(data[5]),int(data[6])
            if reg not in self.reglist:
            #if reg not in ['','','']
                continue
            if contractnum not in self.szchdata:
                if type != 'Canceled':
                    self.szchdata[contractnum] = [price,ordernum,0,0,(reg,stk),bs]
            if type == 'Reject':
                self.szchdata[contractnum][3] = ordernum
                self.szchdata[contractnum][4] = (reg,stk)
                self.szch_Rejectednum += 1
            if type in ['Partially FILL']:
                self.szchdata[contractnum][0] = price
                self.szchdata[contractnum][1] = ordernum
                self.szchdata[contractnum][2] += fillnum
                self.szchdata[contractnum][4] = (reg,stk)
                self.szchdata[contractnum][5] = bs
                self.szch_Fillnum += 1
                if contractnum not in self.szch_filldata:
                    self.szch_filldata[contractnum] = 0
                    self.szch_filldata[contractnum] += 1
                else:
                    self.szch_filldata[contractnum] += 1
            if type in ['FILL']:
                self.szchdata[contractnum][0] = price
                self.szchdata[contractnum][1] = ordernum
                self.szchdata[contractnum][2] += fillnum
                self.szchdata[contractnum][4] = (reg,stk)
                self.szchdata[contractnum][5] = bs
                self.szch_Fillnum += 1
                self.szch_Fullfillednum += 1

                if contractnum not in self.szch_filldata:
                    self.szch_filldata[contractnum] = 0
                    self.szch_filldata[contractnum] += 1
                else:
                    self.szch_filldata[contractnum] += 1

            if type in ['Confirm']:
                self.szch_Confirmnum += 1
            if type in ['Canceled']:
                self.szch_Calcelnum += 1
                if contractnum not in self.szchdata:
                    self.szchdata[contractnum] = [price,ordernum,0,surplusnum,(reg,stk),bs]
                else:
                    self.szchdata[contractnum][3] += surplusnum

        print("撮合确认笔数", self.szch_Confirmnum)
        print("撮合非法笔数", self.szch_Rejectednum)
        print("撮合确认 + 非法",self.szch_Confirmnum + self.szch_Rejectednum)
        print("成交笔数（分笔时分几笔是几笔）", self.szch_Fillnum)
        print("全成交笔数", self.szch_Fullfillednum)
        print("撤单笔数", self.szch_Calcelnum)

        return self.szchdata
    """获取撮合日志记录 订单 委托 - 成交 - 撤单 - 在途 总QTY"""
    def get_szch_numdata(self):
        self.szch_all_orderqty = 0
        self.szch_all_knockqty = 0
        self.szch_all_cancelqty = 0
        self.szch_all_doingqty = 0
        print('撮合委托集合长度:',self.szchdata.__len__())
        data = self.szchdata
        for cont in data:
            self.szch_all_orderqty += data[cont][1]
            self.szch_all_knockqty += data[cont][2]
            self.szch_all_cancelqty += data[cont][3]
            if data[cont][1] != data[cont][2] + data[cont][3]:
                self.szch_all_doingqty += (data[cont][1] - data[cont][2])

    """获取回访工具日志记录 订单 委托 - 成交 - 撤单 - 在途 总QTY"""
    def get_backlog_numdata(self):
        self.backlog_all_orderqty = 0
        self.backlog_all_knockaty = 0
        self.backlog_all_cancelqty = 0
        self.backlog_all_doingqty = 0

        self.backdata = self.getbacklog.analysis()

        self.errnum_D = self.getbacklog.errnum_D
        self.acknum_D = self.getbacklog.acknum_D
        self.errnum_F = self.getbacklog.errnum_F
        self.acknum_F = self.getbacklog.acknum_F

        self.backlog_confirmnum  =  self.getbacklog.confirmnum
        self.backlog_pendingcancelnum = self.getbacklog.pendingcancelnum
        self.backlog_cancelnum = self.getbacklog.cancelednum
        self.backlog_fillnum = self.getbacklog.fillnum
        self.backlog_allfillnum = self.getbacklog.allfillnum
        self.backlog_rejectnum = self.getbacklog.rejectednum

        self.backlog_rejectlist = self.getbacklog.rejectlist

        self.cancel_errlist = self.getbacklog.cancel_errlist

        data = self.backdata

        for cont in data:
            self.backlog_all_orderqty += data[cont][0]
            self.backlog_all_knockaty += data[cont][1]
            self.backlog_all_cancelqty += data[cont][2]

            if data[cont][0] != data[cont][1] + data[cont][2]:
                self.backlog_all_doingqty += (data[cont][0] - data[cont][1])

    """获取数据库记录 订单 委托 - 成交 - 撤单 - 在途 总QTY"""
    def get_db_numdata(self,occurtime):
        self.db_all_orderqty = 0
        self.db_all_knockqty = 0
        self.db_all_cancelqty = 0
        self.db_all_doingqty = 0
        self.db_A_orderqty = 0
        self.db_A_knockqty = 0
        self.db_A_cancelqty = 0
        self.db_A_doingqty = 0
        self.db_B_orderqty = 0
        self.db_B_knockqty = 0
        self.db_B_cancelqty = 0
        self.db_B_doingqty = 0
        self.db_C_orderqty = 0
        self.db_C_knockqty = 0
        self.db_C_cancelqty = 0
        self.db_C_doingqty = 0

        time = occurtime

        self.db_rejlist = self.search.search_rejlist(self.reglist,time)

        self.dbdata = self.search.search_alldata(self.reglist,time)

        self.dbdata_A = self.search.search_alldata(self.reglist_A,time)
        try:
            self.dbdata_B = self.search.search_alldata(self.reglist_B,time)
        except:
            pass
        try:
            self.dbdata_C = self.search.search_alldata(self.reglist_C,time)
        except:
            pass

        self.db_confirmnum_A = self.search.search_allopenorder(self.reglist_A,time)
        self.db_rejectnum_A = self.search.search_allrejectedorder(self.reglist_A,time)
        self.db_cancelnum_A = self.search.search_allcancelorder(self.reglist_A,time)
        self.db_cancellednum_A = self.search.search_allcancelled(self.reglist_A,time)
        self.db_fillnum_A = self.search.search_allfillnum(self.reglist_A,time)

        try:
            self.db_confirmnum_B = self.search.search_allopenorder(self.reglist_B, time)
            self.db_rejectnum_B = self.search.search_allrejectedorder(self.reglist_B, time)
            self.db_cancelnum_B = self.search.search_allcancelorder(self.reglist_B, time)
            self.db_cancellednum_B = self.search.search_allcancelled(self.reglist_B, time)
            self.db_fillnum_B = self.search.search_allfillnum(self.reglist_B, time)
        except:
            pass

        try:
            self.db_confirmnum_C = self.search.search_allopenorder(self.reglist_C, time)
            self.db_rejectnum_C = self.search.search_allrejectedorder(self.reglist_C, time)
            self.db_cancelnum_C = self.search.search_allcancelorder(self.reglist_C, time)
            self.db_cancellednum_C = self.search.search_allcancelled(self.reglist_C, time)
            self.db_fillnum_C = self.search.search_allfillnum(self.reglist_C, time)
        except:
            pass

        self.db_se = self.search.search_se(self.reglist,time)

        self.db_all_fill = self.search.search_full_trading(self.reglist,time)


        data = self.dbdata

        for cont in data:
            self.db_all_orderqty += data[cont][0]
            self.db_all_knockqty += data[cont][1]
            self.db_all_cancelqty += data[cont][2]

            if data[cont][0] != data[cont][1] + data[cont][2]:
                self.db_all_doingqty += (data[cont][0] - data[cont][1])

        data = self.dbdata_A

        for cont in data:
            self.db_A_orderqty += data[cont][0]
            self.db_A_knockqty += data[cont][1]
            self.db_A_cancelqty += data[cont][2]

            if data[cont][0] != data[cont][1] + data[cont][2]:
                self.db_A_doingqty += (data[cont][0] - data[cont][1])
        try:
            data = self.dbdata_B
            for cont in data:
                self.db_B_orderqty += data[cont][0]
                self.db_B_knockqty += data[cont][1]
                self.db_B_cancelqty += data[cont][2]

                if data[cont][0] != data[cont][1] + data[cont][2]:
                    self.db_B_doingqty += (data[cont][0] - data[cont][1])
        except:
            pass

        try:
            data = self.dbdata_C
            for cont in data:
                self.db_C_orderqty += data[cont][0]
                self.db_C_knockqty += data[cont][1]
                self.db_C_cancelqty += data[cont][2]

                if data[cont][0] != data[cont][1] + data[cont][2]:
                    self.db_C_doingqty += (data[cont][0] - data[cont][1])
        except:
            pass

    """获取回放源日志委托、撤单总数"""
    def get_backsourcedata(self):
        self.source_ordernum = 0
        self.source_cancelnum = 0
        self.source_order_clordid_list = []
        self.source_cancel_clordid_list = []
        with open('orderlog/OrderAndCancelInfo.log','r',encoding='utf-8') as f:
            for line in f.readlines():
                linedata = line.strip()
                if '35=D' in linedata:
                    self.source_ordernum += 1
                    self.source_order_clordid_list.append(linedata.split('11=')[1].split('')[0])
                if '35=F' in linedata:
                    self.source_cancelnum += 1
                    self.source_cancel_clordid_list.append(linedata.split('11=')[1].split('')[0])

        return [self.source_ordernum,self.source_cancelnum]
    """打印回放日志有，撮合没有的"""
    def printdata_backerr(self):
        pass

"""查询sqlite库，获取报告所需数据"""
class get_sqlitedata(object):
    def __init__(self):
        self.sqlitedb_path = 'test.db'
    """查询内存以及数据库以及计算所得资金数据"""
    def acctdata_search(self):
        result = []
        conn1 = sqlite3.connect(self.sqlitedb_path)
        cur1 = conn1.cursor()

        sql_frush1 = 'select a.ACCTID,a.N_USABLEAMT,a.N_FROZENAMT,a.F_USABLEAMT,a.F_FROZENAMT,n.N_nc_usableamt,n.N_nc_frozenamt,n.F_nc_usableamt,n.F_nc_frozenamt,a.C_USABLEAMT - a.F_USABLEAMT,a.C_FROZENAMT - a.F_FROZENAMT from account_compare a INNER JOIN nc_acctdata n where a.ACCTID = n.ACCTID ORDER BY a.ACCTID'

        cur1.execute(sql_frush1)
        data = cur1.fetchall()
        for x in data:
            result.append(list(x))
        conn1.commit()
        # 关闭游标
        cur1.close()
        # 关闭连接
        conn1.close()
        return result

    """查询持仓相关数据"""
    def posdata_search(self):
        result = []
        conn1 = sqlite3.connect(self.sqlitedb_path)
        cur1 = conn1.cursor()

        sql = '''select 
            regid, sum(N_ydusableqty), SUM(N_usableqty), sum(N_openqty), SUM(N_openingqty), SUM(N_closeqty), SUM(
            N_closefrozenQTY), sum(F_ydusableqty), SUM(F_usableqty), sum(F_openqty), SUM(F_openingqty), SUM(
            F_closeqty), SUM(F_closefrozenQTY), sum(C_ydusableqty), SUM(c_usableqty), sum(c_openqty), SUM(
            c_openingqty), SUM(c_closeqty), SUM(c_closefrozenQTY)
            FROM
            position_compare
            GROUP
            BY
            regid'''
        cur1.execute(sql)
        data = cur1.fetchall()
        for x in data:
            result.append(list(x))
        conn1.commit()
        # 关闭游标
        cur1.close()
        # 关闭连接
        conn1.close()
        return result

    def get_sqlite_cont(self):
        result = {}
        conn1 = sqlite3.connect(self.sqlitedb_path)
        cur1 = conn1.cursor()
        sql_frush1 = 'select CONTRACTNUM,T_STATUSCODE from order_change'
        cur1.execute(sql_frush1)
        data = cur1.fetchall()
        for x in data:
            result[x[0]] = x[1]
        conn1.commit()
        # 关闭游标
        cur1.close()
        # 关闭连接
        conn1.close()
        return result

def getpos(dic,list):
    result = {}
    for reg_stk in dic:
        if reg_stk in list:
            if reg_stk[0] not in result:
                result[reg_stk[0]] = [int(nn) for nn in dic[reg_stk]]
            else:
                for num in range(5):
                    result[reg_stk[0]][num] += int(dic[reg_stk][num])

    return result

def get_sqlite_reg_stk():
    conn1 = sqlite3.connect('test.db')
    # 创建游标
    cur1 = conn1.cursor()

    sql_frush1 = 'select regid,stkid from position_compare'
    cur1.execute(sql_frush1)
    data = cur1.fetchall()

    conn1.commit()
    # 关闭游标
    cur1.close()
    # 关闭连接
    conn1.close()
    return data

def get_reg_stk_value():
    res = {}
    conn1 = sqlite3.connect('test.db')
    # 创建游标
    cur1 = conn1.cursor()

    sql_frush1 = "select pc.REGID ,pc.STKID ,pc.F_ydusableqty ,N_ydusableqty from position_compare pc;"
    cur1.execute(sql_frush1)
    data = cur1.fetchall()
    for singledata in data:
        regid = singledata[0]
        stkid = singledata[1]
        f_ydusableqty = singledata[2]
        n_ydusableqty = singledata[3]
        res[(regid,stkid)] = [f_ydusableqty,n_ydusableqty]
    conn1.commit()
    # 关闭游标
    cur1.close()
    # 关闭连接
    conn1.close()
    return res


if __name__ == "__main__":
    with open('logbacktime.log','r',encoding='utf-8') as log:
        for line in log:
            occurtime = line.split('  ')[-1].strip()
        print(occurtime)


    list1 = get_sqlite_reg_stk()

    res = getsmlog()
    res_back = res.getbacklog
    res.change()
    res.get_szch_numdata()
    res.get_backlog_numdata()
    res.get_backsourcedata()

    try:
        res.get_db_numdata(occurtime)
    except:
        pass

    res_back.search_doing()
    doingdata = res_back.doingdata

    sqlite_data = get_sqlitedata()
    sqlite_acct = sqlite_data.acctdata_search()
    sqlite_pos = sqlite_data.posdata_search()

    sqlite_cont = sqlite_data.get_sqlite_cont()

    acct_put = [2, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18]
    pos_put = [2, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 51, 52, 53, 54, 55, 56, 33, 34, 35, 36, 38, 39, 40, 41, 42, 44]

    doingqty = 0
    for cont in doingdata:
        if cont in sqlite_cont:
            doingqty += doingdata[cont][0] - doingdata[cont][1]



    print('####################################################')
    print('###############  内存在途',doingqty,'  ################')
    print('####################################################')


    t = time.strftime("%Y-%m-%d", time.localtime())

    if len(res.reglist_B) == 0 and len(res.reglist_C) == 0:

        log = open('log/errlog.txt', 'w', encoding='utf-8')
        for cont in res.dbdata_A:
            if res.dbdata_A[cont][0] != res.dbdata_A[cont][1] + res.dbdata_A[cont][2]:
                if cont not in doingdata:
                    print(cont, "数据库状态未完结,内存在途未查到", file=log)

        for cont in res.szchdata:
            if cont not in res.dbdata_A:
                # print(cont,'撮合已搓,数据库无记录')
                print(cont,'撮合已搓,数据库无记录',file=log)

        for cont in sqlite_cont:
            if sqlite_cont[cont] in ['Pending_Dealing', 'Partially_Filled','Partially_Pending_Cancel','Pending_Cancel']:
                if cont not in doingdata:
                    print('## 内存在途未记录 - contractnum - ', cont, ' - ##',file = log)

        for cont in res.backdata:
            if cont not in res.szchdata:
                print(cont,res.backdata[cont],'回放工具有记录,数据库无记录',file = log)

        for cont in res.dbdata_A:
            try:
                if list(res.dbdata_A[cont]) != res.backdata[cont]:
                    print(cont,'数据库',res.dbdata_A[cont],'-- 回调',res.backdata[cont],'不一致', file = log)
            except:
                print(cont,'回调未收到回报',file = log)

        for cont in res.szchdata:
            if cont not in res.backdata:
                print(' back 无记录',cont,res.szchdata[cont],file = log)

        for cont in res.szchdata:
            try:
                if res.backdata[cont] != list(res.dbdata_A[cont]):
                    print(cont,res.backdata[cont],res.dbdata_A[cont],"回放工具记录与数据库不一致",file = log)
                    print(cont,res.backdata[cont],res.dbdata_A[cont],"回放工具记录与数据库不一致")
            except:
                print(cont,'回放工具未记录或数据库未记录')

        log.close()

        oldfile = 'log_list/dailyreport/dailyreport_V5.xlsx'
        newfile = 'log_list/dailyreport/dailyreport' + t + '.xlsx'

        id = 1

        while True:
            if not os.path.exists(newfile):
                print(newfile)
                break
            else:
                newfile = newfile.split(t)[0] + t + '_' + str(id) + '.xlsx'
                id += 1
        shutil.copyfile(oldfile,newfile)
        wb = openpyxl.load_workbook(newfile)
        sheet = wb['测试报告-汇总']
        """模拟发单工具 --- 委托笔数（不含撤单）"""
        sheet['C28'] = res.source_ordernum
        """模拟发单工具 --- 撤单笔数"""
        sheet['J28'] = res.source_cancelnum
        """模拟发单工具 --- ACK笔数"""
        sheet['D28'] = res.acknum_D
        """模拟发单工具 --- 拒单笔数"""
        sheet['E28'] = res.errnum_D
        """模拟发单工具 --- 撤单笔数"""
        sheet['K28'] = res.acknum_F
        """模拟发单工具 --- 撤单拒单笔数"""
        sheet['L28'] = res.errnum_F
        """模拟发单工具 --- 委托合法笔数"""
        sheet['G28'] = res.backlog_confirmnum
        """模拟发单工具 --- 委托非法笔数"""
        sheet['H28'] = res.backlog_rejectnum
        """模拟发单工具 --- 成交总分笔数"""
        sheet['N28'] = res.backlog_allfillnum

        """深圳撮合 --- 成交总笔数"""
        sheet['N30'] = res.szch_Fillnum
        """深圳撮合 --- 委托总笔数"""
        sheet['C30'] = res.szch_Confirmnum + res.szch_Rejectednum
        """深圳撮合 --- 合法总笔数"""
        sheet['G30'] = res.szch_Confirmnum
        """深圳撮合 --- 非法总笔数"""
        sheet['H30'] = res.szch_Rejectednum
        """深圳撮合 --- 撤单总笔数"""
        sheet['J30'] = res.szch_Calcelnum
        """深圳撮合 --- 撤成总笔数"""
        sheet['K30'] = res.szch_Calcelnum

        """数据库 --- 成交总笔数"""
        sheet['N29'] = res.db_fillnum_A
        """数据库 --- 委托总笔数"""
        sheet['C29'] = res.db_confirmnum_A
        """数据库 --- 合法总笔数"""
        sheet['G29'] = res.db_confirmnum_A - res.db_rejectnum_A
        """数据库 --- 非法总笔数"""
        sheet['H29'] = res.db_rejectnum_A
        """数据库 --- 撤单总笔数"""
        sheet['J29'] = res.db_cancelnum_A
        """数据库 --- 撤成总笔数"""
        sheet['K29'] = res.db_cancellednum_A

        """交易数据量"""
        sheet['C35'] = res.backlog_all_orderqty
        sheet['D35'] = res.backlog_all_knockaty
        sheet['E35'] = res.backlog_all_cancelqty
        sheet['F35'] = res.backlog_all_doingqty

        sheet['C38'] = res.szch_all_orderqty
        sheet['D38'] = res.szch_all_knockqty
        sheet['E38'] = res.szch_all_cancelqty
        sheet['F38'] = res.szch_all_doingqty

        sheet['C37'] = res.db_all_orderqty
        sheet['D37'] = res.db_all_knockqty
        sheet['E37'] = res.db_all_cancelqty
        sheet['F37'] = res.db_all_doingqty

        sheet['F36'] = doingqty

        sheet.cell(6,3).value = t

        # clo = res_back.search_clord()
        # que = res_back.search_que()
        ncdata = res_back.search_nc()
        print(ncdata[1].__len__())
        print(ncdata[2].__len__())
        F_pos = getpos(ncdata[1],list1)
        N_pos = getpos(ncdata[2],list1)
        print('ncdata[1]',ncdata[1])
        print('ncdata[2]',ncdata[2])

        for x in F_pos:
            print(x,F_pos[x])

        for x in N_pos:
            print(x,N_pos[x])

        for x in range(len(sqlite_pos)):
            if sqlite_pos[x][0] in F_pos:
                sqlite_pos[x].extend(F_pos[sqlite_pos[x][0]])

        for x in range(len(sqlite_pos)):
            if sqlite_pos[x][0] in N_pos:
                sqlite_pos[x].extend(N_pos[sqlite_pos[x][0]])

        for line in range(len(sqlite_acct)):
            for index in range(len(acct_put)):
                sheet.cell(45 + line, acct_put[index]).value = sqlite_acct[line][index]

        for line1 in range(len(sqlite_pos)):
            for index1 in range(len(pos_put)):

                try:
                    sheet.cell(56 + line1, pos_put[index1]).value = sqlite_pos[line1][index1]
                except:
                    print(56 + line1,pos_put[index1],'write error')
        wb.save(newfile)
        wb.close()
        for cont in res.szch_filldata:
            try:
                if res.szch_filldata[cont] != res_back.logfullfilldic[cont]:
                    print('#################################',cont,res.szch_filldata[cont],res_back.logfullfilldic[cont])
            except:
                pass

        for cont in res.db_all_fill:
            if res.szch_filldata[cont] != res.db_all_fill[cont]:
                print('@#@#@#@#@#',cont,res.szch_filldata[cont],res.db_all_fill[cont])


        for cont in res.dbdata_A:
            if cont not in res.szchdata:
                print(cont,'数据库有信息，撮合未记录')

    elif len(res.reglist_C) == 0 and  len(res.reglist_B) != 0:

        log = open('log/errlog.txt', 'w', encoding='utf-8')
        for cont in res.db_se:
            if cont in res.dbdata_A:
                try:
                    if res.dbdata_A[cont] != res.dbdata_B[res.db_se[cont]]:
                        print(cont, res.db_se[cont], '不一致',file = log)
                    else:
                        print(cont, res.db_se[cont], '一致',file = log)
                except:
                    print(cont, 'error')

        hy_contlist = []
        dc_contlist = []
        for cont in res.dbdata_A:
            if cont not in res.db_se:
                # print(cont,'合约单无对应对冲单')
                print(cont,'合约单无对应对冲单',file=log)

        for cont in res.szchdata:
            if cont not in res.dbdata_B:
                # print(cont,'撮合已搓,数据库无记录')
                print(cont,'撮合已搓,数据库无记录',file=log)

        for cont in res.backdata:
            # print(cont,res.backdata[cont])

            if cont not in res.dbdata_A:
                print(cont,'回放工具收到回调，数据库无记录',file = log)

        for cont in res.dbdata_A:
            if cont not in res.backdata:
                print(cont,'数据库已记录，回放工具未记录',file = log)

        for cont in res.backdata:
            # print(cont,res.backdata[cont])
            # print(cont,res.dbdata[cont])
            try:
                if res.backdata[cont] != list(res.dbdata_A[cont]):
                    print(cont,res.backdata[cont],list(res.dbdata_A[cont]),'回放工具收到的回调与数据库记录不一致',file = log)
            except:
                print(cont,'回放工具有记录但是数据库未记录',file = log)

        for cont in res.backlog_rejectlist:
            if cont not in res.db_rejlist:
                print(cont,'回放工具状态为rejected，与数据库不一致',file = log)

        for cont_errcode in res.cancel_errlist:
            print(cont_errcode,file=log)

        log.close()

        oldfile = 'log_list/dailyreport/dailyreport_V5_se.xlsx'
        newfile = 'log_list/dailyreport/dailyreport' + t + '.xlsx'

        id = 1

        while True:
            if not os.path.exists(newfile):
                print(newfile)

                break
            else:
                newfile = newfile.split(t)[0] + t + '_' + str(id) + '.xlsx'
                id += 1

        shutil.copyfile(oldfile,newfile)
        wb = openpyxl.load_workbook(newfile)
        sheet = wb['测试报告-汇总']
        """模拟发单工具 --- 委托笔数（不含撤单）"""
        sheet['C27'] = res.source_ordernum
        """模拟发单工具 --- 撤单笔数"""
        sheet['J27'] = res.source_cancelnum
        """模拟发单工具 --- ACK笔数"""
        sheet['D27'] = res.acknum_D
        """模拟发单工具 --- 拒单笔数"""
        sheet['E27'] = res.errnum_D
        """模拟发单工具 --- 撤单笔数"""
        sheet['K27'] = res.acknum_F
        """模拟发单工具 --- 撤单拒单笔数"""
        sheet['L27'] = res.errnum_F
        """模拟发单工具 --- 委托合法笔数"""
        sheet['G27'] = res.backlog_confirmnum
        """模拟发单工具 --- 委托非法笔数"""
        sheet['H27'] = res.backlog_rejectnum
        """模拟发单工具 --- 成交总分笔数"""
        sheet['N27'] = res.backlog_allfillnum

        """深圳撮合 --- 成交总笔数"""
        sheet['N30'] = res.szch_Fillnum
        """深圳撮合 --- 委托总笔数"""
        sheet['C30'] = res.szch_Confirmnum + res.szch_Rejectednum
        """深圳撮合 --- 合法总笔数"""
        sheet['G30'] = res.szch_Confirmnum
        """深圳撮合 --- 非法总笔数"""
        sheet['H30'] = res.szch_Rejectednum
        """深圳撮合 --- 撤单总笔数"""
        sheet['J30'] = res.szch_Calcelnum
        """深圳撮合 --- 撤成总笔数"""
        sheet['K30'] = res.szch_Calcelnum
        """合约账户"""
        """数据库 --- 成交总笔数"""
        sheet['N28'] = res.db_fillnum_A
        """数据库 --- 委托总笔数"""
        sheet['C28'] = res.db_confirmnum_A
        """数据库 --- 合法总笔数"""
        sheet['G28'] = res.db_confirmnum_A - res.db_rejectnum_A
        """数据库 --- 非法总笔数"""
        sheet['H28'] = res.db_rejectnum_A
        """数据库 --- 撤单总笔数"""
        sheet['J28'] = res.db_cancelnum_A
        """数据库 --- 撤成总笔数"""
        sheet['K28'] = res.db_cancellednum_A

        """对冲账户"""
        """数据库 --- 成交总笔数"""
        sheet['N29'] = res.db_fillnum_B
        """数据库 --- 委托总笔数"""
        sheet['C29'] = res.db_confirmnum_B
        """数据库 --- 合法总笔数"""
        sheet['G29'] = res.db_confirmnum_B - res.db_rejectnum_B
        """数据库 --- 非法总笔数"""
        sheet['H29'] = res.db_rejectnum_B
        """数据库 --- 撤单总笔数"""
        sheet['J29'] = res.db_cancelnum_B
        """数据库 --- 撤成总笔数"""
        sheet['K29'] = res.db_cancellednum_B

        """交易数据量"""
        sheet['C35'] = res.backlog_all_orderqty
        sheet['D35'] = res.backlog_all_knockaty
        sheet['E35'] = res.backlog_all_cancelqty
        sheet['F35'] = res.backlog_all_doingqty

        sheet['C39'] = res.szch_all_orderqty
        sheet['D39'] = res.szch_all_knockqty
        sheet['E39'] = res.szch_all_cancelqty
        sheet['F39'] = res.szch_all_doingqty

        sheet['C37'] = res.db_A_orderqty
        sheet['D37'] = res.db_A_knockqty
        sheet['E37'] = res.db_A_cancelqty
        sheet['F37'] = res.db_A_doingqty

        sheet['C38'] = res.db_B_orderqty
        sheet['D38'] = res.db_B_knockqty
        sheet['E38'] = res.db_B_cancelqty
        sheet['F38'] = res.db_B_doingqty

        sheet['F36'] = doingqty

        sheet.cell(6,3).value = t

        ncdata = res_back.search_nc()

        print(ncdata[1].__len__())
        print(ncdata[2].__len__())
        F_pos = getpos(ncdata[1],list1)
        N_pos = getpos(ncdata[2],list1)
        print('ncdata[1]',ncdata[1])
        print('ncdata[2]',ncdata[2])

        db_reg_stk_value = get_reg_stk_value()
        # for reg_stk in ncdata[2]:
        #     if reg_stk not in db_reg_stk_value:
        #         print(reg_stk,ncdata[2][reg_stk],'sqlite数据库无记录')
        #     else:
        #         if int(ncdata[2][reg_stk][0]) != db_reg_stk_value[reg_stk][1]:
        #             print(reg_stk,db_reg_stk_value[reg_stk],
        #                   ncdata[2][reg_stk],'内存期末持仓与数据库不一致')

        # for reg_stk in db_reg_stk_value:
        #     if reg_stk not in ncdata[2]:
        #         print(reg_stk,db_reg_stk_value[reg_stk],'回放后未查到内存持仓数据')


        for x in F_pos:
            print(x,F_pos[x])

        for x in N_pos:
            print(x,N_pos[x])

        for x in range(len(sqlite_pos)):
            if sqlite_pos[x][0] in F_pos:
                sqlite_pos[x].extend(F_pos[sqlite_pos[x][0]])

        for x in range(len(sqlite_pos)):
            if sqlite_pos[x][0] in N_pos:
                sqlite_pos[x].extend(N_pos[sqlite_pos[x][0]])

        for line in range(len(sqlite_acct)):
            for index in range(len(acct_put)):
                sheet.cell(45 + line, acct_put[index]).value = sqlite_acct[line][index]

        for line1 in range(len(sqlite_pos)):
            for index1 in range(len(pos_put)):
                try:
                    sheet.cell(57 + line1, pos_put[index1]).value = sqlite_pos[line1][index1]
                except:
                    print(57 + line1,pos_put[index1],'write error')



        wb.save(newfile)
        wb.close()



    elif len(res.reglist_C) != 0 and  len(res.reglist_B) != 0:
        log = open('log/errlog.txt', 'w', encoding='utf-8')
        # for cont in res.db_se:
        #     if cont in res.dbdata_A:
        #         # try:
        #         if res.dbdata_A[cont] != res.dbdata_A[res.db_se[cont]]:
        #             print(cont, res.db_se[cont], '不一致',file = log)
        #
        #         # except:
        #         #     print(cont, 'error')

        for cont in res.backdata:
            if list(res.backdata[cont]) != list(res.dbdata_A[cont]):
                print(cont,res.backdata[cont] , res.dbdata_A[cont])

        for cont in res.dbdata_C:
            if res.dbdata_C[cont][0] != res.dbdata_C[cont][1] + res.dbdata_C[cont][2]:
                if cont not in doingdata:
                    print(cont,"数据库状态未完结,未查到内存在途")

        hy_contlist = []
        dc_contlist = []

        log.close()

        oldfile = 'log_list/dailyreport/dailyreport_V5_abc.xlsx'
        newfile = 'log_list/dailyreport/dailyreport' + t + '.xlsx'

        id = 1

        while True:
            if not os.path.exists(newfile):
                print(newfile)

                break
            else:
                newfile = newfile.split(t)[0] + t + '_' + str(id) + '.xlsx'
                id += 1

        shutil.copyfile(oldfile,newfile)
        wb = openpyxl.load_workbook(newfile)
        sheet = wb['测试报告-汇总']
        """模拟发单工具 --- 委托笔数（不含撤单）"""
        sheet['C27'] = res.source_ordernum
        """模拟发单工具 --- 撤单笔数"""
        sheet['J27'] = res.source_cancelnum
        """模拟发单工具 --- ACK笔数"""
        sheet['D27'] = res.acknum_D
        """模拟发单工具 --- 拒单笔数"""
        sheet['E27'] = res.errnum_D
        """模拟发单工具 --- 撤单笔数"""
        sheet['K27'] = res.acknum_F
        """模拟发单工具 --- 撤单拒单笔数"""
        sheet['L27'] = res.errnum_F
        """模拟发单工具 --- 委托合法笔数"""
        sheet['G27'] = res.backlog_confirmnum
        """模拟发单工具 --- 委托非法笔数"""
        sheet['H27'] = res.backlog_rejectnum
        """模拟发单工具 --- 成交总分笔数"""
        sheet['N27'] = res.backlog_allfillnum

        """深圳撮合 --- 成交总笔数"""
        sheet['N31'] = res.szch_Fillnum
        """深圳撮合 --- 委托总笔数"""
        sheet['C31'] = res.szch_Confirmnum + res.szch_Rejectednum
        """深圳撮合 --- 合法总笔数"""
        sheet['G31'] = res.szch_Confirmnum
        """深圳撮合 --- 非法总笔数"""
        sheet['H31'] = res.szch_Rejectednum
        """深圳撮合 --- 撤单总笔数"""
        sheet['J31'] = res.szch_Calcelnum
        """深圳撮合 --- 撤成总笔数"""
        sheet['K31'] = res.szch_Calcelnum
        """A"""
        """数据库 --- 成交总笔数"""
        sheet['N28'] = res.db_fillnum_A
        """数据库 --- 委托总笔数"""
        sheet['C28'] = res.db_confirmnum_A
        """数据库 --- 合法总笔数"""
        sheet['G28'] = res.db_confirmnum_A - res.db_rejectnum_A
        """数据库 --- 非法总笔数"""
        sheet['H28'] = res.db_rejectnum_A
        """数据库 --- 撤单总笔数"""
        sheet['J28'] = res.db_cancelnum_A
        """数据库 --- 撤成总笔数"""
        sheet['K28'] = res.db_cancellednum_A

        """B"""
        """数据库 --- 成交总笔数"""
        sheet['N29'] = res.db_fillnum_B
        """数据库 --- 委托总笔数"""
        sheet['C29'] = res.db_confirmnum_B
        """数据库 --- 合法总笔数"""
        sheet['G29'] = res.db_confirmnum_B - res.db_rejectnum_B
        """数据库 --- 非法总笔数"""
        sheet['H29'] = res.db_rejectnum_B
        """数据库 --- 撤单总笔数"""
        sheet['J29'] = res.db_cancelnum_B
        """数据库 --- 撤成总笔数"""
        sheet['K29'] = res.db_cancellednum_B

        """C"""
        """数据库 --- 成交总笔数"""
        sheet['N30'] = res.db_fillnum_C
        """数据库 --- 委托总笔数"""
        sheet['C30'] = res.db_confirmnum_C
        """数据库 --- 合法总笔数"""
        sheet['G30'] = res.db_confirmnum_C - res.db_rejectnum_C
        """数据库 --- 非法总笔数"""
        sheet['H30'] = res.db_rejectnum_C
        """数据库 --- 撤单总笔数"""
        sheet['J30'] = res.db_cancelnum_C
        """数据库 --- 撤成总笔数"""
        sheet['K30'] = res.db_cancellednum_C

        """交易数据量"""
        sheet['C35'] = res.backlog_all_orderqty
        sheet['D35'] = res.backlog_all_knockaty
        sheet['E35'] = res.backlog_all_cancelqty
        sheet['F35'] = res.backlog_all_doingqty

        sheet['C40'] = res.szch_all_orderqty
        sheet['D40'] = res.szch_all_knockqty
        sheet['E40'] = res.szch_all_cancelqty
        sheet['F40'] = res.szch_all_doingqty

        sheet['C37'] = res.db_A_orderqty
        sheet['D37'] = res.db_A_knockqty
        sheet['E37'] = res.db_A_cancelqty
        sheet['F37'] = res.db_A_doingqty

        sheet['C38'] = res.db_B_orderqty
        sheet['D38'] = res.db_B_knockqty
        sheet['E38'] = res.db_B_cancelqty
        sheet['F38'] = res.db_B_doingqty

        sheet['C39'] = res.db_C_orderqty
        sheet['D39'] = res.db_C_knockqty
        sheet['E39'] = res.db_C_cancelqty
        sheet['F39'] = res.db_C_doingqty

        sheet['F36'] = doingqty

        sheet.cell(6,3).value = t

        ncdata = res_back.search_nc()

        F_pos = getpos(ncdata[1],list1)
        N_pos = getpos(ncdata[2],list1)

        for x in F_pos:
            print(x,F_pos[x])

        for x in N_pos:
            print(x,N_pos[x])

        for x in range(len(sqlite_pos)):
            if sqlite_pos[x][0] in F_pos:
                sqlite_pos[x].extend(F_pos[sqlite_pos[x][0]])

        for x in range(len(sqlite_pos)):
            if sqlite_pos[x][0] in N_pos:
                sqlite_pos[x].extend(N_pos[sqlite_pos[x][0]])

        for line in range(len(sqlite_acct)):
            for index in range(len(acct_put)):
                sheet.cell(45 + line, acct_put[index]).value = sqlite_acct[line][index]

        for line1 in range(len(sqlite_pos)):
            for index1 in range(len(pos_put)):
                try:
                    sheet.cell(57 + line1, pos_put[index1]).value = sqlite_pos[line1][index1]
                except:
                    print(57 + line1,pos_put[index1],'write error')

        wb.save(newfile)
        wb.close()

        # ba_data = {}
        # for cont in res.db_se:
        #     if '_' in cont:
        #         ba_data[cont.split('_')[-1]] = res.db_se[cont]
        ba_data = {}
        for cont in res.db_se:
            ba_data[cont] = res.db_se[cont]

        for cont in res.dbdata_A:
            if cont not in ba_data:
                print(cont,'A 存在 无对应 B 订单')

        for b_cont in ba_data:
            # print(b_cont,ba_data[b_cont])
            try:
                if res.dbdata_A[b_cont] != res.dbdata_B[ba_data[b_cont]]:
                    print(b_cont,res.dbdata_A[b_cont],ba_data[b_cont],res.dbdata_B[ba_data[b_cont]],'A B 订单状态不一致')
            except:
                pass
    else:
        pass
