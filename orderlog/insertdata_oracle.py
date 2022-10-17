import cx_Oracle,time
import xlrd
class insert(object):
    def __init__(self):
        self.mapping = {'CICC2':['000000110101', '压测账户1'],
                        'CICC1':['000000110102', '压测账户2'],
                        'CICC7':['000000110103', '压测账户3'],
                        '802500022937': ['000000110104', '压测账户4'],
                        '802500022141': ['000000110105', '压测账户5'],
                        '802500021732': ['000000110106', '压测账户6'],
                        '802500021880': ['000000110107', '压测账户7'],
                        '802500022936': ['000000110108', '压测账户8'],
                        }
        self.insertsql = '''insert into stklist (EXCHID, ACCTID, CUSTID, REGID, REGNAME, OFFERREGID, STKID, STKNAME, 
        EXCHTRUSTEESHIPQTY, PREVIOUSQTY, PREVIOUSCOST, STKVALUE, PREVIOUSINCOME, USABLEQTY, CURRENTQTY, UNSALEABLEQTY, 
        SELLFROZENQTY, BUYFROZENQTY, EXCEPTFROZENQTY, REALTIMECOST, REALTIMEINCOME, CURRENCYID, STKTYPE, TRADETYPE, 
        DESKID, CURRENTSTKVALUE, NEWPRICE, ETFKNOCKQTY, SELLUSEDQTY, ETFUSEDQTY, NEEDSETTLEQTY, IMPAWNQTY, WARRANTUSEDQTY, 
        BONDPLEDGEQTY, BONDPLEDGEUSABLEQTY, HOLDSTATUS, CREDITSHAREUSEDQTY, CURRENTCREDITSHAREQTY, CREDITSHAREFROZENQTY, 
        GUARANTYQTY, CURRENTQTYF, CUSTBRANCHID, GOINGBUYQTY, TOTALSELLQTY, SELLLIMITQTY, WAITSETTLEQTY, EXCHFROZENQTY, 
        DIVIDENDAMT, DIVIDENDQTY, AGREEREPOQTY, QBONDPLEDGEQTY, QBONDPLEDGEUSABLEQTY, EXCHSELLFROZENQTY, ETFPURCHASEFROZENQTY, 
        INTSUSABLEQTY, INTSBUYFROZENQTY, INTSSELLUSEDQTY, TDTOTALOPENAMT, TDTOTALOPENQTY, TDTOTALCLOSEAMT, TDTOTALCLOSEQTY, 
        TDTOTALOPENFEE, TDTOTALCLOSEFEE, COVEREDFROZENQTY, COVEREDFROZENUSABLEQTY, COVEREDFROZENPOSITIONQTY, SETTLEPREVIOUSCOST,
         SETTLESTKVALUE, SETTLEPREVIOUSINCOME, EXCHRATE, CREDITINSHAREUSEDQTY, CREDITINSHAREQTY, CREDITOUTSHAREUSEDQTY, 
         CREDITOUTSHAREQTY, NOUSEINITQTY, USEDUNFROZENQTY, FASTUPDATECNT, CREDITSPAREQTY, TDTOTALNOTBUYQTY, TDTOTALNOTSELLQTY, 
         OCCURCREDITLOCKSHAREQTY, GAPFROZENQTY, ZRTTRANSQTY)
         values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', 0, 100000, 
         0.37803921, 56661.000, 0.000, 10000000, 10000000, 0, 0, 0, 0, 0.000, 0.000, '00', '{}', '{}', '15810', 0.000, 5.555, 0, 
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.00, '000001', 0, 0, 0, 0, 0, 0.000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.000, 0, 
         0.000, 0, 0.000, 0.000, 0, 0, 0, 0.37803921, 56661.000, 0.000, 1.000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

'''
        self.acct_exchlist = {}
        self.line = 0

        self.getstklist()
        self.searchreg()
        # self.get_acct_stk()
        self.searchposition()

    def getstklist(self):
        result = {}
        readbook = xlrd.open_workbook(r'./股票清单.xlsx')

        sheet = readbook.sheet_by_name('深1500+沪500')
        for x in range(1,2001):
            stkid = sheet.cell(x,0).value
            exch = sheet.cell(x,2).value
            print(stkid)
            if exch == '上海':
                exchid = '0'
            else:
                exchid = '1'
            result[stkid] = exchid
        self.acct_exchlist = result
        return result


    def get_acct_stk(self):
        source = open('./OrderAndCancelInfo.log')
        linelist = []
        acct_stk = {}
        for line in source.readlines():
            self.line += 1
            if '207=SZ' in line or '207=SS' in line:
                if '35=D' in line:
                    linelist.append(line.strip())
                    stkid = line.strip().split('55=')[1].split('')[0].replace('.SZ', '')
                    stkid = stkid.replace('.SS','')

                    acctnum = line.strip().split('50=')[1].split('')[0]
                    if acctnum not in acct_stk:
                        acct_stk[acctnum] = []
                        acct_stk[acctnum].append(stkid)
                    else:
                        if stkid not in acct_stk[acctnum]:
                            acct_stk[acctnum].append(stkid)

        self.linelist = linelist
        self.acct_stk = acct_stk
        source.close()
        for x in self.acct_stk:
            print(x,self.acct_stk[x])
        return self.acct_stk

    def searchreg(self):
        acctlist = ['000000110101','000000110102','000000110103','000000110104','000000110105','000000110106','000000110107','000000110108','000000110109','000000110110']
        self.acctlist = acctlist
        result = {}
        for x in acctlist:
            result[x] = {}
        user = "ctsdb"
        passwd = "ctsdb"
        listener = '192.168.0.37:1521/ctsdb'
        db = cx_Oracle.connect(user, passwd, listener)

        cursor = db.cursor()
        sql = "select acctid,exchid,regid,regname from registration where acctid in " + str(tuple(acctlist))
        cursor.execute(sql)
        data = cursor.fetchall()
        for x in data:
            result[(x[0],x[1])] = [x[2],x[3]]
        cursor.close()
        db.close()
        self.acct_reg = result
        for x in self.acct_reg:
            print(x, self.acct_reg[x])
        return result


    def searchposition(self):
        result = {}
        user = "ctsdb"
        passwd = "ctsdb"
        listener = '192.168.0.37:1521/ctsdb'
        db = cx_Oracle.connect(user, passwd, listener)

        cursor = db.cursor()
        sql = "select s.exchid,s.stkId,s.stkName,s.stkType,s.tradeType from stkinfo s where exchid in ('0','1')"
        cursor.execute(sql)
        data = cursor.fetchall()
        for x in data:
            # if x[0] not in result:
            result[(x[0],x[1])] = [x[0],x[1],x[2],x[3],x[4]]
        cursor.close()
        db.close()
        self.positiondata = result
        return result


    def insertdata(self):
        self.get_acct_stk()

        user = "ctsdb"
        passwd = "ctsdb"
        listener = '192.168.0.37:1521/ctsdb'
        db = cx_Oracle.connect(user, passwd, listener)
        cursor = db.cursor()
        allnum = 0
        for acctid in self.acct_stk:
            allnum += len(self.acct_stk[acctid])
        counter = 1
        for acctid in self.acct_stk:
            accountdata = self.mapping[acctid]
            regdata = self.acct_reg[accountdata[0]]
            for stkid in self.acct_stk[acctid]:
                process = counter * 100 // allnum

                try:
                    searchdata = self.positiondata[stkid]
                    true_insertsql = self.insertsql.format(searchdata[0], accountdata[0], accountdata[0],
                                                           regdata[searchdata[0]][0],
                                                           accountdata[1], regdata[searchdata[0]][0], stkid,
                                                           searchdata[2], searchdata[3],
                                                           searchdata[4])
                    cursor.execute(true_insertsql)
                    print('\r 插入持仓进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),stkid,' 持仓插入成功',end = '')
                    counter += 1
                    time.sleep(0.0001)
                except:
                    print('\r 插入持仓进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), stkid,
                          ' 持仓插入失败,数据库中已有该持仓数据', end='')
                    counter += 1
                    time.sleep(0.0001)

        if allnum != 0:
            print('\n --- 全部持仓插入成功')
        else:
            print('\n --- 无需插入持仓')

        cursor.close()
        db.commit()
        db.close()

    def insertdata_new(self):
        acctdata = {'000000110101':'压测账户1', '000000110102':'压测账户2', '000000110103':'压测账户3',
                    '000000110104':'压测账户4', '000000110105':'压测账户5', '000000110106':'压测账户6',
                    '000000110107':'压测账户7', '000000110108':'压测账户8','000000110109':'压测账户9',
                    '000000110110':'压测账户10'}
        # acctdata = {'000000110109':'压测账户9',
        #             '000000110110':'压测账户10'}

        user = "ctsdb"
        passwd = "ctsdb"
        listener = '192.168.0.37:1521/ctsdb'
        db = cx_Oracle.connect(user, passwd, listener)
        cursor = db.cursor()
        allnum = self.acct_exchlist.__len__() - 1
        for acctid in acctdata:
            custid = acctid
            counter = 0
            errlist = []
            for stkid in self.acct_exchlist:
                process = counter * 100 // allnum
                exchid = self.acct_exchlist[stkid]
                regid = self.acct_reg[(acctid,exchid)][0]
                regname = self.acct_reg[(acctid,exchid)][1]
                try:
                    stkname = self.positiondata[(exchid,stkid)][2]
                    stktype = self.positiondata[(exchid,stkid)][3]
                    tradetype = self.positiondata[(exchid,stkid)][4]
                    true_insertsql = self.insertsql.format(exchid, acctid, custid,
                                                           regid,regname,regid,stkid,stkname,stktype,tradetype)
                    # print(true_insertsql)
                    # cursor.execute(true_insertsql)
                    print('\r 插入持仓进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),stkid,' 持仓插入成功',end = '')
                except:
                    print('\r 插入持仓进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), stkid,
                          ' 持仓插入失败,数据库中已有该持仓数据', end='')
                    errlist.append(stkid)
                counter += 1
                srrlist = set(errlist)
                # time.sleep(0.0001)
            print('\n',acctid,'持仓插入结束',len(errlist),errlist)

        cursor.close()
        db.commit()
        db.close()

"""
查询mysql 数据库 删除股东代码对应持仓
"""
def dropposition(acctlist):
    result = {}
    user = "ctsdb"
    passwd = "ctsdb"
    listener = '192.168.0.37:1521/ctsdb'
    db = cx_Oracle.connect(user, passwd, listener)

    cursor = db.cursor()
    for acctid in acctlist:
        sql1 = "delete from stklist where acctid = '" + str(acctid) +"'"
        cursor.execute(sql1)
    cursor.close()
    db.commit()
    db.close()
    return result
if __name__ == '__main__':

    a = insert()
    # if input('是否删除已有持仓数据？(yes/no)') == 'yes':
    #     dropposition(a.acct_reg)
    a.insertdata_new()