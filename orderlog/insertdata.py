import pymysql,time
import configuration.setting as Setting


class insert(object):
    def __init__(self):
        self.host=Setting.host
        self.port=Setting.port
        self.user=Setting.user
        self.password=Setting.password
        self.database=Setting.database

        self.mapping_sh = Setting.mapping_sh

        self.mapping_sz = Setting.mapping_sz


        self.sedata_sh = Setting.mapping_sh_se
        self.sedata_sz = Setting.mapping_sz_se

        self.insertsql = '''INSERT INTO bside_portfolio (`productNum`, `productAcctNum`, `portfolioNum`, `PortfolioName`,
                            `currencyId`, `exchId`, `regId`, `stkId`, `stkName`, `bsFlag`, `F_hedgeFlag`, `coveredFlag`, `stkType`, `tradeType`, `deskId`,
                            `thirdSystemId`, `previousQty`, `PreviousCostAmt`, `previousCost`, `previousStkValue`, `previousIncome`, `ydMarginUsedAmt`, `preSettlementPrice`,
                            `currentQty`, `YdUsableQty`, `tdUsableQty`, `TDCloseFrozenQty`, `YDCloseFrozenQty`, `tdOpeningQty`, `tdOpenFrozenQty`, `ETFKnockQty`, `sellUsedQty`,
                            `ETFUsedQty`, `ExceptFrozenQty`, `needSettleQty`, `UnCirculatingShareQty`, `tdTotalOpenAmt`, `tdTotalOpenQty`, `tdTotalOpenFee`, `tdTotalCloseAmt`,
                             `tdTotalCloseQty`, `tdTotalCloseFee`, `PostQty`, `PostCostAmt`, `PostNoFeeCostAmt`, `todayPositionCost`, `marginUsedAmt`, `StkValue`,
                             `SettlementPrice`, `fairValuePrice`, `closePNL`, `realTimePnl`, `closeNoFeePNL`, `sumClosePNL`, `sumTotalFee`, `ContractTimes`, `StkProperty`,
                             `coveredFrozenQty`, `coveredFrozenUsableQty`, `coveredFrozenPositionQty`, `currentInterCreditUsedQty`, `currentCreditUsedQty`, `currentCreditCostAmt`,
                             `currentCreditCashAmt`, `creditTransferQty`, `updateTime`, `PreviousTradeCostAmt`, `TradePostCostAmt`, `tradeTdTotalOpenAmt`, `tradeTdTotalOpenFee`,
                              `tradeTdTotalCloseAmt`, `tradeTdTotalCloseFee`, `tdCloseLimitQty`, `Interest`, `YdOfferQty`, `TdOfferQty`, `combMarginAmt`) VALUES ({}, {}, {},
                              '{}', 'RMB', '1', '{}', '{}', '{}', 'B', 'SPEC', '0', '{}', '{}', '15810', '2301', 0, 0.00, 0.00000000, 0.00, 0.00, 0.00, 0.0000,
                               1000000, 1000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.00, 0, 0.00, 0.00, 0, 0.00, 0, 444000.00, 444000.00, 4.44000000, 0.00, 444000.00, 0.0000, 0.0000,
                               0.00, 0.000, 0.00, 0.000, 0.000, 1, '0', 0, 0, 0, 0, 0, 0.000, 0.000, 0, 20210926132701, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0, 0.000, 0, 0, 0.00);'''
        self.insertsql_sh = '''INSERT INTO bside_portfolio (`productNum`, `productAcctNum`, `portfolioNum`, `PortfolioName`,
                            `currencyId`, `exchId`, `regId`, `stkId`, `stkName`, `bsFlag`, `F_hedgeFlag`, `coveredFlag`, `stkType`, `tradeType`, `deskId`,
                            `thirdSystemId`, `previousQty`, `PreviousCostAmt`, `previousCost`, `previousStkValue`, `previousIncome`, `ydMarginUsedAmt`, `preSettlementPrice`,
                            `currentQty`, `YdUsableQty`, `tdUsableQty`, `TDCloseFrozenQty`, `YDCloseFrozenQty`, `tdOpeningQty`, `tdOpenFrozenQty`, `ETFKnockQty`, `sellUsedQty`,
                            `ETFUsedQty`, `ExceptFrozenQty`, `needSettleQty`, `UnCirculatingShareQty`, `tdTotalOpenAmt`, `tdTotalOpenQty`, `tdTotalOpenFee`, `tdTotalCloseAmt`,
                             `tdTotalCloseQty`, `tdTotalCloseFee`, `PostQty`, `PostCostAmt`, `PostNoFeeCostAmt`, `todayPositionCost`, `marginUsedAmt`, `StkValue`,
                             `SettlementPrice`, `fairValuePrice`, `closePNL`, `realTimePnl`, `closeNoFeePNL`, `sumClosePNL`, `sumTotalFee`, `ContractTimes`, `StkProperty`,
                             `coveredFrozenQty`, `coveredFrozenUsableQty`, `coveredFrozenPositionQty`, `currentInterCreditUsedQty`, `currentCreditUsedQty`, `currentCreditCostAmt`,
                             `currentCreditCashAmt`, `creditTransferQty`, `updateTime`, `PreviousTradeCostAmt`, `TradePostCostAmt`, `tradeTdTotalOpenAmt`, `tradeTdTotalOpenFee`,
                              `tradeTdTotalCloseAmt`, `tradeTdTotalCloseFee`, `tdCloseLimitQty`, `Interest`, `YdOfferQty`, `TdOfferQty`, `combMarginAmt`) VALUES ({}, {}, {},
                              '{}', 'RMB', '0', '{}', '{}', '{}', 'B', 'SPEC', '0', '{}', '{}', '15820', '2301', 0, 0.00, 0.00000000, 0.00, 0.00, 0.00, 0.0000,
                               1000000, 1000000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.00, 0, 0.00, 0.00, 0, 0.00, 0, 444000.00, 444000.00, 4.44000000, 0.00, 444000.00, 0.0000, 0.0000,
                               0.00, 0.000, 0.00, 0.000, 0.000, 1, '0', 0, 0, 0, 0, 0, 0.000, 0.000, 0, 20210926132701, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0, 0.000, 0, 0, 0.00);'''




        self.line = 0

    def get_acct_stk(self,exch):
        source = open('orderlog/OrderAndCancelInfo.log')
        acct_stk = {}
        self.stklist = []
        for line in source.readlines():
            self.line += 1
            if '207=' + exch in line:
                if '35=D' in line:
                    if '54=2' in line:
                        stkid = line.strip().split('55=')[1].split('')[0].split('.')[0]
                        acctnum = line.strip().split('50=')[1].split('')[0]
                        self.stklist.append(stkid)
                        if acctnum not in acct_stk:
                            acct_stk[acctnum] = []
                            acct_stk[acctnum].append(stkid)
                        else:
                            if stkid not in acct_stk[acctnum]:
                                acct_stk[acctnum].append(stkid)
        self.acct_stk = acct_stk
        for x in self.acct_stk:
            print(x,self.acct_stk[x])
        self.stklist = set(self.stklist)
        source.close()
        return self.acct_stk

    def searchposition(self,exchid):
        result = {}

        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")

        cursor = db.cursor()
        sql = "select s.stkId,s.stkName,s.stkType,s.tradeType from stkinfo s where exchid = " + str(exchid)
        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        for x in data:
            if x[0] not in result:
                result[x[0]] = [x[0],x[1],x[2],x[3]]
        cursor.close()
        db.close()
        self.positiondata = result
        return result


    def insertdata(self,exch,exchid):
        self.get_acct_stk(exch)
        self.searchposition(exchid)
        if exch == 'SZ':
            self.mapping = Setting.mapping_sz
        else:
            self.mapping = Setting.mapping_sh
        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()
        allnum = 0
        for acctid in self.acct_stk:
            allnum += len(self.acct_stk[acctid])
        counter = 1
        for acctid in self.acct_stk:
            accountdata = self.mapping[acctid]
            for stkid in self.acct_stk[acctid]:
                process = counter * 100 // allnum
                try:
                    searchdata = self.positiondata[stkid]
                except:
                    continue
                if exch == 'SZ':
                    true_insertsql = self.insertsql.format(accountdata[0], accountdata[1], accountdata[2], accountdata[3],
                                                      accountdata[4], searchdata[0], searchdata[1], searchdata[2],
                                                      searchdata[3])
                else:
                    true_insertsql = self.insertsql_sh.format(accountdata[0], accountdata[1], accountdata[2], accountdata[3],
                                                      accountdata[4], searchdata[0], searchdata[1], searchdata[2],
                                                      searchdata[3])
                try:
                    cursor.execute(true_insertsql)
                    print('\r 插入持仓进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),stkid,' 持仓插入成功',end = '')
                    counter += 1
                    time.sleep(0.0001)
                except:
                    print('\r 插入持仓进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), stkid,
                          ' 持仓插入失败,数据库中已有该持仓数据', end='')
                    counter += 1
                    time.sleep(0.0001)

        print('\n', end='')

        cursor.close()
        db.commit()
        db.close()

    def insertdata_se(self,exch,exchid):
        self.get_acct_stk(exch)
        self.searchposition(exchid)

        db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,
                             charset="utf8")
        cursor = db.cursor()

        if exch == 'SS':
            if len(self.sedata_sh) == 0:
                pass
            else:
                allnum = len(self.stklist)
                counter = 1
                for stkid in self.stklist:
                    for tp in self.sedata_sh:
                        accountdata = list(tp)
                        accountdata.extend(self.sedata_sh[tp])
                        process = counter * 100 // allnum
                        try:
                            searchdata = self.positiondata[stkid]
                        except:
                            continue
                        true_insertsql = self.insertsql_sh.format(accountdata[0], accountdata[1], accountdata[2], accountdata[3],
                                                              accountdata[4], searchdata[0], searchdata[1], searchdata[2],
                                                              searchdata[3])

                        try:
                            cursor.execute(true_insertsql)
                            print('\r 对冲账户上海持仓插入进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),stkid,' 持仓插入成功',end = '')
                            counter += 1
                            time.sleep(0.0001)
                        except:
                            print('\r 对冲账户上海持仓插入进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), stkid,
                                  ' 对冲账户持仓插入失败,数据库中已有该持仓数据', end='')
                            counter += 1
                            time.sleep(0.0001)


                if allnum != 0:
                    print('\n --- 全部持仓插入成功')
                else:
                    print('\n --- 无需插入持仓')

        if exch == 'SZ':
            if len(self.sedata_sz) == 0:
                pass
            else:
                allnum = len(self.stklist)
                counter = 1
                for stkid in self.stklist:
                    for tp in self.sedata_sz:
                        accountdata = list(tp)
                        accountdata.extend(self.sedata_sz[tp])
                        process = counter * 100 // allnum

                        searchdata = self.positiondata[stkid]
                        true_insertsql = self.insertsql.format(accountdata[0], accountdata[1], accountdata[2], accountdata[3],
                                                              accountdata[4], searchdata[0], searchdata[1], searchdata[2],
                                                              searchdata[3])

                        try:
                            cursor.execute(true_insertsql)
                            print('\r 对冲账户深圳持仓插入进度: {0}{1}%'.format('▉' * process,min(round((process),2),100.00)),stkid,' 持仓插入成功',end = '')
                            counter += 1
                            time.sleep(0.0001)
                        except:
                            print('\r 对冲账户深圳持仓插入进度: {0}{1}%'.format('▉' * process, min(round((process), 2), 100.00)), stkid,
                                  ' 对冲账户持仓插入失败,数据库中已有该持仓数据', end='')
                            counter += 1
                            time.sleep(0.0001)


                if allnum != 0:
                    print('\n --- 全部持仓插入成功')
                else:
                    print('\n --- 无需插入持仓')

        cursor.close()
        db.commit()
        db.close()



if __name__ == '__main__':

    pass
