"""
修改生成筛选后的回放日志
"""
import re,paramiko
import configuration.setting as Setting

def mk_certain_log(exchid,stkid,orderprice,orderqty,bsflag,ifcancel,changeNum,loopNum,addTag):
    orderlog = "1 : 8=FIX.4.21=CICC2100=EXCH207=EXCH35=D44=PRICE54=BSFLAG38=ORDERQTY11=CLORDID55=STKID"
    cancellog = "1 : 8=FIX.4.21=CICC2100=EXCH207=EXCH35=F44=PRICE54=BSFLAG38=ORDERQTY11=CLORDID41=CANCELORIG55=STKID"

    res = []
    ALL_NUM = changeNum * loopNum

    if exchid == 0:
        exch = 'SS'
    else:
        exch = 'SZ'

    if bsflag == 'B':
        bsflag = 1
    else:
        bsflag = 0

    linedata = orderlog.replace('EXCH', str(exch))
    linedata = linedata.replace('PRICE', str(orderprice))
    linedata = linedata.replace('BSFLAG', str(bsflag))
    linedata = linedata.replace('ORDERQTY', str(orderqty))
    linedata = linedata.replace('STKID', str(stkid))

    canceldata = cancellog.replace('EXCH', str(exch))
    canceldata = canceldata.replace('PRICE', str(orderprice))
    canceldata = canceldata.replace('BSFLAG', str(bsflag))
    canceldata = canceldata.replace('ORDERQTY', str(orderqty))
    canceldata = canceldata.replace('STKID', str(stkid))

    if str(ifcancel) == 'False':
        index = 1
        while index <= ALL_NUM:
            clordid = '130_HAS0' + str(index) + addTag
            linedata = linedata.replace('CLORDID', clordid)
            linedata = linedata.replace('130_HAS0' + str(index - 1) + addTag, clordid)
            res.append(linedata)
            index += 1

    elif str(ifcancel) == 'True':
        index = 1
        while index <= ALL_NUM:
            clordid = '130_HAS0' + str(index) + addTag
            linedata = linedata.replace('CLORDID', clordid)
            linedata = linedata.replace('130_HAS0' + str(index - 1) + addTag, clordid)

            res.append(linedata)

            cancelorig = '131_HAS0' + str(index) + addTag
            canceldata = canceldata.replace('CLORDID', clordid)
            canceldata = canceldata.replace('130_HAS0' + str(index - 1) + addTag, cancelorig)
            canceldata = canceldata.replace('CANCELORIG', cancelorig)
            canceldata = canceldata.replace('131_HAS0' + str(index - 1) + addTag, cancelorig)

            res.append(canceldata)
            index += 1
    else:
        print('fixlog_setting.py ifCancel 配置异常')

    return res









def changelog(setnum,repeat,addcloid = ''):
    res = []
    source = open('orderlog/OrderAndCancelInfo_newprice.log')
    # source = open('OrderAndCancelInfo_newprice.log')

    # targrt = open('orderlog/OrderAndCancelInfo.log','w')
    stklist = []
    num = 0
    for line in source.readlines():
        for rep in range(repeat):
            if '207=SZ' in line or '207=SS' in line:
                if num >= setnum * repeat:
                    break
                else:
                    linedata = line.strip()
                    clordid = linedata.split('11=')[1].split('')[0]
                    try:
                        # halfclordid = clordid.split('_')[1]
                        halfclordid = clordid
                        ptdata = linedata.replace('11='+clordid,'11='+ halfclordid + addcloid + str(rep))
                    except:
                        ptdata = linedata.replace('11=' + clordid, '11=' + clordid + addcloid + str(rep))
                    try:
                        cancelid = linedata.split('41=')[1].split('')[0]
                        # halfcancel = cancelid.split('_')[1]
                        halfcancel = cancelid

                        ptdata = ptdata.replace('41='+cancelid,'41='+ halfcancel + addcloid + str(rep))
                    except:
                        pass
                    # print(ptdata,file=targrt)
                    res.append(ptdata)
                    try:
                        a = ptdata.strip().split('55=')[1].split('')[0]
                        stkid = a.replace('.SZ','')
                        stkid = stkid.replace('.SS','')
                        stkid = stkid.replace('.ZK','')
                        stkid = stkid.replace('.SH','')
                        stkid = stkid.replace('.HK','')
                    except:
                        pass
                    stklist.append(stkid)
                    num += 1

    print(set(stklist))
    print(len(set(stklist)))

    source.close()
    return res

def mklog(sourcelist,time = 0):
    targrt = open('orderlog/OrderAndCancelInfo.log','w')
    if time == 0:
        for line in sourcelist:
            print(line,file=targrt)
    else:
        hour = 10
        minute = 10
        second = 0
        millisecond = 0
        for line in sourcelist:
            orderdata = line.split(' : ')[1]
            # timespit = 1000000 // time
            timespit = time
            if len(str(second)) == 1:
                ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + "0" + str(second) + "." + str(millisecond) + "000" + ' : '
                if len(str(millisecond)) < 6:
                    chazhi = 6 - len(str(millisecond))
                    ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + "0" + str(
                        second) + "." + "0" * chazhi + str(
                        millisecond) + "000" + ' : '
            else:
                ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + str(second) + "." + str(millisecond) + "000"+ ' : '
                if len(str(millisecond)) < 6:
                    chazhi = 6 - len(str(millisecond))
                    ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + str(
                        second) + "." + "0" * chazhi + str(
                        millisecond) + "000" + ' : '


            print(ordertime + orderdata,file = targrt)
            millisecond += timespit
            if millisecond >= 1000000:
                millisecond = 0
                second += 1
                if second == 60:
                    second = 0
                    minute += 1
                    if minute == 60:
                        minute =0
                        hour += 1
    targrt.close()

def mklog_mc(sourcelist,time = 500,singlenum = 1000,times = 5):
    targrt = open('orderlog/OrderAndCancelInfo.log','w')
    hour = 10
    minute = 10
    second = 0
    millisecond = 0

    index = 1
    for line in sourcelist:
        orderdata = line.split(' : ')[1]
        # timespit = 1000000 // time
        timespit = time
        if len(str(second)) == 1:
            ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + "0" + str(second) + "." + str(millisecond) + "000" + ' : '
            if len(str(millisecond)) < 6:
                chazhi = 6 - len(str(millisecond))
                ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + "0" + str(
                    second) + "." + "0" * chazhi + str(
                    millisecond) + "000" + ' : '
        else:
            ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + str(second) + "." + str(millisecond) + "000"+ ' : '
            if len(str(millisecond)) < 6:
                chazhi = 6 - len(str(millisecond))
                ordertime = "20220124-" + str(hour) + ":" + str(minute) + ":" + str(
                    second) + "." + "0" * chazhi + str(
                    millisecond) + "000" + ' : '

        print(ordertime + orderdata,file = targrt)
        millisecond += timespit
        if index % int(singlenum) == 0:
            second += times
        if millisecond >= 1000000:
            millisecond = 0
            second += 1
            if second == 60:
                second = 0
                minute += 1
                if minute == 60:
                    minute =0
                    hour += 1
        index += 1
    targrt.close()

def getacct():
    result = []
    with open('./OrderAndCancelInfo.log','r') as f:
        for line in f.readlines():
            acct = line.strip().split('1=')[1].split('')[0]
            result.append(acct)
    print(set(result))
    return set(result)

def putlog():
    hostname = Setting.hf_hostname
    port = Setting.hf_port
    username = Setting.hf_username
    password = Setting.hf_password
    path = Setting.hf_path

    client = paramiko.Transport(hostname,port)
    client.connect(username = username,password = password)
    sftp = paramiko.SFTPClient.from_transport(client)
    sftp.put('orderlog/OrderAndCancelInfo.log',path + 'OrderAndCancelInfo.log')
    sftp.put('Logplayback_setting/config.ini',path + 'config.ini')
    sftp.put('Logplayback_setting/Order.ini',path + 'Order.ini')

    print(' --- OrderAndCancelInfo.log 已更新至机器 -- ' + hostname)
    print(' --- config.ini 已更新至机器 -- ' + hostname)
    print(' --- Order.ini 已更新至机器 -- ' + hostname)
    sftp.close()
    client.close()

if __name__ == '__main__':
    num = 100000
    clordadd = "a"
    repaetnum = 1
    changelog(int(num),int(repaetnum),clordadd)
    print('日志更新完成')
