"""
修改生成筛选后的回放日志
"""
import re,paramiko
import configuration.setting as Setting

'orderlog/OrderAndCancelInfo.log'
def changelog(setnum,repeat,target,addcloid = ''):
    source = open('OrderAndCancelInfo_newprice.log')
    targrt = open(target,'w')
    # source = open('OrderAndCancelInfo_newprice.log')
    # targrt = open('OrderAndCancelInfo.log','w')
    stklist = []
    # for rep in range(repeat):
    num = 0
    for line in source.readlines():
        for rep in range(repeat):
            # if '207=SS' in line or '207=SH' in line :
            # if '15=CNY' in line:
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
                    print(ptdata,file=targrt)
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
    print(' --- OrderAndCancelInfo.log 已更新至机器 -- ' + hostname)
    sftp.close()
    client.close()
if __name__ == '__main__':
    num = 1
    clordadd = 1
    repaetnum = 1

    target = "OrderAndCancelInfo"
    while clordadd <= 10:
        changelog(int(num),int(repaetnum),target + str(clordadd) + ".log",str(clordadd))
        clordadd += 1

