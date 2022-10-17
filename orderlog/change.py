"""
修改生成筛选后的回放日志
"""

def changelog(setnum,repeat,addcloid = ''):
    source = open('OrderAndCancelInfo_newprice.log')
    targrt = open('OrderAndCancelInfo.log','w')
    stklist = []
    # for rep in range(repeat):
    num = 0
    for line in source.readlines():
        for rep in range(repeat):
            if '207=SZ' in line or '207=SS' in line :
            # if '207=SZ' in line:

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

    source.close()
    targrt.close()

if __name__ == '__main__':
    while True:
        num = input('请输入回放条数：')
        clordadd = input('请输入clordid后缀：')
        repaetnum = input('请输入重复次数：')
        try:
            changelog(int(num),int(repaetnum),str(clordadd))
            print('日志更新完成')
            break
        except:
            print('更新出错，检查输入后重试')

    input('输入任意值推出~')
