# coding=gbk
def changelog():
    source = open('fixlog103503.log','r',encoding='utf-8')
    targrt = open('fixlog.log','w')
    dic = {'SS':'0','SZ':'1','ZK':'1','SH':'0'}
    result = []
    for line in source.readlines():
        print(line.strip())
        account = line.strip().split('1=')[1]
        id = account.split('')[0]

        result.append(id)


        if '207=SZ' in line:
            print(line.strip(),file = targrt)

    print(set(result))

    source.close()
    targrt.close()
    return result

changelog()