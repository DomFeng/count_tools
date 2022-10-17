# coding=utf8
import configparser

def get_config_data(ini_path):
    res = {}
    cf = configparser.ConfigParser()
    cf.read(ini_path,encoding = 'utf-8')

    secs = cf.sections()  # 获得所有区域
    for sec in secs:
        res[sec] = {}
        for key_value in cf.items(sec):
            res[sec][key_value[0]] = key_value[1]
    return res

def get_mapping(ini_path):
    mapping_sz = {}
    mapping_sh = {}
    mapping_sz_se = {}
    mapping_sh_se = {}

    source = get_config_data(ini_path)
    for ACCTID in source:
        if 'ACCTID' in ACCTID and 'QUERY' not in ACCTID and 'LOGIN' not in ACCTID:

            if source[ACCTID]['order.exchid'] == '1':
                mapping_sz[source[ACCTID]['acctid']] = [source[ACCTID]['order.productnum'],source[ACCTID]['order.productacctnum'],
                                                     source[ACCTID]['order.portfolionum'],source[ACCTID]['order.regid'],source[ACCTID]['order.regid']]
            else:
                mapping_sh[source[ACCTID]['acctid']] = [source[ACCTID]['order.productnum'],
                                                     source[ACCTID]['order.productacctnum'],
                                                     source[ACCTID]['order.portfolionum'],
                                                     source[ACCTID]['order.regid'], source[ACCTID]['order.regid']]
        elif 'QUERY' in ACCTID:
            if source[ACCTID]['query.exchid'] == '1':
                mapping_sz_se[(source[ACCTID]['query.productnum'],source[ACCTID]['query.productacctnum'],
                               source[ACCTID]['query.portfolionum'])] = [source[ACCTID]['query.regid'],source[ACCTID]['query.regid']]
            else:
                mapping_sh_se[(source[ACCTID]['query.productnum'],source[ACCTID]['query.productacctnum'],
                               source[ACCTID]['query.portfolionum'])] = [source[ACCTID]['query.regid'],source[ACCTID]['query.regid']]


    return mapping_sz,mapping_sh,mapping_sz_se,mapping_sh_se


