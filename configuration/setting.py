from basic.back_config import get_mapping

# 数据库配置
host = "172.20.1.21"
port = 3307
user = "coredb"
password = "coredb"
database = "coredb"

# 回放工具配置
hf_hostname = "172.20.0.171"
hf_port = 22
hf_username = "crootreturn"
hf_password = "croot@123"
hf_path = "/home/crootreturn/LogPlayBack_fzx/bin/"

# 深圳撮合
sz_hostname = "172.20.0.171"
sz_port = 22
sz_username ="crootreturn"
sz_password = "croot@123"
sz_path = '/home/crootreturn/tgw/bin/'

settingpath = 'configuration/setting.yaml'

logback_path = 'Logplayback_setting/'

B = []

C = []

##################往下无需配置##################

mapping_sz,mapping_sh,mapping_sz_se,mapping_sh_se = get_mapping(logback_path + 'Order.ini')

reglist_A = []

reglist_B = []

reglist_C = []

print(mapping_sh)
print(mapping_sz)


for acct in mapping_sh:
    reglist_A.append(mapping_sh[acct][-1])
for acct in mapping_sz:
    reglist_A.append(mapping_sz[acct][-1])

for tp in mapping_sz_se:
    if tp in B:
        reglist_B.append(mapping_sz_se[tp][0])
for tp in mapping_sh_se:
    if tp in B:
        reglist_B.append(mapping_sh_se[tp][0])
for tp in mapping_sz_se:
    if tp in C:
        reglist_C.append(mapping_sz_se[tp][0])
for tp in mapping_sh_se:
    if tp in C:
        reglist_C.append(mapping_sh_se[tp][0])

reglist = []

reglist.extend(reglist_A)
reglist.extend(reglist_B)
reglist.extend(reglist_C)

reglist_A = list(set(reglist_A))
reglist_B = list(set(reglist_B))
reglist_C = list(set(reglist_C))
reglist = list(set(reglist))

print(reglist)
