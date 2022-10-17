# 是否需要更新回放日志？：(仅限输入yes后更新)
ifChangelog = 'yes'

# 更新的回放日志条数
changeNum = 1000
""" 插入clordid的后缀"""
addTag = 'e'
# 循环次数
loopNum = 1
# 回放模式 1)生产原日志  2)匀速模式  3)脉冲模式
playbackPattern = 1
# 发单间隔(单位us)
orderInterval = 1000
# 单批数量 - 脉冲模式填写，其他模式不生效
singleNum = 1000
# 批次间隔(单位s) - 脉冲模式填写，其他模式不生效
singleInterval = 5

# 是否指定单一模式（可配置回放证券代码\数量来控制回放结果）仅在匀速以及脉冲模式生效
# 启用：True 不启用 False
ifCertainType = False
# 启用单一模式时需配置：
# 市场 上海: 0  深圳: 1
exchId = 1
# 证券代码
stkId = '000001'
# 委托价格
orderPrice = 15.00
# 委托数量
orderQty = 200
# 买卖方向
bsFlag = 'B'
# 是否撤单 True/False
ifCancel = False



