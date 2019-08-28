import pandas as pd
from factor.factor_maker import financial_maker_, base_indi_maker_
from industry.leader_maker import LeaderMaker

# 根据已有的表单获得坐落在广东省的公司，并按照一定的权重选股
all_stock_local_info = pd.read_excel(
    'C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/code_location.xlsx')
GD_stock_info = all_stock_local_info[all_stock_local_info['省份'] ==
                                     '广东省'].reset_index(drop=True)

def GD_location_maker_(changeday):
    if len(changeday) == 8:
        changeday = changeday[0:4] + '-' + changeday[4:6] + '-' + changeday[6:8]
    else:
        changeday = changeday
    # 初始化股票名单
    stock_pool = pd.DataFrame(columns=['code', 'date'], index=range(len(GD_stock_info)))
    stock_pool['code'] = GD_stock_info['code']
    stock_pool['date'] = changeday

    # 与龙头股做交集
    a = LeaderMaker(changeday,
                    'total_mv',
                    '000905.SH',
                    None,
                    '000300.SH')
    weighted_result = a.weight()

    stock_pool = stock_pool.merge(weighted_result['code'], on='code',
                                  how='inner').reset_index(drop=True)

    # 从数据库获取、市值、利润同比
    # 扣非ROE
    factor_value_ROE = financial_maker_(changeday,
                                        collection_name="qfa_roe_deducted",
                                        indicator='QFA_ROE_DEDUCTED', ac=True,
                                        database='stock_cn_finance1', type=None)
    # 市值
    factor_value_mv = base_indi_maker_(changeday,
                                       collection_name="stock_indicator",
                                       indicator='total_mv', ac=True,
                                       database='stock_cn',
                                       type=None, info='000300.SH')
    # 利润同比
    factor_value_profit = financial_maker_(changeday,
                                           collection_name="qfa_yoyprofit",
                                           indicator='QFA_YOYPROFIT', ac=True,
                                           database='stock_cn_finance1', type=None)

    # merge为一张表
    stock_pool = stock_pool.merge(factor_value_ROE[['code', 'QFA_ROE_DEDUCTED']],
                                  on='code', how='inner')
    stock_pool = stock_pool.merge(factor_value_mv[['code', 'total_mv']],
                                  on='code', how='inner')
    stock_pool = stock_pool.merge(factor_value_profit[['code', 'QFA_YOYPROFIT']],
                                  on='code', how='inner')

    # 对每个维度进行标准化，然后综合评分，降序
    stock_pool['QFA_ROE_DEDUCTED'] = \
        stock_pool['QFA_ROE_DEDUCTED'].apply(
            lambda x: (x - stock_pool['QFA_ROE_DEDUCTED'].min())
                      / (stock_pool['QFA_ROE_DEDUCTED'].max()
                         - stock_pool['QFA_ROE_DEDUCTED'].min()))
    stock_pool['total_mv'] = \
        stock_pool['total_mv'].apply(
            lambda x: (x - stock_pool['total_mv'].min())
                      / (stock_pool['total_mv'].max()
                         - stock_pool['total_mv'].min()))
    stock_pool['QFA_YOYPROFIT'] = \
        stock_pool['QFA_YOYPROFIT'].apply(
            lambda x: (x - stock_pool['QFA_YOYPROFIT'].min())
                      / (stock_pool['QFA_YOYPROFIT'].max()
                         - stock_pool['QFA_YOYPROFIT'].min()))

    stock_pool['rank'] = stock_pool['QFA_ROE_DEDUCTED'] + \
                         stock_pool['total_mv'] + \
                         stock_pool['QFA_YOYPROFIT']

    stock_pool = stock_pool.sort_values(by='rank', ascending=False).reset_index(drop=True)

    # 取前N
    if len(stock_pool) >= 50:
        stock_pool = stock_pool.iloc[0:50]
    else:
        pass

    stock_pool['weight_adj_sw1'] = stock_pool['total_mv'] / stock_pool['total_mv'].sum()

    # 最大持仓不能超过8%
    print('限制持仓不可超过8%...')

    while stock_pool['weight_adj_sw1'].max() > 0.08:
        for i in range(len(stock_pool)):
            if stock_pool['weight_adj_sw1'].iloc[i] > 0.08:
                stock_pool['weight_adj_sw1'].iloc[i] = 0.08
        stock_pool['weight_adj_sw1'] = stock_pool['weight_adj_sw1'] / \
                                       stock_pool['weight_adj_sw1'].sum()
    print('调整权重完成.')

    return stock_pool
