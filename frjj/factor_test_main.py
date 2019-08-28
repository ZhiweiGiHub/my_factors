# 用来跑一组单因子，批量生成回测图
from industry.core_f import *

factor_list = ['pb', 'pe_ttm', 'ps_ttm', 'turnover_rate_f',
               'momentum', 'total_mv', 'QFA_ROE_DEDUCTED',
               'QFA_ROA', 'QFA_YOYSALES', 'QFA_YOYPROFIT']

factor_type = ['base', 'base', 'base', 'base', 'tech', 'base',
               'financial', 'financial', 'financial', 'financial']

# 获取所有调仓日
start, end = '20180101', "20190801"
change_days, my_tradingdays = get_time_data_(start, end)

for factor, types in zip(factor_list, factor_type):

    total_capitals = factor_test(factor, types)

    # 累计总价值曲线（与指数累计收益曲线作对比）
    banchmark = "000300.SH"
    returns = np.array(total_capitals)[1:] / np.array(total_capitals)[0]
    hs300 = get_data_panel("huangwei", "huangwei",
                           [banchmark],  # 对标指数
                           my_tradingdays['tradingdays'].iloc[0],
                           my_tradingdays['tradingdays'].iloc[-1],
                           collection='index_rtn')
    hs300['cum_chg'] = hs300['pct_chg'] / 100 + 1
    hs300['cum_chg'] = hs300['cum_chg'].cumprod().values
    extra = returns[:, np.newaxis] - hs300['cum_chg'].values[:, np.newaxis]
    performance_(hs300, banchmark, returns, extra, factor_list[0], 'spyl')
    print(factor, 'is ok!')
