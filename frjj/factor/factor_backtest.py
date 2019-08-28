# from frjj_codes.factor.utils import *
# from tqdm import tqdm
import matplotlib.pyplot as plt
# import numpy as np
# from frjj_codes.factor.weighted_return_maker import *
from factor.groups_maker import *
from factor.chooseday_maker import *

"""
因子排序后分组进行简易回测，获得累计收益率的时间序列
"""
tradingdays_all = get_tradingdays("huangwei", "huangwei", '20000104', "20190628")
start, end = '20170101', "20190628"
choose_days, change_days = chooseday_maker_(start, end, period='M')

group_returns = pd.DataFrame()

for index in tqdm(range(len(choose_days) - 1)):
    print('现在回测到：', change_days[index])
    temp = groups_maker_(choose_days[index],
                         change_days[index],
                         change_days[index + 1],
                         change_days, tradingdays_all, filter=False)  # 指标值小的在1组

    # filter表示是否对原始排序剔除风格、市值影响
    group_returns = group_returns.append(temp)

group_returns = group_returns.reset_index(drop=True)

group_returns['group1'] = group_returns['group1'] + 1
group_returns['group1'] = group_returns['group1'].cumprod()
group_returns['group2'] = group_returns['group2'] + 1
group_returns['group2'] = group_returns['group2'].cumprod()
group_returns['group3'] = group_returns['group3'] + 1
group_returns['group3'] = group_returns['group3'].cumprod()
group_returns['group4'] = group_returns['group4'] + 1
group_returns['group4'] = group_returns['group4'].cumprod()
group_returns['group5'] = group_returns['group5'] + 1
group_returns['group5'] = group_returns['group5'].cumprod()

# 计算超额收益（对比指数）
hs300 = get_data_panel("huangwei", "huangwei",
                       ['000300.SH'],  # 沪深300
                       group_returns['date'].iloc[0],
                       group_returns['date'].iloc[-1],
                       collection='index_rtn')

hs300['cum_chg'] = hs300['pct_chg'] / 100 + 1
hs300['cum_chg'] = hs300['cum_chg'].cumprod().values
# plt.plot(hs300['cum_chg'].values, label='hs300')
group_returns_arr = group_returns[['group1', 'group2', 'group3', 'group4', 'group5']].values  # \
# - hs300['cum_chg'].values[:, np.newaxis]

plt.plot(group_returns_arr[:, 0], label='group1')
plt.plot(group_returns_arr[:, 1], label='group2')
plt.plot(group_returns_arr[:, 2], label='group3')
plt.plot(group_returns_arr[:, 3], label='group4')
plt.plot(group_returns_arr[:, 4], label='group5')
plt.plot(hs300['cum_chg'].values[:, np.newaxis], label='hs300')
plt.legend()

# plt.plot(group_returns.values[:, 1:])  # 绝对累计收益率
