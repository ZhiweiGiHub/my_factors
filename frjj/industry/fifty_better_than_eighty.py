import pandas as pd
from api import get_data_panel
from industry.get_all_time_data import get_time_data_
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


# 获取所有调仓日
start, end = '20170301', "20190801"
change_days, my_tradingdays = get_time_data_(start, end)

month_return_of_50 = []
month_return_of_spare = []


for index, changeday in enumerate(change_days[:-2]):
    # 读入股票代码
    all_codes = pd.read_excel(
        'C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/result/industry_leader_df_md_80.xlsx',
        sheet_name=changeday)

    part_codes = pd.read_excel(
        'C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/result/industry_leader_df_md_50.xlsx',
        sheet_name=changeday)

    eighty_stock = set(all_codes['code'])
    fifty_stock = set(part_codes['code'])
    maybe_bad_stock = eighty_stock.difference(fifty_stock)
    all_data_fifty_stock = get_data_panel("huangwei", "huangwei",
                                          list(fifty_stock), changeday, change_days[index+1],
                                          collection='stock_daily')
    all_data_fifty_stock['rtn'] += 1
    all_data_maybe_bad_stock = get_data_panel("huangwei", "huangwei",
                                              list(maybe_bad_stock), changeday, change_days[index+1],
                                              collection='stock_daily')
    all_data_maybe_bad_stock['rtn'] += 1

    # 分组计算累计收益率
    fifty_stock_cum = all_data_fifty_stock.groupby(
        by=all_data_fifty_stock['code']).apply(lambda x: x['rtn'].cumprod().iloc[-1])
    print('50只股票平均收益率：', fifty_stock_cum.mean())
    month_return_of_50.append(fifty_stock_cum.mean())

    all_data_maybe_bad_stock_cum = all_data_maybe_bad_stock.groupby(
        by=all_data_maybe_bad_stock['code']).apply(lambda x: x['rtn'].cumprod().iloc[-1])
    print('80-50只股票平均收益率：', all_data_maybe_bad_stock_cum.mean())
    month_return_of_spare.append(all_data_maybe_bad_stock_cum.mean())

plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(int(len(change_days[:-2]) / 6)))
plt.plot(change_days[:-2], month_return_of_50, label='50 stocks')
plt.plot(change_days[:-2], month_return_of_spare, label='80-50 stocks')
plt.legend()

