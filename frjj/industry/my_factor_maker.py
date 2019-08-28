import pandas as pd
from api import get_data_panel_backwards


def my_factor_maker_(stockpool, chooseday_r):
    # 获取当下日期过去一个月的收益率数据
    info_all = get_data_panel_backwards("huangwei", "huangwei",
                                        list(stockpool['code']),
                                        chooseday_r,
                                        back_years=0, back_months=1,
                                        collection='stock_daily')
    info_all = info_all.rename(columns={'code': 'codes'})
    info_all['rtn'] = info_all['rtn'] + 1
    info_all = info_all[['codes', 'rtn', 'date']]
    # 分组统计
    grouped = info_all.groupby('codes').std()

    factor_value = pd.DataFrame(columns=['date', 'code', 'volatility'],
                                index=range(len(grouped)))
    factor_value['code'] = list(grouped.index)
    factor_value['date'] = info_all['date'].iloc[0]
    factor_value['volatility'] = list(grouped['rtn'].values)
    factor_value = factor_value.sort_values(by='volatility',
                                            ascending=True).reset_index(drop=True)
    return factor_value
