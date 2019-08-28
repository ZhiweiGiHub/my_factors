# 获取一段时间某个股票池成分股的累计收益率
from factor.utils import month, spearman_cor
import pandas as pd
from api import get_data_panel


def period_return_maker_(chooseday_r, factor_value, indicator):
    date_r2 = month(chooseday_r, change_month=-12)
    date = chooseday_r
    if len(date) <= 8:
        date_r = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    else:
        date_r = date

    # 创建某时刻截面指标df
    df_values = pd.DataFrame(columns=['date', 'code', 'factor_value'],
                             index=range(len(factor_value)))

    df_values['date'] = date
    df_values['code'] = factor_value['code']
    df_values['factor_value'] = factor_value[indicator]  # 指标选择

    # 对指标进行标准化
    df_values['factor_value'] = (df_values['factor_value'] -
                                 df_values['factor_value'].min()) \
                                / (df_values['factor_value'].max() -
                                   df_values['factor_value'].min())  # 标准化

    # 创建该段时间所有股票的收益率pool
    returns = get_data_panel("huangwei", "huangwei", list(df_values['code']),
                             date_r2, date_r, collection='stock_daily')

    df_price = pd.DataFrame(columns=['date', 'code'], index=range(len(df_values)))
    df_price['date'] = date
    df_price['code'] = df_values['code']
    df_price['cum_return'] = 0

    # 计算每只股票在这段时间的累计收益，并填入cum_return中
    def caculate_cum_return(code):
        one_stock = returns[returns['code'] == code].reset_index(drop=True)
        unique_index = list(one_stock['date'].drop_duplicates(keep='first', inplace=False).index)
        one_stock = one_stock.iloc[unique_index].sort_values(by='date')
        one_stock['rtn'] += 1
        one_stock['c_rtn'] = one_stock['rtn'].cumprod()
        return one_stock['c_rtn'].iloc[-1] - one_stock['c_rtn'].iloc[0]

    df_price['cum_return'] = df_price['code'].apply(caculate_cum_return)

    # 合并收盘价以及指标值数据，计算两者截面相关性
    df_all = df_price.merge(df_values[['code', 'factor_value']], how='inner', on='code')

    # 剔除不匹配的股票
    df_all = df_all.dropna(axis=0, how='any').reset_index(drop=True)
    ic, p = spearman_cor(df_all['cum_return'].values, df_all['factor_value'].values)

    print('IC值计算完毕')
    return ic, p
