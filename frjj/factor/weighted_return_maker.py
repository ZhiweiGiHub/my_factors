# import pandas as pd
# import numpy as np
# from frjj_codes.api import *
from factor.weight_maker import *


def group_maker_(group1, suspend_stocks, dates):
    user = "huangwei"
    passwd = "huangwei"
    # 以group1为例
    index_out = [i for i in range(len(group1)) if group1['code'].iloc[i] in suspend_stocks]
    group1 = group1.drop(index_out).reset_index(drop=True)

    # 获得每只股票在回测期的日收益率（除权调整），累积日收益率数据
    data = get_data_panel(user, passwd, list(group1['code']), dates['date'].iloc[0], dates['date'].iloc[-1])

    df_group_data = dates
    for code in list(group1['code']):
        temp = data[data['code'] == code].reset_index(drop=True)
        unique_index = list(temp['date'].drop_duplicates(keep='first', inplace=False).index)
        temp = temp.iloc[unique_index]
        # temp['rtn'] = temp['rtn'] + 1
        # temp['rtn'] = temp['rtn'].cumprod()
        df_group_data = df_group_data.merge(temp[['date', 'rtn']], how='outer', on='date')
        df_group_data = df_group_data.rename(columns={'rtn': code})

    df_group_data = df_group_data.dropna(axis=1, how='any')

    # 权重矩阵
    weight = pd.DataFrame(columns=['date', 'code', 'weight'], index=range(len(df_group_data.columns) - 1))
    weight['code'] = df_group_data.columns[1:]
    weight['date'] = group1['date'].iloc[0]    # 按选股日的信息决定权重

    weight = weight_maker_(weight, how=0)  # 加权方式0=等权，1=市值加权，2=波动率倒数加权

    # 加权求和
    group1_return_arr = df_group_data.values[:, 1:]
    weighted_return = [sum(group1_return_arr[i, :] * weight['weight'].values) for i in
                       range(len(df_group_data))]
    return weighted_return
