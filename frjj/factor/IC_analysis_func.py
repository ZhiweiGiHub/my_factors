from factor.factor_maker import *

# 计算最近一个月的ic及对应的p_value
def ic_analysis(changeday):
    if len(changeday) == 8:
        date_r = changeday[0:4] + '-' + changeday[4:6] + '-' + changeday[6:8]
    else:
        date_r = changeday

    factor_value = base_indi_maker_(date_r,
                                    collection_name="d_tech_moneyflow20",
                                    indicator='TECH_MONEYFLOW20',
                                    ac=False, database="stock_cn_finance1", type=None)

    # 创建某时刻截面指标df
    df_values = pd.DataFrame(columns=['date', 'code', 'factor_value'],
                             index=range(len(factor_value)))
    df_values['date'] = date_r
    df_values['code'] = factor_value['code']
    df_values['factor_value'] = factor_value['TECH_MONEYFLOW20']  # 指标选择

    # 获取当下日期过去一个月的收益率数据
    info_all = get_data_panel_backwards("huangwei", "huangwei",
                                        list(df_values['code']),
                                        date_r,
                                        back_years=0, back_months=1,
                                        collection='stock_daily')

    info_all = info_all.rename(columns={'code': 'codes'})
    info_all['rtn'] = info_all['rtn'] + 1
    info_all = info_all[['codes', 'rtn', 'date']]

    # 分组统计
    grouped = info_all.groupby('codes').last()
    grouped_first = info_all.groupby(by=info_all['codes']).first()
    grouped_last = pd.DataFrame(columns=['rtn', 'date'], index=grouped_first.index)
    grouped_last['date'] = info_all['date'].iloc[-1]

    for index in grouped_last.index:
        temp = info_all[info_all['codes'] == index]
        grouped_last['rtn'].loc[index] = list(temp['rtn'].cumprod())[-1]

    momentums = grouped_last['rtn'] - grouped_first['rtn']

    df_price = pd.DataFrame(columns=['date', 'code', 'momentum'], index=range(len(grouped)))
    df_price['code'] = list(grouped.index)
    df_price['date'] = grouped['date'].iloc[0]
    df_price['momentum'] = list(momentums.values)
    df_price = df_price.sort_values(by='momentum', ascending=True).reset_index(drop=True)

    # 合并收盘价以及指标值数据，计算两者截面相关性
    df_all = df_price.merge(df_values[['code', 'factor_value']], how='inner', on='code')
    # 剔除不匹配的股票
    df_all = df_all.dropna(how='any').reset_index(drop=True)
    df_all = df_all.rename(columns={'momentum': 'return'})

    # IC及rankIC
    # Y = df_all['return']
    # X = df_all['factor_value']
    # 写入df_result
    # df_result['slope'] = OLS_param(Y, X)[0]
    # df_result['pvalue_ols'] = OLS_param(Y, X)[1]
    spearman = spearman_cor(df_all['return'].values, df_all['factor_value'].values)[0]
    pvalue_cor = spearman_cor(df_all['return'].values, df_all['factor_value'].values)[1]

    return spearman, pvalue_cor


if __name__ == '__main__':
    changeday = '2017-03-01'
    ic, p_value = ic_analysis(changeday)
