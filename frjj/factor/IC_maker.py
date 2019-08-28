from factor.factor_maker import *


def ic_maker_(now, factor_info):
    back = month(now, change_month=-12)
    date = now
    if len(date) <= 8:
        date_r = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    else:
        date_r = date
    date_r2 = back
    # 获取截面指标值
    factor_value = factor_maker_(date_r, collection_name=factor_info['collection_name'],
                                 indicator=factor_info['indicator'], ac=factor_info['ac'],
                                 database=factor_info['database'], type=factor_info['type'],
                                 info=factor_info['info'], factor_type=factor_info['factor_type'])

    # 创建某时刻截面指标df
    df_values = pd.DataFrame(columns=['date', 'code', 'factor_value'],
                             index=range(len(factor_value)))

    df_values['date'] = date
    df_values['code'] = factor_value['code']
    df_values['factor_value'] = factor_value[factor_info['indicator']]  # 指标选择

    # 创建某时刻截面收盘价df
    closeprice1 = get_data_crosssection("huangwei", "huangwei", "stock_daily", date_r)
    closeprice2 = get_data_crosssection("huangwei", "huangwei", "stock_daily", date_r2)

    df_price = pd.DataFrame(columns=['date', 'code'], index=range(len(closeprice1)))
    df_price['code'] = closeprice1['code']
    df_price['date'] = date

    df_price = df_price.merge(closeprice1[['code', 'close']], how='inner', on='code')
    df_price = df_price.merge(closeprice2[['code', 'close']], how='inner', on='code')

    df_price['return'] = df_price['close_y'] / df_price['close_x'] - 1

    # 合并收盘价以及指标值数据，计算两者截面相关性
    df_all = df_price.merge(df_values[['code', 'factor_value']], how='inner', on='code')
    # 剔除不匹配的股票
    df_all = df_all.dropna(how='any')
    ic = spearman_cor(df_all['return'].values, df_all['factor_value'].values)[0]

    print('IC值计算完毕。')
    return ic, factor_value


if __name__ == "__main__":
    now = '20170215'
    factor_info = {'collection_name': "qfa_roe_deducted", 'indicator': 'QFA_ROE_DEDUCTED',
                   'ac': True, 'database': 'stock_cn_finance1', 'type': "index", 'info': '000300.SH',
                   'factor_type': 'financial'}
    ic, factor_value = ic_maker_(now, factor_info)
