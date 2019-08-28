from api import get_data_panel, get_data_panel_backwards


def weight_maker_(chooseday_r, weight_origin, how=0):
    user = "huangwei"
    passwd = "huangwei"
    weight = weight_origin

    if how == 0:  # 等权
        weight['weight_adj_sw1'] = 1 / len(weight_origin)
        print('持仓赋权方式为：等权')

    elif how == 1:  # 按市值加权，取出这天股票组合的市值信息
        if 'total_mv' not in weight.columns:

            info_all = get_data_panel(user, passwd,
                                      list(weight['code']),
                                      chooseday_r,
                                      chooseday_r,
                                      collection='stock_indicator')
            weight = weight.merge(info_all[['code', 'total_mv']], how='inner', on='code')
        else:
            pass

        weight_sum = sum(weight['total_mv'])
        weight['weight_adj_sw1'] = weight['total_mv'] / weight_sum
        del weight['total_mv']
        print('持仓赋权方式为：市值加权')

    elif how == 2:  # 按过去一年收益率波动率倒数
        # 获取当下日期过去一年的收益率数据
        info_all = get_data_panel_backwards(user, passwd, list(weight['code']),
                                            chooseday_r, back_years=0, back_months=12,
                                            collection='stock_daily')
        info_all = info_all.rename(columns={'code': 'codes'})
        # 分组统计标准差
        grouped = info_all.groupby(by=info_all['codes']).std()
        grouped['code'] = grouped.index
        weight = weight.merge(grouped[['code', 'rtn']], how='inner', on='code')
        weight['rtn'] = 1 / weight['rtn']
        weight['rtn'] = weight['rtn'].ffill()  # 缺失值延续前值
        weight['weight_sum'] = sum(weight['rtn'])
        weight['weight_adj_sw1'] = weight['rtn'] / weight['weight_sum']
        print('持仓赋权方式为：波动率倒数加权')

    return weight
