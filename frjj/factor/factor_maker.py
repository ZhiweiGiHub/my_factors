from api import *
from factor.stock_pool_maker import *


def base_indi_maker_(chooseday_r,
                     collection_name="stock_indicator",
                     indicator='total_mv', ac=True,
                     database='stock_cn',
                     type="index", info='000300.SH'):

    user = "huangwei"
    passwd = "huangwei"

    factor_value = get_data_crosssection(user, passwd, collection_name, chooseday_r, database=database).sort_values(
        by=indicator, ascending=ac).reset_index(drop=True)

    # 限定在某一个指数成分股股票池中
    if type == None:
        return factor_value
    elif type == "index":
        hs300_list = stock_pool_maker(chooseday_r, type, info)
        factor_value = factor_value.merge(hs300_list[['i_weight', 'code']], how='inner', on='code')
        factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
        return factor_value
    elif type == "industry":
        hs300_list = stock_pool_maker(chooseday_r, type, info)
        factor_value = factor_value.merge(hs300_list[['industry_name', 'code']], how='inner', on='code')
        factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
        return factor_value
    elif type == "DIY":
        hs300_list = stock_pool_maker(chooseday_r, type, info)
        factor_value = factor_value.merge(hs300_list[['code']], how='inner', on='code')
        factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
        return factor_value

# 财务指标排序
def financial_maker_(chooseday_r,
                     collection_name="qfa_roe_deducted",
                     indicator='QFA_ROE_DEDUCTED',
                     ac=False, database='stock_cn_finance1',
                     type="index", info='000300.SH'):
    user = "huangwei"
    passwd = "huangwei"

    # 根据财务报告发布的规律，对获取财务指标的时间进行调整
    if int(chooseday_r[5:7]) >= 1 and int(chooseday_r[5:7]) <= 4:
        year = str(int(chooseday_r[0:4]) - 1)
        chooseday_r_adj = year + '-09-30'
    elif int(chooseday_r[5:7]) >= 5 and int(chooseday_r[5:7]) <= 9:
        year = str(int(chooseday_r[0:4]) - 1)
        chooseday_r_adj = year + '-03-31'
    elif int(chooseday_r[5:7]) >= 10 and int(chooseday_r[5:7]) <= 11:
        year = str(int(chooseday_r[0:4]))
        chooseday_r_adj = year + '-06-30'
    elif int(chooseday_r[5:7]) > 11:
        year = str(int(chooseday_r[0:4]))
        chooseday_r_adj = year + '-09-30'

    # 根据日期，取出该时点的所有数据

    factor_value = get_data_crosssection(user, passwd,
                                         collection_name,
                                         chooseday_r_adj, database=database).sort_values(
        by=indicator, ascending=ac).reset_index(drop=True)
    if type == None:
        return factor_value
    elif type == "index":
        hs300_list = stock_pool_maker(chooseday_r, type, info)
        factor_value = factor_value.merge(hs300_list[['i_weight', 'code']], how='inner', on='code')
        factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
        # 恢复为本来的日期
        factor_value['date'] = chooseday_r
        return factor_value
    elif type == "industry":
        hs300_list = stock_pool_maker(chooseday_r, type, info)
        factor_value = factor_value.merge(hs300_list[['industry_name', 'code']], how='inner', on='code')
        factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
        # 恢复为本来的日期
        factor_value['date'] = chooseday_r
    elif type == "DIY":
        hs300_list = stock_pool_maker(chooseday_r, type, info)
        factor_value = factor_value.merge(hs300_list[['code']], how='inner', on='code')
        factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
        return factor_value
    elif type == None:
        return factor_value


# 动量因子的构造
def momentum(chooseday_r, ac=False, type="index", info='000300.SH'):
    user = "huangwei"
    passwd = "huangwei"

    # 取沪深300成分股名单
    hs300_list = stock_pool_maker(chooseday_r, type, info)

    # 获取当下日期过去一个月的收益率数据
    info_all = get_data_panel_backwards(user, passwd,
                                        list(hs300_list['code']),
                                        hs300_list['date'].iloc[0],
                                        back_years=0, back_months=1,
                                        collection='stock_daily')
    info_all = info_all.rename(columns={'code': 'codes'})
    info_all['rtn'] = info_all['rtn'] + 1
    info_all = info_all[['codes', 'rtn', 'date']]
    # 分组统计
    grouped = info_all.groupby(by=info_all['codes']).last()
    grouped_first = info_all.groupby(by=info_all['codes']).first()
    momentums = grouped['rtn'] - grouped_first['rtn']

    factor_value = pd.DataFrame(columns=['date', 'code', 'momentum'], index=range(len(grouped)))
    factor_value['code'] = list(grouped.index)
    factor_value['date'] = grouped['date'].iloc[0]
    factor_value['momentum'] = list(momentums.values)
    factor_value = factor_value.sort_values(by='momentum', ascending=ac).reset_index(drop=True)
    return factor_value


def factor_maker_(chooseday_r,
                  collection_name="qfa_roe_deducted",
                  indicator='QFA_ROE_DEDUCTED',
                  ac=True, database='stock_cn_finance1',
                  type="index", info='000300.SH', factor_type='base'):

    user = "huangwei"
    passwd = "huangwei"

    """factor_type可选： 
        base:           stock_indicator 数据表中的指标
        financial：      finance1 数据表中的指标
        tech:           某些自己构造的量价指标，如动量指标
    """
    if len(chooseday_r) == 8:
        chooseday_r = chooseday_r[0:4] + '-' + chooseday_r[4:6] + '-' + chooseday_r[6:8]
    else:
        pass
    if factor_type == 'base':
        factor_value = get_data_crosssection(user, passwd,
                                             collection_name,
                                             chooseday_r,
                                             database=database).sort_values(
            by=indicator, ascending=ac).reset_index(drop=True)

        # 限定在某一个指数成分股股票池中
        if type is None:
            return factor_value
        elif type == "index":
            hs300_list = stock_pool_maker(chooseday_r, type, info)
            factor_value = factor_value.merge(hs300_list[['i_weight', 'code']],
                                              how='inner', on='code')
            factor_value = factor_value.sort_values(by=indicator,
                                                    ascending=ac).reset_index(drop=True)
            return factor_value
        elif type == "industry":
            hs300_list = stock_pool_maker(chooseday_r, type, info)
            factor_value = factor_value.merge(hs300_list[['industry_name', 'code']],
                                              how='inner', on='code')
            factor_value = factor_value.sort_values(by=indicator,
                                                    ascending=ac).reset_index(drop=True)
            return factor_value
        elif type == 'DIY':
            stock_pool = stock_pool_maker(chooseday_r, type, info)
            factor_value = factor_value.merge(stock_pool[['weight_adj_sw1', 'code']],
                                              how='inner', on='code')
            factor_value = factor_value.sort_values(by=indicator,
                                                    ascending=ac).reset_index(drop=True)
            return factor_value

    elif factor_type == 'financial':
        # 根据财务报告发布的规律，对获取财务指标的时间进行调整
        if 1 <= int(chooseday_r[5:7]) <= 4:
            year = str(int(chooseday_r[0:4]) - 1)
            chooseday_r_adj = year + '-09-30'
        elif 5 <= int(chooseday_r[5:7]) <= 9:
            year = str(int(chooseday_r[0:4]))
            chooseday_r_adj = year + '-03-31'
        elif 10 <= int(chooseday_r[5:7]) <= 11:
            year = str(int(chooseday_r[0:4]))
            chooseday_r_adj = year + '-06-30'
        elif int(chooseday_r[5:7]) > 11:
            year = str(int(chooseday_r[0:4]))
            chooseday_r_adj = year + '-09-30'

        # 根据日期，取出该时点的所有数据
        factor_value = get_data_crosssection(user, passwd,
                                             collection_name,
                                             chooseday_r_adj, database=database).sort_values(
            by=indicator, ascending=ac).reset_index(drop=True)
        if type == None:
            return factor_value
        elif type == "index":
            hs300_list = stock_pool_maker(chooseday_r, type, info)
            factor_value = factor_value.merge(hs300_list[['i_weight', 'code']], how='inner', on='code')
            factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
            # 恢复为本来的日期
            factor_value['date'] = chooseday_r
            return factor_value
        elif type == "industry":
            hs300_list = stock_pool_maker(chooseday_r, type, info)
            factor_value = factor_value.merge(hs300_list[['industry_name', 'code']], how='inner', on='code')
            factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
            # 恢复为本来的日期
            factor_value['date'] = chooseday_r
            return factor_value
        elif type == 'DIY':
            stock_pool = stock_pool_maker(chooseday_r, type, info)
            factor_value = factor_value.merge(stock_pool[['weight_adj_sw1', 'code']], how='inner', on='code')
            factor_value = factor_value.sort_values(by=indicator, ascending=ac).reset_index(drop=True)
            return factor_value
    elif factor_type == 'tech':
        # 取总的股票池
        stockpool = stock_pool_maker(chooseday_r, type)

        # 获取当下日期过去一个月的收益率数据
        info_all = get_data_panel_backwards("huangwei", "huangwei",
                                            list(stockpool['code']),
                                            chooseday_r,
                                            back_years=0, back_months=6,
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
        factor_value = pd.DataFrame(columns=['date', 'code', 'momentum'], index=range(len(grouped)))
        factor_value['code'] = list(grouped.index)
        factor_value['date'] = grouped['date'].iloc[0]
        factor_value['momentum'] = list(momentums.values)
        factor_value = factor_value.sort_values(by='momentum', ascending=ac).reset_index(drop=True)
        return factor_value


if __name__ == "__main__":
    data = base_indi_maker_('2017-01-26')
    data = base_indi_maker_('2017-01-26', indicator='pb')
