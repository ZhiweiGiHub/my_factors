from factor.weighted_return_maker import *
from factor.factor_value_sort import *
# from factor.factor_maker import *
from factor.IC_maker import *


def groups_maker_(chooseday, changeday, next_changeday, change_days, tradingdays, filter=False):
    user = "huangwei"
    passwd = "huangwei"
    chooseday_r = chooseday[0:4] + '-' + chooseday[4:6] + '-' + chooseday[6:8]

    # 获取该段时间所有交易日series，命名为'date'
    start_index = tradingdays[tradingdays['tradingdays'] == changeday].index[0]
    end_index = tradingdays[tradingdays['tradingdays'] == next_changeday].index[0]
    dates = tradingdays.iloc[start_index:end_index].reset_index(drop=True)
    dates = dates.rename(columns={'tradingdays': 'date'})

    for i in range(len(dates)):
        dates['date'].iloc[i] = dates['date'].iloc[i][0:4] + '-' + dates['date'].iloc[i][
                                                                   4:6] + '-' + \
                                dates['date'].iloc[i][6:8]

    ###################################################
    # 因子选股(单变量)
    indicator = 'momentum'
    collection_name = "stock_indicator"
    database = 'stock_cn'

    # factor_value = base_indi_maker_(chooseday_r, indicator=indicator, type="DIY")
    #
    # factor_value = financial_maker_(chooseday_r,
    #                  collection_name=collection_name,
    #                  indicator=indicator, ac=True, database=database, type="DIY")
    factor_value = momentum(chooseday_r, ac=True, type="DIY")

    # 因子选股(多变量)
    # now = chooseday_r
    #
    # factor_info1 = {'collection_name': "stock_indicator", 'indicator': 'total_mv',
    #                 'ac': True, 'database': 'stock_cn', 'type': "industry", 'info': '食品饮料',
    #                 'factor_type': 'base'}
    # factor_info2 = {'collection_name': "qfa_roa", 'indicator': 'QFA_ROA',
    #                 'ac': True, 'database': 'stock_cn_finance1', 'type': "industry", 'info': '食品饮料',
    #                 'factor_type': 'financial'}
    # ic1, factor_value1 = ic_maker_(now, factor_info1)
    # ic2, factor_value2 = ic_maker_(now, factor_info2)
    #
    # factor_value1 = factor_value1.merge(factor_value2[['code', factor_info2['indicator']]], on='code', how='inner')
    # factor_value1['combine_factor'] = ic1 * factor_value1[factor_info1['indicator']] + ic2 * factor_value1[
    #     factor_info2['indicator']]
    #
    # indicator = 'combine_factor'  # 务必在最后的排序指标的名字赋给indicator
    # factor_value = factor_value1

    print('选股完成。')
    ###################################################

    factor_value = factor_value.dropna(how='any').reset_index(drop=True)  # 剔除缺失值,否则没法均匀分组

    ######################################################################

    # # 行业中性化：将指标值减去行业均值
    # try:
    #     all_data_industry = get_data_panel(user, passwd, list(factor_value['code']),
    #                                        chooseday_r, chooseday_r, collection='industry')
    # except:  # 如果调仓日没有行业分类数据，就用2019年7月1号的行业分类数据代替
    #     all_data_industry = get_data_panel(user, passwd, list(factor_value['code']),
    #                                        '2019-07-01', '2019-07-01', collection='industry')
    # if len(all_data_industry) == 0:
    #     all_data_industry = get_data_panel(user, passwd, list(factor_value['code']),
    #                                        '2019-07-01', '2019-07-01', collection='industry')
    #
    # # 将行业merge到factor_value中
    # factor_value = factor_value.merge(all_data_industry[['code', 'industry_name']])
    #
    # # 分组统计行业均值
    # industry_mean = factor_value.groupby('industry_name').mean()
    #
    # for i in range(len(factor_value)):
    #     try:
    #         factor_value[indicator].iloc[i] = factor_value[indicator].iloc[i] - \
    #                                           industry_mean[indicator].loc[factor_value['industry_name'].iloc[i]]
    #     except KeyError as e:
    #         print(factor_value['code'].iloc[i], '没有行业分类数据！赋值为nan')
    #         factor_value[indicator].iloc[i] = np.nan
    #
    # factor_value = factor_value.dropna(how='any').reset_index(drop=True)  # 剔除没有行业分类的股票,否则没法排序

    if filter == True:
        # 排序细化
        factor_value = factor_value_sort_(factor_value, factor_name=indicator, remove=['cap'])
        print('排序细化完成。')
    else:
        factor_value = factor_value.sort_values(by=indicator)

    ###########################################################

    # 选股日期下将股票池中的股票按某一指标值分成5组
    if len(factor_value) <= 20:
        print('股票池太小！')
        pass
    else:
        subsample_num = 5  # 分为5组，第一组指标值最小
        stocks_per_sample = round(len(factor_value) / subsample_num)  # 向下取整，多余的样本归于最后一个样本
        group1 = factor_value.iloc[0:stocks_per_sample].reset_index(drop=True)
        group2 = factor_value.iloc[stocks_per_sample:stocks_per_sample * 2].reset_index(drop=True)
        group3 = factor_value.iloc[stocks_per_sample * 2:stocks_per_sample * 3].reset_index(drop=True)
        group4 = factor_value.iloc[stocks_per_sample * 3:stocks_per_sample * 4].reset_index(drop=True)
        group5 = factor_value.iloc[stocks_per_sample * 4:].reset_index(drop=True)

        # 调仓日期下，
        # 选择每个组中可以在调仓日买到的股票名单
        # 获取调仓日所有停牌的股票信息
        change_day = change_days[0]
        change_days_r = change_day[0:4] + '-' + change_day[4:6] + '-' + change_day[6:8]
        suspend_stocks = list(get_data_crosssection(user, passwd, "code_list_suspend", change_days_r)['code'])

        # 求组合加权收益率
        group_returns = pd.DataFrame(columns=['date'], index=range(len(dates)))
        group_returns['date'] = dates['date']
        weighted_return1 = group_maker_(group1, suspend_stocks, dates)
        weighted_return2 = group_maker_(group2, suspend_stocks, dates)
        weighted_return3 = group_maker_(group3, suspend_stocks, dates)
        weighted_return4 = group_maker_(group4, suspend_stocks, dates)
        weighted_return5 = group_maker_(group5, suspend_stocks, dates)

        # 写入总表
        group_returns['group1'] = weighted_return1
        group_returns['group2'] = weighted_return2
        group_returns['group3'] = weighted_return3
        group_returns['group4'] = weighted_return4
        group_returns['group5'] = weighted_return5
        print('分组完成。')
        return group_returns
