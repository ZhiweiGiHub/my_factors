import numpy as np
from api import *
from factor.utils import *
from scipy import stats


# 获取factor_value矩阵的date和code个字段
def factor_value_sort_(factor_value, factor_name='total_mv',
                       remove=['cap', 'industry', 'beta']):
    to_sort = factor_value[['date', 'code', factor_name, 'weight_adj_sw1']]


    # 剔除factor_name有缺失的数据点
    # print('原有数据长度', len(to_sort))
    to_sort = to_sort.dropna(axis=0, how='any').reset_index(drop=True)
    # print('现有数据长度', len(to_sort))

    # 对指标值缩尾处理(1%)
    if len(to_sort) >= 100:
        to_sort['win_factor'] = stats.mstats.winsorize(to_sort[factor_name], (0.01, 0.01))
        to_sort[factor_name] = to_sort['win_factor']
        del to_sort['win_factor']
    else:
        pass

    # 正态标准化处理
    to_sort['zscore'] = (to_sort[factor_name].values -
                         to_sort[factor_name].values.mean()) / to_sort[factor_name].values.std()

    # 添加个股的beta系数
    to_sort['beta'] = 0

    # 组合中每个股票过去200日的日收益率序列和沪深300的收益率序列
    datee = to_sort['date'][0]
    data_200_before = datetime.datetime.strptime(datee, "%Y-%m-%d") + relativedelta(months=-12)
    data_200_before_str = data_200_before.strftime('%Y-%m-%d')
    hs300 = get_data_panel("huangwei", "huangwei", ['000300.SH'],
                           data_200_before_str, datee,
                           collection='index_rtn')

    # 先获取本组股票过去一年的日收益率序列
    all_data = get_data_panel("huangwei", "huangwei", list(to_sort['code']),
                              data_200_before_str, datee, collection='stock_daily')

    # 添加个股的对数市值
    to_sort['lncap'] = 0
    datee = forwardchangeday2(datee)
    all_data_cap = get_data_panel("huangwei", "huangwei", list(to_sort['code']),
                                  datee, datee, collection='stock_indicator')
    # 添加个股的所属行业
    to_sort['industry'] = 0
    try:
        all_data_industry = get_data_panel("huangwei", "huangwei", list(to_sort['code']),
                                           datee, datee, collection='industry')
    except:
        # 如果调仓日没有行业分类数据，就用2019年7月1号的行业分类数据代替
        all_data_industry = get_data_panel("huangwei", "huangwei", list(to_sort['code']),
                                           '2019-07-01', '2019-07-01', collection='industry')
    if len(all_data_industry) == 0:
        all_data_industry = get_data_panel("huangwei", "huangwei", list(to_sort['code']),
                                           '2019-07-01', '2019-07-01', collection='industry')
    index = 0
    to_sort_arr = to_sort.values
    to_sort_columns = to_sort.columns

    print('中性化处理中...')
    for code in list(to_sort_arr[:, 1]):
        onestock_all_data = all_data[
            all_data['code'] == code].reset_index(drop=True)
        unique_index = list(
            onestock_all_data['date'].drop_duplicates(keep='first',
                                                      inplace=False).index)
        onestock_all_data = onestock_all_data.iloc[unique_index].reset_index(drop=True)

        # 计算个股的beta值
        if len(onestock_all_data['rtn'].values) != len(hs300['pct_chg'].values):
            length = min(len(onestock_all_data['rtn'].values), len(hs300['pct_chg'].values))
            onestock_all_data = onestock_all_data.iloc[0:length]
            hs300 = hs300.iloc[0:length]
        else:
            pass
        to_sort_arr[index, 5] = (np.cov(onestock_all_data['rtn'].values,
                                        hs300['pct_chg'].values / 100) /
                                 np.var(hs300['pct_chg'].values / 100))[0][1]
        try:
            to_sort_arr[index, 6] = np.log(all_data_cap[
                                                      all_data_cap['code'] == code][
                                                      'total_mv'].values[0])
        except:
            to_sort_arr[index, 7] = np.nan
        try:
            to_sort_arr[index, 7] = all_data_industry[
                all_data_industry['code'] == code]['industry_name'].values[0]
        except:
            to_sort_arr[index, 7] = np.nan

        index += 1
        break

    to_sort = pd.DataFrame(to_sort_arr, columns=to_sort_columns)
    # 将行业变量转化为虚拟变量
    to_sort_dummies = pd.get_dummies(
        to_sort,
        columns=['industry'],
        prefix=['industry'],
        prefix_sep="_",
        dummy_na=False,
        drop_first=True
    )

    # 对缺失值进行处理
    print('未处理前有样本:', len(to_sort_dummies))
    to_sort_dummies = to_sort_dummies.dropna(axis=0, how='any')
    print('剔除缺失值后有样本：', len(to_sort_dummies))

    # 对指标值的z-score做回归
    if len(remove) == 1:
        if 'cap' in remove:  # 只剔除市值影响
            y = to_sort_dummies[to_sort_dummies.columns[4]]
            x = to_sort_dummies[to_sort_dummies.columns[6]]
            x = sm.add_constant(x)
            model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
            results = model.fit()
            resids = results.resid
        elif 'industry' in remove:
            y = to_sort_dummies[to_sort_dummies.columns[4]]
            x = to_sort_dummies[to_sort_dummies.columns[7:]]
            x = sm.add_constant(x)
            model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
            results = model.fit()
            resids = results.resid
        elif 'beta' in remove:
            y = to_sort_dummies[to_sort_dummies.columns[4]]
            x = to_sort_dummies[to_sort_dummies.columns[5]]
            x = sm.add_constant(x)
            model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
            results = model.fit()
            resids = results.resid
    elif len(remove) == 2:
        if 'cap' in remove and 'industry' in remove:
            y = to_sort_dummies[to_sort_dummies.columns[4]]
            x = to_sort_dummies[to_sort_dummies.columns[6:]]
            x = sm.add_constant(x)
            model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
            results = model.fit()
            resids = results.resid
        elif 'cap' in remove and 'beta' in remove:
            y = to_sort_dummies[to_sort_dummies.columns[4]]
            x = to_sort_dummies[to_sort_dummies.columns[5:7]]
            x = sm.add_constant(x)
            model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
            results = model.fit()
            resids = results.resid
        elif 'beta' in remove and 'industry' in remove:
            y = to_sort_dummies[to_sort_dummies.columns[4]]
            x = to_sort_dummies[[to_sort_dummies.columns[5]] + list(to_sort_dummies.columns[7:])]
            x = sm.add_constant(x)
            model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
            results = model.fit()
            resids = results.resid
        else:
            print('因变量设置错误！')
    elif len(remove) == 3:
        y = to_sort_dummies[to_sort_dummies.columns[4]]
        x = to_sort_dummies[to_sort_dummies.columns[5:]]
        x = sm.add_constant(x)
        model = sm.OLS(y.astype(float), x.astype(float))  # 注意转换为OLS需要的浮点格式
        results = model.fit()
        resids = results.resid
    else:
        return to_sort.sort_values(by=factor_name, ascending=True).reset_index(drop=True)

    to_sort_dummies[factor_name] = resids.values
    to_sort_dummies = to_sort_dummies.sort_values(by=factor_name,
                                                  ascending=True).reset_index(drop=True)
    print('中性化处理完成.')
    return to_sort_dummies


if __name__ == '__main__':
    from factor.factor_maker import momentum
    factor_value = momentum("2018-06-28", ac=True, type="index", info='000300.SH')
    cc = factor_value_sort_(factor_value,
                            factor_name='momentum',
                            remove=['cap', 'industry', 'beta'])
