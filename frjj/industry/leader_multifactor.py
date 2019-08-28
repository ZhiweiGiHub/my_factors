# 使用factor_maker_c类，形成多因子选股名单
from factor.factor_maker_c import FactorMaker
import pandas as pd
from industry.get_all_not_available import get_all_not_available_
from industry.ortho import lowdin_orthogonal
from industry.period_return_maker import period_return_maker_
from industry.hsgt_info import hsgt_info_
from factor.weight_maker import weight_maker_
# from api import get_data_crosssection_genaral

IC_history = {}  # 用来记录每个月各因子的IC值，以计算IR值
p_history = {}  # 用来记录每个月各因子的IC值，以计算IR值


# index_content = get_data_crosssection_genaral("huangwei", "huangwei",
#                                               "index_weight", 'index_code',
#                                               "000905.SH")


def leader_multifactor_maker(chooseday_r, factor_list, factor_type,
                             weighted=False, include_ir=False,
                             orthogonalization=False):
    indicator_list = []
    ic_list = []
    p_list = []
    global IC_history
    if len(chooseday_r) == 8:
        chooseday_r = chooseday_r[0:4] + '-' \
                      + chooseday_r[4:6] + '-' \
                      + chooseday_r[6:8]

    for factor, type_ in zip(factor_list, factor_type):

        if type_ == 'base':
            a = FactorMaker(chooseday_r, collection_name="stock_indicator",
                            indicator=factor, ac=True,
                            database='stock_cn', type="DIY",
                            info='医药生物', factor_type=type_)
        elif type_ == 'financial' or type_ == 'tech' or type_ == 'my_factor':
            a = FactorMaker(chooseday_r, collection_name=factor.lower(),
                            indicator=factor, ac=False,
                            database='stock_cn_finance1', type="DIY",
                            info='医药生物', factor_type=type_)
        elif type_ == 'other':
            a = FactorMaker(chooseday_r, collection_name='d_' + factor.lower(),
                            indicator=factor, ac=False,
                            database='stock_cn_finance1', type="DIY",
                            info='医药生物', factor_type=type_)

        # 类中只得到中性化、标准化之后的因子值。在本函数中进行正交化以及IC/IR值得计算
        factor_value = a.neutralization(remove=['industry'])
        indicator_name = a.indicator
        indicator_list.append(indicator_name)

        if factor == factor_list[0]:
            all = pd.DataFrame(columns=['code', 'date', 'weight_adj_sw1'],
                               index=range(len(factor_value)))
            all['code'] = factor_value['code']
            all['date'] = factor_value['date']
            all['weight_adj_sw1'] = factor_value['weight_adj_sw1']

        # 因子值的合并
        all = all.merge(factor_value[['code', indicator_name]],
                        on='code', how='inner')
        print('股票池有股票：', len(all))

    # 加入沪港通因子
    hgt_list = hsgt_info_(chooseday_r)

    all = all.merge(hgt_list[['code', 'hgt_ratio']], on='code', how='left')
    indicator_list.append('hgt_ratio')
    all = all.dropna(axis=0, how='any').reset_index(drop=True)
    print('加入沪港通因子，剔除缺失还有股票：', len(all))

    # # 与指数成分股取交集
    # index_content_now = index_content[index_content['date'] == list(index_content['date'][
    #                                       index_content['date']
    #                                       >= chooseday_r])[0]].reset_index(drop=True)
    # index_content_now.rename(columns={'stock_code': 'code'}, inplace=True)
    # all = all.merge(index_content_now['code'], on='code', how='inner').reset_index(drop=True)

    if orthogonalization:
        # 对因子汇总表all进行对称正交化
        all = lowdin_orthogonal(all)

    # 调用IC_maker计算分别计算IC值，形成ic_list
    for indicator in indicator_list:
        ic, p = period_return_maker_(chooseday_r, all, indicator)
        ic_list.append(ic)
        p_list.append(p)

    IC_history[chooseday_r] = ic_list
    p_history[chooseday_r] = p_list

    IC_history_df = pd.DataFrame(IC_history).T
    ir = list(IC_history_df.mean(axis=0) / IC_history_df.std(axis=0))

    # 使用IR
    if include_ir and len(IC_history) > 1:
        ic_list = ir

    # 剔除当天停牌状态、退市状态、ST状态的股票
    not_available = get_all_not_available_(chooseday_r, remove_st=True)
    drop_index = [index for index in range(len(all)) if all['code'].iloc[index] in not_available]
    all = all.drop(drop_index).reset_index(drop=True)

    all['multi'] = 0

    for indicator, ic in zip(indicator_list, ic_list):
        if weighted:
            all['multi'] += all[indicator]*ic
        else:
            if indicator == 'pb' or indicator == 'momentum' or indicator == 'pe_ttm':
                all['multi'] += all[indicator]*-1
            else:
                all['multi'] += all[indicator]
    print('该行业有股票：', len(all))
    all = all.sort_values(by='multi', ascending=False).reset_index(drop=True).iloc[0:20]

    # 不把龙头股作为总的股票池时，使用这里的weight_maker_函数
    all = weight_maker_(chooseday_r, all, how=1)

    return all


if __name__ == "__main__":
    factor_list = ['pb', 'QFA_ROE_DEDUCTED']
    factor_type = ['base', 'financial']
    chooseday_r = "2019-06-28"
    all = leader_multifactor_maker(chooseday_r, factor_list, factor_type)
