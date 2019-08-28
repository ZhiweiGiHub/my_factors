import pandas as pd
from api import get_data_crosssection
from scipy.stats import zscore
from industry.sw_info import sw_info_
from industry.hsgt_info import hsgt_info_

# 截面日期

def stock_select_(chooseday_r):



    select_rule = "share"

    """
    select_rule是用于设立筛选条件
    “mv”       市值
    “share”    沪港通占比
    “all”      两个条件都考虑
    """

    # 市值前100
    if len(chooseday_r) == 8:
        chooseday_r = chooseday_r[0:4] + '-' + chooseday_r[4:6] + '-' + chooseday_r[6:8]
    else:
        pass

    if chooseday_r > "2019-06-28":

        total_mv = get_data_crosssection("huangwei", "huangwei",
                                         "stock_indicator",
                                         "2019-06-28",
                                         database="stock_cn").sort_values(
            by='total_mv', ascending=False).reset_index(drop=True).iloc[0:100]
    else:
        total_mv = get_data_crosssection("huangwei", "huangwei",
                                         "stock_indicator",
                                         chooseday_r,
                                         database="stock_cn").sort_values(
            by='total_mv', ascending=False).reset_index(drop=True).iloc[0:100]

    # 外资持股占流通股比例前50（沪深股通两者并集）

    top50_t = hsgt_info_(chooseday_r).sort_values(by='占自由流通股比(%)',
                                                  ascending=False).reset_index(drop=True).iloc[0:50]
    if select_rule == 'all':
        # 取市值和沪港通占比并集
        mv_and_hst = list(set(top50_t['code']).union(set(total_mv['code'])))
    elif select_rule == 'mv':
        mv_and_hst = list(set(total_mv['code']))
    elif select_rule == 'share':
        mv_and_hst = list(set(top50_t['code']))

    # 获取其申万一级行业名称、代码
    # 读取股票信息表
    stock_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/stock_info_20190801.xlsx')

    df_mvhst = pd.DataFrame(columns=['code'], index=range(len(mv_and_hst)))

    df_mvhst['code'] = mv_and_hst
    remain = [index for index in range(len(stock_info)) if stock_info['证券代码'].iloc[index] in list(df_mvhst['code'])]
    stock_info = stock_info.iloc[remain].reset_index(drop=True).rename(columns={"证券代码": "code"})
    df_mvhst = df_mvhst.merge(stock_info[['code', '证券简称']], on='code', how='left')

    sw1 = sw_info_(chooseday_r, "sw1")

    # 有4只股票没有行业信息
    df_mvhst = df_mvhst.merge(sw1[['code', 'sw_name']],
                              on='code', how='left').dropna(axis=0).reset_index(drop=True)

    # 按指标（与市值信息匹配的财报）选股，每个行业至少保留一个，总数不超过50个
    # 根据当下时间调整为财年
    # 根据财务报告发布的规律，对获取财务指标的时间进行调整

    if int(chooseday_r[5:7]) >= 1 and int(chooseday_r[5:7]) <= 4:
        year = str(int(chooseday_r[0:4]) - 1)
        chooseday_r_adj = year + '-09-30'
    elif int(chooseday_r[5:7]) >= 5 and int(chooseday_r[5:7]) <= 9:
        year = str(int(chooseday_r[0:4]) - 1)
        chooseday_r_adj = year + '-03-31'  # '-12-31'
    elif int(chooseday_r[5:7]) >= 10 and int(chooseday_r[5:7]) <= 11:
        year = str(int(chooseday_r[0:4]))
        chooseday_r_adj = year + '-06-30'
    elif int(chooseday_r[5:7]) > 11:
        year = str(int(chooseday_r[0:4]))
        chooseday_r_adj = year + '-09-30'

    opprofit = get_data_crosssection("huangwei", "huangwei",
                                     "opprofit",
                                     chooseday_r_adj,
                                     database="stock_cn_finance1")
    roe_de = get_data_crosssection("huangwei", "huangwei",
                                   "qfa_roe_deducted",
                                   chooseday_r_adj,
                                   database="stock_cn_finance1")
    or_ttm = get_data_crosssection("huangwei", "huangwei",
                                   "or_ttm",
                                   chooseday_r_adj,
                                   database="stock_cn_finance1")

    # 将财务信息合并进原表中
    df_mvhst = df_mvhst.merge(opprofit[['code', 'OPPROFIT']],
                              on='code', how='left').dropna(axis=0).reset_index(drop=True)
    df_mvhst = df_mvhst.merge(or_ttm[['code', 'OR_TTM']],
                              on='code', how='left').dropna(axis=0).reset_index(drop=True)
    df_mvhst = df_mvhst.merge(roe_de[['code', 'QFA_ROE_DEDUCTED']],
                              on='code', how='left').dropna(axis=0).reset_index(drop=True)

    # 标准化及评分
    df_mvhst['OPPROFIT'] = zscore(df_mvhst['OPPROFIT'])
    df_mvhst['OR_TTM'] = zscore(df_mvhst['OR_TTM'])
    df_mvhst['QFA_ROE_DEDUCTED'] = zscore(df_mvhst['QFA_ROE_DEDUCTED'])
    df_mvhst['score'] = df_mvhst['OPPROFIT'] + df_mvhst['OR_TTM'] + df_mvhst['QFA_ROE_DEDUCTED']

    # df_mvhst.set_index('code').to_excel(
    #     'C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/result_all.xls')

    # 筛选出top50
    indu_num = len(df_mvhst.groupby('sw_name').count())
    df_mvhst_sum = df_mvhst.groupby('sw_name').sum().sort_values(by='score',
                                                                 ascending=False)
    # 获得最后名单
    last_list = []
    index = 0
    indu_list = list(df_mvhst_sum.index)
    top = 0
    df_mvhst2 = df_mvhst  # 保持df_mvhst完整

    while index < indu_num:
        temp = df_mvhst2[df_mvhst2['sw_name'] == indu_list[index]].sort_values(
            by='score', ascending=False)
        last_list.append(temp.index[top])
        index += 1

    df_mvhst2 = df_mvhst2.drop(last_list).sort_values('score', ascending=False)
    if select_rule == 'all':
        append_list = list(df_mvhst2.index[0: 50 - len(last_list)])
    elif select_rule == 'mv' or select_rule == 'share':
        if indu_num < 25:
            append_list = list(df_mvhst2.index[0: 25 - len(last_list)])
        else:  # 如果最后名单中申万一级行业数超过25，则全部选中（不会超过28只）
            append_list = []

    last_list += append_list

    df_mvhst_50 = df_mvhst.iloc[last_list].sort_values('sw_name').reset_index(drop=True)
    # .set_index('code').to_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/result_50.xls')
    return df_mvhst_50
