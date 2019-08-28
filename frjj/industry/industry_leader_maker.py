import pandas as pd
from tqdm import tqdm
from api import get_data_crosssection_genaral, get_data_panel, get_tradingdays, get_data_crosssection
from factor.utils import forwardchangeday2
from industry.sw_info import sw_info_


def industry_leader_maker_(cal_day, indicator,
                           industry_weight,
                           top_num,
                           index_for_simple_weight):
    """

    :param stock_info: 当下A股市场的所有股票名称、代码信息
    :param cal_day: 选股日期
    :param indicator: 选股时引入的财务指标，包括总市值（total_mv）、流通市值（circ_mv）
    :param industry_weight: 根据某指数，复制其行业权重。或者'market'：按全市场行业权重
    :param top_num: 是否选择前几只
    :param index_for_simple_weight:对于按某指数成分简单赋权
    :return:
    """
    # 读取股票信息表
    stock_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/stock_info_20190801.xlsx')

    stock_info = stock_info.dropna(axis=0, how='any').reset_index(drop=True)
    # 读取申万一级行业划分
    sw1 = sw_info_(cal_day, "sw1")
    sw1.rename(columns={'sw_name': 'sw1_name'}, inplace=True)
    # 读取申万三级行业划分
    sw3 = sw_info_(cal_day, "sw3")
    sw3.rename(columns={'sw_name': 'sw3_name'}, inplace=True)

    all = pd.DataFrame(columns=['code', 'code_name'], index=range(len(stock_info)))
    all['code'] = stock_info['证券代码']
    all['code_name'] = stock_info['证券简称']
    all = all.merge(sw1[['code', 'sw1_code', 'sw1_name']], on='code', how='left')
    all = all.merge(sw3[['code', 'sw3_code', 'sw3_name']], on='code', how='left')

    # 使cal_day适应api
    cal_day = cal_day[0:4] + '-' + cal_day[4:6] + '-' + cal_day[6:8]
    # 直接从数据库收集股票的流通市值（或总市值）
    data = get_data_panel("huangwei", "huangwei",
                          list(all['code']),
                          forwardchangeday2(cal_day),
                          forwardchangeday2(cal_day), collection='stock_indicator')
    # 替换原表的cir_mv
    all = all.merge(data[['code', indicator]], on='code', how='left')
    all['cir_mv'] = all[indicator]
    del all[indicator]

    # 计算申万三级行业内部流通市值比例
    tot_industry = all.groupby('sw3_code').sum()

    tot_industry['sw3_code'] = tot_industry.index
    tot_industry = tot_industry.reset_index(drop=True)
    all = all.merge(tot_industry, on='sw3_code', how='inner')
    all['weight_in_industry'] = all['cir_mv_x'] / all['cir_mv_y']
    all.rename(columns={'cir_mv_x': 'cir_mv', 'cir_mv_y': 'industry_total'},
               inplace=True)

    if industry_weight == 'market':
        # 获得全市场每个申万一级行业的比重
        tot_industry_sw1 = all.groupby('sw1_code').sum()
        tot_industry_sw1['sw1_code'] = tot_industry_sw1.index
        total_mv = tot_industry_sw1['cir_mv'].sum()
        tot_industry_sw1['industryweight_in_all'] = tot_industry_sw1['cir_mv'] / total_mv
        tot_industry_sw1.index.name = 'sw1_codes'  # 更改level_name
        all = all.merge(tot_industry_sw1[['industryweight_in_all', 'sw1_code']],
                        on='sw1_code', how='inner')
    else:
        # 获得成分股中每个申万一级行业的比重
        zz500_weight = get_data_crosssection_genaral("huangwei", "huangwei",
                                                     "index_weight", 'index_code',
                                                     industry_weight)
        zz500_weight = zz500_weight[
            zz500_weight['date'] ==
            list(zz500_weight['date'][zz500_weight['date']
                                      >= cal_day])[0]].reset_index(drop=True)
        # 与成分股权重表匹配最接近的日期
        zz500_weight.rename(columns={'stock_code': 'code'},
                            inplace=True)

        # 获取中证500中成分股的申万一级行业信息及流通市值信息
        zz500_weight = zz500_weight.merge(all[['code', 'sw1_code', 'cir_mv']])
        tot_industry_sw1 = zz500_weight.groupby('sw1_code').sum()
        tot_industry_sw1['sw1_code'] = tot_industry_sw1.index
        total_mv = tot_industry_sw1['cir_mv'].sum()
        tot_industry_sw1['industryweight_in_all'] = tot_industry_sw1['cir_mv'] / total_mv
        tot_industry_sw1.index.name = 'sw1_codes'  # 更改level_name
        all = all.merge(tot_industry_sw1[['industryweight_in_all', 'sw1_code']],
                        on='sw1_code', how='inner')

    # 添加申万三级行业总市值排名rank
    tot_industry = tot_industry.sort_values(by='cir_mv',
                                            ascending=False).reset_index(drop=True)
    tot_industry['rank'] = tot_industry.index + 1
    all = all.merge(tot_industry[['sw3_code', 'rank']],
                    on='sw3_code',
                    how='inner').sort_values(by='rank').reset_index(drop=True)
    # 追加行业个股数量字段
    count_industry = all.groupby('sw3_code').count()
    count_industry['indu_num'] = count_industry['code']
    count_industry['sw3_code'] = count_industry.index
    count_industry.index.name = 'sw3_codes'
    all = all.merge(count_industry[['sw3_code', 'indu_num']],
                    on='sw3_code',
                    how='inner')
    # 每个行业先保留前5个
    delete_list = []
    indu_list = list(all['sw3_code'].unique())
    for indu in indu_list:
        one_indu = all[all['sw3_code'] == indu]
        one_indu['global_index'] = one_indu.index
        if one_indu['indu_num'].iloc[0] > 5:
            one_indu = one_indu.sort_values(by='weight_in_industry',
                                            ascending=False).reset_index(drop=True)
            delete_list = delete_list + list(one_indu['global_index'].iloc[5:])

    top5_industry = all.drop(delete_list).reset_index(drop=True)
    top5_industry['ten_percent'] = round(top5_industry['indu_num'] * 0.1)

    delete_list = []
    for indu in indu_list:
        one_indu = top5_industry[top5_industry['sw3_code'] == indu]
        one_indu['global_index'] = one_indu.index
        if one_indu['ten_percent'].iloc[0] < 5:
            one_indu = one_indu.sort_values(by='weight_in_industry',
                                            ascending=False)
            one_indu_withoutbig = one_indu[one_indu['weight_in_industry']
                                           < 0.2].reset_index(drop=True)
            if len(one_indu_withoutbig) > 0:
                remain_num = one_indu_withoutbig['ten_percent'].iloc[0] \
                             - (len(one_indu) - len(one_indu_withoutbig))
                if remain_num <= 0:
                    delete_list = delete_list + list(one_indu_withoutbig['global_index'])
                else:
                    delete_list = delete_list + \
                                  list(one_indu_withoutbig['global_index'].iloc[int(remain_num):])

    result = top5_industry.drop(delete_list).reset_index(drop=True)

    if top_num == None:
        result_top100mv = result
    else:
        result_top100mv = result[result['rank'] <= top_num]

    # 按成分股权重对龙头加权
    hs300_weight = get_data_crosssection_genaral("huangwei", "huangwei",
                                                 "index_weight", 'index_code',
                                                 index_for_simple_weight)
    hs300_weight = hs300_weight[hs300_weight['date'] ==
                                list(hs300_weight['date'][hs300_weight['date']
                                                          >= cal_day])[0]].reset_index(drop=True)
    hs300_weight.rename(columns={'stock_code': 'code'},
                        inplace=True)

    # 与沪深300股票取交集
    result_top100mv = result_top100mv.merge(hs300_weight[['code', 'i_weight']],
                                            on='code',
                                            how='inner')
    weight_sum = result_top100mv['i_weight'].sum()
    result_top100mv['weight_adj'] = result_top100mv['i_weight'] / weight_sum

    # 不与指数取交集，按照申万一级行业划分计算权重
    tot_industry_sw1 = result.groupby('sw1_code').sum()
    tot_industry_sw1['sw1_code'] = tot_industry_sw1.index
    tot_industry_sw1 = tot_industry_sw1.reset_index(drop=True)
    result = result.merge(tot_industry_sw1[['sw1_code', 'cir_mv']],
                          on='sw1_code',
                          how='inner')
    result['weight_in_industry_sw1'] = result['cir_mv_x'] / result['cir_mv_y']
    result.rename(columns={'cir_mv_x': 'cir_mv', 'cir_mv_y': 'industry_total_sw1'},
                  inplace=True)

    # 申万一级行业的比重*行业内部个股占整个行业的比重
    result['weight_adj_sw1'] = result['industryweight_in_all'] * result['weight_in_industry_sw1']
    result['weight_adj_sw1'] = result['weight_adj_sw1'] / result['weight_adj_sw1'].sum()

    # 还原cal_day
    # cal_day = cal_day[0:4] + cal_day[5:7] + cal_day[8:10]
    # file_name = 'result_' + cal_day + '.xls'
    # result.set_index('code').to_excel(file_name, encoding='utf-8')
    return result


if __name__ == "__main__":
    index_for_simple_weight = '000905.SH'
    top_num = None
    industry_weight = '000905.SH'  # 或者'market'
    indicator = 'total_mv'  # 流通市值：circ_mv，总市值：total_mv
    cal_day = '20170630'
    result = industry_leader_maker_(cal_day, indicator,
                                    industry_weight,
                                    top_num,
                                    index_for_simple_weight)
