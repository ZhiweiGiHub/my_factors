from industry.leader_get_data import *
from industry.get_all_time_data import get_time_data_


class LeaderMaker(object):

    def __init__(self, cal_day, indicator,
                 industry_weight, top_num,
                 index_for_simple_weight):
        if len(cal_day) == 8:
            self.cal_day = cal_day[0:4] + '-' + cal_day[4:6] + '-' + cal_day[6:8]
        else:
            self.cal_day = cal_day
        self.indicator = indicator
        self.industry_weight = industry_weight
        self.top_num = top_num
        self.index_for_simple_weight = index_for_simple_weight
        self.all = get_data_(self.cal_day, indicator)

        # 获得成分股中每个申万一级行业的比重
        self.zz500_weight = get_data_crosssection_genaral("huangwei", "huangwei",
                                                          "index_weight", 'index_code',
                                                          self.industry_weight)
        self.tot_industry = self.all.groupby('sw3_code').sum()

        self.hs300_weight = get_data_crosssection_genaral("huangwei", "huangwei",
                                                          "index_weight", 'index_code',
                                                          index_for_simple_weight)

    # 计算申万三级行业内部流通市值比例
    def add_weight_in_industry_sw3(self):
        self.tot_industry['sw3_code'] = self.tot_industry.index
        self.tot_industry = self.tot_industry.reset_index(drop=True)
        self.all = self.all.merge(self.tot_industry, on='sw3_code', how='inner')
        self.all['weight_in_industry'] = self.all['cir_mv_x'] / self.all['cir_mv_y']
        self.all.rename(columns={'cir_mv_x': 'cir_mv', 'cir_mv_y': 'industry_total'},
                        inplace=True)

    def add_industry_weight_in_all(self):
        if self.industry_weight == 'market':
            # 获得全市场每个申万一级行业的比重
            tot_industry_sw1 = self.all.groupby('sw1_code').sum()
            tot_industry_sw1['sw1_code'] = tot_industry_sw1.index
            total_mv = tot_industry_sw1['cir_mv'].sum()
            tot_industry_sw1['industryweight_in_all'] = \
                tot_industry_sw1['cir_mv'] / total_mv
            tot_industry_sw1.index.name = 'sw1_codes'  # 更改level_name
            self.all = self.all.merge(
                tot_industry_sw1[['industryweight_in_all', 'sw1_code']],
                on='sw1_code', how='inner')
        else:

            # TODO:更新行业划分表后删除下面三行
            if self.cal_day > '2019-06-28':
                cal_day = '2019-06-28'
            else:
                cal_day = self.cal_day
            self.zz500_weight = self.zz500_weight[
                self.zz500_weight['date'] ==
                list(self.zz500_weight['date'][self.zz500_weight['date']
                                               >= cal_day])[0]].reset_index(drop=True)
            # 与成分股权重表匹配最接近的日期
            self.zz500_weight.rename(columns={'stock_code': 'code'},
                                     inplace=True)

            # 获取中证500中成分股的申万一级行业信息及流通市值信息
            self.zz500_weight = self.zz500_weight.merge(
                self.all[['code', 'sw1_code', 'cir_mv']]
            )
            tot_industry_sw1 = self.zz500_weight.groupby('sw1_code').sum()
            tot_industry_sw1['sw1_code'] = tot_industry_sw1.index
            total_mv = tot_industry_sw1['cir_mv'].sum()
            tot_industry_sw1['industryweight_in_all'] = \
                tot_industry_sw1['cir_mv'] / total_mv
            tot_industry_sw1.index.name = 'sw1_codes'  # 更改level_name
            self.all = self.all.merge(
                tot_industry_sw1[['industryweight_in_all', 'sw1_code']],
                on='sw1_code', how='inner')

    # 添加申万三级行业总市值排名rank
    def add_rank(self):
        self.tot_industry = self.tot_industry.sort_values(
            by='cir_mv', ascending=False).reset_index(drop=True
                                                      )
        self.tot_industry['rank'] = self.tot_industry.index + 1
        self.all = self.all.merge(
            self.tot_industry[['sw3_code', 'rank']], on='sw3_code',
            how='inner').sort_values(by='rank').reset_index(drop=True)

    # 追加行业个股数量字段
    def add_industry_num(self):
        count_industry = self.all.groupby('sw3_code').count()
        count_industry['indu_num'] = count_industry['code']
        count_industry['sw3_code'] = count_industry.index
        count_industry.index.name = 'sw3_codes'
        self.all = self.all.merge(
            count_industry[['sw3_code', 'indu_num']],
            on='sw3_code', how='inner')

    # 每个行业先保留前5个
    def filter(self):
        self.add_weight_in_industry_sw3()
        self.add_industry_weight_in_all()
        self.add_rank()
        self.add_industry_num()

        delete_list = []
        indu_list = list(self.all['sw3_code'].unique())
        for indu in indu_list:
            one_indu = self.all[self.all['sw3_code'] == indu].copy()
            one_indu['global_index'] = one_indu.index
            if one_indu['indu_num'].iloc[0] > 5:
                one_indu = one_indu.sort_values(by='weight_in_industry',
                                                ascending=False).reset_index(drop=True)
                delete_list = delete_list + list(one_indu['global_index'].iloc[5:])

        top5_industry = self.all.drop(delete_list).reset_index(drop=True)
        top5_industry['ten_percent'] = round(top5_industry['indu_num'] * 0.1)

        delete_list = []
        for indu in indu_list:
            one_indu = top5_industry[top5_industry['sw3_code'] == indu].copy()
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

        self.result = top5_industry.drop(delete_list).reset_index(drop=True)

        if self.top_num is None:
            self.result_top100mv = self.result
        else:
            self.result_top100mv = self.result[self.result['rank'] <= self.top_num]

        return self.result, self.result_top100mv

    def weight(self):

        self.result, self.result_top100mv = self.filter()
        # 按成分股权重对龙头加权

        #TODO:指数成分更新完后删除148-151
        if self.cal_day > '2019-07-02':
            date = '2019-07-02'
        else:
            date = self.cal_day
        self.hs300_weight = self.hs300_weight[self.hs300_weight['date']
                                              == list(self.hs300_weight['date'][
                                                          self.hs300_weight['date']
                                                          >= date])[0]].reset_index(
            drop=True)
        self.hs300_weight.rename(columns={'stock_code': 'code'}, inplace=True)

        # # 与沪深300股票取交集
        # self.result_top100mv = self.result_top100mv.merge(
        #     self.hs300_weight[['code', 'i_weight']], on='code', how='inner')
        # weight_sum = self.result_top100mv['i_weight'].sum()
        # self.result_top100mv['weight_adj'] = self.result_top100mv['i_weight'] / weight_sum

        # 不与指数取交集，按照申万一级行业划分计算权重
        tot_industry_sw1 = self.result.groupby('sw1_code').sum()
        tot_industry_sw1['sw1_code'] = tot_industry_sw1.index
        tot_industry_sw1 = tot_industry_sw1.reset_index(drop=True)
        self.result = self.result.merge(tot_industry_sw1[['sw1_code', 'cir_mv']],
                                        on='sw1_code',
                                        how='inner')
        self.result['weight_in_industry_sw1'] = self.result['cir_mv_x'] / self.result['cir_mv_y']
        self.result.rename(columns={'cir_mv_x': 'cir_mv', 'cir_mv_y': 'industry_total_sw1'},
                           inplace=True)

        # 申万一级行业的比重*行业内部个股占整个行业的比重
        self.result['weight_adj_sw1'] = self.result['industryweight_in_all'] \
                                        * self.result['weight_in_industry_sw1']
        self.result['weight_adj_sw1'] = self.result['weight_adj_sw1'] / \
                                        self.result['weight_adj_sw1'].sum()
        return self.result


if __name__ == "__main__":
    a = LeaderMaker('2019-06-28',
                    'total_mv',
                    '000905.SH',
                    None,
                    '000300.SH')
    weighted_result = a.weight()

    # 导出每个月的龙头股筛选结果
    start, end = '20170301', "20190801"
    change_days, my_tradingdays = get_time_data_(start, end)
    for changeday in change_days:
        a = LeaderMaker(changeday,
                        'total_mv',
                        '000905.SH',
                        None,
                        '000300.SH')
        weighted_result = a.weight()
        filename = changeday
        weighted_result.to_excel(
            'C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/'+filename+'.xls')