from factor.stock_pool_maker import stock_pool_maker
from api import get_data_crosssection, get_data_panel_backwards
from industry.period_return_maker import period_return_maker_
import pandas as pd
from factor.factor_value_sort import factor_value_sort_
from factor.utils import first_in_month
from industry.my_factor_maker import my_factor_maker_


class FactorMaker(object):

    def __init__(self, chooseday_r,
                 collection_name="qfa_roe_deducted",
                 indicator='QFA_ROE_DEDUCTED', ac=True,
                 database='stock_cn_finance1', type="DIY",
                 info='000300.SH', factor_type='financial'):

        if len(chooseday_r) == 8:
            self.chooseday_r = chooseday_r[0:4] + '-' + \
                               chooseday_r[4:6] + '-' + \
                               chooseday_r[6:8]
        else:
            self.chooseday_r = chooseday_r  # 选股日期

        self.collection_name = collection_name  # 数据库表名
        self.indicator = indicator  # 因子（指标）名称
        self.ac = ac  # 是否按indicator升序对股票排名
        self.database = database  # 数据库名
        self.type = type  # 总股票池范围：index：指数成分股；industry：行业内选股；DIY：自定义选股方法
        self.info = info  # 股票池范围信息
        self.factor_type = factor_type  # 因子类别base:基本指标；financial

    def stock_pool_prepare(self):
        stock_pool = stock_pool_maker(self.chooseday_r, self.type, self.info)
        return stock_pool

    def factor_value_prepare(self):
        if self.factor_type == 'base':
            factor_value = get_data_crosssection("huangwei", "huangwei",
                                                 self.collection_name,
                                                 self.chooseday_r,
                                                 database=self.database).sort_values(
                by=self.indicator, ascending=self.ac).reset_index(drop=True)

        elif self.factor_type == 'financial':
            # 根据财务报告发布的规律，对获取财务指标的时间进行调整
            if 4 >= int(self.chooseday_r[5:7]) >= 1:
                year = str(int(self.chooseday_r[0:4]) - 1)
                self.chooseday_r_adj = year + '-09-30'
            elif 9 >= int(self.chooseday_r[5:7]) >= 5:
                year = str(int(self.chooseday_r[0:4]))
                self.chooseday_r_adj = year + '-03-31'
            elif 11 >= int(self.chooseday_r[5:7]) >= 10:
                year = str(int(self.chooseday_r[0:4]))
                self.chooseday_r_adj = year + '-06-30'
            elif int(self.chooseday_r[5:7]) > 11:
                year = str(int(self.chooseday_r[0:4]))
                self.chooseday_r_adj = year + '-09-30'

            # 根据日期，取出该时点的所有数据
            factor_value = get_data_crosssection("huangwei", "huangwei",
                                                 self.collection_name,
                                                 self.chooseday_r_adj,
                                                 database=self.database).sort_values(
                by=self.indicator, ascending=self.ac).reset_index(drop=True)
        elif self.factor_type == 'tech':
            # 取总的股票池
            stockpool = stock_pool_maker(self.chooseday_r, self.type, self.info)

            # 获取当下日期过去一个月的收益率数据
            info_all = get_data_panel_backwards("huangwei", "huangwei",
                                                list(stockpool['code']),
                                                self.chooseday_r,
                                                back_years=0, back_months=1,
                                                collection='stock_daily')
            info_all = info_all.rename(columns={'code': 'codes'})
            info_all['rtn'] = info_all['rtn'] + 1
            info_all = info_all[['codes', 'rtn', 'date']]
            # 分组统计
            grouped = info_all.groupby('codes').last()
            grouped_first = info_all.groupby(by=info_all['codes']).first()
            grouped_last = pd.DataFrame(columns=['rtn', 'date'],
                                        index=grouped_first.index)
            grouped_last['date'] = info_all['date'].iloc[-1]
            for index in grouped_last.index:
                temp = info_all[info_all['codes'] == index]
                grouped_last['rtn'].loc[index] = list(temp['rtn'].cumprod())[-1]
            momentums = grouped_last['rtn'] - grouped_first['rtn']

            factor_value = pd.DataFrame(columns=['date', 'code', 'momentum'],
                                        index=range(len(grouped)))
            factor_value['code'] = list(grouped.index)
            factor_value['date'] = grouped['date'].iloc[0]
            factor_value['momentum'] = list(momentums.values)
            factor_value = factor_value.sort_values(by='momentum',
                                                    ascending=self.ac).reset_index(drop=True)
        elif self.factor_type == 'other':
            chooseday_r_first = first_in_month(self.chooseday_r)
            factor_value = get_data_crosssection("huangwei", "huangwei",
                                                 self.collection_name,
                                                 chooseday_r_first,
                                                 database=self.database).sort_values(
                by=self.indicator, ascending=self.ac).reset_index(drop=True)
        elif self.factor_type == 'my_factor':
            stockpool = stock_pool_maker(self.chooseday_r, self.type)
            factor_value = my_factor_maker_(stockpool, self.chooseday_r)
        else:
            factor_value = None
            print('未知的因子类型！')

        return factor_value

    def factor_value_maker(self):
        return self.stock_pool_prepare(), self.factor_value_prepare()

    def factor_in_stock_pool(self):
        stock_pool, factor_value = self.factor_value_maker()
        if self.type == "index":
            factor_value = factor_value.merge(stock_pool[['i_weight', 'code']],
                                              how='inner', on='code')
        elif self.type == "industry":
            factor_value = factor_value.merge(stock_pool[['industry_name', 'code']],
                                              how='inner', on='code')
        elif self.type == 'DIY':
            factor_value = factor_value.merge(stock_pool[['weight_adj_sw1', 'code']],
                                              how='inner', on='code')

        self.factor_value = factor_value.sort_values(by=self.indicator,
                                                     ascending=self.ac).reset_index(drop=True)
        return self.factor_value

    def neutralization(self, remove=None):
        if remove is None:
            factor_value = self.factor_in_stock_pool()
        else:
            factor_value = self.factor_in_stock_pool()

            # 针对股票池不是龙头股的情况
            if 'weight_adj_sw1' not in factor_value.columns:
                factor_value['weight_adj_sw1'] = 0
            # 中心化
            factor_value = factor_value_sort_(factor_value,
                                              factor_name=self.indicator,
                                              remove=remove)
        # 标准化
        factor_value[self.indicator] = (factor_value[self.indicator] -
                                        factor_value[self.indicator].min()) \
                                       / (factor_value[self.indicator].max() -
                                          factor_value[self.indicator].min())

        if 'weight_adj_sw1' not in factor_value.columns:
            factor_value['weight_adj_sw1'] = 0
        return factor_value


if __name__ == "__main__":
    factor = FactorMaker("2019-06-28")
    factor_value = factor.factor_in_stock_pool()
    ic, factor_value2 = factor.neutralization()
