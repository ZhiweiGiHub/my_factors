import pandas as pd
# from industry.sw_info_new import sw_info_
from api import get_data_panel, get_data_crosssection_genaral
from factor.utils import forwardchangeday2
from api import get_sw_info

index_content = get_data_crosssection_genaral("huangwei", "huangwei",
                                              "index_weight", 'index_code',
                                              "000905.SH")
# 读取所有股票信息表
stock_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/stock_info_20190801.xlsx')
stock_info = stock_info.dropna(axis=0, how='any').reset_index(drop=True)

# 读取全部时间段申万行业划分
sw = get_sw_info(list(stock_info['证券代码']), "2017-01-01", "2019-08-01")


def get_data_(cal_day, indicator):
    # 与指数成分股取交集
    index_content_now = index_content[index_content['date'] == list(index_content['date'][
                                                                        index_content['date']
                                                                        >= cal_day])[0]].reset_index(drop=True)

    index_content_now.rename(columns={'stock_code': '证券代码'}, inplace=True)

    stock_info_ = stock_info.merge(index_content_now['证券代码'], on='证券代码', how='inner')

    # 取当天的申万行业划分
    sw_ = sw[sw['date'] == cal_day].reset_index(drop=True)

    all = pd.DataFrame(columns=['code', 'code_name'], index=range(len(stock_info_)))
    all['code'] = stock_info_['证券代码']
    all['code_name'] = stock_info_['证券简称']
    all = all.merge(sw_[['code', 'sw1_code', 'sw1_name',
                         'sw2_code', 'sw2_name', 'sw3_code',
                         'sw3_name']], on='code', how='left')

    # 使cal_day适应api
    if len(cal_day) == 8:
        cal_day = cal_day[0:4] + '-' + cal_day[4:6] + '-' + cal_day[6:8]
    else:
        pass

    # 直接从数据库收集股票的流通市值（或总市值）
    data = get_data_panel("huangwei", "huangwei",
                          list(all['code']),
                          forwardchangeday2(cal_day),
                          forwardchangeday2(cal_day), collection='stock_indicator')

    # 用total_mv替换原表的cir_mv
    all = all.merge(data[['code', indicator]], on='code', how='left')
    all['cir_mv'] = all[indicator]
    del all[indicator]

    return all


if __name__ == '__main__':
    cc = get_data_('2018-05-31', "total_mv")