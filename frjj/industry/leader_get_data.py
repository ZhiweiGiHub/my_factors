import pandas as pd
from industry.sw_info import sw_info_
from api import get_data_panel, get_data_crosssection_genaral
from factor.utils import forwardchangeday2

index_content = get_data_crosssection_genaral("huangwei", "huangwei",
                                              "index_weight", 'index_code',
                                              "000905.SH")


def get_data_(cal_day, indicator):
    # 读取所有股票信息表
    stock_info = pd.read_excel('C:/Users/Lenovo/PycharmProjects/my_project/frjj_codes/stock_info_20190801.xlsx')
    stock_info = stock_info.dropna(axis=0, how='any').reset_index(drop=True)

    # 与指数成分股取交集
    index_content_now = index_content[index_content['date'] == list(index_content['date'][
                                          index_content['date']
                                          >= cal_day])[0]].reset_index(drop=True)

    index_content_now.rename(columns={'stock_code': '证券代码'}, inplace=True)
    stock_info = stock_info.merge(index_content_now['证券代码'], on='证券代码', how='inner')

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
    cc = get_data_("2019-06-28", "total_mv")