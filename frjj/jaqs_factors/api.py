import pymongo
import pandas as pd
# import datetime
# from dateutil.relativedelta import relativedelta
from utils import *
import numpy as np
from tqdm import tqdm


def get_tradingdays(user, passwd, begin_date, end_date):
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb['tradeday']
    myquery = {"datetime": {"$gte": begin_date, "$lte": end_date}}
    # myprojection = "datetime"
    mydoc = mycol.find(myquery)  # , projection=myprojection)
    tradingday = pd.DataFrame(list(mydoc))
    del tradingday['_id']
    tradingday.columns = ["tradingdays"]
    return tradingday


def get_data_many(user, passwd, code, data_type, begin_date, end_date):
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb[data_type]
    myquery = {"$and": [{"date": {"$gte": begin_date, "$lte": end_date}}, {"code": code}]}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    return df


def get_data_single(user, passwd, code, data_type, date):
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb[data_type]
    myquery = {"$and": [{"date": date}, {"code": code}]}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    return df


def get_data_single2(user, passwd, code, data_type, date):
    date = date + " 00:00:00"
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb[data_type]
    myquery = {"$and": [{"date": date}, {"code": code}]}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    return df


def get_data_crosssection(user, passwd, data_type, date, database="stock_cn"):
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient[database]
    mycol = mydb[data_type]
    myquery = {"date": date}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    return df


def get_data_crosssection_genaral(user, passwd, data_type, field_name, field_value):
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb[data_type]
    myquery = {field_name: field_value}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    return df


def get_data_single_newID(user, passwd, code, date):
    # date = '20050104'
    # code = '000001.SZ'
    # t0 = datetime.datetime.now()
    ID = '1' + code[0:6] + date
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb['stock_daily']
    # myquery = {{"index": {"$gte": begin_date, "$lte": end_date}}}
    myquery = {"index": int(ID)}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    # print("job of getting data done, time elapsed: ", datetime.datetime.now() - t0)
    return df


def get_data_many_newID(user, passwd, code, begin_date, end_date):
    # t0 = datetime.datetime.now()
    ID1 = '1' + code[0:6] + begin_date
    ID2 = '1' + code[0:6] + end_date
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb['stock_daily']
    myquery = {"index": {"$gte": int(ID1), "$lte": int(ID2)}}
    # myquery = {"index": int(ID)}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    # print("job of getting data done, time elapsed: ", datetime.datetime.now() - t0)
    return df


def get_data_panel(user, passwd, code_list, begin_date, end_date, collection='stock_daily'):
    if collection == "A_index_PCT_CHG":
        begin_date = begin_date + " 00:00:00"
        end_date = end_date + " 00:00:00"
    # t0 = datetime.datetime.now()
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb[collection]
    myquery = {"$and": [{"date": {"$gte": begin_date, "$lte": end_date}}, {"code": {"$in": code_list}}]}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    # print("job of getting data done, time elapsed: ", datetime.datetime.now() - t0)
    return df


def get_data_panel_date(user, passwd, begin_date, end_date, collection='stock_daily', database="stock_cn"):
    if collection == "A_index_PCT_CHG":
        begin_date = begin_date + " 00:00:00"
        end_date = end_date + " 00:00:00"
    # t0 = datetime.datetime.now()
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient[database]
    mycol = mydb[collection]
    myquery = {"date": {"$gte": begin_date, "$lte": end_date}}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    # print("job of getting data done, time elapsed: ", datetime.datetime.now() - t0)
    return df


def get_data_panel_backwards(user, passwd, code_list,
                             date, back_years=0, back_months=12,
                             collection='stock_daily'):
    # 获得回溯的日期，命名为begin_date
    next_date = datetime.datetime.strptime(date, "%Y-%m-%d") + relativedelta(months=-back_months)
    next_date_str = next_date.strftime('%Y%m%d')
    next_date_str = forwardchangeday(next_date_str,
                                     list(get_tradingdays(
                                         user, passwd,
                                         '20000104', '20190628')['tradingdays']))
    begin_date = next_date_str[0:4] + '-' + next_date_str[4:6] + '-' + next_date_str[6:8]

    # 获得当下的日期，命名为end_date
    end_date = date

    if collection == "A_index_PCT_CHG":
        begin_date = begin_date + " 00:00:00"
        end_date = end_date + " 00:00:00"

    # t0 = datetime.datetime.now()
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb[collection]
    myquery = {"$and": [{"date": {"$gte": begin_date, "$lte": end_date}}, {"code": {"$in": code_list}}]}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    # print("job of getting data done, time elapsed: ", datetime.datetime.now() - t0)
    return df


def get_data_panel_newID(user, passwd, code_min, code_max, begin_date, end_date):
    # t0 = datetime.datetime.now()
    ID1 = '1' + code_min[0:6] + begin_date
    ID2 = '1' + code_max[0:6] + end_date
    myclient = pymongo.MongoClient("192.168.10.33", 27017)
    myclient.admin.authenticate(user, passwd)
    mydb = myclient["stock_cn"]
    mycol = mydb['stock_daily']
    myquery = {"index": {"$gte": int(ID1), "$lte": int(ID2)}}
    # myquery = {"index": int(ID)}
    mydoc = mycol.find(myquery)
    df = pd.DataFrame(list(mydoc))
    del df['_id']
    # print("job of getting data done, time elapsed: ", datetime.datetime.now() - t0)
    return df


# 对应JAQS的因子分析数据需求

def get_signal(begin, end, factor_name, collection='stock_indicator', database="stock_cn"):
    # 这里假定begin, end = '20170424', '20171026'
    begin_r = begin[0:4] + '-' + begin[4:6] + '-' + begin[6:8]
    end_r = end[0:4] + '-' + end[4:6] + '-' + end[6:8]

    rawdata = get_data_panel_date("huangwei", "huangwei", begin_r, end_r,
                                  collection=collection,
                                  database=database)
    # 将数据处理为行为时间，列为股票代码的形式
    tradingdays = get_tradingdays("huangwei", "huangwei", begin, end)

    # all_signal = pd.DataFrame(columns=list(pd.DataFrame(rawdata['code'].unique()).dropna()[0]),
    #                           index=tradingdays['tradingdays'])

    all_signal = pd.DataFrame(columns=['date'], index=range(len(tradingdays['tradingdays'])))
    all_signal['date'] = tradingdays['tradingdays']

    all_signal1 = all_signal
    all_signal2 = all_signal
    all_signal3 = all_signal


    # 对于某一个股票
    grouped = rawdata.groupby('code')

    #将所有的股票集分成三个子集
    all_stock_list = rawdata['code'].unique()

    stock_list1 = all_stock_list[0:round(len(all_stock_list)/3)]
    stock_list2 = all_stock_list[round(len(all_stock_list)/3):2*round(len(all_stock_list)/3)]
    stock_list3 = all_stock_list[2*round(len(all_stock_list)/3):]

    for code in tqdm(stock_list1):
        if type(code) != str:
            continue
        else:
            # print(code)
            one_stock_rawdata_index = grouped.groups[code]
            temp = rawdata.iloc[one_stock_rawdata_index][['date', 'code', factor_name]]
            temp = temp.rename(columns={factor_name: temp['code'].iloc[0]})
            temp['date'] = temp['date'].apply(lambda x: x[0:4] + x[5:7] + x[8:10])
            all_signal1 = all_signal1.merge(temp[['date', temp['code'].iloc[0]]], how='outer', on='date')

    for code in tqdm(stock_list2):
        if type(code) != str:
            continue
        else:
            # print(code)
            one_stock_rawdata_index = grouped.groups[code]
            temp = rawdata.iloc[one_stock_rawdata_index][['date', 'code', factor_name]]
            temp = temp.rename(columns={factor_name: temp['code'].iloc[0]})
            temp['date'] = temp['date'].apply(lambda x: x[0:4] + x[5:7] + x[8:10])
            all_signal2 = all_signal2.merge(temp[['date', temp['code'].iloc[0]]], how='outer', on='date')

    for code in tqdm(stock_list3):
        if type(code) != str:
            continue
        else:
            # print(code)
            one_stock_rawdata_index = grouped.groups[code]
            temp = rawdata.iloc[one_stock_rawdata_index][['date', 'code', factor_name]]
            temp = temp.rename(columns={factor_name: temp['code'].iloc[0]})
            temp['date'] = temp['date'].apply(lambda x: x[0:4] + x[5:7] + x[8:10])
            all_signal3 = all_signal3.merge(temp[['date', temp['code'].iloc[0]]], how='outer', on='date')


    # 剔除重复值
    all_signal1 = all_signal1.drop_duplicates('date', 'first').reset_index(drop=True)
    all_signal2 = all_signal2.drop_duplicates('date', 'first').reset_index(drop=True)
    all_signal3 = all_signal3.drop_duplicates('date', 'first').reset_index(drop=True)

    # 将三张子表合并
    all_signal = all_signal1
    all_signal = all_signal.merge(all_signal2, how='outer', on='date')
    all_signal = all_signal.merge(all_signal3, how='outer', on='date')

    return all_signal

if __name__ == "__main__":
    user = "huangwei"
    passwd = "huangwei"
    begin_date = '2000-01-04'
    end_date = '2000-01-04'
    data_type = 'market_value'
    code = '000001.SZ'
    tradingdays = get_tradingdays(user, passwd, begin_date, end_date)
    closeprice = get_data_many(user, passwd, code, data_type, begin_date, end_date)
    closeprice_single = get_data_single(user, passwd, code, data_type, '2013-04-01')
    pe = get_data_many(user, passwd, code, data_type, begin_date, end_date)

    # 获取某个时点所有的股票
    stocks = get_data_crosssection(user, passwd, data_type, '20140104')

    data = get_data_single_newID(user, passwd, '000001.SZ', '20050104')

    # 读取2012.1.1到2014.1.1
    data = get_data_many_newID(user, passwd, '000001.SZ', '20120101', '20150101')

    # 读取某些股票2012.1.1到2014.1.1
    stocks = get_data_crosssection(user, passwd, "market_value", '2000-01-04')
    stocks_list = list(stocks.sort_values(by='code')['code'])
    data = get_data_panel_newID(user, passwd, stocks_list[0], stocks_list[5], '20140101', '20150101')

    # 读取指定一部分股票在2012.1.1到2014.1.1的赋权收益率数据
    data = get_data_panel(user, passwd, list(stocks['code'])[0:20], '2005-01-04', '2011-02-11')

    # 指数成分股限制（获取某个时点的某个股票的成分股名单）
    content_stocks = get_data_crosssection(user, passwd,
                                           "index_weight",
                                           "2019-03-19",
                                           database="stock_cn")
    # 沪深300成分股名单
    hs300_list = content_stocks[content_stocks['index_code'] == '000300.SH']


    # 根据JAQS的数据需求处理数据

    begin, end = '20170424', '20171026'

    # pb因子
    all_signal = get_signal(begin, end, 'pb', collection='stock_indicator', database="stock_cn")
    all_signal = all_signal.set_index('date')
    all_signal.to_csv('C:/Users/Lenovo/PycharmProjects/my_factor/all_signal_pb.csv')

    # close（未调整）
    all_signal = get_signal(begin, end, 'close', collection='stock_daily', database="stock_cn")
    all_signal = all_signal.set_index('date')
    all_signal.to_csv('C:/Users/Lenovo/PycharmProjects/my_factor/all_signal_close.csv')

    # 行业
    all_signal = get_signal(begin, end, 'industry_name', collection='industry', database="stock_cn")
    all_signal = all_signal.ffill()
    all_signal = all_signal.bfill()
    all_signal = all_signal.set_index('date')
    all_signal.to_csv('C:/Users/Lenovo/PycharmProjects/my_factor/all_signal_industry.csv')
