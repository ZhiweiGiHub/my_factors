from utils import *

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
    # TODO:指数成分更新完后删除下两行
    if date > '2019-07-02':
        date = '2019-07-02'
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

    if len(begin_date) == 8:
        begin_date = begin_date[0:4] + '-' + begin_date[4:6] + '-' + begin_date[6:8]
    else:
        pass

    if len(end_date) == 8:
        end_date = end_date[0:4] + '-' + end_date[4:6] + '-' + end_date[6:8]
    else:
        pass

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
