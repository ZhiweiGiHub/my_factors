from dateutil.relativedelta import relativedelta
import statsmodels.api as sm
from scipy.stats import spearmanr
import datetime
import pandas as pd
import pymongo

def get_tradingdays_(begin_date, end_date):
    user = "huangwei"
    passwd = "huangwei"
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

def forwardchangeday(date, trading_days):
    # 这里的date,change_days都为字符串，方便直接进行比较
    for i in range(len(trading_days)):
        if trading_days[-i - 1] <= date:
            return trading_days[-i - 1]
        else:
            continue


def forwardchangeday2(date):
    trading_days = list(get_tradingdays_('20000104', "20190628")['tradingdays'])
    if len(date) > 8:
        date_int = date[0:4] + date[5:7] + date[8:10]
    else:
        date_int = date
    # 这里的date,change_days都为字符串，方便直接进行比较
    for i in range(len(trading_days)):
        if trading_days[-i - 1] <= date_int:
            trade_day = trading_days[-i - 1]
            return trade_day[0:4] + '-' + trade_day[4:6] + '-' + trade_day[6:8]
        else:
            continue


# 获取某日一个月后的交易日
def next_month(date, trading_days):
    next_date = datetime.datetime.strptime(date, "%Y%m%d") + relativedelta(months=+1)
    next_date_str = next_date.strftime('%Y%m%d')
    return forwardchangeday(next_date_str, trading_days)

# 获取某日一个月后的交易日
def month(date, change_month=+1):
    if len(date) > 8:
        date_int = date[0:4] + date[5:7] + date[8:10]
    else:
        date_int = date
    next_date = datetime.datetime.strptime(date_int, "%Y%m%d") + relativedelta(months=change_month)
    next_date_str = next_date.strftime('%Y%m%d')
    return forwardchangeday2(next_date_str)

# 普通OLS回归
def OLS_param(Y, X):
    # Y = df_all['return']
    # X = df_all['factor_value']
    X = sm.add_constant(X)
    model = sm.OLS(Y.astype(float), X.astype(float))  # 注意转换为OLS需要的浮点格式
    results = model.fit()
    return results.params[1], results.pvalues[1]


def spearman_cor(data1, data2):
    coef, p = spearmanr(data1, data2)
    return coef, p


if __name__ == "__main__":
    date = '2018-01-01'
    user = "huangwei"
    passwd = "huangwei"
    begin_date = '2000-01-04'
    end_date = '2000-01-04'
    # trading_days = list(get_tradingdays(user, passwd, begin_date, end_date)['tradingdays'])
