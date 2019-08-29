from dateutil.relativedelta import relativedelta
import statsmodels.api as sm
from scipy.stats import spearmanr
import datetime

def forwardchangeday(date, trading_days):
    # 这里的date,change_days都为字符串，方便直接进行比较
    for i in range(len(trading_days)):
        if trading_days[-i - 1] <= date:
            return trading_days[-i - 1]
        else:
            continue

# 获取某日一个月后的交易日
def next_month(date, trading_days):
    next_date = datetime.datetime.strptime(date, "%Y%m%d") + relativedelta(months=+1)
    next_date_str = next_date.strftime('%Y%m%d')
    return forwardchangeday(next_date_str, trading_days)

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


