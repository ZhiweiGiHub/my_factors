# 本程序用于从tushare取数据并导入本地MongoDB数据库中
import tushare as ts
from pymodm.connection import connect
from tqdm import tqdm
import time
from odm.collections import *
from odm.add_key import add_key_
import pandas as pd

# 与tushare的连接
token = '8e05784c0dff3703c1f620faab7c741af4b8a1fb24131ddc25f5ec9c'
ts.set_token(token)
pro = ts.pro_api()


# 与本地MongoDB的连接（有认证要求）
# connect("mongodb://huangwei:huangwei@192.168.10.33:27017/stockdb?authSource=admin",
#         alias="dailybar")


def get_tradedays(start, end):
    connect("mongodb://localhost:27017/stock?", alias="trade_days")
    df = pro.trade_cal(exchange='', start_date=start, end_date=end)
    df = df[df['is_open'] == 1]
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            TradeDays(arr[i, 0], arr[i, 1], arr[i, 2]).save()
    print('日历数据导入完成：', end)


def get_dailybar(date):
    connect("mongodb://localhost:27017/stock?", alias="origin_daily_bar")
    df = pro.daily(trade_date=date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            OriginDailyBar(arr[i, 0], arr[i, 1], arr[i, 2],
                           arr[i, 3], arr[i, 4], arr[i, 5],
                           arr[i, 6], arr[i, 7], arr[i, 8],
                           arr[i, 9], arr[i, 10], arr[i, 11]).save()
    print('行情数据导入完成：', date)


def get_adjfactor(date):
    connect("mongodb://localhost:27017/stock?", alias="adjust_factor")
    df = pro.adj_factor(ts_code='', trade_date=date)  # 复权因子无论从任何时间去下载都是一样的
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            AdjustFactor(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3]).save()
    print('复权因子数据导入完成：', date)


def get_stock_main(date):
    connect("mongodb://localhost:27017/stock?", alias="stock_main")
    df_sh = pro.stock_company(exchange='SSE')
    df_sz = pro.stock_company(exchange='SZSE')
    df = pd.concat([df_sh, df_sz], axis=0).reset_index(drop=True)
    df['employees'] = df['employees'].fillna(0)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            StocksMain(arr[i, 0], arr[i, 1], arr[i, 2],
                       arr[i, 3], arr[i, 4], arr[i, 5],
                       arr[i, 6], arr[i, 7], arr[i, 8],
                       arr[i, 9], arr[i, 10], arr[i, 11]).save()
    print('股票主要信息数据导入完成：', date)


def get_stock_list(date):
    connect("mongodb://localhost:27017/stock?", alias="stock_list")
    # df = pro.stock_basic()
    data_L = pro.stock_basic(exchange='', list_status='L',
                             fields='ts_code,name,area,industry,list_date,exchange,list_status,delist_date,is_hs')
    data_D = pro.stock_basic(exchange='', list_status='D',
                             fields='ts_code,name,area,industry,list_date,exchange,list_status,delist_date,is_hs')
    data_P = pro.stock_basic(exchange='', list_status='P',
                             fields='ts_code,name,area,industry,list_date,exchange,list_status,delist_date,is_hs')
    df = pd.concat([data_L, data_D, data_P], axis=0).reset_index(drop=True)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            StockList(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3],
                      arr[i, 4], arr[i, 5], arr[i, 6], arr[i, 7],
                      arr[i, 8]).save()
    print('股票列表数据导入完成：', date)


def get_suspend_list(date):
    connect("mongodb://localhost:27017/stock?", alias="suspend_list")
    df = pro.suspend(suspend_date=date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            SuspendList(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3],
                        arr[i, 4]).save()
    print('停牌数据导入完成：', date)


def get_daily(date):
    connect("mongodb://localhost:27017/stock?", alias="daily")
    df = pro.daily_basic(trade_date=date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            Daily(arr[i, 0], arr[i, 1], arr[i, 2],
                  arr[i, 3], arr[i, 4], arr[i, 5],
                  arr[i, 6], arr[i, 7], arr[i, 8],
                  arr[i, 9], arr[i, 10], arr[i, 11],
                  arr[i, 12], arr[i, 13], arr[i, 14],
                  arr[i, 15], arr[i, 16]).save()
    print('每日指标数据导入完成：', date)


def get_income_sheet(code, end_date):
    connect("mongodb://localhost:27017/stock?", alias="income_sheet")
    df = pro.income(ts_code=code, start_date='20050101', end_date=end_date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            IncomeSheet(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3], arr[i, 4], arr[i, 5],
                        arr[i, 6], arr[i, 7], arr[i, 8], arr[i, 9], arr[i, 10], arr[i, 11],
                        arr[i, 12], arr[i, 13], arr[i, 14], arr[i, 15], arr[i, 16], arr[i, 17],
                        arr[i, 18], arr[i, 19], arr[i, 20], arr[i, 21], arr[i, 22], arr[i, 23],
                        arr[i, 24], arr[i, 25], arr[i, 26], arr[i, 27], arr[i, 28], arr[i, 29],
                        arr[i, 30], arr[i, 31], arr[i, 32], arr[i, 33], arr[i, 34], arr[i, 35],
                        arr[i, 36], arr[i, 37], arr[i, 38], arr[i, 39], arr[i, 40], arr[i, 41],
                        arr[i, 42], arr[i, 43], arr[i, 44], arr[i, 45], arr[i, 46], arr[i, 47],
                        arr[i, 48], arr[i, 49], arr[i, 50], arr[i, 51], arr[i, 52], arr[i, 53],
                        arr[i, 54], arr[i, 55], arr[i, 56], arr[i, 57], arr[i, 58], arr[i, 59],
                        arr[i, 60], arr[i, 61], arr[i, 62], arr[i, 63], arr[i, 64], arr[i, 65]).save()
    print('利润表数据导入完成：', end_date)


def get_balance_sheet(code, end_date):
    connect("mongodb://localhost:27017/stock?", alias="balance_sheet")
    df = pro.balancesheet(ts_code=code, start_date='20050101', end_date=end_date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            BalanceSheet(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3], arr[i, 4], arr[i, 5],
                         arr[i, 6], arr[i, 7], arr[i, 8], arr[i, 9], arr[i, 10], arr[i, 11],
                         arr[i, 12], arr[i, 13], arr[i, 14], arr[i, 15], arr[i, 16], arr[i, 17],
                         arr[i, 18], arr[i, 19], arr[i, 20], arr[i, 21], arr[i, 22], arr[i, 23],
                         arr[i, 24], arr[i, 25], arr[i, 26], arr[i, 27], arr[i, 28], arr[i, 29],
                         arr[i, 30], arr[i, 31], arr[i, 32], arr[i, 33], arr[i, 34], arr[i, 35],
                         arr[i, 36], arr[i, 37], arr[i, 38], arr[i, 39], arr[i, 40], arr[i, 41],
                         arr[i, 42], arr[i, 43], arr[i, 44], arr[i, 45], arr[i, 46], arr[i, 47],
                         arr[i, 48], arr[i, 49], arr[i, 50], arr[i, 51], arr[i, 52], arr[i, 53],
                         arr[i, 54], arr[i, 55], arr[i, 56], arr[i, 57], arr[i, 58], arr[i, 59],
                         arr[i, 60], arr[i, 61], arr[i, 62], arr[i, 63], arr[i, 64], arr[i, 65],
                         arr[i, 66], arr[i, 67], arr[i, 68], arr[i, 69], arr[i, 70], arr[i, 71],
                         arr[i, 72], arr[i, 73], arr[i, 74], arr[i, 75], arr[i, 76], arr[i, 77],
                         arr[i, 78], arr[i, 79], arr[i, 80], arr[i, 81], arr[i, 82], arr[i, 83],
                         arr[i, 84], arr[i, 85], arr[i, 86], arr[i, 87], arr[i, 88], arr[i, 89],
                         arr[i, 90], arr[i, 91], arr[i, 92], arr[i, 93], arr[i, 94], arr[i, 95],
                         arr[i, 96], arr[i, 97], arr[i, 98], arr[i, 99], arr[i, 100], arr[i, 101],
                         arr[i, 102], arr[i, 103], arr[i, 104], arr[i, 105], arr[i, 106], arr[i, 107],
                         arr[i, 108], arr[i, 109], arr[i, 110], arr[i, 111], arr[i, 112], arr[i, 113],
                         arr[i, 114], arr[i, 115], arr[i, 116], arr[i, 117], arr[i, 118], arr[i, 119],
                         arr[i, 120], arr[i, 121], arr[i, 122], arr[i, 123], arr[i, 124], arr[i, 125],
                         arr[i, 126], arr[i, 127], arr[i, 128], arr[i, 129], arr[i, 130], arr[i, 131],
                         arr[i, 132], arr[i, 133], arr[i, 134], arr[i, 135], arr[i, 136], arr[i, 137]).save()
    print('资产负债表数据导入完成：', end_date)


def get_cashflow_sheet(code, end_date):
    connect("mongodb://localhost:27017/stock?", alias="cashflow_sheet")
    df = pro.cashflow(ts_code=code, start_date='20050101', end_date=end_date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            CashFlowSheet(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3], arr[i, 4], arr[i, 5],
                          arr[i, 6], arr[i, 7], arr[i, 8], arr[i, 9], arr[i, 10], arr[i, 11],
                          arr[i, 12], arr[i, 13], arr[i, 14], arr[i, 15], arr[i, 16], arr[i, 17],
                          arr[i, 18], arr[i, 19], arr[i, 20], arr[i, 21], arr[i, 22], arr[i, 23],
                          arr[i, 24], arr[i, 25], arr[i, 26], arr[i, 27], arr[i, 28], arr[i, 29],
                          arr[i, 30], arr[i, 31], arr[i, 32], arr[i, 33], arr[i, 34], arr[i, 35],
                          arr[i, 36], arr[i, 37], arr[i, 38], arr[i, 39], arr[i, 40], arr[i, 41],
                          arr[i, 42], arr[i, 43], arr[i, 44], arr[i, 45], arr[i, 46], arr[i, 47],
                          arr[i, 48], arr[i, 49], arr[i, 50], arr[i, 51], arr[i, 52], arr[i, 53],
                          arr[i, 54], arr[i, 55], arr[i, 56], arr[i, 57], arr[i, 58], arr[i, 59],
                          arr[i, 60], arr[i, 61], arr[i, 62], arr[i, 63], arr[i, 64], arr[i, 65],
                          arr[i, 66], arr[i, 67], arr[i, 68], arr[i, 69], arr[i, 70], arr[i, 71],
                          arr[i, 72], arr[i, 73], arr[i, 74], arr[i, 75], arr[i, 76], arr[i, 77],
                          arr[i, 78], arr[i, 79], arr[i, 80], arr[i, 81], arr[i, 82], arr[i, 83],
                          arr[i, 84], arr[i, 85], arr[i, 86], arr[i, 87], arr[i, 88], arr[i, 89],
                          arr[i, 90]).save()
    print('现金流量表数据导入完成：', code, end_date)


def get_financial_indicator(code):
    connect("mongodb://localhost:27017/stock?", alias="financial_indicator")
    df = pro.fina_indicator(ts_code=code)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            FinancialIndicator(arr[i, 0], arr[i, 1], arr[i, 2], arr[i, 3], arr[i, 4], arr[i, 5],
                               arr[i, 6], arr[i, 7], arr[i, 8], arr[i, 9], arr[i, 10], arr[i, 11],
                               arr[i, 12], arr[i, 13], arr[i, 14], arr[i, 15], arr[i, 16], arr[i, 17],
                               arr[i, 18], arr[i, 19], arr[i, 20], arr[i, 21], arr[i, 22], arr[i, 23],
                               arr[i, 24], arr[i, 25], arr[i, 26], arr[i, 27], arr[i, 28], arr[i, 29],
                               arr[i, 30], arr[i, 31], arr[i, 32], arr[i, 33], arr[i, 34], arr[i, 35],
                               arr[i, 36], arr[i, 37], arr[i, 38], arr[i, 39], arr[i, 40], arr[i, 41],
                               arr[i, 42], arr[i, 43], arr[i, 44], arr[i, 45], arr[i, 46], arr[i, 47],
                               arr[i, 48], arr[i, 49], arr[i, 50], arr[i, 51], arr[i, 52], arr[i, 53],
                               arr[i, 54], arr[i, 55], arr[i, 56], arr[i, 57], arr[i, 58], arr[i, 59],
                               arr[i, 60], arr[i, 61], arr[i, 62], arr[i, 63], arr[i, 64], arr[i, 65],
                               arr[i, 66], arr[i, 67], arr[i, 68], arr[i, 69], arr[i, 70], arr[i, 71],
                               arr[i, 72], arr[i, 73], arr[i, 74], arr[i, 75], arr[i, 76], arr[i, 77],
                               arr[i, 78], arr[i, 79], arr[i, 80], arr[i, 81], arr[i, 82], arr[i, 83],
                               arr[i, 84], arr[i, 85], arr[i, 86], arr[i, 87], arr[i, 88], arr[i, 89],
                               arr[i, 90], arr[i, 91], arr[i, 92], arr[i, 93], arr[i, 94], arr[i, 95],
                               arr[i, 96], arr[i, 97], arr[i, 98], arr[i, 99], arr[i, 100], arr[i, 101],
                               arr[i, 102], arr[i, 103], arr[i, 104], arr[i, 105], arr[i, 106], arr[i, 107],
                               arr[i, 108]).save()
    print('财务指标数据导入完成：', code)


def get_index_bar(code):
    connect("mongodb://localhost:27017/stock?", alias="index_bar")
    df = pro.index_daily(ts_code=code)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            IndexBar(arr[i, 0], arr[i, 1], arr[i, 2],
                     arr[i, 3], arr[i, 4], arr[i, 5],
                     arr[i, 6], arr[i, 7], arr[i, 8],
                     arr[i, 9], arr[i, 10], arr[i, 11]).save()
    print('指数行情数据导入完成：', code)


def get_index_weight(code, date):
    connect("mongodb://localhost:27017/stock?", alias="index_weight")
    if code == '000300.SH':
        code = '399300.SZ'
    elif code == '000905.SH':
        code = '399905.SZ'
    print('已将上交所指数代码改为深交所指数代码.')
    df = pro.index_weight(index_code=code, trade_date=date)
    df = add_key_(df)
    arr = df.values
    if len(arr) == 0:
        print('非交易日：' + time.strftime("%Y%m%d", time.localtime(time.time())))
        pass
    else:
        for i in tqdm(range(len(arr))):
            IndexWeight(arr[i, 0], arr[i, 1], arr[i, 2],
                        arr[i, 3], arr[i, 4]).save()
    print('指数权重数据导入完成：', code)


if __name__ == '__main__':
    # 先统一下载近三个月的数据
    start = '20000101'
    end = '20190819'
    date = end
    get_tradedays(start, end)
    get_dailybar(date)
    get_adjfactor(date)
    get_stock_main(date)
    get_stock_list(date)
    get_suspend_list(date)
    get_dailybar(date)
    get_daily(date)
    get_income_sheet('600000.SH', '20190819')
    get_balance_sheet('600000.SH', '20190819')
    get_cashflow_sheet('600000.SH', '20190819')
    get_financial_indicator('600000.SH')
    get_index_bar('000300.SH')
    get_index_weight('000300.SH', '20180928')
