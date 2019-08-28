from api import get_data_panel
import pandas as pd
import math


def position_init_(stock_pool, changeday, total_capitals):
    # 持仓矩阵初始化
    position = pd.DataFrame(columns=['date', 'code', 'weight', 'account',
                                     'open_price',
                                     'lots', 'account_surplus', 'close_price'],
                            index=range(len(stock_pool)))
    position['code'] = stock_pool['code']
    position['weight'] = stock_pool['weight_adj_sw1']
    position['date'] = changeday
    position['account'] = total_capitals[-1] * position['weight']
    # 计算每个股票的应买手数
    price = get_data_panel("huangwei", "huangwei", list(position['code']),
                           changeday, changeday,
                           collection='stock_daily').drop_duplicates(['code'])

    if 'open' in position.columns:
        del position['open']
    if 'rtn' in position.columns:
        del position['rtn']

    position = position.merge(price[['code', 'open', 'rtn']],
                              on='code', how='inner')
    position['open_price'] = position['open']
    position['close_price'] = position['open_price'] * (1 + position['rtn'])
    position['lots'] = position['account'] / (100 * position['open_price'])

    # 对于获取不到开盘价的股票，可买手数一律赋0
    position['lots'] = position['lots'].fillna(0)

    # 为了方便计算剩余资金，创造facked字段，开盘价为nan的赋为0
    position['open_price_faked'] = position['open_price'].fillna(0)
    position['lots'] = position['lots'].apply(lambda x: math.floor(x))
    position['account_realized'] = 100 * position['open_price_faked'] * position['lots']
    position['account_surplus'] = position['account'] - position['account_realized']

    return position