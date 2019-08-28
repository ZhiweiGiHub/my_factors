

def change_position_(position, last_position, transaction_cost_rate):
    # 注意，两个position表需要要一致的股票名单

    # 记录当天的损益
    # 计算换仓成本（手数变化*当日开盘价）
    trans = (position['lots'] - last_position['lots']) * 100 * \
            position['open_price'] * transaction_cost_rate
    trans = trans.apply(abs).dropna(axis=0).sum()

    temp = position.dropna(axis=0)
    pnl = (temp['close_price'] / temp['open_price'] - 1) \
          * temp['account_realized']
    return pnl.sum() - trans
