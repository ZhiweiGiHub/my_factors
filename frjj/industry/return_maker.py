

def return_maker_(position, pnls, total_capitals, transaction_cost_rate):
    trans = position['account_realized'] * transaction_cost_rate
    trans = trans.sum()
    temp = position.dropna(axis=0)
    pnl = (temp['close_price'] / temp['open_price'] - 1) * temp['account_realized']
    pnls.append(pnl.sum() - trans)

    # 记录总价值
    total_value = position['account_realized'] + position['account_surplus']
    total_capitals.append(total_value.sum() + pnl.sum() - trans)

    return pnls, total_capitals