# 处理调仓日的仓位及收益率的计算
from industry.change_position import change_position_
from industry.leader_multifactor import leader_multifactor_maker
from industry.position_init import position_init_
from industry.get_all_not_available import get_all_not_available_


def position_manager(changeday, last_position, position, transaction_cost_rate):
    # 对比新旧持仓的股票，可以分为3种情况:
    # inter:新旧持仓的交集、new:新持仓独有、sell:旧持仓特有

    inter = list(set(last_position['code']).intersection(set(position['code'])))
    new = list(set(position['code']).difference(set(inter)))
    sell = list(set(last_position['code']).difference(set(inter)))

    inter_position = position.iloc[
        [index for index in range(len(position)) if position[
            'code'].iloc[index] in inter]]

    inter_last_position = last_position.iloc[
        [index for index in range(len(last_position)) if last_position[
            'code'].iloc[index] in inter]]

    # 由于新选的股票中不会有停牌和退市的，所以这里计算收益也不需要剔除任何股票
    adj_pnl = change_position_(inter_position,
                               inter_last_position,
                               transaction_cost_rate)

    # 对于未在新旧仓位交集中的股票，计算其交易时产生的交易费用
    new_position = position.iloc[
        [index for index in range(len(position)) if position[
            'code'].iloc[index] in new]]

    sell_position = last_position.iloc[
        [index for index in range(len(last_position)) if last_position[
            'code'].iloc[index] in sell]]

    # 获取当日停牌和退市的股票N只，从sell_position中剔除，同时剔除new_position中后面N只股票
    not_available = get_all_not_available_(changeday, remove_st=False)

    not_available_index = [index for index in list(sell_position.index) if sell_position[
        'code'].loc[index] in not_available]

    remain_position = sell_position.loc[not_available_index]
    sell_position = sell_position.drop(not_available_index)

    if len(remain_position) > 0:
        new_position = new_position.drop(list(new_position.index)[-len(remain_position):])
    else:
        pass

    pnl_new = (new_position['close_price'] / new_position['open_price'] - 1) * new_position[
        'account_realized'] - new_position['account_realized'] * transaction_cost_rate
    pnl_sell = (sell_position['close_price'] / sell_position['open_price'] - 1) * sell_position[
        'account_realized'] - sell_position['account_realized'] * transaction_cost_rate
    adj_pnl = adj_pnl + pnl_new.sum() + pnl_sell.sum()

    # 新的position为remain_position + new_position + inter_position
    position_new = remain_position.append(new_position).append(inter_position)
    position_new = position_new.reset_index(drop=True)
    return position_new, adj_pnl


if __name__ == "__main__":
    factor_list = ['QFA_ROE_DEDUCTED']
    factor_type = ['financial']
    ic_weighter = False  # 多因子是否按ic值加权平均
    total_capitals = [10000000]
    transaction_cost_rate = 0.003
    changeday = "2017-04-28"

    stock_pool = leader_multifactor_maker("2017-04-28", factor_list, factor_type, ic_weighter)
    last_position = position_init_(stock_pool, "2017-04-28", total_capitals)

    stock_pool = leader_multifactor_maker("2017-03-31", factor_list, factor_type, ic_weighter)
    position = position_init_(stock_pool, "2017-03-31", total_capitals)

    cc, dd = position_manager(changeday, last_position, position, transaction_cost_rate)
