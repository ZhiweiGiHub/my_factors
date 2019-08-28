import math
from tqdm import tqdm
import numpy as np
from api import get_data_panel
from industry.get_all_time_data import get_time_data_
from industry.position_management import position_manager
from industry.leader_multifactor import *
from industry.position_init import position_init_
from industry.return_maker import return_maker_
from industry.performance import *
from industry.export_result import export_result_


def factor_test(factor, types):
    # 导入股票名单和相应权重
    total_capitals = [10000000]
    pnls = []
    transaction_cost_rate = 0.003  # 交易费用
    factor_list = [factor]
    factor_type = [types]

    result_export = False  # 是否输出每天持仓信息
    ic_weighter = True  # 多因子是否按ic值加权平均
    include_ir = True
    orthogonalization = False

    if result_export:
        result_path = './result/'  # 当下根目录
        strategy_name = 'industry_leader'
        file_name1 = strategy_name + '_df_md_80.xlsx'

    # 获取所有调仓日
    start, end = '20180101', "20190801"
    change_days, my_tradingdays = get_time_data_(start, end)

    # 开始回测
    for changeday in tqdm(list(my_tradingdays['tradingdays'])):

        if changeday == change_days[0]:  # 建仓日

            # stock_pool = GD_location_maker_(changeday)
            stock_pool = leader_multifactor_maker(changeday, factor_list, factor_type,
                                                  ic_weighter, include_ir=include_ir,
                                                  orthogonalization=orthogonalization)
            # stock_pool = leader_factor_maker(changeday)

            # 持仓矩阵初始化
            position = position_init_(stock_pool, changeday, total_capitals)

            if result_export:
                # 输出持仓信息
                export_result_(position, changeday, result_path, file_name1)

            # 记录当天的损益
            pnls, total_capitals = return_maker_(position, pnls,
                                                 total_capitals,
                                                 transaction_cost_rate)
            # last_stock_pool = stock_pool_class
            # print(changeday, '开仓')

        elif changeday != change_days[0] and changeday in change_days:  # 调仓日

            # 更新开盘价数据
            price = get_data_panel("huangwei", "huangwei", list(position['code']),
                                   changeday, changeday,
                                   collection='stock_daily').drop_duplicates(['code'])
            # 保存过去的持仓情况
            last_position = position
            if 'open' in last_position.columns:
                del last_position['open']
            if 'rtn' in last_position.columns:
                del last_position['rtn']
            last_position = last_position.merge(price[['code', 'open', 'rtn']],
                                                on='code', how='inner')
            last_position['open_price'] = last_position['open']
            last_position['close_price'] = last_position['open_price'] \
                                           * (1 + last_position['rtn'])
            # 按现在价格计算过去持仓的手数
            last_position['lots'] = last_position['account'] / (100 * last_position['open_price'])
            last_position['lots'] = last_position['lots'].fillna(0)  # 没有开盘价的股票被“冻结”
            last_position['lots'] = last_position['lots'].apply(lambda x: math.floor(x))

            # stock_pool = stock_select_(changeday)
            # stock_pool, stock_pool_class = leader_moderate(changeday, last_stock_pool, 0.02)
            stock_pool = leader_multifactor_maker(changeday, factor_list, factor_type,
                                                  ic_weighter, include_ir=include_ir,
                                                  orthogonalization=orthogonalization)
            # stock_pool = leader_factor_maker(changeday)
            # stock_pool = GD_location_maker_(changeday)

            # 持仓初始化
            position = position_init_(stock_pool, changeday, total_capitals)

            position, adj_pnl = position_manager(changeday, last_position,
                                                 position, transaction_cost_rate)

            pnls.append(adj_pnl)
            # 记录总价值
            total_capitals.append(total_capitals[-1] + adj_pnl)
            # 更新每只股票的总市值
            position['account_realized'] = position['account_realized'] * (1 + position['rtn'])
            position['account'] = position['account_realized'] + position['account_surplus']

            if result_export:
                # 输出持仓信息
                export_result_(position, changeday, result_path, file_name1)

            # last_stock_pool = stock_pool_class
            # print(changeday, '换仓')
        else:
            # 非调仓日
            # 更新价格数据
            position['date'] = changeday
            price = get_data_panel("huangwei", "huangwei", list(position['code']),
                                   changeday, changeday,
                                   collection='stock_daily').drop_duplicates(['code'])
            if 'open' in position.columns:
                del position['open']
            if 'rtn' in position.columns:
                del position['rtn']

            position = position.merge(price[['code', 'open', 'rtn']],
                                      on='code', how='left')
            position['open_price'] = position['open']
            position['close_price'] = position['open_price'] * (1 + position['rtn'])

            # 计算当日持仓（可能不是正确值，因为open价格未经调整，但可以作为一个基准计算合理的收益率）
            position['account_realized'] = 100 * position['open_price'] * position['lots']
            # 记录当天的损益
            temp = position.dropna(axis=0)
            pnl = (temp['close_price'] / temp['open_price'] - 1) * temp['account_realized']
            pnls.append(pnl.sum())
            # 记录总价值
            total_capitals.append(total_capitals[-1] + pnl.sum())
            # 更新每只股票的总市值
            position['account_realized'] = position['account_realized'] * (1 + position['rtn'])
            position['account'] = position['account_realized'] + position['account_surplus']

            # if result_export == True:
            #     # 输出持仓信息
            #     export_result_(position, changeday, result_path, file_name1)
            # print(changeday, '持仓')

    # 累计总价值曲线（与指数累计收益曲线作对比）
    banchmark = "000300.SH"
    returns = np.array(total_capitals)[1:] / np.array(total_capitals)[0]
    hs300 = get_data_panel("huangwei", "huangwei",
                           [banchmark],  # 对标指数
                           my_tradingdays['tradingdays'].iloc[0],
                           my_tradingdays['tradingdays'].iloc[-1],
                           collection='index_rtn')
    hs300['cum_chg'] = hs300['pct_chg'] / 100 + 1
    hs300['cum_chg'] = hs300['cum_chg'].cumprod().values
    extra = returns[:, np.newaxis] - hs300['cum_chg'].values[:, np.newaxis]
    performance_(hs300, banchmark, returns, extra, factor_list[0], 'spyl')
    return total_capitals
