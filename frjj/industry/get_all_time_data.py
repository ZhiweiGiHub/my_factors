# 确定好回测期后，获取相关数据
from factor.chooseday_maker import chooseday_maker_
from api import get_tradingdays

def get_time_data_(start, end):
    tradingdays_all = get_tradingdays("huangwei", "huangwei", '20000104', "20191231")

    choose_days, change_days = chooseday_maker_(start, end, period='M')
    change_days = choose_days  # 调整为月末调仓

    # 回测涉及的所有交易日
    my_tradingdays = tradingdays_all[(tradingdays_all['tradingdays'] >= choose_days[0]) & (
            tradingdays_all['tradingdays'] <= end)].reset_index(drop=True)

    return change_days, my_tradingdays