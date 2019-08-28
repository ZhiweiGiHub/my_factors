# 限定股票池（某一个行业、某一个指数成分股）
from api import get_data_crosssection, get_tradingdays
from industry.leader_maker_new import LeaderMaker
# from industry.leader_maker import LeaderMaker


def stock_pool_maker(chooseday, type, info=None):

    all_tradingdays = get_tradingdays("huangwei", "huangwei", '20000104', "20191231")['tradingdays']
    # if len(chooseday) >= 8:
    #     chooseday = chooseday[0:4] + chooseday[5:7] + chooseday[8:10]

    # chooseday_month1 = chooseday[0:6] + '01'
    # chooseday_adj = all_tradingdays[(all_tradingdays <= chooseday) & (all_tradingdays >= chooseday_month1)].iloc[0]
    # chooseday_r = chooseday_adj[0:4] + "-" + chooseday_adj[4:6] + "-" + chooseday_adj[6:8]
    if len(chooseday) == 8:
        chooseday = chooseday[0:4] + "-" + chooseday[4:6] + "-" + chooseday[6:8]
    # 如果是行业，调整为，每个月第一个交易日
    if type == "index":

        content_stocks = get_data_crosssection("huangwei", "huangwei",
                                               "index_weight",
                                               chooseday,
                                               database='stock_cn')
        hs300_list = content_stocks[content_stocks['index_code'] == info].rename(
            columns={'stock_code': 'code'})

        return hs300_list

    elif type == "industry":
        content_stocks = get_data_crosssection("huangwei", "huangwei",
                                               "industry",
                                               chooseday,
                                               database='stock_cn')
        hs300_list = content_stocks[
            content_stocks['industry_name'] == info].rename(
            columns={'stock_code': 'code'})
        if 'weight_adj_sw1' not in hs300_list.columns:
            hs300_list['weight_adj_sw1'] = 0
        return hs300_list

    elif type == "DIY":
        stock_pool = LeaderMaker(chooseday, 'total_mv',
                                 '000905.SH', None,
                                 '000300.SH', 'sw1_code').weight()
        return stock_pool


if __name__ == "__main__":
    stock_pool = stock_pool_maker("2018-10-20", "industry", "电子")
    # print(len(stock_pool))
    stock_pool = stock_pool_maker("2018-10-20", "index", "000300.SH")
