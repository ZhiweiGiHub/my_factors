from industry.leader_maker import LeaderMaker
import pandas as pd


def leader_moderate(changeday, last_stock_pool, ratio):


    now = LeaderMaker(changeday, 'total_mv', '000905.SH', None, '000300.SH')
    now_weighted_result = now.weight()
    last = last_stock_pool
    last_weighted_result = last.result


    # 对于某个行业，查看其所选股票的差异
    sw_name_list = now_weighted_result['sw3_name']

    new_industry = pd.DataFrame()

    for sw_name in list(sw_name_list.unique()):
        now_industry = now_weighted_result[now_weighted_result['sw3_name'] == sw_name]
        now_origin_industry = now.all[now.all['sw3_name'] == sw_name]

        last_industry = last_weighted_result[last_weighted_result['sw3_name'] == sw_name]
        last_origin_industry = last.all[last.all['sw3_name'] == sw_name]

        if set(now_industry['code_name']).intersection(set(last_industry['code_name'])) \
                != set(now_industry['code_name']):
            # print(sw_name)

            # 找到该行业中哪些股票被剔除
            lost_stocks = set(last_industry['code_name']).difference(
                set(now_industry['code_name']).intersection(set(last_industry['code_name'])))

            for stock in lost_stocks:
                # 搜索被剔除股票的情况
                cc = now_origin_industry[now_origin_industry['code_name'] == stock]  # 上个调仓日的情况
                dd = last_origin_industry[last_origin_industry['code_name'] == stock]  # 这个调仓日的情况

                # 如果在申万三级行业中的比例未下降到0.02, 仍然保留。否则加入新名单中的末尾
                if len(dd) > 0 and len(cc) > 0:
                    if dd['weight_in_industry'].values[0] - \
                            cc['weight_in_industry'].values[0] <= ratio:
                        last_industry = last_industry.append(
                            last_origin_industry[last_origin_industry['code_name'] == stock]
                        )
                        print(stock, '保留在新组合中')
                    else:
                        continue

                elif len(dd) == 0:  # 说明第二年没有这只股票，不用再恢复
                    # print('已经没有该股')
                    continue

        new_industry = new_industry.append(last_industry)

    # 对补充进的股票依据上下单元格进行数据补全
    new_industry = new_industry.reset_index(drop=True)

    for i in range(len(new_industry)):
        if new_industry['industry_total_sw1'].iloc[i] > 0: # 判断是否出现nan
            continue
        else:
            if i != len(new_industry) and i != 0:
                if new_industry['industryweight_in_all'].iloc[i] == new_industry['industryweight_in_all'].iloc[i + 1]:
                    t = i
                    while not new_industry['industry_total_sw1'].iloc[t + 1] > 0:
                        t += 1
                    new_industry['industry_total_sw1'].iloc[i] = \
                        new_industry['industry_total_sw1'].iloc[t + 1]
                else:
                    t = i
                    while not new_industry['industry_total_sw1'].iloc[t - 1] > 0:
                        t -= 1
                    new_industry['industry_total_sw1'].iloc[i] = \
                        new_industry['industry_total_sw1'].iloc[t - 1]

            elif i == len(new_industry):
                while not new_industry['industry_total_sw1'].iloc[t - 1] > 0:
                    t -= 1
                new_industry['industry_total_sw1'].iloc[i] = \
                    new_industry['industry_total_sw1'].iloc[t - 1]
            else:
                while not new_industry['industry_total_sw1'].iloc[t + 1] > 0:
                    t += 1
                new_industry['industry_total_sw1'].iloc[i] = \
                    new_industry['industry_total_sw1'].iloc[t + 1]

    new_industry['weight_in_industry_sw1'] = new_industry['cir_mv'] \
                                     / new_industry['industry_total_sw1']

    new_industry['weight_adj_sw1'] = new_industry['industryweight_in_all'] \
                                     * new_industry['weight_in_industry_sw1']

    # 权重归一化
    new_industry['weight_adj_sw1'] = new_industry['weight_adj_sw1'] \
                                     / new_industry['weight_adj_sw1'].sum()

    # print(a_weighted_result['weight_adj_sw1'].sum())
    # print(new_industry['weight_adj_sw1'].sum())

    return new_industry, now

if __name__ == "__main__":
    changeday = '2017-07-31'
    last_changeday = '2017-06-30'
    result = leader_moderate(changeday, last_changeday, 0.1)