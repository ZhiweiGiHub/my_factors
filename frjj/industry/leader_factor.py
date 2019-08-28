from factor.factor_maker import factor_maker_


def leader_factor_maker(changeday):

    # factor_value = factor_maker_(changeday,
    #                   collection_name="stock_indicator",
    #                   indicator='pe_ttm',
    #                   ac=True, database='stock_cn',
    #                   type="DIY", factor_type='base')
    # 将权重归一化
    # factor_value['weight_adj_sw1'] = factor_value['weight_adj_sw1']\
    #                                  /factor_value['weight_adj_sw1'].sum()

    factor_value = factor_maker_(changeday,
                      collection_name="stock_indicator",
                      indicator='pe_ttm',
                      ac=False, database='stock_cn',
                      type="DIY", factor_type='tech')




    factor_value = factor_value.iloc[0:50]
    return factor_value