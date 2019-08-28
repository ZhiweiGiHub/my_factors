from api import get_data_crosssection


# 当天停牌状态、退市状态、ST状态的股票


def get_all_not_available_(chooseday_r, remove_st=True):
    print('获取可以交易的股票...')
    try:
        suspend = set(get_data_crosssection("huangwei", "huangwei",
                                            "code_list_suspend", chooseday_r,
                                            database="stock_cn")['code'])
    except KeyError as e:
        print(e)
        suspend = set()

    try:
        delisting = set(get_data_crosssection("huangwei", "huangwei",
                                              "code_list_delisting", chooseday_r,
                                              database="stock_cn")['code'])
    except KeyError as e:
        print(e)
        delisting = set()

    try:
        st = set(get_data_crosssection("huangwei", "huangwei",
                                       "code_list_st", chooseday_r,
                                       database="stock_cn")['code'])
    except KeyError as e:
        print(e)
        st = set()

    if remove_st:
        not_available = suspend.union(delisting).union(st)
    else:
        not_available = suspend.union(delisting)

    return list(not_available)


if __name__ == '__main__':
    chooseday_r = '2019-06-28'
    cc = get_all_not_available_(chooseday_r)
