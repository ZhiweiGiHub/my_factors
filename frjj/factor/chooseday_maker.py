from api import get_tradingdays


def chooseday_maker_(start, end, period='M'):
    # start, end = '20170101', "20190628"
    # 获取所有的交易日
    user = "huangwei"
    passwd = "huangwei"
    tradingdays = get_tradingdays(user, passwd, start, end)

    # 设定选股日期、调仓日期
    change_days = []
    choose_days = []

    # 持仓周期
    if period == 'M':
        for i in range(len(tradingdays) - 1):
            if tradingdays['tradingdays'].iloc[i][4:6] != tradingdays['tradingdays'].iloc[i + 1][4:6]:
                choose_days.append(tradingdays['tradingdays'][i])
                change_days.append(tradingdays['tradingdays'][i + 1])
    else:
        for i in range(len(tradingdays) - 1):
            if (i + 1) % period == 0:
                choose_days.append(tradingdays['tradingdays'][i])
                change_days.append(tradingdays['tradingdays'][i + 1])

    return choose_days, change_days
