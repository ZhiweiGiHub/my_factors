import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def performance_(hs300, banchmark, returns, extra, factor_name, industry):

    fig, ax1 = plt.subplots()
    # plt.subplot(2, 1, 1)
    ax1.plot(hs300.values[:, 1],
             hs300.values[:, -1],
             label=banchmark, color='tomato')

    ax1.plot(hs300.values[:, 1],
             returns,
             label='strategy', color='orange')
    plt.legend(loc=2)

    # plt.subplot(2, 1, 2)
    ax2 = ax1.twinx()
    ax2.plot(hs300.values[:, 1],
             extra,
             label='extra_return',
             color='dodgerblue')

    plt.legend(loc=1)
    try:
        plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(int(len(hs300) / 6)))
    except ValueError:
        pass
    plt.grid(True)  # 增加格点
    plt.axis('tight')  # 坐标轴适应数据量 axis 设置坐标轴
    plt.xlabel('Date')
    plt.title(factor_name + ' ' + industry)
    # plt.savefig(factor_name + industry)