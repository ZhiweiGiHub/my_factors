# import h5py
import warnings
from jaqs_fxdayu.research import SignalDigger
from jaqs_fxdayu.data import DataView
from api import *

warnings.filterwarnings('ignore')

# step 1：实例化SignalDigger 通过output_folder和output_format指定因子绩效表现的输出路径和输出格式，通过signal_name指定绩效文件名称
sd = SignalDigger(output_folder=".", output_format='pdf', signal_name="signal")

folders = ['./data_2016_1_000300', './data_2016_2_000300',
           './data_2017_1_000300', './data_2017_2_000300',
           './data_2018_1_000300', './data_2018_2_000300',
           './data_2019_1_000300']


def get_(folder):
    # step 2 因子数据预处理
    # 加载dataview数据集
    dv = DataView()
    dataview_folder = folder
    dv.load_dataview(dataview_folder)
    # 定义信号过滤条件-非指数成分
    # df_index_member = dv.get_ts('index_member')

    signal = dv.get_ts("pb")
    price = dv.get_ts("close_adj")
    group = dv.get_ts("sw1")

    # mask
    mask = dv.get_ts('index_member') == 0  # 定义信号过滤条件-非指数成分

    # 定义可买入卖出条件——未停牌、未涨跌停
    trade_status = dv.get_ts('trade_status')
    can_trade = trade_status == 1  # 可以交易
    # 涨停
    up_limit = dv.add_formula('up_limit',
                              '(open - Delay(close, 1)) / Delay(close, 1) > 0.095',
                              is_quarterly=False)
    # 跌停
    down_limit = dv.add_formula('down_limit',
                                '(open - Delay(close, 1)) / Delay(close, 1) < -0.095',
                                is_quarterly=False)
    can_enter = np.logical_and(up_limit < 1, can_trade)  # 未涨停未停牌
    can_exit = np.logical_and(down_limit < 1, can_trade)  # 未跌停未停牌
    benchmark = dv.data_benchmark
    return signal, price, mask, group, can_enter, can_exit, benchmark


def data_maker_():
    signal, price, mask, group, can_enter, can_exit, benchmark = get_(folders[0])
    for i in range(len(folders)):
        if i != 0:
            signal_t, price_t, mask_t, group_T, can_enter_t, can_exit_t, benchmark_t = get_(folders[i])
            mask = mask.append(mask_t)
            can_enter = can_enter.append(can_enter_t)
            can_exit = can_exit.append(can_exit_t)
            benchmark = benchmark.append(benchmark_t)
            signal = signal.append(signal_t)
            price = price.append(price_t)
            mask = mask.append(mask_t)

    # 数据清洗
    mask['date'] = mask.index
    mask = mask.drop_duplicates()
    del mask['date']

    benchmark['date'] = benchmark.index
    benchmark = benchmark.drop_duplicates()
    del benchmark['date']

    mask = mask.fillna(True)
    can_enter = can_enter.fillna(False)
    can_exit = can_exit.fillna(False)
    return signal, price, mask, group, can_enter, can_exit, benchmark


if __name__ == '__main__':
    # pb因子
    begin = '20170103'
    end = '20190628'
    all_signal = get_signal(begin, end, 'pb', collection='stock_indicator', database="stock_cn")
    all_signal = all_signal.set_index('date')
    all_signal.to_csv('C:/Users/Lenovo/PycharmProjects/my_factor/all_signal_pb.csv')
#
# 读取data.hd5文件
h = pd.HDFStore('C:/Users/Lenovo/PycharmProjects/my_factor/data_2017_1_000300/data.hd5')
keys = h.keys()
a = h[keys[1]]
