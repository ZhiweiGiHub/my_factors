# SignalDigger是digger模块中的一个核心类，使用SigalDigger可以分析股票单因子各项表现
from data_maker import *



signal, price, mask, group, can_enter, can_exit, benchmark = data_maker_()

# 自有数据
tradingdays = get_tradingdays("huangwei", "huangwei", '20170103', '20190628')
range = (tradingdays['tradingdays'] >= str(signal.index[0])) & (tradingdays['tradingdays'] <= str(signal.index[-1]))
my_signal = pd.read_csv('all_signal_pb.csv').iloc[list(range)].set_index('date')
# my_price = pd.read_csv('all_signal_close.csv').iloc[list(range)].set_index('date')
# my_group = pd.read_csv('all_signal_industry.csv').iloc[list(range)].set_index('date')

# 对于缺失值都向前向后填充,只截取原数据集的股票列表
# my_signal = my_signal.bfill().ffill().dropna(how='any', axis=1)[signal.columns]
# my_price = my_price.bfill().ffill().dropna(how='any', axis=1)[signal.columns]
# my_group = my_group.bfill().ffill().dropna(how='any', axis=1)[signal.columns]
my_signal = my_signal[price.columns]

sd.process_signal_before_analysis(signal=signal,
                                  price=price,
                                  # high=dv.get_ts("high_adj"),
                                  # low=dv.get_ts("low_adj"),
                                  group=group,
                                  # 按申万一级行业分类
                                  n_quantiles=5,
                                  mask=mask,  # 对总的股票池进行限定
                                  can_enter=can_enter,  # 停牌或涨跌停的股票不可交易
                                  can_exit=can_exit,  # 停牌或涨跌停的股票不可交易
                                  period=5,  # 持有周期,默认为5,即持有5天
                                  benchmark_price=benchmark,
                                  forward=True,
                                  # 收益对齐方式,forward=True则在当期因子下对齐下一期实现的收益
                                  # forward=False则在当期实现收益下对齐上一期的因子值。默认为True
                                  commission=0.0008
                                  )

# step 3 进行因子研究和分析
# heads = sd.signal_data

# ic分析
sd.create_information_report()

# 收益分析
sd.create_returns_report()
