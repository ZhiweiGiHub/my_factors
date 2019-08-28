import datetime
import numpy as np

from backtest import Backtest
from data import HistoricMongoDBDataHandler
from event import SignalEvent
from execution import SimulatedExecutionHandler
from portfolio import Portfolio
from strategy import Strategy
import time


class MovingAverageCrossStrategy(Strategy):
    """
    Carries out a basic Moving Average Crossover strategy with a
    short/long simple weighted moving average. Default short/long
    windows are 100/400 periods respectively.
    """

    def __init__(self, bars, events, short_window=5, middle_window=10, long_window=60):
        """
        Initialises the buy and hold strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        short_window - The short moving average lookback.
        long_window - The long moving average lookback.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.short_window = short_window
        self.long_window = long_window
        self.middle_window = middle_window

        # Set to True if a symbol is in the market
        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to 'OUT'.
        """
        bought = {}  # bought是一个字典
        for s in self.symbol_list:
            bought[s] = 'OUT'  # 先全部初始化为out
        return bought

    def calculate_signals(self, event):
        # 抽象基类所强制要求的一个属性
        """
        Generates a new set of signals based on the MAC
        SMA with the short window crossing the long window
        meaning a long entry and vice versa for a short entry.    

        Parameters
        event - A MarketEvent object. 
        """
        if event.type == 'MARKET':
            # data模块最后一行生成这个事件
            for symbol in self.symbol_list:

                bars = self.bars.get_latest_bars_values(symbol, "close", N=self.long_window)
                if bars is not None and bars != []:
                    short_sma = np.mean(bars[-self.short_window:])
                    long_sma = np.mean(bars[-self.long_window:])
                    middle_sma = np.mean(bars[-self.middle_window:])

                    dt = self.bars.get_latest_bar_datetime(symbol)
                    strength = 1.0
                    strategy_id = 1

                    if short_sma > middle_sma > long_sma and self.bought[symbol] == "OUT":
                        # 两重均线上穿
                        sig_dir = 'LONG'  # 交易方向
                        signal = SignalEvent(strategy_id, symbol, dt, sig_dir, strength)
                        self.events.put(signal)
                        self.bought[symbol] = 'LONG'

                    elif self.bought[symbol] == "LONG":
                        if short_sma < middle_sma or short_sma > 1.2 * long_sma:
                            # 第一个条件止损，第二个条件止盈（其实是更快的止损）
                            sig_dir = 'EXIT'
                            signal = SignalEvent(strategy_id, symbol, dt, sig_dir, strength)
                            self.events.put(signal)
                            self.bought[symbol] = 'OUT'


if __name__ == "__main__":
    time_start = time.time()
    symbol_list = ['601318.SH', '000651.SZ', '002294.SZ', '002572.SZ', '300376.SZ', '300498.SZ']
    initial_capital = 100000.0
    start_date = datetime.datetime(2017, 3, 1, 0, 0, 0)
    heartbeat = 0.0
    backtest = Backtest(symbol_list,
                        initial_capital,
                        heartbeat,
                        start_date,
                        HistoricMongoDBDataHandler,
                        SimulatedExecutionHandler,
                        Portfolio,
                        MovingAverageCrossStrategy)

    backtest.simulate_trading()

    time_end = time.time()
    print('totally cost', time_end - time_start, ' seconds')
