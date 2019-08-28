#!/usr/bin/python
# -*- coding: utf-8 -*-

# data.py

from __future__ import print_function

from abc import ABCMeta, abstractmethod
import os, os.path

import numpy as np
import pandas as pd
from api import get_data_panel

from event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class抽象的基类(ABC) providing an interface for
    all subsequent随后的 (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested.

    This will replicate模拟 how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite回测系统.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """
        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        from the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low, 
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.bar_index = 0

        self._open_convert_csv_files()  # 实例化这个类时，已经读取好了所有股票数据

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.io.parsers.read_csv(
                os.path.join(self.csv_dir, '%s.csv' % s),
                # 每只股票以股票代码为名称，csv格式存储
                header=0, index_col=0, parse_dates=True,  # 解析原csv中的时间，变为datatime格式，并作为index
                names=[
                    'datetime', 'open', 'high',
                    'low', 'close', 'volume', 'adj_close'
                ]
            ).sort_values(by="datetime", ascending="TRUE")  # 这里将日期已经处理为升序排列
            # 这里的symbol_data是一个字典，每个股票代码对应的是一个DataFrame

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)  # 这里对于不同股票的日期会不断叠加，直到得到一列所有股票里面最长的

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()
            # pad/ffill：用前一个非缺失值去填充该缺失值（向后填充） backfill/bfill：用下一个非缺失值填充该缺失值（向前填充）
            # 使用iterrows将dataframe变成了一个生成器，可以直接写入循环

    def _get_new_bar(self, symbol):  # 一行一行地读取数据（按时间顺序）
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:  # 对于某个symbol的某行数据，逐个生成bar
            yield b

    def get_latest_bar(self, symbol):  # get_latest_bar存储的是从回测开始累积的bars
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]  # 返回最后一个时点的所有信息（时间、值）

    def get_latest_bars(self, symbol, N=1):  # 获取多行
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]  # 返回最后一个时点的时间数据（在dataframe的位置0）
            # 这里返回的bars_list是一个timestamp，可以用下面两个函数来判断是星期几
            # isoweekday（）返回的1-7代表周一--周日；
            # date.weekday（）返回的0-6代表周一--到周日

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
            # val_type表示返回的是哪一个值（开盘价等）

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))  # 与yeild语句对应，下一行数据
            except StopIteration:
                self.continue_backtest = False  # 到达数据的最近日期，令flag continue_backtest转变为False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
                    # 这里赋予了latest_symbol_data的值
        self.events.put(MarketEvent())  # mac（策略）执行的开端。MarketEvent的属性就是type == ‘market’


class HistoricMongoDBDataHandler(DataHandler):
    """
    HistoricMongoDBDataHandler is designed to get data for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self, events, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.bar_index = 0

        self._open_convert_database()  # 实例化这个类时，已经读取好了所有股票数据

    def _open_convert_database(self):
        all_data = get_data_panel("huangwei", "huangwei",
                                  self.symbol_list, '2017-03-01',
                                  '2019-08-01', collection='stock_daily')
        comb_index = None
        for s in self.symbol_list:
            one_stock = all_data[all_data['code'] == s][['date', 'open',
                                                         'high', 'low',
                                                         'close', 'vol',
                                                         'rtn']]
            one_stock = one_stock.rename(columns={"date": "datetime", "vol": "volume"})
            one_stock['adj_close'] = 0

            one_stock_arr = one_stock.values

            for i in range(len(one_stock_arr)):
                if i == 0:
                    one_stock_arr[i, 7] = one_stock_arr[i, 4] \
                                                     * (1 + one_stock_arr[i, 6])
                else:
                    one_stock_arr[i, 7] = one_stock_arr[i - 1, 7] \
                                                     * (1 + one_stock_arr[i, 6])

            one_stock = pd.DataFrame(one_stock_arr, columns=one_stock.columns, index=range(len(one_stock)))
            del one_stock['rtn']

            # 将datatime作为index，并改为datatime格式
            one_stock['datetime'] = pd.to_datetime(one_stock['datetime'])
            one_stock.set_index(["datetime"], inplace=True)
            one_stock = one_stock.fillna(method='ffill')
            # ffill：用前一个非缺失值去填充该缺失值（向后填充）
            # bfill：用下一个非缺失值填充该缺失值（向前填充）

            self.symbol_data[s] = one_stock.sort_values(by="datetime", ascending=True)

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)
                # 这里对于不同股票的日期会不断叠加，直到得到一列所有股票里面最长的

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_index, method='pad').iterrows()

    def _get_new_bar(self, symbol):  # 一行一行地读取数据（按时间顺序）
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:  # 对于某个symbol的某行数据，逐个生成bar
            yield b

    def get_latest_bar(self, symbol):  # get_latest_bar存储的是从回测开始累积的bars
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]  # 返回最后一个时点的所有信息（时间、值）

    def get_latest_bars(self, symbol, N=1):  # 获取多行
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]  # 返回最后一个时点的时间数据

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
            # val_type表示返回的是哪一个值（开盘价等）

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))  # 与yeild语句对应，下一行数据
            except StopIteration:
                self.continue_backtest = False  # 到达数据的最近日期，令flag continue_backtest转变为False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
                    # 这里赋予了latest_symbol_data的值
        self.events.put(MarketEvent())  # mac（策略）执行的开端。MarketEvent的属性就是type == ‘market’
