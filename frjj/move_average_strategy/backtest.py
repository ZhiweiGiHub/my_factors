#!/usr/bin/python
# -*- coding: utf-8 -*-

# backtest.py

from __future__ import print_function

import datetime
import pprint
try:
    import Queue as queue
except ImportError:
    import queue
import time


class Backtest(object):
    """
    Enscapsulates the settings and components for carrying out
    an event-driven backtest.
    封装用于执行事件驱动回测的设置和组件。
    """

    def __init__(
        self, symbol_list, initial_capital,
        heartbeat, start_date, data_handler, 
        execution_handler, portfolio, strategy
    ):
        """
        Initialises the backtest.

        Parameters:
        symbol_list - The list of symbol strings.
        initial_capital - The starting capital for the portfolio.
        heartbeat - Backtest "heartbeat" in seconds
        start_date - The start datetime of the strategy.
        data_handler - (Class) Handles the market data feed.
        execution_handler - (Class) Handles the orders/fills for trades.
        portfolio - (Class) Keeps track of portfolio current and prior positions.
        strategy - (Class) Generates signals based on market data.
        """
        self.symbol_list = symbol_list
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.start_date = start_date

        self.data_handler_cls = data_handler   # 注意这里赋的是一个类 HistoricCSVDataHandler
        self.execution_handler_cls = execution_handler  # SimulatedExecutionHandler
        self.portfolio_cls = portfolio         # 注意这里赋的是一个类 Portfolio
        self.strategy_cls = strategy           # 注意这里赋的是一个类 MovingAverageCrossStrategy

        self.events = queue.Queue()
        
        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1
       
        self._generate_trading_instances()     

    def _generate_trading_instances(self):  # 第一个被调用的方法。创建各个类的实例
        """
        Generates the trading instance实例 objects from 
        their class types.
        """
        print(
            "Creating DataHandler, Strategy, Portfolio and ExecutionHandler"
        )
        self.data_handler = self.data_handler_cls(self.events, self.symbol_list)
        self.strategy = self.strategy_cls(self.data_handler, self.events)      # 这里才真正调用了mac中编写的策略
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.start_date, 
                                            self.initial_capital)
        self.execution_handler = self.execution_handler_cls(self.events)

    def _run_backtest(self):  # 第三个被调用的方法
        """
        Executes the backtest.
        """
        i = 0
        while True:
            i += 1
            print(i)
            # Update the market bars 读取新的（历史）市场数据
            if self.data_handler.continue_backtest:  # 未到达最近的日期
                self.data_handler.update_bars()
            else:
                break

            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)  # 计算策略所需要的值
                            self.portfolio.update_timeindex(event)  # 持有的资产组合随时间价值变动

                        elif event.type == 'SIGNAL':   
                            self.signals += 1                            
                            self.portfolio.update_signal(event)

                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)

                        elif event.type == 'FILL':  
                            self.fills += 1
                            self.portfolio.update_fill(event)

            time.sleep(self.heartbeat)

    def _output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """
        self.portfolio.create_equity_curve_dataframe()
        
        print("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()
        
        print("Creating equity curve...")
        print(self.portfolio.equity_curve.tail(10))
        pprint.pprint(stats)

        print("Signals: %s" % self.signals)
        print("Orders: %s" % self.orders)
        print("Fills: %s" % self.fills)

    def simulate_trading(self):
        """
        Simulates the backtest and outputs portfolio performance.
        """
        self._run_backtest()
        self._output_performance()
