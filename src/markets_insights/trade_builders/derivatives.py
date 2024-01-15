from markets_insights.core.column_definition import DerivativesBaseColumns, DerivativesCalculatedColumns
from markets_insights.core.core import TypeHelper, MarketDaysHelper
import datetime
import pandas as pd
import numpy as np

from markets_insights.trade_builders.results import BiDirectionalTradesResult, TradesResultComposite, TradesResult

class DerivativeTradeOptions:
  holding_days: int = 7
  budget_min: int = 8000
  budget_max: int = 12000
  min_lots_oi: int = 50
  entry_amount_col: str = DerivativesCalculatedColumns.CloseAmount
  exit_amount_profit_col: str = DerivativesCalculatedColumns.HighAmount
  exit_amount_loss_col: str = DerivativesCalculatedColumns.LowAmount
  exit_amount_close_col: str = DerivativesCalculatedColumns.CloseAmount
  profit_factor: float = 2
  loss_factor: float = 0.5
  entry_lag_days: int = 0

class DerivativesNonTradablesDiagnostic:
  class EntryOptions:
    skip_budget_check = False
    skip_expiry_date_check = False
    skip_oi_check = False

  class ExitOptions:
    skip_holding_days_check = False
    skip_oi_check = False

class OptionsTradeBuilder():
  _trade_options: DerivativeTradeOptions
  _option_type: str
  
  def __init__(self, trade_options, option_type: str):
    self._trade_options = trade_options
    self._option_type = option_type

  def get_last_exit_date(self, signal_date):
     return self.get_first_entry_date(signal_date) + datetime.timedelta(days = self._trade_options.holding_days)
  
  def get_first_entry_date(self, signal_date):
    if self._trade_options.entry_lag_days > 0:
      for_date = pd.to_datetime(signal_date)
      for i in range(0, self._trade_options.entry_lag_days):
        for_date = MarketDaysHelper.get_next_market_day(for_date)
      return pd.to_datetime(for_date)
    else:
      return pd.to_datetime(signal_date)
  
  def get_full_trade(self, buy_option, sell_option, index):
    return pd.DataFrame({
        DerivativesBaseColumns.Identifier: buy_option[DerivativesBaseColumns.Identifier].values[0],
        DerivativesBaseColumns.StrikePrice: buy_option[DerivativesBaseColumns.StrikePrice].values[0],
        DerivativesBaseColumns.ExpiryDate: buy_option[DerivativesBaseColumns.ExpiryDate].values[0],
        DerivativesBaseColumns.OptionType: buy_option[DerivativesBaseColumns.OptionType].values[0],
        'EntryDate': buy_option[DerivativesBaseColumns.Date].values[0],
        'Debit': -1 * buy_option['Credit'].values[0],
        'ExitDate': sell_option[DerivativesBaseColumns.Date].values[0],
        'Credit': sell_option['Credit'].values[0],
        'Quantity': 1 * buy_option['Quantity'].values[0],
        'Turnover': abs(buy_option['Credit'].values[0]) + abs(sell_option['Credit'].values[0]),
        'Net': buy_option['Credit'].values[0] + sell_option['Credit'].values[0]
    }, index=index)

  def get_entry_option(self, data, symbol, signal_date, index = 0, diagnostic = DerivativesNonTradablesDiagnostic.EntryOptions()):
    hold_till_date = self.get_last_exit_date(signal_date)
    entry_option = data[
        (data[DerivativesBaseColumns.Identifier] == symbol)
        & (data[DerivativesBaseColumns.OptionType] == self._option_type)
        & (data[DerivativesBaseColumns.Date] == self.get_first_entry_date(signal_date))
        & (
            (data[self._trade_options.entry_amount_col].between(self._trade_options.budget_min, self._trade_options.budget_max))
            |
            (diagnostic.skip_budget_check == True)
          )
        & (
            (pd.to_datetime(data[DerivativesBaseColumns.ExpiryDate]) >= hold_till_date)
            |
            (diagnostic.skip_expiry_date_check == True)
          )
        & (
            (data[DerivativesBaseColumns.OpenInterest] >= (data[DerivativesCalculatedColumns.LotSize] * self._trade_options.min_lots_oi))
            |
            (diagnostic.skip_oi_check == True)
          )
    ].sort_values(DerivativesBaseColumns.StrikePrice, ascending=False).head(1)

    if len(entry_option) > 0:
        strike_price = entry_option[DerivativesBaseColumns.StrikePrice].values[0]
        expiry_date = entry_option[DerivativesBaseColumns.ExpiryDate].values[0]
        buy_amount = entry_option[self._trade_options.entry_amount_col].values[0]
        lotsize = entry_option[DerivativesCalculatedColumns.LotSize].values[0]
        entry_date = entry_option[DerivativesBaseColumns.Date].values[0]

        buy = pd.DataFrame({
                    DerivativesBaseColumns.Identifier: symbol,
                    DerivativesBaseColumns.StrikePrice: strike_price,
                    DerivativesBaseColumns.ExpiryDate: expiry_date,
                    DerivativesBaseColumns.OptionType: self._option_type,
                    DerivativesBaseColumns.Date: entry_date,
                    'Credit': -1 * buy_amount,
                    'Quantity': 1 * lotsize,
                    'Type': 'Buy'
                }, index=[(index*2)])
        return buy
    else:
        return []
    
  def get_exit_option_for_buy_option(self, data, buy_option, index = 0, diagnostic = DerivativesNonTradablesDiagnostic.ExitOptions()):
    return self.get_exit_option(
        data,
        symbol = buy_option[DerivativesBaseColumns.Identifier].values[0],
        expiry_date = buy_option[DerivativesBaseColumns.ExpiryDate].values[0],
        trade_date = buy_option[DerivativesBaseColumns.Date].values[0],
        strike_price = buy_option[DerivativesBaseColumns.StrikePrice].values[0],
        lotsize = buy_option['Quantity'].values[0],
        buy_amount = abs(buy_option['Credit'].values[0]),
        index = index,
        diagnostic = diagnostic
      )
      
  def get_exit_option(self, data, symbol, expiry_date, trade_date, strike_price, lotsize, buy_amount, index = 0, diagnostic = DerivativesNonTradablesDiagnostic.ExitOptions()):
    hold_till_date = self.get_last_exit_date(trade_date)
    exit_options = data[
        (data[DerivativesBaseColumns.Identifier] == symbol)
        & (data[DerivativesBaseColumns.ExpiryDate] == expiry_date)
        & (data[DerivativesBaseColumns.StrikePrice] == strike_price)
        & (data[DerivativesBaseColumns.OptionType] == self._option_type)
        & (
            (data[DerivativesBaseColumns.Date].between(pd.to_datetime(trade_date) + datetime.timedelta(days=1), hold_till_date))
            |
            (diagnostic.skip_holding_days_check == True)
          )
        & (
            (data[DerivativesBaseColumns.OpenInterest] >= (data[DerivativesCalculatedColumns.LotSize] * self._trade_options.min_lots_oi))
            |
            (diagnostic.skip_oi_check == True)
          )
    ].sort_values(DerivativesBaseColumns.Date)

    if len(exit_options) > 0:
        exit_option = exit_options[(
            (exit_options[self._trade_options.exit_amount_profit_col] >= (buy_amount * self._trade_options.profit_factor))
            |
            (exit_options[self._trade_options.exit_amount_loss_col] <= (buy_amount * self._trade_options.loss_factor))
          )]
        
        sell_amount = 0

        if len(exit_option) <= 0:
            exit_option = exit_options.sort_values(DerivativesBaseColumns.Date, ascending=False)[0:1]
            sell_amount = exit_option[self._trade_options.exit_amount_close_col].values[0]
        else:
          high_amount = exit_option[self._trade_options.exit_amount_profit_col].values[0]
          low_amount = exit_option[self._trade_options.exit_amount_loss_col].values[0]
          
          if (high_amount >= (buy_amount * self._trade_options.profit_factor)):
            sell_amount = (buy_amount * self._trade_options.profit_factor)
          else:
            sell_amount = min(buy_amount * self._trade_options.loss_factor, high_amount)

        #if (high_amount >= (buy_amount * self._trade_options.profit_factor)):
        #    sell_amount = (buy_amount * self._trade_options.profit_factor)
        #else:
        #    sell_amount = (buy_amount * self._trade_options.loss_factor)

        sell = pd.DataFrame({
            DerivativesBaseColumns.Identifier: symbol,
            DerivativesBaseColumns.StrikePrice: strike_price,
            DerivativesBaseColumns.ExpiryDate: expiry_date,
            DerivativesBaseColumns.OptionType: self._option_type,
            DerivativesBaseColumns.Date: pd.to_datetime(exit_option[DerivativesBaseColumns.Date].values[0]),
            'Credit': sell_amount,
            'Quantity': -1 * lotsize,
            'Type': 'Sell'
        }, index=[(index*2)+1])
        return sell
    else:
        return []

class BearOptionsTradeBuilder(OptionsTradeBuilder):
  def __init__(self, trade_options):
     super().__init__(trade_options, 'PE')

class BullOptionsTradeBuilder(OptionsTradeBuilder):
  def __init__(self, trade_options):
     super().__init__(trade_options, 'CE')

class DerivatesMultiTradeOptions:
  avoid_overlap_trades: bool = True

class OptionsMultiTradesBuilder:
    def __init__(self, trade_options: DerivatesMultiTradeOptions):
        self.trade_options = trade_options

    def get_trades(self, symbol_signals, options_data, trade_builder: OptionsTradeBuilder):
        trades: pd.DataFrame = pd.DataFrame()
        full_trades: pd.DataFrame = pd.DataFrame()
        non_tradables: pd.DataFrame() = pd.DataFrame()

        symbols = symbol_signals[DerivativesBaseColumns.Identifier].unique()
        for symbol in symbols:
            symbol_options_data = options_data[options_data[DerivativesBaseColumns.Identifier] == symbol]
            for index, symbol_signal in symbol_signals[symbol_signals[DerivativesBaseColumns.Identifier] == symbol].iterrows():
                signal_date = pd.to_datetime(symbol_signal[DerivativesBaseColumns.Date])
                if len(trades) > 0:
                  symbol_outstanding_trade_quantity = trades[
                     (trades[DerivativesBaseColumns.Identifier] == symbol)
                     &
                     (trades[DerivativesBaseColumns.Date] <= signal_date)
                    ]['Quantity'].sum()
                else:
                   symbol_outstanding_trade_quantity = 0

                if (symbol_outstanding_trade_quantity == 0 or self.trade_options.avoid_overlap_trades == False):
                  symbol_options_data_for_dates = symbol_options_data[symbol_options_data[DerivativesBaseColumns.Date].between(signal_date, trade_builder.get_last_exit_date(signal_date))]
                  
                  buy_option = trade_builder.get_entry_option(symbol_options_data_for_dates, symbol, signal_date, index * 2)
                  
                  if len(buy_option) > 0:
                      sell_option = trade_builder.get_exit_option_for_buy_option(symbol_options_data_for_dates, buy_option, (index * 2) + 1)
                      
                      if len(sell_option) > 0:
                          trades = pd.concat([trades, buy_option, sell_option], ignore_index=True)
                          full_trades = pd.concat([full_trades, trade_builder.get_full_trade(buy_option, sell_option, index=[(index*2)])], ignore_index=True)
                      else:
                          non_tradables = pd.concat([non_tradables, pd.DataFrame({DerivativesBaseColumns.Identifier: symbol, DerivativesBaseColumns.Date: pd.to_datetime(signal_date), DerivativesBaseColumns.OptionType: trade_builder._option_type, 'reason': 'No Exit'}, index=[(index*2)])])
                  else:
                      non_tradables = pd.concat([non_tradables, pd.DataFrame({DerivativesBaseColumns.Identifier: symbol, DerivativesBaseColumns.Date: pd.to_datetime(signal_date), DerivativesBaseColumns.OptionType: trade_builder._option_type, 'reason': 'No Entry'}, index=[(index*2)+1])])
        
        result = TradesResult(trades, full_trades, non_tradables)
        result.prepare_summary()

        return result
    
class StrategyAnalyser:
    multi_trade_options: dict = {}
    trade_options: dict = {}
    results: TradesResultComposite
    output_folder: str = ''

    def __init__(self, output_sub_folder: str):
        self.results = TradesResultComposite()
        self.multi_trade_options = {}
        self.trade_options = {}
        self.output_folder = '../output/' + output_sub_folder
        
    def set_multi_trade_options(self, options: dict):
        self.multi_trade_options = options
        return self
    
    def set_trade_options(self, options: dict):
        self.trade_options = options
        return self

    def iterator(self, callback):
        for key1 in self.multi_trade_options:
            for key2 in self.trade_options:
              callback(key1, key2, self.multi_trade_options[key1], self.trade_options[key2])

    def load_from_files(self):
        for key1 in self.multi_trade_options:
            for key2 in self.trade_options:    
                result = BiDirectionalTradesResult.load_from_file(f'{self.output_folder}/{key1}-{key2}.xlsx')
                self.results.add_result(result, f'{key1}-{key2}')
        return self.results

    def analyze(self, options_data: pd.DataFrame, sell_signal_data_file: str = 'sell_signal_symbols.csv', buy_signal_data_file: str = 'buy_signal_symbols.csv'):
        sell_signal_data = pd.read_csv(f'{self.output_folder}/{sell_signal_data_file}')
        buy_signal_data = pd.read_csv(f'{self.output_folder}/{buy_signal_data_file}')

        sell_signal_data[DerivativesBaseColumns.Date] = pd.to_datetime(sell_signal_data[DerivativesBaseColumns.Date])
        buy_signal_data[DerivativesBaseColumns.Date] = pd.to_datetime(buy_signal_data[DerivativesBaseColumns.Date])

        sell_signal_symbols = sell_signal_data[DerivativesBaseColumns.Identifier].unique()
        buy_signal_symbols = buy_signal_data[DerivativesBaseColumns.Identifier].unique()

        all_symbols = np.unique(np.concatenate((sell_signal_symbols, buy_signal_symbols)))

        options_data = options_data[options_data[DerivativesBaseColumns.Identifier].isin(all_symbols)]

        self.results = TradesResultComposite()
        for key1 in self.multi_trade_options:
            multi_trade_builder = OptionsMultiTradesBuilder(self.multi_trade_options[key1])
            for key2 in self.trade_options:
                bear_trade_builder = BearOptionsTradeBuilder(self.trade_options[key2])
                bull_trade_builder = BullOptionsTradeBuilder(self.trade_options[key2])
                bear_trades = multi_trade_builder.get_trades(sell_signal_data, options_data, bear_trade_builder)
                bull_trades = multi_trade_builder.get_trades(buy_signal_data, options_data, bull_trade_builder)
                
                result = BiDirectionalTradesResult(bear_trades, bull_trades)
                result.save_result(f'{self.output_folder}/{key1}-{key2}.xlsx')
                self.results.add_result(result, f'{key1}-{key2}')
        #self.results.save_result(f'{self.output_folder}/final.xlsx')
        return self.results