from markets_insights.core.column_definition import DerivativesBaseColumns, DerivativesCalculatedColumns
from markets_insights.core.core import TypeHelper
import pandas as pd
import datetime
import os

class TradesResultBaseMetrics:
  pnl: float = 0
  capital_required: float = 0
  no_of_trades: int = 0
  non_tradables_no_entry_count: int = 0
  non_tradables_no_exit_count: int = 0
  turnover: float = 0

class TradesResultMetrics (TradesResultBaseMetrics):
  transaction_fees: float = 0
  brokerage: float = 0
  total_expense: float = 0
  net_pnl: float = 0
  roi: float = 0
  break_even_date: datetime.datetime = None

class TradesResultBase (TradesResultMetrics):
  summary: pd.DataFrame

  def __init__(self):
    self.summary = None

  def calculate(self):
    self.transaction_fees = round(self.turnover * 0.01)
    self.brokerage = round(self.no_of_trades * 20)
    self.total_expense = round(self.transaction_fees + self.no_of_trades)
    self.net_pnl = round(self.pnl - self.total_expense)
    if self.capital_required > 0:
      self.roi = round(self.net_pnl / self.capital_required * 100)

  def prepare_summary_df(self):
    self.calculate()
    summary_dict: dict = {}
   
    props = TypeHelper.get_class_props(TradesResultMetrics)

    for prop in props:
      summary_dict[prop] = getattr(self, prop)

    self.summary = pd.DataFrame(summary_dict, index=[0])
  
  def __str__(self):
     return f'Net PnL: {round(self.net_pnl)}, Capital Required: {round(self.capital_required)}, ROI: {round(self.roi)}%, Non tradables (No Entry): {self.non_tradables_no_entry_count}, Non tradables (No Exit): {self.non_tradables_no_exit_count}'

class TradesResult(TradesResultBase):
    trades: pd.DataFrame
    non_tradables: pd.DataFrame
    full_trades: pd.DataFrame
    
    def __init__(self, trades, full_trades, non_tradables):
      super().__init__()
      self.trades = trades
      self.non_tradables = non_tradables
      self.full_trades = full_trades

    def caclulate_from_df(self):
      if len(self.trades) > 0:
        self.trades = self.trades.sort_values(DerivativesBaseColumns.Date).reset_index(drop=True)
        self.trades['CumCredit'] = self.trades['Credit'].cumsum()
        self.capital_required = round(abs(self.trades['CumCredit'].min()))
        self.no_of_trades = self.trades[DerivativesBaseColumns.Identifier].count()
        self.pnl = round(self.full_trades['Net'].sum())
        self.turnover = round(self.full_trades['Turnover'].sum())
        self.break_even_date = self.get_break_even_date()

      super().calculate()
      
    def get_break_even_date(self):
      break_even_date = self.trades[self.trades['CumCredit'] <= 0].tail(1)[DerivativesBaseColumns.Date].values[0]
      return pd.to_datetime(break_even_date)

    def prepare_summary(self):
      self.caclulate_from_df()
      
      if (len(self.non_tradables)) > 0:
        self.non_tradables_no_entry_count = self.non_tradables[self.non_tradables['reason'] == 'No Entry'][DerivativesBaseColumns.Identifier].count()
        self.non_tradables_no_exit_count = self.non_tradables[self.non_tradables['reason'] == 'No Exit'][DerivativesBaseColumns.Identifier].count()

      super().prepare_summary_df()
    
    def save_result(self, excel_writer: pd.ExcelWriter, suffix: str):
      if self.summary is not None:
        self.summary.to_excel(excel_writer, f'{suffix}-summary')
      
      self.trades.to_excel(excel_writer, f'{suffix}-trades')
      self.full_trades.to_excel(excel_writer, f'{suffix}-full-trades')
      self.non_tradables.to_excel(excel_writer, f'{suffix}-non-tradables')

    def load_from_file(file_path: str, suffix: str):
      trades = pd.read_excel(file_path, f'{suffix}-trades')
      full_trades = pd.read_excel(file_path, f'{suffix}-full-trades')
      non_tradables = pd.read_excel(file_path, f'{suffix}-non-tradables')
      instance = TradesResult(trades, full_trades, non_tradables)
      instance.prepare_summary()
      return instance
      
class TradesResultComposite(TradesResult):
  results: dict
  underlying_summaries: pd.DataFrame = None

  def __init__(self):
    super().__init__(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    self.results = {}
    self.underlying_summaries = pd.DataFrame()

  def add_result(self, result: TradesResult, key: str):
    self.results[key] = result
    
    if len(result.trades) > 0:
      result.trades['id'] = key
      self.trades = pd.concat([self.trades, result.trades], ignore_index=True)
    
    if len(result.non_tradables) > 0:
      result.non_tradables['id'] = key
      self.non_tradables = pd.concat([self.non_tradables, result.non_tradables], ignore_index=True)

    if len(result.full_trades) > 0:
      result.full_trades['id'] = key
      self.full_trades = pd.concat([self.full_trades, result.full_trades], ignore_index=True)

    if result.summary is not None and len(result.summary) > 0:
      result.summary['id'] = key
      self.underlying_summaries = pd.concat([self.underlying_summaries, result.summary], ignore_index=True)
  
    self.prepare_summary()

  def get_underlying_summaries(self):
    return self.underlying_summaries

  def save_result(self, path: str):
    excel_writer = pd.ExcelWriter(path)
    super().save_result(excel_writer, '')
    self.get_underlying_summaries().to_excel(excel_writer, 'underlying-summaries')
    excel_writer.close()

class BiDirectionalTradesResult(TradesResultComposite):
  bear_trades_result: TradesResult
  bull_trades_result: TradesResult

  def __init__(self, bear_trades_result: TradesResult, bull_trades_result: TradesResult):
    super().__init__()
    self.bear_trades_result = bear_trades_result
    self.bull_trades_result = bull_trades_result
    self.add_result(bear_trades_result, 'bear')
    self.add_result(bull_trades_result, 'bull')

  def load_from_file(base_path: str):
    bear_trades_result = TradesResult.load_from_file(base_path, 'bear')
    bull_trades_result = TradesResult.load_from_file(base_path, 'bull')
    return BiDirectionalTradesResult(bear_trades_result, bull_trades_result)

  def save_result(self, path):
    excel_writer = pd.ExcelWriter(path)
    self.summary.to_excel(excel_writer, f'full-summary')
    
    for key in self.results:
      result = self.results[key]
      result.save_result(excel_writer, key)

    excel_writer.close()