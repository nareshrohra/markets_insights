from market_insights.core.column_definition import BaseColumns, CalculatedColumns, DerivativesBaseColumns, DerivativesCalculatedColumns
from market_insights.core.core import Instrumentation
from market_insights.calculations.base import CalculationWorker
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from market_insights.datareader.data_reader import NseDerivatiesOldReader
from market_insights.core.core import MarketDaysHelper

class LowestPriceInNextNDaysCalculationWorker (CalculationWorker):
  def __init__(self, time_window):
    self._time_window = time_window
    self._val_column_name = f'LowestPriceInNext{str(self._time_window)}Days'
    self._perc_column_name = f'HighestPercFallInNext{str(self._time_window)}Days'

  @Instrumentation.trace(name="LowestPriceInNextNDaysCalculationWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._val_column_name] = identifier_grouped_data[BaseColumns.Low].transform(lambda x: x.rolling(self._time_window).min().shift(-self._time_window))
    data[self._perc_column_name] = (data[BaseColumns.Close] - data[self._val_column_name]) / data[BaseColumns.Close] * 100

class HighestPriceInNextNDaysCalculationWorker (CalculationWorker):
  def __init__(self, time_window):
    self._time_window = time_window
    self._val_column_name = f'HighestPriceInNext{str(self._time_window)}Days'
    self._perc_column_name = f'HighestPercRiseInNext{str(self._time_window)}Days'

  @Instrumentation.trace(name="HighestPriceInNextNDaysCalculationWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._val_column_name] = identifier_grouped_data[BaseColumns.High].transform(lambda x: x.rolling(self._time_window).max().shift(-self._time_window))
    data[self._perc_column_name] = (data[self._val_column_name] - data[BaseColumns.Close]) / data[BaseColumns.Close] * 100

class IsInDerivativesFlagCalculationWorker (CalculationWorker):
  @Instrumentation.trace(name="IsInDerivativesFlagCalculationWorker")
  def add_calculated_columns(self, data):
    from_date = data[BaseColumns.Date].min()
    to_date = data[BaseColumns.Date].max()

    all_symbols = pd.DataFrame()
    current_date = datetime(from_date.year, from_date.month, 1)
    derivatives_reader = NseDerivatiesOldReader()
    
    while current_date <= to_date:
      derivatives_symbols = derivatives_reader.read(MarketDaysHelper.get_this_or_next_market_day(current_date))[[DerivativesBaseColumns.Identifier, DerivativesBaseColumns.Date]]
      derivatives_symbols[DerivativesCalculatedColumns.MonthNo] = current_date.month
      derivatives_symbols[DerivativesCalculatedColumns.Year] = current_date.year
      derivatives_symbols.drop_duplicates(inplace=True)
      all_symbols = pd.concat([all_symbols, derivatives_symbols], ignore_index=True)
      current_date = current_date + relativedelta(months=+1)

      all_symbols.drop_duplicates(inplace=True)
      all_symbols.drop(columns=[BaseColumns.Date], inplace=True)
      all_symbols[CalculatedColumns.IsInDerivatives] = True
    
    data = pd.merge(data, all_symbols, how='left', on=[BaseColumns.Identifier, CalculatedColumns.Year, CalculatedColumns.MonthNo])
    return data