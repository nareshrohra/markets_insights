from core.column_definition import BaseColumns, CalculatedColumns
from core.core import Instrumentation
from calculations.base import CalculationWorker, CalculationPipeline
import pandas as pd

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