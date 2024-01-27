  
from markets_insights.core.core import Instrumentation
from markets_insights.core.column_definition import BaseColumns, CalculatedColumns
import pandas as pd
import pandas_ta as ta

class CalculationWorker:
  def add_calculated_columns(self, df):
    raise NotImplementedError("add_calculated_fields")

class CalculationPipeline:
  _pipeline: list
  
  def __init__(self, workers = None):
    if workers is not None:
      self._pipeline = workers
    else:
      self._pipeline = []

  def add_calculation_worker(self, worker: CalculationWorker):
    self._pipeline.append(worker)

  def run(self, data):
    for worker in self._pipeline:
      result = worker.add_calculated_columns(data)
      if result is not None:
        data = result
    return data
  
class ValueCrossedAboveFlagWorker (CalculationWorker):
  def __init__(self, value_column, value):
    self._value_column = value_column
    self._value = value
    self._column_name = f'{value_column}CrossedAbove'

  @Instrumentation.trace(name="ValueCrossedAboveFlagWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data[self._value_column].transform(lambda x: 
      (x.shift(1) < self._value) & (x >= self._value)
    )

class ValueCrossedBelowFlagWorker (CalculationWorker):
  def __init__(self, value_column, value):
    self._value_column = value_column
    self._value = value
    self._column_name = f'{value_column}CrossedBelow'

  @Instrumentation.trace(name="ValueCrossedBelowFlagWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data[self._value_column].transform(lambda x: 
      (x.shift(-1) > self._value) & (x <= self._value)
    )

class PriceCrossedAboveValueFlagWorker (CalculationWorker):
  def __init__(self, value_column):
    self._value_column = value_column
    self._column_name = f'PriceCrossedAbove{value_column}'

  @Instrumentation.trace(name="PriceCrossedAboveValueFlagWorker")
  def add_calculated_columns(self, data):
    data[self._column_name] = (data[BaseColumns.Close] >= data[self._value_column]) & (data[BaseColumns.PreviousClose] < data[self._value_column])

class PriceCrossedBelowValueFlagWorker (CalculationWorker):
  def __init__(self, value_column):
    self._value_column = value_column
    self._column_name = f'PriceCrossedBelow{value_column}'

  @Instrumentation.trace(name="PriceCrossedBelowValueFlagWorker")
  def add_calculated_columns(self, data):
    data[self._column_name] = (data[BaseColumns.Close] < data[self._value_column]) & (data[BaseColumns.PreviousClose] >= data[self._value_column])

class DatePartsCalculationWorker (CalculationWorker):
  @Instrumentation.trace(name="DatePartsCalculationWorker")
  def add_calculated_columns(self, data):
    data[CalculatedColumns.Year] = data[BaseColumns.Date].dt.year
    data[CalculatedColumns.MonthNo] = data[BaseColumns.Date].dt.month
    data[CalculatedColumns.Month] = data[BaseColumns.Date].dt.strftime('%b')
    data[CalculatedColumns.Day] = data[BaseColumns.Date].dt.strftime('%A')

class SmaCalculationWorker (CalculationWorker):
  def __init__(self, time_window):
    self._time_window = time_window
    self._column_name = 'Sma' + str(self._time_window)

  @Instrumentation.trace(name="SMACalculationWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data[BaseColumns.Close].transform(
        lambda x: x.rolling(self._time_window).mean()
      )
    
class StdDevCalculationWorker (CalculationWorker):
  def __init__(self, time_window: int = 200):
    self._time_window = time_window
    self._column_name = 'StdDev' + str(self._time_window)

  @Instrumentation.trace(name="StdDevCalculationWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data[BaseColumns.Close].transform(
        lambda x: x.rolling(self._time_window).std()
      )
    
class BollingerBandCalculationWorker (CalculationWorker):
  def __init__(self, time_window: int = 200, deviation: int = 2):
    self._time_window = time_window
    self._deviation = deviation
    self._column_name_upper_band = 'Bb' + str(self._time_window) + 'Dev' + str(deviation) + 'Upper'
    self._column_name_lower_band = 'Bb' + str(self._time_window) + 'Dev' + str(deviation) + 'Lower'

  @Instrumentation.trace(name="StdDevCalculationWorker")
  def add_calculated_columns(self, data):
    data[self._column_name_upper_band] = data['Sma' + str(self._time_window)] + (data['StdDev' + str(self._time_window)] * self._deviation)
    data[self._column_name_lower_band] = data['Sma' + str(self._time_window)] - (data['StdDev' + str(self._time_window)] * self._deviation)
    
class RsiOldCalculationWorker(CalculationWorker):
  def __init__(self, time_window):
    self._time_window = time_window
    self._column_name = 'Rsi' + str(self._time_window)

  def calculate_wsm_average(self, raw_data, data, avg_col_name, abs_col_name):
    step = 1
    for i, row in enumerate(data[avg_col_name].iloc[self._time_window + step:]):
      raw_data.at[data.index[i + self._time_window + step], avg_col_name] =\
        (data[avg_col_name].iloc[i + self._time_window] *
        (self._time_window - step) +
        data[abs_col_name].iloc[i + self._time_window + step])\
        / self._time_window

  @Instrumentation.trace(name="RsiOldCalculationWorker")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[CalculatedColumns.ClosePriceDiff] = identifier_grouped_data[BaseColumns.Close].transform(lambda x: x.diff(1))
    data[CalculatedColumns.Gain] = identifier_grouped_data[CalculatedColumns.ClosePriceDiff].transform(lambda x: x.clip(lower=0).round(2))
    data[CalculatedColumns.Loss] = identifier_grouped_data[CalculatedColumns.ClosePriceDiff].transform(lambda x: x.clip(upper=0).abs().round(2))

    data[CalculatedColumns.ClosePriceDiff].fillna(0)
    data[CalculatedColumns.Gain].fillna(0)
    data[CalculatedColumns.Loss].fillna(0)

    # Get initial Averages
    data[CalculatedColumns.AvgGain] = identifier_grouped_data[CalculatedColumns.Gain].transform(lambda x: 
                                        x.rolling(window=self._time_window, min_periods=self._time_window).mean())
    data[CalculatedColumns.AvgLoss] = identifier_grouped_data[CalculatedColumns.Loss].transform(lambda x: 
                                        x.rolling(window=self._time_window, min_periods=self._time_window).mean())
    
    for identifier in data[BaseColumns.Identifier].unique():
      self.calculate_wsm_average(data, identifier_grouped_data.get_group(identifier), CalculatedColumns.AvgGain, CalculatedColumns.Gain) 
      self.calculate_wsm_average(data, identifier_grouped_data.get_group(identifier), CalculatedColumns.AvgLoss, CalculatedColumns.Loss)
    
    data[CalculatedColumns.RelativeStrength] = data[CalculatedColumns.AvgGain] / data[CalculatedColumns.AvgLoss]
    data[CalculatedColumns.RelativeStrengthIndex] = 100 - (100 / (1.0 + data[CalculatedColumns.RelativeStrength]))

class RsiCalculationWorker(CalculationWorker):
  def __init__(self, time_window):
    self._time_window = time_window
    self._column_name = 'Rsi' + str(self._time_window)

  def calculate_rsi(self, group):
    group[CalculatedColumns.RelativeStrengthIndex] = ta.rsi(group[BaseColumns.Close])
    return group

  @Instrumentation.trace(name="RsiCalculationWorker")
  def add_calculated_columns(self, data):
    result = data.groupby([BaseColumns.Identifier], group_keys=True).apply(self.calculate_rsi)
    return result.reset_index(drop=True)
  
class StochRsiCalculationWorker(CalculationWorker):
  def __init__(self, time_window: int = 14):
    self._time_window = time_window

  def calculate_stoch_rsi(self, group: pd.DataFrame):
    data = ta.stochrsi(group[BaseColumns.Close], window=14, smooth1=3, smooth2=3)
    if data is not None:
      group[CalculatedColumns.StochRsi_K] = data['STOCHRSIk_14_14_3_3']
      group[CalculatedColumns.StochRsi_D] = data['STOCHRSId_14_14_3_3']
    return group

  @Instrumentation.trace(name="StochRsiCalculationWorker")
  def add_calculated_columns(self, data):
    result = data.groupby([BaseColumns.Identifier], group_keys=True).apply(self.calculate_stoch_rsi)
    return result.reset_index(drop=True)

class VwapCalculationWorker (CalculationWorker):
  def __init__(self, time_window):
    self._time_window = time_window

  @Instrumentation.trace(name="VwapCalculationWorker")
  def add_calculated_columns(self, data):
    data[BaseColumns.Turnover] = data[BaseColumns.Turnover].replace('-', 0)
    data[BaseColumns.Volume] = data[BaseColumns.Volume].replace('-', 0)
    data[CalculatedColumns.Vwap] = data.groupby(BaseColumns.Identifier).apply(lambda x: 
      x[BaseColumns.Turnover].rolling(self._time_window).sum() / x.rolling(self._time_window)[BaseColumns.Volume].sum()
    ).reset_index(level=0, drop=True)

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