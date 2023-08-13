  
from core.core import Instrumentation
from core.column_definition import BaseColumns, CalculatedColumns

class CalculationWorker:
  def add_calculated_columns(self, df):
    raise NotImplementedError("add_calculated_fields")

class CalculationPipeline:
  _pipeline: None
  
  def __init__(self):
    self._pipeline = []

  def add_calculation_worker(self, worker: CalculationWorker):
    self._pipeline.append(worker)

  def run(self, data):
    for worker in self._pipeline:
      result = worker.add_calculated_columns(data)
      if result is not None:
        data = result
    return data
  
class ValueCrosseAboveFlagWorker (CalculationWorker):
  def __init__(self, value_column, value):
    self._value_column = value_column
    self._value = value
    self._column_name = f'{value_column}CrossedAbove'

  @Instrumentation.trace(name="ValueCrossingAbove")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data[self._value_column].transform(lambda x: 
      (x.shift(-1) < self._value) & (x >= self._value)
    )

class ValueCrossedBelowFlagWorker (CalculationWorker):
  def __init__(self, value_column, value):
    self._value_column = value_column
    self._value = value
    self._column_name = f'{value_column}CrossedBelow'

  @Instrumentation.trace(name="ValueCrossingAbove")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data[self._value_column].transform(lambda x: 
      (x.shift(-1) > self._value) & (x <= self._value)
    )

class PriceCrosseAboveFlagWorker (CalculationWorker):
  def __init__(self, value_column):
    self._value_column = value_column
    self._column_name = f'PriceCrossedAbove{value_column}'

  @Instrumentation.trace(name="ValueCrossingAbove")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data.apply(lambda x: 
      (x[BaseColumns.Close].shift(-1) >= x[self._value_column]) & (x[BaseColumns.Close].shift(-1) < x[self._value_column])
    )

class PriceCrossedBelowFlagWorker (CalculationWorker):
  def __init__(self, value_column):
    self._value_column = value_column
    self._column_name = f'PriceCrossedBelow{value_column}'

  @Instrumentation.trace(name="ValueCrossingAbove")
  def add_calculated_columns(self, data):
    identifier_grouped_data = data.groupby(BaseColumns.Identifier)
    data[self._column_name] = identifier_grouped_data.apply(lambda x: 
      (x[BaseColumns.Close].shift(-1) <= x[self._value_column]) & (x[BaseColumns.Close].shift(-1) > x[self._value_column])
    )