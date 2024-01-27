import pytest
import pandas as pd

from helper import (
  check_col_values,
  setup, 
  check_cols_present,
  check_base_cols_present,
  PresetDates
)
from markets_insights.core.column_definition import BaseColumns, DerivativesBaseColumns
from markets_insights.core.core import TypeHelper

setup()

import markets_insights

from markets_insights.datareader.data_reader import (
  DataReader,
  BhavCopyReader, NseIndicesReader, NseDerivatiesReader,
  MultiDatesDataReader,
  DateRangeDataReader
)

@pytest.mark.parametrize("reader,rows", [
  (BhavCopyReader(), 1797),
  (NseIndicesReader(), 107),
  (NseDerivatiesReader(), 12450)
])
def test_single_day_reader(reader: DataReader, rows: int):
  data = reader.read(PresetDates.dec_start)
  check_base_cols_present(data, reader.name)
  assert data.shape[0] == rows

def test_derivatives_data_reader_columns():
  data = NseDerivatiesReader().read(PresetDates.dec_start)
  check_cols_present(data, [col_name for col_name in TypeHelper.get_class_static_values(DerivativesBaseColumns)], "derivatives")

def test_multi_dates_reader():
  reader = MultiDatesDataReader(NseIndicesReader())
  data = reader.read([PresetDates.year_start, PresetDates.dec_end, PresetDates.dec_start])
  check_base_cols_present(data, reader)
  assert data.shape[0] == 319

def test_date_range_reader():
  reader = DateRangeDataReader(NseIndicesReader())
  data = reader.read(from_date=PresetDates.dec_start, to_date=PresetDates.dec_end)
  check_base_cols_present(data, reader)
  assert data.shape[0] == 2146

@pytest.mark.parametrize("reader,turnover", [
  (BhavCopyReader(), 19493616.1),
  (NseIndicesReader(), 224615300000.0)
])
def test_turnover_rescaled(reader: DataReader, turnover: float):
  data = reader.read(PresetDates.dec_start)
  check_col_values(
    data = data,
    col_value_pairs = {
      BaseColumns.Turnover: turnover
    }
  )