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
from markets_insights.core.core import IdentifierFilter, InstrumentTypeFilter, TypeHelper

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

@pytest.mark.parametrize("reader", [
  BhavCopyReader(),
  NseIndicesReader(),
  NseDerivatiesReader()
])
def test_recent_day_read(reader: DataReader):
  data = reader.read(PresetDates.recent_day)
  check_base_cols_present(data, reader.name)
  assert data.shape[0] > 1

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

@pytest.mark.parametrize("operation,close", [
  (lambda x, y: x + y, 4801.4),
  (lambda x, y: x - y, 12.799999999999727),
  (lambda x, y: x / y, 1.0053460301549513),
  (lambda x, y: x * y, 5763319.53)
])
def test_arithmatic_op(operation, close):
  l_reader = NseDerivatiesReader().set_filter(InstrumentTypeFilter('FUTSTK'))
  r_reader = BhavCopyReader()
  op_reader = operation(l_reader, r_reader)
  data = op_reader.read(for_date = PresetDates.dec_start).query(str(IdentifierFilter("RELIANCE")))
  check_col_values(
    data,
    col_value_pairs = {
      BaseColumns.Close: close
    }
  )
  assert f"{l_reader.col_prefix}{BaseColumns.Close}" in data.columns
  assert f"{r_reader.col_prefix}{BaseColumns.Close}" in data.columns

@pytest.mark.parametrize("operation,close", [
  (lambda x, y: x + y, 20280.280000000002),
  (lambda x, y: x - y, 20255.52),
  (lambda x, y: x / y, 1637.1486268174476),
  (lambda x, y: x * y, 250916.60200000004)
])
def test_arithmatic_op_single_id(operation, close):
  indices_reader = NseIndicesReader()
  vix_reader = NseIndicesReader().set_filter(IdentifierFilter("India VIX"))
  op_reader = operation(indices_reader, vix_reader)
  data = op_reader.read(for_date = PresetDates.dec_start).query(str(IdentifierFilter("Nifty 50")))
  check_col_values(
    data,
    col_value_pairs = {
      BaseColumns.Close: close
    }
  )
  assert f"{indices_reader.col_prefix}{BaseColumns.Close}" in data.columns
  assert f"India VIX-{BaseColumns.Close}" in data.columns

def test_arithmatic_multiple_ops():
  eq_reader = BhavCopyReader()
  fut_reader = NseDerivatiesReader().set_filter(InstrumentTypeFilter("FUTSTK"))
  op_reader = (fut_reader - eq_reader)
  data = op_reader.read(for_date = PresetDates.dec_start).query(str(IdentifierFilter("RELIANCE"))).sort_values(DerivativesBaseColumns.ExpiryDate)
  check_col_values(
    data,
    col_value_pairs = {
      BaseColumns.Close: 29.0
    }
  )
  assert f"{eq_reader.col_prefix}{BaseColumns.Close}" in data.columns
  assert f"{fut_reader.col_prefix}{BaseColumns.Close}" in data.columns