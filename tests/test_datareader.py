import pytest
import pandas as pd

from helper import (
  setup, 
  check_base_cols_present,
  PresetDates
)

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
  check_base_cols_present(data, reader)
  assert data.shape[0] == rows

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