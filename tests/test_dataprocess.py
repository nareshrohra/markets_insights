import pytest
import pandas as pd

from helper import (
  check_col_values,
  setup, 
  check_base_cols_present,
  PresetDates,
  Presets
)
from markets_insights.core.core import DateFilter, DatePartsFilter, IdentifierFilter

setup()

import markets_insights
from markets_insights.core.column_definition import AggregationPeriods, BaseColumns, CalculatedColumns
from markets_insights.datareader.data_reader import NseIndicesReader
from markets_insights.dataprocess.data_processor import (
  HistoricalDataProcessOptions,
  HistoricalDataProcessor,
  HistoricalDataset
)

filter_nifty50 = IdentifierFilter("NIFTY 50")
filter_date_year_end = DateFilter(PresetDates.year_end)
filter_year = DatePartsFilter(year=2023)

def test_historical_data_processor_without_options():
  processor = HistoricalDataProcessor()
  result: HistoricalDataset = processor.process(NseIndicesReader(), {
    'from_date': PresetDates.dec_start,
    'to_date': PresetDates.dec_end
  })
  check_base_cols_present(result.get_daily_data(), "Daily")
  assert result.get_daily_data().shape[0] == 2146

  check_base_cols_present(result.get_monthly_data(), AggregationPeriods.Monthly)
  assert result.get_monthly_data().shape[0] == 108

  check_base_cols_present(result.get_annual_data(), AggregationPeriods.Annual)
  assert result.get_monthly_data().shape[0] == 108
  

@pytest.mark.parametrize("options", [
  HistoricalDataProcessOptions(include_monthly_data=False),
  HistoricalDataProcessOptions(include_annual_data=False),
  HistoricalDataProcessOptions(include_monthly_data=False, include_annual_data=False)
])
def test_historical_data_processor_with_options(options: HistoricalDataProcessOptions):
  processor = HistoricalDataProcessor(options)
  result = processor.process(NseIndicesReader(), {
    'from_date': PresetDates.dec_start,
    'to_date': PresetDates.dec_end
  })
  check_base_cols_present(result.get_daily_data(), "Daily")

  if options.include_monthly_data:
    check_base_cols_present(result.get_monthly_data(), AggregationPeriods.Monthly)
  else:
    assert result.get_monthly_data() is None

  if options.include_annual_data:
    check_base_cols_present(result.get_annual_data(), AggregationPeriods.Annual)
  else:
    assert result.get_annual_data() is None

def test_historical_data_processor_monthly_aggregration():
  processor = HistoricalDataProcessor()
  result = processor.process(NseIndicesReader(), {
    'from_date': PresetDates.dec_start,
    'to_date': PresetDates.dec_end
  })

  check_col_values(
    data = result.get_monthly_data().query(str(filter_nifty50 & filter_date_year_end)),
    col_value_pairs = {
      BaseColumns.Open: 20194.1,
      BaseColumns.High: 21801.45,
      BaseColumns.Low: 20183.7,
      BaseColumns.Close: 21731.4,
      BaseColumns.Turnover: 23697.88
    }
  )

def test_historical_data_processor_annual_aggregration():
  processor = HistoricalDataProcessor()
  result = processor.process(NseIndicesReader(), {
    'from_date': PresetDates.year_start,
    'to_date': PresetDates.year_end
  })

  check_col_values(
    data = result.get_annual_data().query(str(filter_nifty50 & filter_year)),
    col_value_pairs = {
      BaseColumns.Open: 	18131.7,
      BaseColumns.High: 21801.45,
      BaseColumns.Low: 16828.35,
      BaseColumns.Close: 21731.4,
      BaseColumns.Turnover: 23697.88
    }
  )