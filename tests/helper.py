import os
import sys
import datetime
import pytest
import pandas as pd


test_year = 2023

def setup():
  code_dir = os.path.join(os.path.abspath('..'), 'src')
  sys.path.append(code_dir)
  from _pytest.assertion import truncate
  truncate.DEFAULT_MAX_LINES = 9999
  truncate.DEFAULT_MAX_CHARS = 9999  

setup()

from markets_insights.core.column_definition import BaseColumns, BasePriceColumns
from markets_insights.datareader.data_reader import DataReader
from markets_insights.core.core import DateFilter, DatePartsFilter, IdentifierFilter, TypeHelper, MarketDaysHelper

class PresetDates:
  year_start: datetime.datetime = MarketDaysHelper.get_this_or_next_market_day(datetime.date(year=test_year, month=1, day=1))
  year_end: datetime.datetime = MarketDaysHelper.get_this_or_previous_market_day(datetime.date(year=test_year, month=12, day=31))
  dec_start: datetime.datetime = MarketDaysHelper.get_this_or_next_market_day(datetime.date(year=test_year, month=12, day=1))
  dec_end: datetime.datetime = year_end
  q4_start: datetime.datetime = MarketDaysHelper.get_this_or_next_market_day(datetime.date(year=test_year, month=10, day=1))
  q4_end: datetime.datetime = year_end
  recent_day: datetime.datetime = MarketDaysHelper.get_this_or_previous_market_day(datetime.date.today() - datetime.timedelta(days=1))
  
class PresetFilters:
  nifty50 = IdentifierFilter("NIFTY 50")
  date_year_end = DateFilter(PresetDates.year_end)
  year = DatePartsFilter(year=2023)

class Presets:
  cols_to_test = [
    BaseColumns.Identifier,
    BaseColumns.Date,
    BaseColumns.Close,
    BaseColumns.Open,
    BaseColumns.High,
    BaseColumns.Turnover,
    BaseColumns.Volume
  ]
  filters = PresetFilters
  dates = PresetDates

def check_cols_present(data: pd.DataFrame, cols_to_test: [int], case_identifier: str):
  for col_name in cols_to_test:
    if not col_name in data.columns:
      assert pytest.fail(f"column {col_name} not present in {case_identifier} data")

def check_base_cols_present(data: pd.DataFrame, case_identifier: str):
  check_cols_present(data, Presets.cols_to_test, case_identifier)

def check_price_cols_present(data: pd.DataFrame, period: str):
  check_cols_present(
                      data, 
                      [col_template.substitute({"period": period}) for col_template in TypeHelper.get_class_static_values(BasePriceColumns)],
                      period
                    )

def check_col_values(data: pd.DataFrame, col_value_pairs: dict):
  for col_name in col_value_pairs:
    if not float(data[col_name].values[0]) == pytest.approx(col_value_pairs[col_name]):
      assert pytest.fail(f"column `{col_name}` value check: {data[col_name].values[0]} <> {col_value_pairs[col_name]}")