import os
import sys
import datetime
import pytest
import pandas as pd


test_year = 2023

def setup():
  code_dir = os.path.join(os.path.abspath('..'), 'src')
  sys.path.append(code_dir)

setup()

from markets_insights.core.column_definition import BaseColumns, BasePriceColumns
from markets_insights.datareader.data_reader import DataReader
from markets_insights.core.core import TypeHelper, MarketDaysHelper

class Presets:
  cols_to_test = [
    BaseColumns.Identifier,
    BaseColumns.Date,
    BaseColumns.Close,
    BaseColumns.Open,
    BaseColumns.High
  ]

class PresetDates:
  year_start: datetime.datetime = MarketDaysHelper.get_this_or_next_market_day(datetime.date(year=test_year, month=1, day=1))
  dec_start: datetime.datetime = MarketDaysHelper.get_this_or_next_market_day(datetime.date(year=test_year, month=12, day=1))
  dec_end: datetime.datetime = MarketDaysHelper.get_this_or_previous_market_day(datetime.date(year=test_year, month=12, day=31))
  year_end = dec_end

def check_base_cols_present(data: pd.DataFrame, case_identifier: str):
  for col_name in Presets.cols_to_test:
    if not col_name in data.columns:
      assert pytest.fail(f"column {col_name} not present in {case_identifier} data")

def check_price_cols_present(data: pd.DataFrame, period: str):
  for col_name in [col_template.substitute({"period": period}) for col_template in TypeHelper.get_class_static_values(BasePriceColumns)]:
    if not col_name in data.columns:
      assert pytest.fail(f"column {col_name} not present in {period} data")

def check_col_values(data: pd.DataFrame, col_value_pairs: dict):
  for col_name in col_value_pairs:
    if not float(data[col_name].values[0]) == float(col_value_pairs[col_name]):
      assert pytest.fail(f"column `{col_name}` value check: {data[col_name].values[0]} <> {col_value_pairs[col_name]}")