from settings import MarketDaysSettings
from datetime import date
import pandas as pd

Weekends = ['Saturday', 'Sunday']

class MarketDaysHelper:
  def is_open_for_day(for_date):
    day = for_date.strftime("%A")
    return not (day in Weekends or for_date in MarketDaysSettings.MarketHolidays)
  
  def get_this_or_next_market_day(for_date):
    current_date = for_date
    i = 1
    while i < 5:
      if not MarketDaysHelper.is_open_for_day(current_date):
        current_date = current_date + pd.Timedelta(days=1)
      else:
        return current_date
      i = i + 1
    return current_date
  
  def get_this_or_previous_market_day(for_date):
    current_date = for_date
    i = 1
    while i < 5:
      if not MarketDaysHelper.is_open_for_day(current_date):
        current_date = current_date - pd.Timedelta(days=1)
      else:
        return current_date
      i = i + 1
    return current_date
  
  def get_days_list_for_range(from_date, to_date):
    return pd.date_range(from_date, to_date, freq='B').tolist()