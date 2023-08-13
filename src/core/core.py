from core.settings import MarketDaysSettings
from core.environment import EnvironmentSettings
from datetime import date
import time
import pandas as pd
from dateutil.relativedelta import relativedelta
import calendar

Weekends = ['Saturday', 'Sunday']

class TypeHelper:
  def get_class_props(c_type):
    return [x for x in dir(c_type) if x.startswith('__') == False and type(getattr(c_type, x)) != 'function']

class InstrumentationType:
  Info = 1
  Trace = 2
  Debug = 4

InstrumentationLevel = EnvironmentSettings.Development['InstrumentationLevel']

class Instrumentation:
  
  def debug(message):
    if InstrumentationLevel & InstrumentationType.Debug == InstrumentationType.Debug:
      print(message)
  
  def info(message):
    if InstrumentationLevel & InstrumentationType.Info == InstrumentationType.Info:
      print(message)
  
  def startTracing(name):
    if InstrumentationLevel & InstrumentationType.Trace == InstrumentationType.Trace:
      return Trace(name)
    else:
      return NoTrace(name)
  
  def trace(name):
    def decorator(function):
        def wrapper(*args, **kwargs):
            tracer = Instrumentation.startTracing(name)
            result = function(*args, **kwargs)
            tracer.endTracing()
            return result
        return wrapper
    return decorator

class Trace:
  def __init__(self, name):
    self.startTime = time.time()
    self.name = name

  def endTracing(self):
    print(f"{self.name} took {round(time.time() - self.startTime)} seconds")

class NoTrace:
  def __init__(self, name):
    self.name = name

  def endTracing(self):
    return

class MarketDaysHelper:
  def is_open_for_day(for_date):
    day = for_date.strftime("%A")
    if type(for_date) == 'pandas._libs.tslibs.timestamps.Timestamp':
      for_date = for_date.date()
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
  
  def get_next_market_day(for_date):
    current_date = for_date + pd.Timedelta(days=1)
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
  
  def get_previous_market_day(for_date):
    current_date = for_date - pd.Timedelta(days=1)
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
  
  def get_monthly_expiry_dates(no_of_recent_expiries):
    expiries = []
    today = date.today()
    for i in range(1, no_of_recent_expiries):
      cur_month = date(today.year, today.month, 1) - relativedelta(months=i)
      monthly_expiry = MarketDaysHelper.get_this_or_previous_market_day(MarketDaysHelper.get_last_thursday(cur_month.year, cur_month.month))
      expiries.append(monthly_expiry)
    return expiries
  
  def get_last_thursday(year, month):
    cal = calendar.Calendar(firstweekday=calendar.MONDAY)
    thursday_number = -1
    monthcal = cal.monthdatescalendar(year, month)
    last_thursday = [day for week in monthcal for day in week if \
        day.weekday() == calendar.THURSDAY and \
        day.month == month][thursday_number]
    return last_thursday