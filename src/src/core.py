from settings import MarketDaysSettings
from environment import EnvironmentSettings
from datetime import date
import time
import pandas as pd

Weekends = ['Saturday', 'Sunday']

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