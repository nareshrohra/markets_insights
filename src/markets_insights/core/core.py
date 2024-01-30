from markets_insights.core.column_definition import BaseColumns, CalculatedColumns, CalculatedColumnsBase, DerivativesBaseColumns
from markets_insights.core.settings import MarketDaysSettings
from markets_insights.core.environment import EnvironmentSettings
from datetime import date
import time
import pandas as pd
from dateutil.relativedelta import relativedelta
import calendar

Weekends = ['Saturday', 'Sunday']

class TypeHelper:
  def get_class_props(c_type):
    return [x for x in dir(c_type) if x.startswith('__') == False and type(getattr(c_type, x)) != 'function']
  
  def get_class_static_values(c_type):
    return [getattr(c_type, x) for x in dir(c_type) if x.startswith('__') == False and type(getattr(c_type, x)) != 'function']

  def get_class_props_for_value(c_type, val):
    props = TypeHelper.get_class_props(c_type)
    return [prop for prop in props if val in getattr(c_type, prop)]

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
    if hasattr(for_date, 'date'):
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

class FilterCriteria:
  _col_to_filter: str = None
  _condition: str = None
  _condition_value = None
  
  def __init__(self, col_to_filter: str = None, condition: str = None, condition_value = None, ):
    self._col_to_filter = col_to_filter
    self._condition = condition
    self._condition_value = condition_value

  def __str__(self) -> str:
      return self.get_query()

  def get_query(self) -> str:
      return f"`{self._col_to_filter}` {self._condition} {self._condition_value}"

class FilterBase:
  _filter_criterias: [FilterCriteria]

  def __init__(self):
    self._filter_criterias = []
    
  def add_criteria(self, criteria: FilterCriteria):
    self._filter_criterias.append(criteria)

  def __str__(self) -> str:
      return f"{self.get_query()}"

  def get_query(self) -> str:
      return " & ".join(
            [
                f"{k.get_query()}"
                for k in self._filter_criterias
            ]
        )

  def __and__(self, other: object):
    new_filter = FilterBase()
    for criteria in self._filter_criterias:
      new_filter.add_criteria(criteria)
    for criteria in other._filter_criterias:
      new_filter.add_criteria(criteria)
    return new_filter

class IdentifierFilter(FilterBase):
  def __init__(self, identifier: str):
    super().__init__()
    self.add_criteria(FilterCriteria(
        col_to_filter = BaseColumns.Identifier,
        condition = "==",
        condition_value = f"'{identifier}'"
      )
    )

class InstrumentTypeFilter(FilterBase):
  def __init__(self, identifier: str):
    super().__init__()
    self.add_criteria(FilterCriteria(
        col_to_filter = DerivativesBaseColumns.InstrumentType,
        condition = "==",
        condition_value = f"'{identifier}'"
      )
    )

class DateFilter(FilterBase):
  def __init__(self, for_date: date = None):
    super().__init__()
    self.add_criteria(FilterCriteria(
        col_to_filter = BaseColumns.Date,
        condition = "==",
        condition_value = f"'{for_date}'"
      )
    )
  
class DateRangeFilter(FilterBase):
  def __init__(self, from_date: date = None, to_date: date = None):
    super().__init__()
    if from_date is not None:
      self.add_criteria(FilterCriteria(
          col_to_filter = BaseColumns.Date,
          condition = ">=",
          condition_value = f"'{from_date}'"
        )
      )

    if to_date is not None:
      self.add_criteria(FilterCriteria(
          col_to_filter=BaseColumns.Date,
          condition = "<=",
          condition_value = f"'{to_date}'"
        )
      )

class DatePartsFilter(FilterBase):
  def __init__(self, month_no: int = None, year: int = None):
    super().__init__()
    if month_no is not None:
      self.add_criteria(FilterCriteria(
          col_to_filter = CalculatedColumns.MonthNo,
          condition = "==",
          condition_value = month_no
        )
      )
    if year is not None:
      self.add_criteria(FilterCriteria(
          col_to_filter=CalculatedColumns.Year,
          condition = "==",
          condition_value = year
        )
      )