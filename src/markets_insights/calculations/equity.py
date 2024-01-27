from markets_insights.core.column_definition import BaseColumns, CalculatedColumns, DerivativesBaseColumns, DerivativesCalculatedColumns
from markets_insights.core.core import Instrumentation
from markets_insights.calculations.base import CalculationWorker
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from markets_insights.datareader.data_reader import NseDerivatiesOldReader
from markets_insights.core.core import MarketDaysHelper

class IsInDerivativesFlagCalculationWorker (CalculationWorker):
  @Instrumentation.trace(name="IsInDerivativesFlagCalculationWorker")
  def add_calculated_columns(self, data):
    from_date = data[BaseColumns.Date].min()
    to_date = data[BaseColumns.Date].max()

    all_symbols = pd.DataFrame()
    current_date = datetime(from_date.year, from_date.month, 1)
    derivatives_reader = NseDerivatiesOldReader()
    
    while current_date <= to_date:
      derivatives_symbols = derivatives_reader.read(MarketDaysHelper.get_this_or_next_market_day(current_date))[[DerivativesBaseColumns.Identifier, DerivativesBaseColumns.Date]]
      derivatives_symbols[DerivativesCalculatedColumns.MonthNo] = current_date.month
      derivatives_symbols[DerivativesCalculatedColumns.Year] = current_date.year
      derivatives_symbols.drop_duplicates(inplace=True)
      all_symbols = pd.concat([all_symbols, derivatives_symbols], ignore_index=True)
      current_date = current_date + relativedelta(months=+1)

      all_symbols.drop_duplicates(inplace=True)
      all_symbols.drop(columns=[BaseColumns.Date], inplace=True)
      all_symbols[CalculatedColumns.IsInDerivatives] = True
    
    data = pd.merge(data, all_symbols, how='left', on=[BaseColumns.Identifier, CalculatedColumns.Year, CalculatedColumns.MonthNo])
    return data