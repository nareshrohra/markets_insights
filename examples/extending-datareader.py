import os
import sys
code_dir = os.path.join(os.path.abspath('.'), 'src')
os.chdir(code_dir)
sys.path.append(code_dir)

import datetime

import markets_insights

from markets_insights.datareader.data_reader import DateRangeDataReader
from markets_insights.core.core import Instrumentation
from markets_insights.core.column_definition import BaseColumns

import yfinance as yf
import pandas

class NasdaqDataReader (DateRangeDataReader):
  def __init__(self, tickers: list = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'NVDA']):
    super().__init__(reader=None)
    self.tickers = tickers
    self.name = "NasdaqDataReader"

  @Instrumentation.trace(name="NasdaqDataReader.read")
  def read(self, from_date, to_date):
    df_list = list()
    for ticker in self.tickers:
        data = yf.download(ticker, group_by="Ticker", start=from_date, end=to_date)
        data['ticker'] = ticker
        df_list.append(data)

    # combine all dataframes into a single dataframe
    df = pandas.concat(df_list)

    data = df.reset_index().rename(columns = self.get_column_name_mappings())
    data[BaseColumns.Date] = pandas.to_datetime(data[BaseColumns.Date])
    return data
  
  def get_column_name_mappings(self):
    return {
      'ticker': BaseColumns.Identifier,
      'OPEN': BaseColumns.Open,
      'HIGH': BaseColumns.High,
      'LOW': BaseColumns.Low,
      'CLOSE': BaseColumns.Close
    }
  
# import classes & setup options
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, MultiDataCalculationPipelines, CalculationPipelineBuilder, HistoricalDataProcessOptions
from markets_insights.calculations.base import DatePartsCalculationWorker

reader = NasdaqDataReader()
options = HistoricalDataProcessOptions()
options.include_monthly_data = False
options.include_annual_data = False
histDataProcessor = HistoricalDataProcessor(options)

# Fetch the data
year_start = datetime.date(2023, 1, 1)
to_date = datetime.date(2023, 12, 31)
result = histDataProcessor.process(reader, {'from_date': year_start, 'to_date': to_date})

# Prepare calculation pipeline
pipelines = MultiDataCalculationPipelines()
pipelines.set_item('date_parts', CalculationPipelineBuilder.create_pipeline_for_worker(DatePartsCalculationWorker()))
pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
histDataProcessor.set_calculation_pipelines(pipelines)

# Run the pipeline
histDataProcessor.run_calculation_pipelines()
