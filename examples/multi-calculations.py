import os
import sys

code_dir = os.path.join(os.path.abspath("."), "src")
os.chdir(code_dir)
sys.path.append(code_dir)

from markets_insights.core.environment import Environment
from datetime import date, timedelta
import pandas as pd
from markets_insights.datareader.data_reader import BhavCopyReader, DateRangeCriteria
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, MultiDataCalculationPipelines, CalculationPipelineBuilder, HistoricalDataProcessOptions
from markets_insights.calculations.base import DatePartsCalculationWorker

Environment.setup(cache_data_base_path='../../cache-data')

end_date = date(2026, 6, 1)
start_date = end_date - timedelta(days=30)

histDataProcessor: HistoricalDataProcessor = None

def get_data():
    print(f"Fetching stock data from {start_date} to {end_date}\n")
    global histDataProcessor
    
    # Create reader and processor
    reader = BhavCopyReader()
    options = HistoricalDataProcessOptions(include_monthly_data=False, include_annual_data=False)
    histDataProcessor = HistoricalDataProcessor(options)

    # Fetch data for date range
    result = histDataProcessor.process(reader, DateRangeCriteria(start_date, end_date))

    # Get daily data
    df = result.get_daily_data()

    print(f"Successfully fetched {len(df)} records")
    print(f"Columns: {list(df.columns)}")

    # Display sample
    print(f"\nData Summary:")
    if len(df) > 0:
        print(f"Date range in data: {df.iloc[:, 0]} to {df.iloc[-1, 0]}")
        print(f"\nFirst 5 records:")
        print(f"\nRecords: {histDataProcessor.dataset.get_daily_data().shape[0]}")
        print(df.head(5))
    else:
        print("No data was fetched")

def process_data():
    from markets_insights.dataprocess import data_processor

    # prepare calculation pipeline
    periods = [5, 10, 15]

    pipelines = data_processor.MultiDataCalculationPipelines()
    pipelines.set_item('forward_looking_fall', data_processor.CalculationPipelineBuilder.create_forward_looking_price_fall_pipeline(periods))
    pipelines.set_item('forward_looking_rise', data_processor.CalculationPipelineBuilder.create_forward_looking_price_rise_pipeline(periods))
    pipelines.set_item('rsi', data_processor.CalculationPipelineBuilder.create_rsi_calculation_pipeline(crossing_above_flag_value = 75, crossing_below_flag_value = 30, window = 14))
    #pipelines.set_item('stoch_rsi', data_processor.CalculationPipelineBuilder.create_stoch_rsi_calculation_pipeline(crossing_above_flag_value = 80, crossing_below_flag_value = 20, window = 14))
    histDataProcessor.set_calculation_pipelines(pipelines=pipelines)

    # run the pipeline and show results
    histDataProcessor.run_calculation_pipelines()

get_data()
process_data()
