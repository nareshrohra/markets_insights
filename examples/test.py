from datetime import date
import os
import sys

code_dir = os.path.join(os.path.abspath("."), "src")
os.chdir(code_dir)
sys.path.append(code_dir)

from datetime import date
import pandas as pd

# import classes
# import classes
import datetime
from markets_insights.datareader import data_reader
from markets_insights.dataprocess import data_processor

reader = data_reader.MemoryCachedDataReader(data_reader.BhavCopyReader())
criteria = data_reader.DateRangeCriteria(from_date=date(2024, 1, 1), to_date=date(2024, 1, 31))
reader.read(criteria)

# Fetch the data
# reader = data_reader.BhavCopyReader()
# options = data_processor.HistoricalDataProcessOptions(include_monthly_data = False, include_annual_data=False)
# histDataProcessor = data_processor.HistoricalDataProcessor(options)

# from_date = datetime.date(2023, 12, 1)
# to_date = datetime.date(2023, 12, 31)
# result = histDataProcessor.process(data_reader.BhavCopyReader(), data_reader.DateRangeCriteria(from_date, to_date))