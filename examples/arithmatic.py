from datetime import date
import os
import sys

code_dir = os.path.join(os.path.abspath("."), "src")
os.chdir(code_dir)
sys.path.append(code_dir)

import datetime
from markets_insights.datareader.data_reader import DateRangeCriteria, NseIndicesReader
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, HistoricalDataProcessOptions

from markets_insights.datareader.data_reader import NseIndicesReader, DateRangeDataReader, DateRangeCriteria
from markets_insights.core.core import IdentifierFilter

nifty50_reader = NseIndicesReader().set_filter(IdentifierFilter("NIFTY 50"))
indiavix_reader = NseIndicesReader().set_filter(IdentifierFilter("India VIX"))
reader = nifty50_reader / indiavix_reader

histDataProcessor = HistoricalDataProcessor()

# Fetch the data
from_date = datetime.date(2024, 1, 1)
to_date = datetime.date(2024, 1, 31)
result = histDataProcessor.process(reader, DateRangeCriteria(from_date, to_date))