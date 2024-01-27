from datetime import date
import os
import sys


code_dir = os.path.join(os.path.abspath('.'), 'src')
os.chdir(code_dir)
sys.path.append(code_dir)

from markets_insights.dataprocess.data_processor import HistoricalDataProcessor
from markets_insights.datareader.data_reader import NseIndicesReader

processor = HistoricalDataProcessor()
result = processor.process(NseIndicesReader(), {
  'from_date': date(2023, 1, 1),
  'to_date': date(2023, 12, 31)
})