import os
import sys

code_dir = os.path.join(os.path.abspath("."), "src")
os.chdir(code_dir)
sys.path.append(code_dir)

import datetime
from markets_insights.datareader.data_reader import BhavCopyReader, DateRangeCriteria, NseIndicesReader
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, MultiDataCalculationPipelines, CalculationPipelineBuilder, HistoricalDataProcessOptions
from markets_insights.calculations.base import DatePartsCalculationWorker

reader = BhavCopyReader()
histDataProcessor = HistoricalDataProcessor()

# Fetch the data
year_start = datetime.date(2023, 1, 1)
to_date = datetime.date(2023, 12, 31)
result = histDataProcessor.process(reader, DateRangeCriteria(year_start, to_date))

# # Prepare calculation pipeline
# pipelines = MultiDataCalculationPipelines()
# pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
# histDataProcessor.set_calculation_pipelines(pipelines)

# # Run the pipeline
# histDataProcessor.run_calculation_pipelines()