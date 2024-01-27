from datetime import date
import pytest
import pandas as pd

from helper import (
  check_col_values,
  setup,
  Presets
)
from markets_insights.calculations.base import PriceCrossedAboveValueFlagWorker, PriceCrossedBelowValueFlagWorker, \
  StochRsiCalculationWorker, VwapCalculationWorker
from markets_insights.core.core import DateFilter, IdentifierFilter

setup()

import markets_insights
from markets_insights.core.column_definition import CalculatedColumns
from markets_insights.datareader.data_reader import BhavCopyReader, NseIndicesReader
from markets_insights.dataprocess.data_processor import (
  CalculationPipelineBuilder,
  HistoricalDataProcessor,
  HistoricalDataset,
  MultiDataCalculationPipelines
)

indices_processor = HistoricalDataProcessor()
indices_result: HistoricalDataset = indices_processor.process(NseIndicesReader(), {
  'from_date': Presets.dates.year_start,
  'to_date': Presets.dates.year_end
})

equity_processor = HistoricalDataProcessor()
equity_result: HistoricalDataset = equity_processor.process(BhavCopyReader(), {
  'from_date': Presets.dates.q4_start,
  'to_date': Presets.dates.q4_end
})
  
def test_calculations_rsi():  
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(Presets.filters.nifty50 & Presets.filters.date_year_end)),
    col_value_pairs={
      CalculatedColumns.RelativeStrengthIndex: 76.39132974363618
    }
  )

def test_calculations_crossing_flags():
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  for for_date in [date(2023, 2, 28), date(2023, 10, 26)]:
    check_col_values(
      data = daily_data.query(str(Presets.filters.nifty50 & DateFilter(for_date=for_date))),
      col_value_pairs={
        "RsiCrossedBelow": True
      }
    )

  for for_date in [date(2023, 7, 4), date(2023, 9, 15), date(2023, 12, 1), date(2023, 12, 27)]:
    check_col_values(
      data = daily_data.query(str(Presets.filters.nifty50 & DateFilter(for_date=for_date))),
      col_value_pairs={
        "RsiCrossedAbove": True
      }
    )

def test_calculations_sma():  
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('sma', CalculationPipelineBuilder.create_sma_calculation_pipeline())
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(Presets.filters.nifty50 & Presets.filters.date_year_end)),
    col_value_pairs={
      'Sma50': 20171.113,
      'Sma100': 19891.132,
      'Sma200': 19142.190749999998
    }
  )

def test_calculations_stoch_rsi():  
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  stoch_rsi_worker = StochRsiCalculationWorker()
  pipelines.set_item('stoch_rsi', CalculationPipelineBuilder.create_pipeline_for_worker(stoch_rsi_worker))
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(Presets.filters.nifty50 & Presets.filters.date_year_end)),
    col_value_pairs={
      CalculatedColumns.StochRsi_K: 53.10131491044907,
      CalculatedColumns.StochRsi_D: 45.248290876233135
    }
  )

@pytest.mark.parametrize("identifier,for_date,vwap", [
    ("RELIANCE", date(2023, 12, 21), 2383.908377), 
    ("INFY", date(2023, 12, 27), 1457.663944), 
    ("HDFCBANK", date(2023, 12, 29), 1572.029516), 
  ]
)
def test_calculations_vwap(identifier: str, for_date: date, vwap: float):  
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  worker = VwapCalculationWorker(time_window=50)
  pipelines.set_item('vwap', CalculationPipelineBuilder.create_pipeline_for_worker(worker))
  equity_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  equity_processor.run_calculation_pipelines()
  daily_data = equity_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(IdentifierFilter(identifier) & DateFilter(for_date))),
    col_value_pairs={
      CalculatedColumns.Vwap: vwap
    }
  )

def test_calculations_bb():
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('bb', CalculationPipelineBuilder.create_bb_calculation_pipeline())
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(Presets.filters.nifty50 & Presets.filters.date_year_end)),
    col_value_pairs={
      'StdDev200': 1084.4860011940468,
      'Bb200Dev2Upper': 21311.162752388092,
      'Bb200Dev2Lower': 16973.218747611903,
      'Bb200Dev3Upper': 22395.64875358214,
      'Bb200Dev3Lower': 15888.732746417858
    }
  )

@pytest.mark.parametrize("n_day,lowest_price,price_fall_pert", [
    (5, 20507.75, -1.183398378717077), 
    (10, 20507.75, -1.183398378717077),
    (15, 20507.75, -1.183398378717077)
])
def test_calculations_fwd_looking_fall(n_day: int, lowest_price: float, price_fall_pert: float):
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('fwd_price_fall', CalculationPipelineBuilder.create_forward_looking_price_fall_pipeline([n_day]))
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  test_day_data = daily_data.query(str(Presets.filters.nifty50 & DateFilter(for_date=Presets.dates.dec_start)))
  check_col_values(
    data = test_day_data,
    col_value_pairs={
      f"LowestPriceInNext{str(n_day)}Days": lowest_price,
      f"HighestPercFallInNext{str(n_day)}Days": price_fall_pert
    }
  )

@pytest.mark.parametrize("n_day,highest_price,price_rise_pert", [
    (5, 21006.1, 3.6422125627223196), 
    (10, 21492.3, 6.041079736923893),
    (15, 21593.0, 6.537924501305012)
  ]
)
def test_calculations_fwd_looking_rise(n_day: int, highest_price: float, price_rise_pert: float):  
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('fwd_price_rise', CalculationPipelineBuilder.create_forward_looking_price_rise_pipeline([n_day]))
  indices_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  indices_processor.run_calculation_pipelines()
  daily_data = indices_result.get_daily_data()
  
  test_day_data = daily_data.query(str(Presets.filters.nifty50 & DateFilter(for_date=Presets.dates.dec_start)))
  check_col_values(
    data = test_day_data,
    col_value_pairs={
      f"HighestPriceInNext{str(n_day)}Days": highest_price,
      f"HighestPercRiseInNext{str(n_day)}Days": price_rise_pert
    }
  )

@pytest.mark.parametrize("identifier,for_date,flag", [
  ("WONDERLA", date(2023, 12, 14), True),
  ("GAEL", date(2023, 12, 28), True),
  ("RELIANCE", date(2023, 12, 14), False),
  ("INFY", date(2023, 12, 29), False)
])
def test_calculations_price_crossing_below_flag(identifier: str, for_date: date, flag: bool):
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('vwap', CalculationPipelineBuilder.create_pipeline_for_worker(VwapCalculationWorker(time_window=50)))
  pipelines.set_item('vwap_crossed', CalculationPipelineBuilder.create_pipeline_for_worker(
      PriceCrossedBelowValueFlagWorker(value_column = CalculatedColumns.Vwap)
  ))
  equity_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  equity_processor.run_calculation_pipelines()
  daily_data = equity_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))),
    col_value_pairs={
      "PriceCrossedBelowVwap": flag
    }
  )

@pytest.mark.parametrize("identifier,for_date,flag", [
  ("ZYDUSWELL", date(2023, 12, 14), True),
  ("HINDZINC", date(2023, 12, 29), True),
  ("RELIANCE", date(2023, 12, 14), False),
  ("INFY", date(2023, 12, 29), False)
])
def test_calculations_price_crossing_above_flag(identifier: str, for_date: date, flag: bool):
  # Prepare calculation pipeline
  pipelines = MultiDataCalculationPipelines()
  pipelines.set_item('vwap', CalculationPipelineBuilder.create_pipeline_for_worker(VwapCalculationWorker(time_window=50)))
  pipelines.set_item('vwap_crossed', CalculationPipelineBuilder.create_pipeline_for_worker(
      PriceCrossedAboveValueFlagWorker(value_column = CalculatedColumns.Vwap)
  ))
  equity_processor.set_calculation_pipelines(pipelines)

  # Run the pipeline and get data
  equity_processor.run_calculation_pipelines()
  daily_data = equity_result.get_daily_data()
  
  check_col_values(
    data = daily_data.query(str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))),
    col_value_pairs={
      "PriceCrossedAboveVwap": flag
    }
  )