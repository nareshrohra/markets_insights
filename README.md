
# Markets Data Manager 📝  
This package fetches and processes capital markets data from NSE (National Stock Exchange, India). Following data can be retrieved
1. Index (Nifty, Bank Nifty, NiftyIT)
2. Stocks
3. Derivatives (Futures and Options)

The package can perform technical functions on price of Index and Stocks. Following functions are supported.

1. Simple Moving Averages (SMA)
2. Relative Strength Index (RSI)
3. Stochastic RSI
4. Bollinger Bands (with standard deviations)

The calculation pipeline is quite extensible and more functions can be added externally.

## Getting Started 🚀
### Get Index data for date range
```python
from datareader.data_reader import NseIndicesReader
reader = NseIndicesReader()

from datareader.data_reader import DateRangeDataReader
daterange_reader = DateRangeDataReader(reader)

from_date = datetime.date(1990, 1, 1)
to_date = datetime.date.today() + datetime.timedelta(days=-1)
result = daterange_reader.read(from_date = from_date, to_date = to_date)
```

### Calculation pipeline for RSI
Below example demonstrates calculating RSI using the calculation pipeline. The datepart calculation is pre-requisite for RSI calculation.

```python
# import classes & setup options
from dataprocess.data_processor import HistoricalDataProcessor, MultiDataCalculationPipelines, CalculationPipelineBuilder, HistoricalDataProcessOptions
from calculations.base import DatePartsCalculationWorker
options = HistoricalDataProcessOptions()
options.include_monthly_data = False
options.include_annual_data = False
histDataProcessor = HistoricalDataProcessor(options)

# Fetch the data
year_start = datetime.date(2023, 1, 1)
to_date = datetime.date.today() + datetime.timedelta(days=-1)
result = histDataProcessor.process(reader, {'from_date': year_start, 'to_date': to_date})

# Prepare calculation pipeline
pipelines = MultiDataCalculationPipelines()
pipelines.set_item('date_parts', CalculationPipelineBuilder.create_pipeline_for_worker(DatePartsCalculationWorker()))
pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
histDataProcessor.set_calculation_pipelines(pipelines)

# Run the pipeline
histDataProcessor.run_calculation_pipelines()
result.get_daily_data()
```

### A real use case: Understand the affect of RSI and Stochastic RSI on price
In this use case, understand the affect of RSI and Stochastic RSI on price

#### Preparing the data
- Calculate RSI and Stochastic RSI for each day.
- Add a flag for whenever the RSI crosses the control limits (eg: above 75 and below 30)
- Calculate the highest and lowest price change in the next 10 trading sessions.

#### Analyse
- Find the median for highest price change and lowest price change whenever the RSI crosses the threshold.

```python
# prepare calculation pipeline
periods = [1, 7, 15, 30, 45]

pipelines = MultiDataCalculationPipelines()
pipelines.set_item('date_parts', CalculationPipelineBuilder.create_pipeline_for_worker(DatePartsCalculationWorker()))
pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline(crossing_above_flag_value = 75, crossing_below_flag_value = 30, window = 14))
pipelines.set_item('stoch_rsi', CalculationPipelineBuilder.create_stoch_rsi_calculation_pipeline(crossing_above_flag_value = 80, crossing_below_flag_value = 20, window = 14))
pipelines.set_item('foward_looking_fall', CalculationPipelineBuilder.create_forward_looking_price_fall_pipeline(periods))
pipelines.set_item('foward_looking_rise', CalculationPipelineBuilder.create_forward_looking_price_rise_pipeline(periods))
histDataProcessor.set_calculation_pipelines(pipelines=pipelines)

# run the pipeline and show results
histDataProcessor.run_calculation_pipelines()

daily_data = result.get_daily_data()

# Import constants so its easier to refer to column names
from core.column_definition import BaseColumns, CalculatedColumns

# get names of fwd looking price column names. Since, these column names are auto-generated there no constants for them
fwd_looking_price_fall_cols, fwd_looking_price_rise_cols = [x for x in daily_data.columns if 'HighestPercFallInNext' in x], \
  [x for x in daily_data.columns if 'HighestPercRiseInNext' in x]

# analyse the median price change % for highest price fall whenever the RSI crosses above
daily_data[
(daily_data[CalculatedColumns.RsiCrossedAbove])
][fwd_looking_price_fall_cols].median()

# analyse the median price change % for highest price rise whenever the RSI crosses below
daily_data[
(daily_data[CalculatedColumns.RsiCrossedAbove])
][fwd_looking_price_rise_cols].median()
```
